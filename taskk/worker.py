"""
Worker node implementation.

Each worker instance:
- Polls the task queue using an atomic Lua-powered dequeue
- Executes registered task handlers with configurable timeouts
- Retries failed tasks with exponential back-off
- Publishes JSON heartbeats every ``HEARTBEAT_INTERVAL`` seconds
- Tracks per-worker stats (tasks processed, succeeded, failed, avg latency)
- Shuts down gracefully on SIGINT / SIGTERM
"""
import json
import signal
import threading
import time
import concurrent.futures
from typing import Any, Callable, Dict, Optional

from config import (
    HEARTBEAT_INTERVAL,
    MAX_RETRIES,
    RETRY_DELAY_BASE,
    TASK_TIMEOUT,
    WORKER_HEARTBEAT_KEY,
    WORKER_POLL_INTERVAL,
)
from logger import setup_logger
from redis_client import get_redis_client
from task_queue import TaskQueue


class WorkerStats:
    """Thread-safe per-worker performance counters."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.processed: int = 0
        self.succeeded: int = 0
        self.failed: int = 0
        self._total_duration_ms: float = 0.0

    def record_success(self, duration_ms: float) -> None:
        with self._lock:
            self.processed += 1
            self.succeeded += 1
            self._total_duration_ms += duration_ms

    def record_failure(self) -> None:
        with self._lock:
            self.processed += 1
            self.failed += 1

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            avg = (
                round(self._total_duration_ms / self.succeeded, 1)
                if self.succeeded
                else 0.0
            )
            return {
                "processed": self.processed,
                "succeeded": self.succeeded,
                "failed": self.failed,
                "avg_duration_ms": avg,
            }


class Worker:
    """
    A worker that dequeues tasks from Redis and executes them.

    Supports:
    - Named task handler registration
    - Automatic retries with exponential back-off
    - Graceful shutdown on SIGINT / SIGTERM
    - Per-task execution timeout
    - JSON-serialised heartbeats (parseable by CLI and dashboard)
    - In-process performance statistics
    """

    def __init__(self, worker_id: str = "worker-1") -> None:
        self.worker_id = worker_id
        self.logger = setup_logger(f"worker.{worker_id}")
        self.queue = TaskQueue()
        self.redis_client = get_redis_client()
        self.task_handlers: Dict[str, Callable] = {}
        self.running = False
        self.current_task_id: Optional[str] = None
        self.stats = WorkerStats()

        self._heartbeat_thread: Optional[threading.Thread] = None
        self._heartbeat_stop_event = threading.Event()

        signal.signal(signal.SIGINT, self._shutdown_handler)
        signal.signal(signal.SIGTERM, self._shutdown_handler)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def register_task(self, task_name: str, handler: Callable) -> None:
        """Register a callable as the handler for ``task_name``."""
        self.task_handlers[task_name] = handler
        self.logger.info(f"Registered handler: {task_name}")

    def run(self) -> None:
        """
        Start the worker event loop.

        Blocks until a shutdown signal is received or ``self.running`` is
        set to ``False``.
        """
        self.running = True
        self._start_heartbeat()

        self.logger.info(
            f"Worker started | handlers={list(self.task_handlers)} "
            f"| timeout={TASK_TIMEOUT}s | max_retries={MAX_RETRIES}"
        )

        try:
            while self.running:
                try:
                    task_data = self.queue.dequeue()
                    if task_data:
                        self._execute_task(task_data)
                    else:
                        time.sleep(WORKER_POLL_INTERVAL)
                except Exception as exc:
                    self.logger.exception(f"Unexpected worker error: {exc}")
                    time.sleep(WORKER_POLL_INTERVAL)
        finally:
            self._stop_heartbeat()
            self.logger.info(
                "Worker stopped",
                extra={"worker_id": self.worker_id, **self.stats.snapshot()},
            )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _shutdown_handler(self, signum: int, frame: Any) -> None:
        self.logger.info(
            "Shutdown signal received – finishing current task then stopping…",
            extra={"worker_id": self.worker_id},
        )
        self.running = False
        self._heartbeat_stop_event.set()

    def _calculate_retry_delay(self, retry_count: int) -> int:
        """Exponential back-off: base^(n+1).  e.g. 2s, 4s, 8s, …"""
        return RETRY_DELAY_BASE ** (retry_count + 1)

    def _start_heartbeat(self) -> None:
        """Spawn a daemon thread that publishes a JSON heartbeat to Redis."""

        def _loop() -> None:
            while not self._heartbeat_stop_event.is_set():
                try:
                    heartbeat = json.dumps(
                        {
                            "worker_id": self.worker_id,
                            "timestamp": time.time(),
                            "current_task": self.current_task_id,
                            "status": "busy" if self.current_task_id else "idle",
                            "stats": self.stats.snapshot(),
                        }
                    )
                    self.redis_client.hset(WORKER_HEARTBEAT_KEY, self.worker_id, heartbeat)
                    self.redis_client.expire(WORKER_HEARTBEAT_KEY, HEARTBEAT_INTERVAL * 3)
                except Exception as exc:
                    self.logger.warning(f"Heartbeat write failed: {exc}")
                self._heartbeat_stop_event.wait(HEARTBEAT_INTERVAL)

        self._heartbeat_thread = threading.Thread(target=_loop, daemon=True, name=f"hb-{self.worker_id}")
        self._heartbeat_thread.start()

    def _stop_heartbeat(self) -> None:
        self._heartbeat_stop_event.set()
        if self._heartbeat_thread:
            self._heartbeat_thread.join(timeout=5)
        try:
            self.redis_client.hdel(WORKER_HEARTBEAT_KEY, self.worker_id)
        except Exception:
            pass

    def _execute_with_timeout(
        self, handler: Callable, payload: Dict[str, Any], timeout: int
    ) -> Any:
        """
        Run ``handler(payload)`` in a background thread with a hard timeout.

        The executor is created with ``cancel_futures=True`` and
        ``wait=False`` on shutdown so that timed-out threads are abandoned
        immediately rather than blocking the caller.
        """
        executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        try:
            future = executor.submit(handler, payload)
            try:
                return future.result(timeout=timeout)
            except concurrent.futures.TimeoutError:
                future.cancel()
                raise TimeoutError(f"Task exceeded {timeout}s timeout")
        finally:
            # shutdown(wait=False) returns immediately; the daemon thread
            # is abandoned if it is still running after a timeout.
            executor.shutdown(wait=False)

    def _execute_task(self, task_data: Dict[str, Any]) -> bool:
        """
        Execute a single task end-to-end, handling retries and timeouts.

        Returns ``True`` on success, ``False`` on failure.
        """
        task_id = task_data["task_id"]
        task_name = task_data["task_name"]
        payload = task_data["payload"]
        retry_count = task_data.get("retry_count", 0)

        self.current_task_id = task_id
        start_ms = time.monotonic() * 1000

        self.logger.info(
            f"Executing task: {task_name}",
            extra={"task_id": task_id, "task_name": task_name, "retry_count": retry_count},
        )

        if task_name not in self.task_handlers:
            error_msg = f"No handler registered for task: {task_name}"
            self.logger.error(error_msg, extra={"task_id": task_id})
            self.queue.fail_task(task_id, error_msg, retry=False)
            self.stats.record_failure()
            self.current_task_id = None
            return False

        try:
            handler = self.task_handlers[task_name]
            result = self._execute_with_timeout(handler, payload, TASK_TIMEOUT)
            duration_ms = time.monotonic() * 1000 - start_ms

            self.queue.complete_task(task_id, result, duration_ms=duration_ms)
            self.stats.record_success(duration_ms)
            self.logger.info(
                f"Task succeeded: {task_name}",
                extra={"task_id": task_id, "duration_ms": round(duration_ms, 1)},
            )
            self.current_task_id = None
            return True

        except TimeoutError as exc:
            self.logger.error(str(exc), extra={"task_id": task_id})
            self.queue.fail_task(task_id, str(exc), retry=False)
            self.stats.record_failure()
            self.current_task_id = None
            return False

        except Exception as exc:
            error_msg = f"{type(exc).__name__}: {exc}"
            self.logger.error(
                f"Task failed: {task_name} – {error_msg}",
                extra={"task_id": task_id, "task_name": task_name},
            )

            if retry_count < MAX_RETRIES:
                delay = self._calculate_retry_delay(retry_count)
                self.logger.info(
                    f"Scheduling retry in {delay}s "
                    f"(attempt {retry_count + 2}/{MAX_RETRIES + 1})",
                    extra={"task_id": task_id},
                )
                self.queue.fail_task(task_id, error_msg, retry=True)
                self.queue.requeue_task(task_id, delay_seconds=delay)
            else:
                self.logger.warning(
                    "Max retries exceeded – moving to dead-letter queue",
                    extra={"task_id": task_id},
                )
                self.queue.fail_task(task_id, error_msg, retry=False)

            self.stats.record_failure()
            self.current_task_id = None
            return False


# ---------------------------------------------------------------------------
# Decorator helper (for simple single-file usage)
# ---------------------------------------------------------------------------

def task(task_name: str) -> Callable:
    """
    Marker decorator that attaches ``._task_name`` to a function.

    Pass the function to ``worker.register_task`` or collect via
    ``ALL_TASKS`` in ``tasks.py``.

    Example::

        @task("send_email")
        def send_email_task(payload):
            ...
    """
    def decorator(func: Callable) -> Callable:
        func._task_name = task_name  # type: ignore[attr-defined]
        return func
    return decorator

