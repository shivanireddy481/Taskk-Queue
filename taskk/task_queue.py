"""
Redis-backed distributed task queue.

Design decisions
----------------
* **Priority queue** – Redis sorted set (ZSET) keyed by ``-priority`` so
  higher-priority tasks get lower scores and are popped first.
* **Scheduled tasks** – A second ZSET keyed by UNIX execution timestamp.
  An atomic Lua script promotes ready tasks to the main queue on every
  ``dequeue`` call, eliminating race conditions between workers.
* **Idempotency** – Callers may pass an ``idempotency_key``; the queue
  stores it in Redis with a configurable TTL and silently skips duplicate
  enqueue calls within that window.
* **Metrics** – Lightweight counters in Redis (INCR / INCRBYFLOAT) track
  enqueues, completions, failures, and cumulative execution time.
* **Atomic dequeue** – The Lua script makes the scheduled→pending
  promotion + zpopmin a single round trip so no task is double-delivered.
"""
import json
import time
import uuid
from typing import Any, Dict, List, Optional

from config import (
    DEAD_LETTER_QUEUE_NAME,
    ENABLE_METRICS,
    IDEMPOTENCY_KEY_PREFIX,
    IDEMPOTENCY_TTL,
    METRICS_KEY_PREFIX,
    METRICS_TTL,
    PROCESSING_QUEUE_NAME,
    RESULT_TTL,
    RESULTS_KEY_PREFIX,
    SCHEDULED_QUEUE_NAME,
    TASK_KEY_PREFIX,
    TASK_QUEUE_NAME,
)
from logger import setup_logger
from redis_client import get_redis_client

logger = setup_logger("task_queue")

# ---------------------------------------------------------------------------
# Lua script – atomic: move ready scheduled tasks → main queue, then pop one
# ---------------------------------------------------------------------------
# KEYS[1] = scheduled ZSET   KEYS[2] = main queue ZSET
# ARGV[1] = current UNIX timestamp
#
# Returns the task_id of the next task to execute, or false.
_DEQUEUE_LUA = """
local now   = tonumber(ARGV[1])
local sched = KEYS[1]
local main  = KEYS[2]

-- Promote all scheduled tasks whose execution time has arrived
local ready = redis.call('ZRANGEBYSCORE', sched, '-inf', now)
for _, task_id in ipairs(ready) do
    -- Retrieve stored priority for correct ordering in main queue
    local raw = redis.call('HGET', 'taskq:task:' .. task_id, 'priority')
    local pri = tonumber(raw) or 0
    redis.call('ZADD', main, -pri, task_id)
    redis.call('ZREM', sched, task_id)
end

-- Pop the highest-priority (lowest score) pending task
local result = redis.call('ZPOPMIN', main, 1)
if #result == 0 then
    return false
end
return result[1]   -- task_id string
"""


class TaskQueue:
    """
    A Redis-backed distributed task queue.

    Supports FIFO, priority ordering, delayed scheduling, at-least-once
    delivery, automatic retries with exponential back-off, a dead-letter
    queue, idempotent enqueuing, and lightweight operational metrics.
    """

    def __init__(self) -> None:
        self.redis_client = get_redis_client()
        self.queue_name = TASK_QUEUE_NAME
        self.processing_queue = PROCESSING_QUEUE_NAME
        self.dead_letter_queue = DEAD_LETTER_QUEUE_NAME
        self.scheduled_queue = SCHEDULED_QUEUE_NAME
        self.results_prefix = RESULTS_KEY_PREFIX
        self._task_prefix = TASK_KEY_PREFIX
        self._idem_prefix = IDEMPOTENCY_KEY_PREFIX
        self._metrics_prefix = METRICS_KEY_PREFIX

        # Register the Lua script once per instance (cached server-side)
        self._dequeue_script = self.redis_client.register_script(_DEQUEUE_LUA)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def enqueue(
        self,
        task_name: str,
        payload: Dict[str, Any],
        priority: int = 0,
        delay_seconds: int = 0,
        idempotency_key: Optional[str] = None,
    ) -> str:
        """
        Add a task to the queue.

        Args:
            task_name:        Name of the registered task handler.
            payload:          Arbitrary JSON-serialisable data for the handler.
            priority:         Higher value = processed sooner (default 0).
            delay_seconds:    Seconds to wait before the task becomes runnable.
            idempotency_key:  If set and a task with this key was enqueued
                              within ``IDEMPOTENCY_TTL`` seconds, the call is
                              a no-op and the original task ID is returned.

        Returns:
            Unique task ID (UUID4 string).
        """
        # --- Idempotency check -------------------------------------------
        if idempotency_key:
            idem_redis_key = f"{self._idem_prefix}{idempotency_key}"
            existing_id = self.redis_client.get(idem_redis_key)
            if existing_id:
                logger.info(
                    "Duplicate enqueue suppressed by idempotency key",
                    extra={"task_name": task_name, "task_id": existing_id},
                )
                return existing_id

        task_id = str(uuid.uuid4())

        task_data: Dict[str, Any] = {
            "task_id": task_id,
            "task_name": task_name,
            "payload": payload,
            "priority": priority,
            "retry_count": 0,
            "created_at": time.time(),
            "status": "pending",
        }

        pipe = self.redis_client.pipeline()

        # Store task metadata hash
        pipe.hset(
            f"{self._task_prefix}{task_id}",
            mapping={"data": json.dumps(task_data), "status": "pending", "priority": priority},
        )

        if delay_seconds > 0:
            execute_at = time.time() + delay_seconds
            pipe.zadd(self.scheduled_queue, {task_id: execute_at})
        else:
            pipe.zadd(self.queue_name, {task_id: -priority})

        # Persist idempotency key with TTL
        if idempotency_key:
            pipe.setex(idem_redis_key, IDEMPOTENCY_TTL, task_id)

        pipe.execute()

        if ENABLE_METRICS:
            self._increment_metric("enqueued")

        logger.info(
            f"Task enqueued: {task_name}",
            extra={"task_id": task_id, "task_name": task_name},
        )
        return task_id
        """
        Add a task to the queue.
        
        Args:
            task_name: Name of the task function to execute
            payload: Data to pass to the task
            priority: Higher priority = processed first (0 is default)
            delay_seconds: Seconds to wait before task becomes available
            
        Returns:
            task_id: Unique identifier for the task
        """
        task_id = str(uuid.uuid4())
        
        task_data = {
            "task_id": task_id,
            "task_name": task_name,
            "payload": payload,
            "priority": priority,
            "retry_count": 0,
            "created_at": time.time(),
            "status": "pending"
        }
        
        # Store task metadata
        self.redis_client.hset(f"task:{task_id}", mapping={
            "data": json.dumps(task_data),
            "status": "pending"
        })
        
        if delay_seconds > 0:
            # Schedule for later - use sorted set with execution time as score
            execute_at = time.time() + delay_seconds
            self.redis_client.zadd(self.scheduled_queue, {task_id: execute_at})
        else:
            # Add to main queue immediately
            # Using negative priority so higher priority = lower score = processed first
            self.redis_client.zadd(self.queue_name, {task_id: -priority})
        
        logger.info(f"Task enqueued: {task_id} ({task_name})")
        return task_id
    
    def dequeue(self) -> Optional[Dict[str, Any]]:
        """
        Atomically promote ready scheduled tasks and pop the next pending task.

        Uses a registered Lua script so the scheduled→pending promotion and
        the ZPOPMIN are executed as a single atomic operation; no two workers
        can receive the same task.

        Returns:
            Task data dict with ``status`` set to ``"processing"``, or ``None``
            if the queue is empty.
        """
        now = time.time()
        task_id = self._dequeue_script(
            keys=[self.scheduled_queue, self.queue_name],
            args=[now],
        )

        if not task_id:
            return None

        task_data_raw = self.redis_client.hget(f"{self._task_prefix}{task_id}", "data")
        if not task_data_raw:
            return None

        task_data = json.loads(task_data_raw)
        task_data["status"] = "processing"
        task_data["started_at"] = now

        pipe = self.redis_client.pipeline()
        pipe.zadd(self.processing_queue, {task_id: now})
        pipe.hset(
            f"{self._task_prefix}{task_id}",
            mapping={"data": json.dumps(task_data), "status": "processing"},
        )
        pipe.execute()

        return task_data

    def complete_task(self, task_id: str, result: Any = None, duration_ms: float = 0.0) -> None:
        """
        Mark a task as completed and optionally persist its result.

        Args:
            task_id:     The task identifier.
            result:      Return value from the handler (must be JSON-serialisable).
            duration_ms: Wall-clock execution time in milliseconds (for metrics).
        """
        pipe = self.redis_client.pipeline()
        pipe.zrem(self.processing_queue, task_id)
        pipe.hset(f"{self._task_prefix}{task_id}", "status", "completed")
        pipe.execute()

        if result is not None:
            self.store_result(task_id, result, success=True)

        if ENABLE_METRICS:
            self._increment_metric("completed")
            if duration_ms:
                self._add_metric_float("duration_ms_total", duration_ms)

        logger.info("Task completed", extra={"task_id": task_id, "duration_ms": round(duration_ms, 2)})

    def fail_task(self, task_id: str, error: str, retry: bool = False) -> None:
        """
        Mark a task as failed.

        Args:
            task_id: The task identifier.
            error:   Human-readable error description.
            retry:   If ``True`` the status is set to ``"pending"`` so the
                     caller can requeue it; otherwise it moves to the DLQ.
        """
        self.redis_client.zrem(self.processing_queue, task_id)

        task_data_raw = self.redis_client.hget(f"{self._task_prefix}{task_id}", "data")
        if task_data_raw:
            task_data = json.loads(task_data_raw)
            task_data["last_error"] = error
            task_data["status"] = "pending" if retry else "failed"
            self.redis_client.hset(
                f"{self._task_prefix}{task_id}",
                mapping={"data": json.dumps(task_data), "status": task_data["status"]},
            )

        if not retry:
            self.redis_client.zadd(self.dead_letter_queue, {task_id: time.time()})
            self.store_result(task_id, {"error": error}, success=False)
            if ENABLE_METRICS:
                self._increment_metric("failed")
            logger.warning("Task moved to dead-letter queue", extra={"task_id": task_id})

    def requeue_task(self, task_id: str, delay_seconds: int = 0) -> None:
        """
        Requeue a task for retry with optional back-off delay.

        Args:
            task_id:        The task to requeue.
            delay_seconds:  If > 0, place in the scheduled queue instead.
        """
        task_data_raw = self.redis_client.hget(f"{self._task_prefix}{task_id}", "data")
        if not task_data_raw:
            return

        task_data = json.loads(task_data_raw)
        task_data["retry_count"] = task_data.get("retry_count", 0) + 1
        task_data["status"] = "pending"

        self.redis_client.hset(
            f"{self._task_prefix}{task_id}",
            mapping={"data": json.dumps(task_data), "status": "pending"},
        )

        if delay_seconds > 0:
            execute_at = time.time() + delay_seconds
            self.redis_client.zadd(self.scheduled_queue, {task_id: execute_at})
        else:
            priority = task_data.get("priority", 0)
            self.redis_client.zadd(self.queue_name, {task_id: -priority})

        logger.info(
            f"Task requeued (retry #{task_data['retry_count']})",
            extra={"task_id": task_id, "retry_count": task_data["retry_count"]},
        )

    def get_task_status(self, task_id: str) -> Optional[str]:
        """Return the current status string of a task, or ``None``."""
        return self.redis_client.hget(f"{self._task_prefix}{task_id}", "status")

    def get_queue_stats(self) -> Dict[str, Any]:
        """Return counts for each queue plus cumulative metrics."""
        stats: Dict[str, Any] = {
            "pending": self.redis_client.zcard(self.queue_name),
            "scheduled": self.redis_client.zcard(self.scheduled_queue),
            "processing": self.redis_client.zcard(self.processing_queue),
            "dead_letter": self.redis_client.zcard(self.dead_letter_queue),
        }
        if ENABLE_METRICS:
            stats["metrics"] = self.get_metrics()
        return stats

    def store_result(self, task_id: str, result: Any, success: bool = True) -> None:
        """Persist the outcome of a task execution with a configurable TTL."""
        result_data = {
            "task_id": task_id,
            "result": result,
            "success": success,
            "timestamp": time.time(),
        }
        self.redis_client.setex(
            f"{self.results_prefix}{task_id}",
            RESULT_TTL,
            json.dumps(result_data),
        )

    def get_result(self, task_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve the stored result for a task, or ``None``."""
        raw = self.redis_client.get(f"{self.results_prefix}{task_id}")
        return json.loads(raw) if raw else None

    def get_task_info(self, task_id: str) -> Optional[Dict[str, Any]]:
        """Return full task metadata including its stored result."""
        raw = self.redis_client.hget(f"{self._task_prefix}{task_id}", "data")
        if not raw:
            return None
        info = json.loads(raw)
        info["result"] = self.get_result(task_id)
        return info

    def enqueue_batch(self, tasks: List[Dict[str, Any]]) -> List[str]:
        """
        Enqueue multiple tasks in a single pipeline round-trip.

        Args:
            tasks: List of dicts with keys ``task_name`` (required),
                   ``payload`` (required), ``priority`` (opt, default 0),
                   ``delay_seconds`` (opt, default 0),
                   ``idempotency_key`` (opt).

        Returns:
            Ordered list of task IDs corresponding to ``tasks``.
        """
        task_ids: List[str] = []
        pipe = self.redis_client.pipeline()
        now = time.time()

        for task in tasks:
            priority = task.get("priority", 0)
            delay = task.get("delay_seconds", 0)
            task_id = str(uuid.uuid4())
            task_ids.append(task_id)

            task_data: Dict[str, Any] = {
                "task_id": task_id,
                "task_name": task["task_name"],
                "payload": task["payload"],
                "priority": priority,
                "retry_count": 0,
                "created_at": now,
                "status": "pending",
            }

            pipe.hset(
                f"{self._task_prefix}{task_id}",
                mapping={"data": json.dumps(task_data), "status": "pending", "priority": priority},
            )

            if delay > 0:
                pipe.zadd(self.scheduled_queue, {task_id: now + delay})
            else:
                pipe.zadd(self.queue_name, {task_id: -priority})

        pipe.execute()

        if ENABLE_METRICS:
            self._increment_metric("enqueued", len(task_ids))

        logger.info(f"Batch enqueued {len(task_ids)} tasks")
        return task_ids

    def cancel_task(self, task_id: str) -> bool:
        """
        Cancel a pending or scheduled task.

        Returns ``True`` if the task was found and cancelled.
        """
        removed = self.redis_client.zrem(self.queue_name, task_id)
        if not removed:
            removed = self.redis_client.zrem(self.scheduled_queue, task_id)

        if removed:
            self.redis_client.hset(f"{self._task_prefix}{task_id}", "status", "cancelled")
            logger.info("Task cancelled", extra={"task_id": task_id})
            return True
        return False

    def get_pending_tasks(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Return up to ``limit`` pending task info dicts (highest priority first)."""
        task_ids = self.redis_client.zrange(self.queue_name, 0, limit - 1)
        return [info for tid in task_ids if (info := self.get_task_info(tid))]

    def get_failed_tasks(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Return up to ``limit`` failed task info dicts from the dead-letter queue."""
        task_ids = self.redis_client.zrange(self.dead_letter_queue, 0, limit - 1)
        return [info for tid in task_ids if (info := self.get_task_info(tid))]

    def retry_failed_task(self, task_id: str) -> bool:
        """
        Move a dead-letter task back to the main queue with a reset retry counter.

        Returns ``True`` if the task was found and requeued.
        """
        score = self.redis_client.zscore(self.dead_letter_queue, task_id)
        if score is None:
            return False

        self.redis_client.zrem(self.dead_letter_queue, task_id)

        raw = self.redis_client.hget(f"{self._task_prefix}{task_id}", "data")
        if raw:
            task_data = json.loads(raw)
            task_data["retry_count"] = 0
            task_data["status"] = "pending"
            self.redis_client.hset(
                f"{self._task_prefix}{task_id}",
                mapping={"data": json.dumps(task_data), "status": "pending"},
            )

        priority = 0
        if raw:
            priority = task_data.get("priority", 0)  # type: ignore[possibly-undefined]
        self.redis_client.zadd(self.queue_name, {task_id: -priority})
        logger.info("Retrying dead-letter task", extra={"task_id": task_id})
        return True

    def clear_dead_letter_queue(self) -> int:
        """Delete all tasks from the dead-letter queue. Returns the count cleared."""
        count = self.redis_client.zcard(self.dead_letter_queue)
        self.redis_client.delete(self.dead_letter_queue)
        logger.info(f"Cleared {count} tasks from dead-letter queue")
        return count

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _increment_metric(self, name: str, amount: int = 1) -> None:
        key = f"{self._metrics_prefix}{name}"
        pipe = self.redis_client.pipeline()
        pipe.incrby(key, amount)
        pipe.expire(key, METRICS_TTL)
        pipe.execute()

    def _add_metric_float(self, name: str, amount: float) -> None:
        key = f"{self._metrics_prefix}{name}"
        pipe = self.redis_client.pipeline()
        pipe.incrbyfloat(key, amount)
        pipe.expire(key, METRICS_TTL)
        pipe.execute()

    def get_metrics(self) -> Dict[str, Any]:
        """Return cumulative operational counters from Redis."""
        keys = ["enqueued", "completed", "failed", "duration_ms_total"]
        pipe = self.redis_client.pipeline()
        for k in keys:
            pipe.get(f"{self._metrics_prefix}{k}")
        values = pipe.execute()

        metrics: Dict[str, Any] = {}
        for k, v in zip(keys, values):
            metrics[k] = float(v) if v else 0

        total = metrics["completed"] + metrics["failed"]
        metrics["success_rate"] = (
            round(metrics["completed"] / total * 100, 1) if total else 0.0
        )
        avg_ms = (
            round(metrics["duration_ms_total"] / metrics["completed"], 1)
            if metrics["completed"]
            else 0.0
        )
        metrics["avg_duration_ms"] = avg_ms
        return metrics

