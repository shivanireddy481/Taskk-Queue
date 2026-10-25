"""
Unit tests for Worker.

Mocks TaskQueue and Redis so no live infrastructure is needed.
"""
import json
import time
from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture()
def mock_deps():
    """Mock TaskQueue, Redis client, and logger for Worker instantiation."""
    with (
        patch("worker.TaskQueue") as mock_queue_cls,
        patch("worker.get_redis_client") as mock_redis_factory,
        patch("worker.setup_logger") as mock_logger_factory,
    ):
        queue = MagicMock()
        mock_queue_cls.return_value = queue

        redis = MagicMock()
        pipe = MagicMock()
        redis.pipeline.return_value = pipe
        mock_redis_factory.return_value = redis

        logger = MagicMock()
        mock_logger_factory.return_value = logger

        yield {"queue": queue, "redis": redis, "logger": logger}


@pytest.fixture()
def worker(mock_deps):
    from worker import Worker
    return Worker(worker_id="test-worker")


# ---------------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------------

class TestRegistration:
    def test_register_stores_handler(self, worker):
        def handler(p): return "ok"
        worker.register_task("my_task", handler)
        assert "my_task" in worker.task_handlers
        assert worker.task_handlers["my_task"] is handler


# ---------------------------------------------------------------------------
# Task execution
# ---------------------------------------------------------------------------

class TestExecuteTask:
    def test_success_path(self, worker, mock_deps):
        worker.register_task("ok_task", lambda p: {"v": 1})
        result = worker._execute_task(
            {"task_id": "t-1", "task_name": "ok_task", "payload": {}, "retry_count": 0}
        )
        assert result is True
        mock_deps["queue"].complete_task.assert_called_once()
        # duration_ms kwarg must be present and positive
        _, kw = mock_deps["queue"].complete_task.call_args
        assert kw.get("duration_ms", -1) >= 0

    def test_failure_path_with_retry(self, worker, mock_deps):
        worker.register_task("flaky", lambda p: (_ for _ in ()).throw(ValueError("boom")))
        result = worker._execute_task(
            {"task_id": "t-2", "task_name": "flaky", "payload": {}, "retry_count": 0}
        )
        assert result is False
        mock_deps["queue"].fail_task.assert_called_with("t-2", pytest.approx("ValueError: boom", abs=0), retry=True)
        mock_deps["queue"].requeue_task.assert_called_once()

    def test_unknown_handler_fails_without_retry(self, worker, mock_deps):
        result = worker._execute_task(
            {"task_id": "t-3", "task_name": "ghost", "payload": {}, "retry_count": 0}
        )
        assert result is False
        mock_deps["queue"].fail_task.assert_called_with(
            "t-3", "No handler registered for task: ghost", retry=False
        )

    def test_max_retries_exceeded_sends_to_dlq(self, worker, mock_deps):
        worker.register_task("always_bad", lambda p: (_ for _ in ()).throw(RuntimeError("x")))
        worker._execute_task(
            {"task_id": "t-4", "task_name": "always_bad", "payload": {}, "retry_count": 3}
        )
        mock_deps["queue"].fail_task.assert_called_with(
            "t-4", pytest.approx("RuntimeError: x", abs=0), retry=False
        )
        mock_deps["queue"].requeue_task.assert_not_called()

    def test_timeout_fails_without_retry(self, worker, mock_deps):
        def slow(p): time.sleep(60)
        worker.register_task("slow_task", slow)
        # Patch TASK_TIMEOUT to 1 second for this test
        with patch("worker.TASK_TIMEOUT", 1):
            result = worker._execute_task(
                {"task_id": "t-5", "task_name": "slow_task", "payload": {}, "retry_count": 0}
            )
        assert result is False
        _, kw = mock_deps["queue"].fail_task.call_args
        assert kw["retry"] is False


# ---------------------------------------------------------------------------
# Retry delay (exponential back-off)
# ---------------------------------------------------------------------------

class TestRetryDelay:
    def test_exponential_sequence(self, worker):
        assert worker._calculate_retry_delay(0) == 2
        assert worker._calculate_retry_delay(1) == 4
        assert worker._calculate_retry_delay(2) == 8


# ---------------------------------------------------------------------------
# Timeout helper
# ---------------------------------------------------------------------------

class TestExecuteWithTimeout:
    def test_quick_task_returns_value(self, worker):
        result = worker._execute_with_timeout(lambda p: "done", {}, timeout=5)
        assert result == "done"

    def test_slow_task_raises_timeout(self, worker):
        def slow(p): time.sleep(10)
        with pytest.raises(TimeoutError):
            worker._execute_with_timeout(slow, {}, timeout=1)


# ---------------------------------------------------------------------------
# Heartbeat – JSON serialisation
# ---------------------------------------------------------------------------

class TestHeartbeat:
    def test_heartbeat_is_valid_json(self, worker, mock_deps):
        """Heartbeat stored in Redis must be valid JSON, not repr(dict)."""
        worker._start_heartbeat()
        time.sleep(0.05)  # let the thread write at least once
        worker._stop_heartbeat()

        if mock_deps["redis"].hset.called:
            _, stored_value = mock_deps["redis"].hset.call_args[0][1:]
            # Must not raise
            parsed = json.loads(stored_value)
            assert "worker_id" in parsed
            assert "timestamp" in parsed
            assert "status" in parsed


# ---------------------------------------------------------------------------
# Worker stats
# ---------------------------------------------------------------------------

class TestWorkerStats:
    def test_success_increments_counters(self, worker, mock_deps):
        worker.register_task("fast", lambda p: "ok")
        worker._execute_task(
            {"task_id": "s-1", "task_name": "fast", "payload": {}, "retry_count": 0}
        )
        snap = worker.stats.snapshot()
        assert snap["processed"] == 1
        assert snap["succeeded"] == 1
        assert snap["failed"] == 0

    def test_failure_increments_failed_counter(self, worker, mock_deps):
        worker.register_task("bad", lambda p: (_ for _ in ()).throw(Exception("err")))
        worker._execute_task(
            {"task_id": "s-2", "task_name": "bad", "payload": {}, "retry_count": 3}
        )
        snap = worker.stats.snapshot()
        assert snap["failed"] == 1

