"""
Unit tests for TaskQueue.

All Redis calls are mocked so no live Redis instance is required.
"""
import json
import time
from unittest.mock import MagicMock, call, patch

import pytest


@pytest.fixture()
def mock_redis():
    """Patch ``get_redis_client`` globally and return the Redis mock."""
    with patch("task_queue.get_redis_client") as mock_factory:
        redis_mock = MagicMock()
        # pipeline() must return a context manager that exposes the same mock
        pipe_mock = MagicMock()
        redis_mock.pipeline.return_value = pipe_mock
        pipe_mock.__enter__ = MagicMock(return_value=pipe_mock)
        pipe_mock.__exit__ = MagicMock(return_value=False)
        mock_factory.return_value = redis_mock
        yield redis_mock


@pytest.fixture()
def queue(mock_redis):
    """TaskQueue wired to the mocked Redis client."""
    from task_queue import TaskQueue
    return TaskQueue()


# ---------------------------------------------------------------------------
# Enqueue
# ---------------------------------------------------------------------------

class TestEnqueue:
    def test_returns_uuid(self, queue, mock_redis):
        task_id = queue.enqueue("test_task", {"k": "v"})
        assert task_id is not None
        assert len(task_id) == 36  # UUID4 format

    def test_stores_task_hash(self, queue, mock_redis):
        queue.enqueue("send_email", {"to": "a@b.com"})
        # pipeline.hset should have been called with the task key
        pipe = mock_redis.pipeline.return_value
        pipe.hset.assert_called()
        key_arg = pipe.hset.call_args[0][0]
        assert key_arg.startswith("taskq:task:")

    def test_priority_stored_as_negative_score(self, queue, mock_redis):
        queue.enqueue("hi_pri", {}, priority=10)
        pipe = mock_redis.pipeline.return_value
        pipe.zadd.assert_called()
        score_dict = pipe.zadd.call_args[0][1]
        assert list(score_dict.values())[0] == -10

    def test_delayed_task_goes_to_scheduled_queue(self, queue, mock_redis):
        queue.enqueue("delayed", {}, delay_seconds=60)
        pipe = mock_redis.pipeline.return_value
        pipe.zadd.assert_called()
        queue_key = pipe.zadd.call_args[0][0]
        assert queue_key == "taskq:scheduled"

    def test_idempotency_key_suppresses_duplicate(self, queue, mock_redis):
        # First call: no existing key
        mock_redis.get.return_value = None
        first_id = queue.enqueue("t", {}, idempotency_key="idem-1")

        # Second call: Redis returns the original task_id
        mock_redis.get.return_value = first_id
        second_id = queue.enqueue("t", {}, idempotency_key="idem-1")

        assert first_id == second_id

    def test_idempotency_key_sets_redis_expiry(self, queue, mock_redis):
        mock_redis.get.return_value = None
        queue.enqueue("t", {}, idempotency_key="my-key")
        pipe = mock_redis.pipeline.return_value
        # setex should be called on the pipeline with the idempotency key
        pipe.setex.assert_called()
        key_arg = pipe.setex.call_args[0][0]
        assert "my-key" in key_arg


# ---------------------------------------------------------------------------
# Dequeue
# ---------------------------------------------------------------------------

class TestDequeue:
    def test_returns_task_data(self, queue, mock_redis):
        task_data = {
            "task_id": "t-1",
            "task_name": "do_work",
            "payload": {},
            "priority": 0,
            "retry_count": 0,
            "created_at": time.time(),
            "status": "pending",
        }
        # Lua script returns task_id; hget returns serialised task_data
        queue._dequeue_script = MagicMock(return_value="t-1")
        mock_redis.hget.return_value = json.dumps(task_data)

        result = queue.dequeue()

        assert result is not None
        assert result["task_id"] == "t-1"
        assert result["status"] == "processing"

    def test_returns_none_when_empty(self, queue, mock_redis):
        queue._dequeue_script = MagicMock(return_value=None)
        assert queue.dequeue() is None

    def test_lua_script_receives_correct_keys(self, queue, mock_redis):
        queue._dequeue_script = MagicMock(return_value=None)
        queue.dequeue()
        call_kwargs = queue._dequeue_script.call_args
        keys = call_kwargs[1]["keys"]
        assert keys[0] == "taskq:scheduled"
        assert keys[1] == "taskq:pending"


# ---------------------------------------------------------------------------
# Complete / Fail / Requeue
# ---------------------------------------------------------------------------

class TestTaskLifecycle:
    def test_complete_removes_from_processing(self, queue, mock_redis):
        pipe = mock_redis.pipeline.return_value
        queue.complete_task("t-2", {"out": 1})
        pipe.zrem.assert_called_with("taskq:processing", "t-2")

    def test_complete_stores_result(self, queue, mock_redis):
        queue.complete_task("t-2", {"out": 1})
        mock_redis.setex.assert_called()

    def test_fail_without_retry_adds_to_dlq(self, queue, mock_redis):
        mock_redis.hget.return_value = json.dumps(
            {"task_id": "t-3", "task_name": "bad", "status": "processing"}
        )
        queue.fail_task("t-3", "oops", retry=False)
        mock_redis.zadd.assert_called()
        dlq_key = mock_redis.zadd.call_args[0][0]
        assert dlq_key == "taskq:dead_letter"

    def test_fail_with_retry_does_not_add_to_dlq(self, queue, mock_redis):
        mock_redis.hget.return_value = json.dumps(
            {"task_id": "t-4", "task_name": "retry_me", "status": "processing"}
        )
        queue.fail_task("t-4", "transient", retry=True)
        # zadd must NOT be called for the dead-letter queue
        for c in mock_redis.zadd.call_args_list:
            assert "dead_letter" not in c[0][0]

    def test_requeue_increments_retry_count(self, queue, mock_redis):
        task_data = {"task_id": "t-5", "task_name": "t", "retry_count": 1, "priority": 0, "status": "pending"}
        mock_redis.hget.return_value = json.dumps(task_data)
        queue.requeue_task("t-5")
        stored = json.loads(mock_redis.hset.call_args[1]["mapping"]["data"])
        assert stored["retry_count"] == 2


# ---------------------------------------------------------------------------
# Stats & Metrics
# ---------------------------------------------------------------------------

class TestStats:
    def test_get_queue_stats_keys(self, queue, mock_redis):
        mock_redis.zcard.side_effect = [5, 2, 1, 3]
        mock_redis.get.return_value = None  # metrics counters empty
        stats = queue.get_queue_stats()
        assert stats["pending"] == 5
        assert stats["scheduled"] == 2
        assert stats["processing"] == 1
        assert stats["dead_letter"] == 3

    def test_store_and_retrieve_result(self, queue, mock_redis):
        payload = {"out": "ok"}
        queue.store_result("t-6", payload, success=True)
        mock_redis.setex.assert_called()

        mock_redis.get.return_value = json.dumps(
            {"task_id": "t-6", "result": payload, "success": True, "timestamp": time.time()}
        )
        result = queue.get_result("t-6")
        assert result["success"] is True
        assert result["result"] == payload

    def test_cancel_pending_task(self, queue, mock_redis):
        mock_redis.zrem.return_value = 1
        assert queue.cancel_task("t-7") is True
        mock_redis.hset.assert_called()

    def test_cancel_nonexistent_task(self, queue, mock_redis):
        mock_redis.zrem.return_value = 0
        assert queue.cancel_task("ghost-id") is False

