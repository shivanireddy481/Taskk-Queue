#!/usr/bin/env python3
"""
End-to-end demo for the Distributed Task Queue.

Spins up two in-process worker threads, enqueues a variety of tasks
(normal, high-priority, delayed, batch, and idempotent), then polls
until all tasks finish and prints a summary.

Prerequisites
-------------
* Redis running on localhost:6379  (or set REDIS_HOST / REDIS_PORT env vars)
* pip install -r requirements.txt

Usage
-----
    python demo.py
"""
import sys
import threading
import time
from typing import Any, Dict

# Make sure the source directory is on the path when run directly
import os
sys.path.insert(0, os.path.dirname(__file__))

from task_queue import TaskQueue
from worker import Worker

# ---------------------------------------------------------------------------
# Task handlers used in this demo
# ---------------------------------------------------------------------------

def send_email(payload: Dict[str, Any]) -> Dict[str, Any]:
    time.sleep(0.3)
    return {"status": "sent", "to": payload.get("to")}


def process_image(payload: Dict[str, Any]) -> Dict[str, Any]:
    time.sleep(0.5)
    return {"status": "processed", "url": payload.get("url")}


def generate_report(payload: Dict[str, Any]) -> Dict[str, Any]:
    time.sleep(0.4)
    return {"status": "generated", "type": payload.get("type")}


def flaky_task(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Fails twice then succeeds – demonstrates retry with exponential back-off."""
    attempt = payload.get("_attempt", 0)
    if attempt < 2:
        raise RuntimeError(f"Simulated transient failure (attempt {attempt + 1})")
    return {"status": "recovered"}


DEMO_HANDLERS = [
    ("send_email", send_email),
    ("process_image", process_image),
    ("generate_report", generate_report),
    ("flaky_task", flaky_task),
]


def _start_worker(worker_id: str) -> Worker:
    w = Worker(worker_id=worker_id)
    for name, fn in DEMO_HANDLERS:
        w.register_task(name, fn)
    t = threading.Thread(target=w.run, daemon=True, name=worker_id)
    t.start()
    return w


def _separator(title: str) -> None:
    print(f"\n{'=' * 60}")
    print(f"  {title}")
    print(f"{'=' * 60}")


def main() -> None:
    queue = TaskQueue()

    _separator("🚀 Distributed Task Queue – Live Demo")
    print("  Starting 2 worker threads …")
    workers = [_start_worker("worker-A"), _start_worker("worker-B")]
    time.sleep(0.2)  # let workers register heartbeats

    # ------------------------------------------------------------------
    # Enqueue tasks
    # ------------------------------------------------------------------
    _separator("📥 Enqueueing tasks")

    ids: Dict[str, str] = {}

    ids["email_1"] = queue.enqueue("send_email", {"to": "alice@example.com"})
    print(f"  [normal]    send_email        → {ids['email_1'][:8]}…")

    ids["email_2"] = queue.enqueue("send_email", {"to": "bob@example.com"}, priority=5)
    print(f"  [priority=5] send_email       → {ids['email_2'][:8]}…")

    ids["image"] = queue.enqueue("process_image", {"url": "https://cdn.example.com/img.jpg"}, priority=10)
    print(f"  [priority=10] process_image   → {ids['image'][:8]}…")

    ids["report"] = queue.enqueue("generate_report", {"type": "monthly"}, delay_seconds=3)
    print(f"  [delay=3s]  generate_report   → {ids['report'][:8]}…")

    # Idempotent enqueue – second call must return same ID
    ids["idem"] = queue.enqueue("send_email", {"to": "ops@example.com"}, idempotency_key="demo-ops-email")
    dup_id = queue.enqueue("send_email", {"to": "ops@example.com"}, idempotency_key="demo-ops-email")
    idem_ok = ids["idem"] == dup_id
    print(f"  [idempotent] send_email        → {ids['idem'][:8]}…  (duplicate suppressed: {idem_ok})")

    # Batch enqueue
    batch_ids = queue.enqueue_batch([
        {"task_name": "send_email", "payload": {"to": "user1@example.com"}},
        {"task_name": "send_email", "payload": {"to": "user2@example.com"}},
        {"task_name": "process_image", "payload": {"url": "https://cdn.example.com/batch.jpg"}},
    ])
    print(f"  [batch x3]  send_email / process_image → {len(batch_ids)} tasks enqueued")

    # ------------------------------------------------------------------
    # Wait for completion
    # ------------------------------------------------------------------
    _separator("⏳ Waiting for workers to process all tasks …")
    deadline = time.time() + 20
    all_ids = list(ids.values()) + batch_ids

    while time.time() < deadline:
        statuses = {tid: queue.get_task_status(tid) for tid in all_ids}
        done = sum(1 for s in statuses.values() if s in ("completed", "failed", "cancelled"))
        print(f"\r  Progress: {done}/{len(all_ids)} finished", end="", flush=True)
        if done >= len(all_ids):
            break
        time.sleep(0.5)
    print()

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------
    _separator("📊 Results")
    for label, tid in ids.items():
        status = queue.get_task_status(tid)
        result = queue.get_result(tid)
        outcome = result.get("result") if result and result.get("success") else (result or {}).get("result", "–")
        print(f"  {label:<18} status={status:<12} result={outcome}")

    stats = queue.get_queue_stats()
    _separator("📈 Queue stats + metrics")
    print(f"  Pending:     {stats['pending']}")
    print(f"  Scheduled:   {stats['scheduled']}")
    print(f"  Processing:  {stats['processing']}")
    print(f"  Dead letter: {stats['dead_letter']}")
    if "metrics" in stats:
        m = stats["metrics"]
        print(f"  Enqueued:    {int(m.get('enqueued', 0))}")
        print(f"  Completed:   {int(m.get('completed', 0))}")
        print(f"  Failed:      {int(m.get('failed', 0))}")
        print(f"  Success rate:{m.get('success_rate', 0):.1f}%")
        print(f"  Avg latency: {m.get('avg_duration_ms', 0):.1f} ms")

    print("\n  Demo complete. Workers will shut down automatically.\n")
    for w in workers:
        w.running = False


if __name__ == "__main__":
    main()
