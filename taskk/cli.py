#!/usr/bin/env python3
"""
Command-line interface for the Distributed Task Queue.

Usage:
    python cli.py status              - Queue statistics + metrics
    python cli.py workers             - List active workers with stats
    python cli.py enqueue <task> <payload>  - Add a task
    python cli.py info <task_id>      - Task details + result
    python cli.py list <queue_type>   - List tasks in a queue
    python cli.py cancel <task_id>    - Cancel a pending task
    python cli.py retry <task_id>     - Requeue from dead-letter queue
    python cli.py clear-dead          - Clear dead-letter queue
    python cli.py metrics             - Show operational metrics
"""
import argparse
import json
import sys
from datetime import datetime

from config import WORKER_HEARTBEAT_KEY, WORKER_TIMEOUT
from redis_client import check_redis_health, get_redis_client
from task_queue import TaskQueue


def format_timestamp(ts):
    """Convert timestamp to readable format."""
    if ts:
        return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")
    return "N/A"


def cmd_status(args):
    """Show queue statistics."""
    queue = TaskQueue()
    stats = queue.get_queue_stats()
    
    print("\n" + "=" * 50)
    print("📊 QUEUE STATISTICS")
    print("=" * 50)
    print(f"  📋 Pending:      {stats['pending']:>5} tasks")
    print(f"  ⏰ Scheduled:    {stats['scheduled']:>5} tasks")
    print(f"  ⚙️  Processing:   {stats['processing']:>5} tasks")
    print(f"  💀 Dead Letter:  {stats['dead_letter']:>5} tasks")
    print("=" * 50)
    
    # Redis health
    if check_redis_health():
        print("  ✅ Redis: Connected")
    else:
        print("  ❌ Redis: Disconnected")
    print("=" * 50 + "\n")


def cmd_workers(args):
    """List active workers with live stats."""
    redis_client = get_redis_client()
    heartbeats = redis_client.hgetall(WORKER_HEARTBEAT_KEY)

    print("\n" + "=" * 60)
    print("👷  ACTIVE WORKERS")
    print("=" * 60)

    if not heartbeats:
        print("  No active workers found.")
    else:
        current_time = datetime.now().timestamp()

        for worker_id, data_str in heartbeats.items():
            try:
                data = json.loads(data_str)          # ← JSON, not ast.literal_eval
                last_seen = current_time - data.get("timestamp", 0)
                status = data.get("status", "unknown")
                current_task = data.get("current_task")
                stats = data.get("stats", {})

                if last_seen > WORKER_TIMEOUT:
                    state = "❌ DEAD"
                elif status == "busy":
                    state = "🔄 BUSY"
                else:
                    state = "✅ IDLE"

                print(f"\n  Worker : {worker_id}")
                print(f"    State      : {state}  (last seen {int(last_seen)}s ago)")
                if current_task:
                    print(f"    Current    : {current_task}")
                if stats:
                    print(
                        f"    Stats      : processed={stats.get('processed', 0)}  "
                        f"ok={stats.get('succeeded', 0)}  "
                        f"fail={stats.get('failed', 0)}  "
                        f"avg={stats.get('avg_duration_ms', 0)}ms"
                    )
            except (json.JSONDecodeError, KeyError):
                print(f"\n  Worker: {worker_id} (heartbeat parse error)")

    print("\n" + "=" * 60 + "\n")


def cmd_enqueue(args):
    """Enqueue a new task."""
    queue = TaskQueue()
    
    try:
        payload = json.loads(args.payload)
    except json.JSONDecodeError:
        # Treat as simple string payload
        payload = {"data": args.payload}
    
    task_id = queue.enqueue(
        task_name=args.task_name,
        payload=payload,
        priority=args.priority,
        delay_seconds=args.delay
    )
    
    print("\n✅ Task enqueued successfully!")
    print(f"   Task ID: {task_id}")
    print(f"   Task Name: {args.task_name}")
    print(f"   Priority: {args.priority}")
    if args.delay > 0:
        print(f"   Scheduled: in {args.delay} seconds")
    print()


def cmd_info(args):
    """Get task information."""
    queue = TaskQueue()
    info = queue.get_task_info(args.task_id)
    
    if not info:
        print(f"\n❌ Task not found: {args.task_id}\n")
        return
    
    print("\n" + "=" * 50)
    print("📋 TASK INFORMATION")
    print("=" * 50)
    print(f"  ID:         {info.get('task_id')}")
    print(f"  Name:       {info.get('task_name')}")
    print(f"  Status:     {info.get('status')}")
    print(f"  Priority:   {info.get('priority', 0)}")
    print(f"  Retries:    {info.get('retry_count', 0)}")
    print(f"  Created:    {format_timestamp(info.get('created_at'))}")
    
    if info.get('last_error'):
        print(f"  Last Error: {info.get('last_error')}")
    
    result = info.get('result')
    if result:
        print(f"  Result:     {result.get('result')}")
        print(f"  Success:    {result.get('success')}")
    
    print("=" * 50 + "\n")


def cmd_cancel(args):
    """Cancel a pending task."""
    queue = TaskQueue()
    cancelled = queue.cancel_task(args.task_id)
    if cancelled:
        print(f"\n✅ Task cancelled: {args.task_id}\n")
    else:
        print(f"\n❌ Task not found in pending/scheduled queue: {args.task_id}")
        print("   (Task may already be processing or completed)\n")


def cmd_retry(args):
    """Retry a failed task from dead-letter queue."""
    queue = TaskQueue()
    requeued = queue.retry_failed_task(args.task_id)
    if requeued:
        print(f"\n✅ Task requeued for retry: {args.task_id}\n")
    else:
        print(f"\n❌ Task not found in dead-letter queue: {args.task_id}\n")


def cmd_clear_dead(args):
    """Clear all tasks from the dead-letter queue."""
    queue = TaskQueue()
    count = queue.redis_client.zcard(queue.dead_letter_queue)

    if count == 0:
        print("\n📭 Dead-letter queue is already empty.\n")
        return

    confirm = input(f"\n⚠️  This will delete {count} failed tasks. Continue? (y/N): ")
    if confirm.lower() == "y":
        cleared = queue.clear_dead_letter_queue()
        print(f"\n✅ Cleared {cleared} tasks from dead-letter queue.\n")
    else:
        print("\n❌ Operation cancelled.\n")


def cmd_metrics(_args):
    """Display cumulative operational metrics."""
    queue = TaskQueue()
    m = queue.get_metrics()

    print("\n" + "=" * 50)
    print("📈  OPERATIONAL METRICS")
    print("=" * 50)
    print(f"  Enqueued        : {int(m.get('enqueued', 0))}")
    print(f"  Completed       : {int(m.get('completed', 0))}")
    print(f"  Failed          : {int(m.get('failed', 0))}")
    print(f"  Success rate    : {m.get('success_rate', 0.0):.1f}%")
    print(f"  Avg duration    : {m.get('avg_duration_ms', 0.0):.1f} ms")
    print("=" * 50 + "\n")


def cmd_list(args):
    """List tasks in a queue."""
    queue = TaskQueue()
    redis_client = get_redis_client()

    queue_map = {
        "pending": queue.queue_name,
        "scheduled": queue.scheduled_queue,
        "processing": queue.processing_queue,
        "dead": queue.dead_letter_queue
    }

    queue_key = queue_map.get(args.queue_type, queue.queue_name)
    tasks = redis_client.zrange(queue_key, 0, args.limit - 1, withscores=True)

    print("\n" + "=" * 60)
    print(f"📋 {args.queue_type.upper()} TASKS (showing {len(tasks)} of max {args.limit})")
    print("=" * 60)

    if not tasks:
        print("  No tasks found.")
    else:
        for task_id, score in tasks:
            info = queue.get_task_info(task_id)
            if info:
                print(f"\n  ID: {task_id[:8]}...")
                print(f"      Name: {info.get('task_name')}")
                print(f"      Created: {format_timestamp(info.get('created_at'))}")

    print("\n" + "=" * 60 + "\n")


def main():
    parser = argparse.ArgumentParser(
        description="🚀 Distributed Task Queue CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python cli.py status
  python cli.py metrics
  python cli.py workers
  python cli.py enqueue send_email '{"to": "user@example.com"}'
  python cli.py enqueue process_image '{"url": "image.jpg"}' --priority 5
  python cli.py info <task-id>
  python cli.py list pending --limit 20
  python cli.py cancel <task-id>
  python cli.py retry <task-id>
  python cli.py clear-dead
        """,
    )

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    subparsers.add_parser("status", help="Show queue statistics")
    subparsers.add_parser("workers", help="List active workers with stats")
    subparsers.add_parser("metrics", help="Show operational metrics")
    subparsers.add_parser("clear-dead", help="Clear dead-letter queue")

    enqueue_parser = subparsers.add_parser("enqueue", help="Enqueue a new task")
    enqueue_parser.add_argument("task_name", help="Name of the task handler")
    enqueue_parser.add_argument("payload", help="JSON payload for the task")
    enqueue_parser.add_argument("--priority", "-p", type=int, default=0)
    enqueue_parser.add_argument("--delay", "-d", type=int, default=0, help="Delay in seconds")

    info_parser = subparsers.add_parser("info", help="Get task information")
    info_parser.add_argument("task_id")

    cancel_parser = subparsers.add_parser("cancel", help="Cancel a pending task")
    cancel_parser.add_argument("task_id")

    retry_parser = subparsers.add_parser("retry", help="Retry a failed task")
    retry_parser.add_argument("task_id")

    list_parser = subparsers.add_parser("list", help="List tasks in a queue")
    list_parser.add_argument(
        "queue_type", choices=["pending", "scheduled", "processing", "dead"]
    )
    list_parser.add_argument("--limit", "-l", type=int, default=10)

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return

    commands = {
        "status": cmd_status,
        "workers": cmd_workers,
        "metrics": cmd_metrics,
        "enqueue": cmd_enqueue,
        "info": cmd_info,
        "cancel": cmd_cancel,
        "retry": cmd_retry,
        "clear-dead": cmd_clear_dead,
        "list": cmd_list,
    }

    commands[args.command](args)


if __name__ == "__main__":
    main()

