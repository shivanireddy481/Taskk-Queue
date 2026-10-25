"""
Script to enqueue sample tasks for testing.
Demonstrates different task types, priorities, and delays.
"""
from task_queue import TaskQueue


def main():
    queue = TaskQueue()
    
    print("=" * 50)
    print("Enqueuing sample tasks...")
    print("=" * 50)
    
    # 1. Regular priority tasks
    queue.enqueue(
        task_name="send_email",
        payload={
            "to": "user@example.com",
            "subject": "Welcome to our platform!",
            "body": "Thanks for signing up."
        }
    )
    
    queue.enqueue(
        task_name="send_email",
        payload={
            "to": "admin@example.com",
            "subject": "New user signup",
            "body": "A new user has registered."
        }
    )
    
    # 2. High priority task (will be processed first)
    queue.enqueue(
        task_name="process_image",
        payload={
            "url": "https://example.com/urgent-photo.jpg",
            "operation": "resize"
        },
        priority=10  # Higher priority
    )
    
    # 3. Delayed task (will be processed after 5 seconds)
    queue.enqueue(
        task_name="generate_report",
        payload={
            "type": "daily",
            "user_id": "user-123"
        },
        delay_seconds=5
    )
    
    # 4. Regular tasks
    queue.enqueue(
        task_name="process_image",
        payload={
            "url": "https://example.com/photo1.jpg",
            "operation": "compress"
        }
    )
    
    queue.enqueue(
        task_name="sync_data",
        payload={
            "source": "crm_system",
            "destination": "analytics_db"
        }
    )
    
    # 5. Low priority task
    queue.enqueue(
        task_name="generate_report",
        payload={
            "type": "monthly",
            "user_id": "user-456"
        },
        priority=-5  # Lower priority
    )
    
    # Show queue stats
    print("\n" + "=" * 50)
    stats = queue.get_queue_stats()
    print("Queue Statistics:")
    print(f"  Pending: {stats['pending']}")
    print(f"  Scheduled: {stats['scheduled']}")
    print(f"  Processing: {stats['processing']}")
    print(f"  Dead Letter: {stats['dead_letter']}")
    print("=" * 50)


if __name__ == "__main__":
    main()

