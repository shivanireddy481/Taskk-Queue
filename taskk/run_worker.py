"""
Script to run a worker node.
Usage: python run_worker.py [worker_id]
"""
import sys
from worker import Worker
from tasks import ALL_TASKS


def main():
    # Get worker ID from command line or use default
    worker_id = sys.argv[1] if len(sys.argv) > 1 else "worker-1"
    
    # Create worker instance
    worker = Worker(worker_id=worker_id)
    
    # Register all task handlers
    for task_name, handler in ALL_TASKS:
        worker.register_task(task_name, handler)
    
    # Start processing tasks
    print("=" * 50)
    print(f"Starting worker: {worker_id}")
    print("Press Ctrl+C to stop")
    print("=" * 50)
    
    worker.run()


if __name__ == "__main__":
    main()

