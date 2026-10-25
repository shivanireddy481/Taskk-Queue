# Distributed Task Queue - Complete Explanation

## 📖 What Is This?

A **distributed task queue** is a system for running jobs (tasks) across multiple machines (workers) in parallel. Instead of processing everything on your main server, you offload work to background workers.

### Real-World Analogy
Think of a **restaurant kitchen**:
- **Customer orders** = Tasks coming into your system
- **Order tickets** = Messages in the queue
- **Multiple cooks** = Worker nodes
- **Kitchen ticket board** = Redis (the message broker)

When a customer orders food:
1. The waiter writes the order on a ticket (enqueue task)
2. The ticket goes on the board (stored in Redis)
3. The first available cook grabs it (worker dequeues)
4. The cook prepares the food (task execution)
5. If the cook messes up, they retry or the manager handles it (retry mechanism)

---

## 🤔 Why Do We Need This?

### Problems This Solves

| Problem | Without Task Queue | With Task Queue |
|---------|-------------------|-----------------|
| **Slow API responses** | User waits 30s for email to send | API returns immediately, email sent in background |
| **Server overload** | One server handles everything | Work distributed across many workers |
| **Lost work on crash** | If server crashes, task is lost | Tasks persist in Redis, workers can restart |
| **No retry on failure** | Failed tasks stay failed | Automatic retry with exponential backoff |
| **No scheduling** | Can't "send email in 1 hour" | Delayed task execution supported |

### When to Use

✅ **Good Use Cases:**
- Sending emails/notifications
- Processing uploaded images/videos
- Generating reports (PDF, Excel)
- Syncing data with external APIs
- Running ML inference
- Batch data processing

❌ **Not Ideal For:**
- Real-time chat (use WebSockets)
- Sub-millisecond responses (use direct calls)
- Simple CRUD operations (overkill)

---

## 🏗️ How Does It Work?

### Architecture Overview

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Your App  │────▶│    Redis    │◀────│  Worker 1   │
│  (Producer) │     │   (Queue)   │     │ (Consumer)  │
└─────────────┘     └─────────────┘     └─────────────┘
                           │
                           │            ┌─────────────┐
                           └───────────▶│  Worker 2   │
                                        │ (Consumer)  │
                                        └─────────────┘
```

### The Flow

```
1. ENQUEUE: Your app adds a task
   ┌─────────────────────────────────────────────────────┐
   │ queue.enqueue("send_email", {"to": "user@test.com"})│
   └─────────────────────────────────────────────────────┘
                         │
                         ▼
2. STORE: Task saved to Redis with metadata
   ┌─────────────────────────────────────────────────────┐
   │ {                                                   │
   │   "task_id": "abc-123",                            │
   │   "task_name": "send_email",                       │
   │   "payload": {"to": "user@test.com"},              │
   │   "priority": 0,                                   │
   │   "retry_count": 0,                                │
   │   "status": "pending"                              │
   │ }                                                  │
   └─────────────────────────────────────────────────────┘
                         │
                         ▼
3. DEQUEUE: Worker grabs the task (atomically)
   ┌─────────────────────────────────────────────────────┐
   │ task = queue.dequeue()  # Only ONE worker gets it   │
   └─────────────────────────────────────────────────────┘
                         │
                         ▼
4. EXECUTE: Worker runs the task function
   ┌─────────────────────────────────────────────────────┐
   │ send_email_handler({"to": "user@test.com"})        │
   └─────────────────────────────────────────────────────┘
                         │
              ┌──────────┴──────────┐
              ▼                     ▼
5a. SUCCESS: Mark complete    5b. FAILURE: Retry or dead letter
```

---

## 📁 Project Structure Explained

```
taskk/
├── config.py           # All configuration settings
├── redis_client.py     # Redis connection with pooling & retry
├── logger.py           # Centralized logging setup
├── task_queue.py       # The queue implementation (core!)
├── worker.py           # Worker that executes tasks
├── tasks.py            # Example task definitions
├── enqueue_tasks.py    # Script to add sample tasks
├── run_worker.py       # Script to start a worker
├── requirements.txt    # Python dependencies
└── tests/              # Unit tests
    ├── test_task_queue.py
    └── test_worker.py
```

### File Responsibilities

| File | What It Does |
|------|--------------|
| `config.py` | Centralizes all settings (Redis host, timeouts, retry counts) |
| `redis_client.py` | Manages Redis connections with pooling, auto-reconnect |
| `logger.py` | Structured logging instead of print statements |
| `task_queue.py` | Enqueue, dequeue, priority, scheduling, results |
| `worker.py` | Task execution, timeouts, retries, heartbeats |
| `tasks.py` | Your actual task functions (email, image, etc.) |

---

## 🔑 Key Features Explained

### 1. Priority Queue

Tasks with higher priority get processed first:

```python
# Urgent - processed first (priority=10)
queue.enqueue("urgent_alert", data, priority=10)

# Normal - default priority
queue.enqueue("regular_task", data, priority=0)

# Low priority - processed last (priority=-5)
queue.enqueue("cleanup_job", data, priority=-5)
```

**How it works:** We use Redis sorted sets (ZSET). The score is negative priority, so `priority=10` becomes `score=-10`, which sorts first.

### 2. Delayed/Scheduled Tasks

Execute tasks in the future:

```python
# Send reminder email in 1 hour
queue.enqueue("send_reminder", data, delay_seconds=3600)
```

**How it works:** Delayed tasks go to a separate sorted set with `execute_at` timestamp as score. Workers periodically check and move ready tasks to the main queue.

### 3. Retry with Exponential Backoff

When tasks fail, they retry with increasing delays:

```
Attempt 1: Fails → Wait 2 seconds
Attempt 2: Fails → Wait 4 seconds
Attempt 3: Fails → Wait 8 seconds
Attempt 4: Fails → Move to Dead Letter Queue
```

**Why exponential backoff?** If a service is down, hammering it immediately won't help. Waiting longer each time gives it time to recover.

### 4. Dead Letter Queue

Tasks that fail all retries go to a separate queue for:
- Manual inspection
- Debugging
- Possible manual retry after fixing the issue

### 5. Task Timeouts

Prevents hung tasks from blocking workers forever:

```python
TASK_TIMEOUT = 300  # 5 minutes max per task
```

If a task runs longer than this, it's killed and marked as failed.

### 6. Worker Heartbeats

Workers send periodic "I'm alive" signals to Redis:

```python
HEARTBEAT_INTERVAL = 10  # Every 10 seconds
```

This helps detect dead workers so their tasks can be recovered.

### 7. Result Storage

Task results are stored for later retrieval:

```python
# After task completes
result = queue.get_result("task-id-123")
# Returns: {"result": {...}, "success": True, "timestamp": ...}
```

---

## 🚀 How to Run

### Prerequisites
1. **Python 3.8+**
2. **Redis server** running on localhost:6379

### Setup

```bash
# Install dependencies
pip install -r requirements.txt

# Copy environment file
cp .env.example .env
# Edit .env if Redis is not on localhost
```

### Running

**Terminal 1 - Start a worker:**
```bash
python run_worker.py worker-1
```

**Terminal 2 - Start another worker (parallel processing):**
```bash
python run_worker.py worker-2
```

**Terminal 3 - Enqueue tasks:**
```bash
python enqueue_tasks.py
```

### Running Tests

```bash
pytest tests/ -v
```

---

## ⚙️ Configuration Reference

| Setting | Default | Description |
|---------|---------|-------------|
| `REDIS_HOST` | localhost | Redis server address |
| `REDIS_PORT` | 6379 | Redis server port |
| `MAX_RETRIES` | 3 | Retry attempts before dead letter |
| `RETRY_DELAY_BASE` | 2 | Base delay in seconds (exponential) |
| `TASK_TIMEOUT` | 300 | Max seconds per task |
| `HEARTBEAT_INTERVAL` | 10 | Seconds between heartbeats |
| `LOG_LEVEL` | INFO | DEBUG, INFO, WARNING, ERROR |
| `RESULT_TTL` | 86400 | Seconds to keep results (24h) |

---

## 🔄 Production vs This Implementation

| Feature | This Project | Production (Celery/RQ) |
|---------|-------------|------------------------|
| Basic queueing | ✅ | ✅ |
| Priority | ✅ | ✅ |
| Delayed tasks | ✅ | ✅ |
| Retries | ✅ | ✅ |
| Timeouts | ✅ | ✅ |
| Heartbeats | ✅ | ✅ |
| Result storage | ✅ | ✅ |
| Monitoring dashboard | ❌ | ✅ (Flower) |
| Rate limiting | ❌ | ✅ |
| Task chaining | ❌ | ✅ |
| Cron scheduling | ❌ | ✅ (Celery Beat) |
| Multiple brokers | ❌ | ✅ (RabbitMQ, SQS) |

**This project is great for learning!** For production with complex needs, consider Celery or RQ.

---

## 🐛 Troubleshooting

| Issue | Cause | Solution |
|-------|-------|----------|
| "Connection refused" | Redis not running | Start Redis: `redis-server` |
| Tasks not processing | No workers running | Start a worker: `python run_worker.py` |
| Tasks stuck in processing | Worker crashed mid-task | Restart worker (task will timeout) |
| All tasks failing | Handler has bug | Check logs, fix the task function |

---

## 📚 Learn More

- **Redis Commands Used:** ZADD, ZPOPMIN, HSET, HGET, ZRANGEBYSCORE
- **Python Concepts:** Threading, Signals, Context Managers, Decorators
- **Design Patterns:** Producer-Consumer, Singleton (Redis pool)

This implementation teaches you the fundamentals of how systems like Celery work under the hood!

