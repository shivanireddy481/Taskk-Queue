"""
Configuration settings for the distributed task queue.

All settings can be overridden via environment variables. Use a .env file
for local development (see .env.example). In production, inject environment
variables via Docker, Kubernetes Secrets, or your CI/CD pipeline.
"""
import os
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Redis connection settings
# ---------------------------------------------------------------------------
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_DB = int(os.getenv("REDIS_DB", 0))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", None)

# Redis connection pool settings
REDIS_MAX_CONNECTIONS = int(os.getenv("REDIS_MAX_CONNECTIONS", 20))
REDIS_SOCKET_TIMEOUT = int(os.getenv("REDIS_SOCKET_TIMEOUT", 5))
REDIS_RETRY_ON_TIMEOUT = True
REDIS_HEALTH_CHECK_INTERVAL = 30

# ---------------------------------------------------------------------------
# Queue key names  (keep all Redis key names in one place for easy auditing)
# ---------------------------------------------------------------------------
TASK_QUEUE_NAME = "taskq:pending"
PROCESSING_QUEUE_NAME = "taskq:processing"
DEAD_LETTER_QUEUE_NAME = "taskq:dead_letter"
SCHEDULED_QUEUE_NAME = "taskq:scheduled"
RESULTS_KEY_PREFIX = "taskq:result:"
TASK_KEY_PREFIX = "taskq:task:"
IDEMPOTENCY_KEY_PREFIX = "taskq:idem:"
WORKER_HEARTBEAT_KEY = "taskq:workers"
METRICS_KEY_PREFIX = "taskq:metrics:"

# ---------------------------------------------------------------------------
# Worker settings
# ---------------------------------------------------------------------------
MAX_RETRIES = int(os.getenv("MAX_RETRIES", 3))
RETRY_DELAY_BASE = int(os.getenv("RETRY_DELAY_BASE", 2))  # exponential: 2, 4, 8…
WORKER_POLL_INTERVAL = float(os.getenv("WORKER_POLL_INTERVAL", 1.0))  # seconds
TASK_TIMEOUT = int(os.getenv("TASK_TIMEOUT", 300))  # 5 minutes max per task

# ---------------------------------------------------------------------------
# Heartbeat settings
# ---------------------------------------------------------------------------
HEARTBEAT_INTERVAL = int(os.getenv("HEARTBEAT_INTERVAL", 10))  # seconds
WORKER_TIMEOUT = int(os.getenv("WORKER_TIMEOUT", 30))  # dead if no heartbeat for this long

# ---------------------------------------------------------------------------
# Logging settings
# ---------------------------------------------------------------------------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
LOG_FORMAT = os.getenv("LOG_FORMAT", "text")   # "text" or "json"
LOG_FILE = os.getenv("LOG_FILE", "")           # empty = console only; e.g. "logs/taskq.log"
LOG_FILE_MAX_BYTES = int(os.getenv("LOG_FILE_MAX_BYTES", 10 * 1024 * 1024))  # 10 MB
LOG_FILE_BACKUP_COUNT = int(os.getenv("LOG_FILE_BACKUP_COUNT", 5))

# ---------------------------------------------------------------------------
# Result storage
# ---------------------------------------------------------------------------
RESULT_TTL = int(os.getenv("RESULT_TTL", 86400))  # 24 hours

# ---------------------------------------------------------------------------
# Idempotency
# ---------------------------------------------------------------------------
IDEMPOTENCY_TTL = int(os.getenv("IDEMPOTENCY_TTL", 86400))  # 24 hours dedup window

# ---------------------------------------------------------------------------
# Metrics
# ---------------------------------------------------------------------------
ENABLE_METRICS = os.getenv("ENABLE_METRICS", "true").lower() == "true"
METRICS_TTL = int(os.getenv("METRICS_TTL", 86400))  # keep counters for 24 h

