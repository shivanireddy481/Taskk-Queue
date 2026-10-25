# =============================================================================
# Distributed Task Queue – Worker Image
# =============================================================================
# Multi-stage build: dependencies are installed in a builder stage so the
# final image contains no build tools and has a minimal attack surface.
# =============================================================================

# ---- Stage 1: build dependencies ----------------------------------------
FROM python:3.11-slim AS builder

WORKDIR /build

COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt


# ---- Stage 2: runtime image ----------------------------------------------
FROM python:3.11-slim AS runtime

LABEL org.opencontainers.image.title="distributed-task-queue-worker" \
      org.opencontainers.image.description="Worker node for the Distributed Task Queue" \
      org.opencontainers.image.licenses="MIT"

# Prevent .pyc files and buffer stdout/stderr so logs appear immediately
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

WORKDIR /app

# Copy installed packages from builder
COPY --from=builder /install /usr/local

# Copy application source
COPY . .

# Create a non-root user for security
RUN addgroup --system taskq && adduser --system --ingroup taskq taskq
USER taskq

# Health check: verifies the worker process is running
HEALTHCHECK --interval=30s --timeout=10s --start-period=15s --retries=3 \
    CMD python -c "from redis_client import check_redis_health; exit(0 if check_redis_health() else 1)"

# Default command – override via docker-compose or docker run
CMD ["python", "run_worker.py", "worker-docker"]

