"""
Redis client with connection pooling, retry logic, and health checks.
Provides a reliable connection to Redis for production use.
"""
import time
import redis
from redis.exceptions import ConnectionError, TimeoutError, RedisError
from typing import Optional
from functools import wraps

from config import (
    REDIS_HOST, REDIS_PORT, REDIS_DB, REDIS_PASSWORD,
    REDIS_MAX_CONNECTIONS, REDIS_SOCKET_TIMEOUT,
    REDIS_RETRY_ON_TIMEOUT, REDIS_HEALTH_CHECK_INTERVAL
)
from logger import setup_logger

logger = setup_logger("redis_client")


class RedisConnectionManager:
    """
    Manages Redis connections with pooling, retries, and health checks.
    
    Features:
    - Connection pooling for efficiency
    - Automatic reconnection on failure
    - Configurable retry logic
    - Health check support
    """
    
    _instance = None
    _pool = None
    
    def __new__(cls):
        """Singleton pattern - one connection pool per application."""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialize_pool()
        return cls._instance
    
    def _initialize_pool(self):
        """Create the connection pool."""
        logger.info(f"Initializing Redis connection pool to {REDIS_HOST}:{REDIS_PORT}")
        
        self._pool = redis.ConnectionPool(
            host=REDIS_HOST,
            port=REDIS_PORT,
            db=REDIS_DB,
            password=REDIS_PASSWORD if REDIS_PASSWORD else None,
            max_connections=REDIS_MAX_CONNECTIONS,
            socket_timeout=REDIS_SOCKET_TIMEOUT,
            socket_connect_timeout=REDIS_SOCKET_TIMEOUT,
            retry_on_timeout=REDIS_RETRY_ON_TIMEOUT,
            health_check_interval=REDIS_HEALTH_CHECK_INTERVAL,
            decode_responses=True
        )
        
        self._client = redis.Redis(connection_pool=self._pool)
    
    def get_client(self) -> redis.Redis:
        """Get a Redis client from the pool."""
        return self._client
    
    def health_check(self) -> bool:
        """Check if Redis connection is healthy."""
        try:
            self._client.ping()
            return True
        except (ConnectionError, TimeoutError) as e:
            logger.error(f"Redis health check failed: {e}")
            return False
    
    def close(self):
        """Close all connections in the pool."""
        if self._pool:
            self._pool.disconnect()
            logger.info("Redis connection pool closed")


def with_retry(max_retries: int = 3, delay: float = 1.0):
    """
    Decorator to retry Redis operations on connection failures.
    
    Args:
        max_retries: Maximum number of retry attempts
        delay: Base delay between retries (exponential backoff applied)
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_error = None
            
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except (ConnectionError, TimeoutError) as e:
                    last_error = e
                    if attempt < max_retries:
                        wait_time = delay * (2 ** attempt)
                        logger.warning(
                            f"Redis operation failed (attempt {attempt + 1}/{max_retries + 1}), "
                            f"retrying in {wait_time}s: {e}"
                        )
                        time.sleep(wait_time)
                    else:
                        logger.error(f"Redis operation failed after {max_retries + 1} attempts: {e}")
            
            raise last_error
        return wrapper
    return decorator


def get_redis_client() -> redis.Redis:
    """Get the shared Redis client instance."""
    return RedisConnectionManager().get_client()


def check_redis_health() -> bool:
    """Check Redis connection health."""
    return RedisConnectionManager().health_check()

