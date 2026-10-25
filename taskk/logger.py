"""
Centralized logging configuration for the task queue system.

Supports:
- Text and JSON structured output formats
- Optional rotating file handler (configured via LOG_FILE env var)
- Extra context fields (task_id, worker_id, duration_ms) surfaced in JSON logs
- A convenience helper `get_logger` that workers/queue modules call once
"""
import json
import logging
import logging.handlers
import os
import sys
from datetime import datetime, timezone
from typing import Optional

from config import (
    LOG_LEVEL, LOG_FORMAT,
    LOG_FILE, LOG_FILE_MAX_BYTES, LOG_FILE_BACKUP_COUNT,
)

# Extra fields that the JSON formatter will promote to top-level keys
_EXTRA_FIELDS = ("task_id", "worker_id", "task_name", "duration_ms", "retry_count")

_TEXT_FORMAT = "%(asctime)s | %(name)-20s | %(levelname)-8s | %(message)s"
_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


class JsonFormatter(logging.Formatter):
    """
    JSON formatter for structured logging.

    Produces one JSON object per line, suitable for ingestion by log
    aggregators (Splunk, ELK, Loki, Datadog, …).
    """

    def format(self, record: logging.LogRecord) -> str:
        log_data: dict = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "logger": record.name,
            "level": record.levelname,
            "message": record.getMessage(),
        }

        # Promote well-known extra fields to top-level keys
        for field in _EXTRA_FIELDS:
            value = getattr(record, field, None)
            if value is not None:
                log_data[field] = value

        # Attach exception traceback if present
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_data, default=str)


def _build_formatter() -> logging.Formatter:
    if LOG_FORMAT == "json":
        return JsonFormatter()
    return logging.Formatter(_TEXT_FORMAT, datefmt=_DATE_FORMAT)


def setup_logger(name: str, level: Optional[str] = None) -> logging.Logger:
    """
    Return a configured logger instance.

    Handlers are added only once per logger name (idempotent).  A rotating
    file handler is added automatically when LOG_FILE is set.

    Args:
        name:  Logger name – typically the module name or ``worker.<id>``.
        level: Optional override for log level (DEBUG/INFO/WARNING/ERROR).

    Returns:
        A ready-to-use :class:`logging.Logger`.
    """
    logger = logging.getLogger(name)

    # Guard against adding duplicate handlers on repeated calls
    if logger.handlers:
        return logger

    log_level = getattr(logging, (level or LOG_LEVEL).upper(), logging.INFO)
    logger.setLevel(log_level)
    logger.propagate = False  # prevent double-printing via root logger

    formatter = _build_formatter()

    # --- Console handler --------------------------------------------------
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # --- Rotating file handler (optional) ---------------------------------
    if LOG_FILE:
        log_dir = os.path.dirname(LOG_FILE)
        if log_dir:
            os.makedirs(log_dir, exist_ok=True)

        file_handler = logging.handlers.RotatingFileHandler(
            LOG_FILE,
            maxBytes=LOG_FILE_MAX_BYTES,
            backupCount=LOG_FILE_BACKUP_COUNT,
            encoding="utf-8",
        )
        file_handler.setLevel(log_level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger


# Convenience alias used by some modules
get_logger = setup_logger

