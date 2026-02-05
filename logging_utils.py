"""
logging_utils.py

Centralized logging setup.

Supports:
- get_logger()
- get_logger("TBot")  (backward compatible)
- setup_logging(log_file=..., to_stdout=bool)

No stdout logging unless explicitly enabled.
"""

from __future__ import annotations

import logging
import logging.handlers
import os
import queue
import sys
from typing import Optional


_DEFAULT_LOGGER_NAME = "TBot"
_CONFIGURED = False


def get_logger(name: Optional[str] = None) -> logging.Logger:
    """
    Return the application logger.

    Args:
        name: Optional logger name. Ignored; kept for backward compatibility.
    """
    return logging.getLogger(_DEFAULT_LOGGER_NAME)


def setup_logging(
    log_file: str = "bot_log.txt",
    to_stdout: bool = False,
    stdout: Optional[bool] = None,
    level: int = logging.INFO,
) -> None:
    """
    Configure application logging once.

    Args:
        log_file: Path to log file.
        to_stdout: If True, also log to console.
        stdout: Backward-compatible alias for to_stdout.
        level: Base logging level.
    """
    global _CONFIGURED
    if _CONFIGURED:
        return

    if stdout is not None:
        to_stdout = bool(stdout)

    logger = get_logger()
    logger.setLevel(level)
    logger.propagate = False

    # Ensure directory exists
    log_dir = os.path.dirname(os.path.abspath(log_file))
    if log_dir and not os.path.exists(log_dir):
        os.makedirs(log_dir, exist_ok=True)

    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    )

    # Queue-based logging to avoid blocking hot paths
    q: queue.Queue = queue.Queue(maxsize=10000)
    qh = logging.handlers.QueueHandler(q)
    qh.setLevel(level)
    logger.addHandler(qh)

    handlers = []

    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(level)
    file_handler.setFormatter(fmt)
    handlers.append(file_handler)

    if to_stdout:
        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setLevel(level)
        stream_handler.setFormatter(fmt)
        handlers.append(stream_handler)

    listener = logging.handlers.QueueListener(
        q, *handlers, respect_handler_level=True
    )
    listener.daemon = True
    listener.start()

    # Prevent garbage collection
    logger._tbot_queue_listener = listener  # type: ignore[attr-defined]

    _CONFIGURED = True
