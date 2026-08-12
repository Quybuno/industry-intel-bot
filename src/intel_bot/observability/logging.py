"""Structured logging setup for intel-bot jobs."""

from __future__ import annotations

import logging
import sys
from pathlib import Path

from src.intel_bot.config import settings


def setup_logging(level: str | None = None, log_dir: str = "logs") -> None:
    """Configure root logger with console + optional file handler."""
    log_level = getattr(logging, (level or settings.LOG_LEVEL).upper(), logging.INFO)
    root = logging.getLogger()
    if root.handlers:
        root.setLevel(log_level)
        return

    fmt = logging.Formatter(
        "%(asctime)s %(levelname)s [%(name)s] %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )
    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(fmt)
    root.addHandler(console)
    root.setLevel(log_level)

    log_path = Path(log_dir)
    log_path.mkdir(parents=True, exist_ok=True)
    file_handler = logging.FileHandler(log_path / "app.log", encoding="utf-8")
    file_handler.setFormatter(fmt)
    root.addHandler(file_handler)


def log_event(logger: logging.Logger, event: str, **fields: object) -> None:
    """Log a structured event as key=value pairs."""
    parts = " ".join(f"{k}={v!r}" for k, v in fields.items())
    logger.info("event=%s %s", event, parts)
