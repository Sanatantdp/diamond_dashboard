"""
logger.py — Shared logger for diamond scrapers.

Features:
  • Writes logs to  logs/<name>.log
  • If the log file is older than 2 days it is deleted and recreated fresh
  • Console + file output with timestamps
"""

import logging
import os
import time
from pathlib import Path

# ── Config ──────────────────────────────────────────────────────────
LOG_DIR        = Path(os.getcwd()) / "logs"
MAX_AGE_DAYS   = 2
MAX_AGE_SECS   = MAX_AGE_DAYS * 24 * 3600
LOG_FORMAT     = "%(asctime)s  [%(levelname)-8s]  %(name)s — %(message)s"
DATE_FORMAT    = "%Y-%m-%d %H:%M:%S"
# ────────────────────────────────────────────────────────────────────


def _get_log_path(name: str) -> Path:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    return LOG_DIR / f"{name}.log"


def _rotate_if_old(log_path: Path) -> None:
    """Delete the log file if it is older than MAX_AGE_DAYS."""
    if not log_path.exists():
        return
    age = time.time() - log_path.stat().st_mtime
    if age > MAX_AGE_SECS:
        log_path.unlink()
        # brief notice to stdout (logger not yet set up)
        print(
            f"[logger] '{log_path.name}' was >{MAX_AGE_DAYS} days old — "
            "deleted and recreated fresh."
        )


def get_logger(name: str, level: int = logging.DEBUG) -> logging.Logger:
    """
    Return a named logger that writes to  logs/<name>.log
    (and to the console).  The file is rotated if older than 2 days.

    Parameters
    ----------
    name  : identifier used for both the logger name and the filename
    level : minimum log level (default: DEBUG)
    """
    log_path = _get_log_path(name)
    _rotate_if_old(log_path)

    logger = logging.getLogger(name)

    # Avoid adding duplicate handlers if called more than once
    if logger.handlers:
        return logger

    logger.setLevel(level)
    formatter = logging.Formatter(LOG_FORMAT, datefmt=DATE_FORMAT)

    # ── File handler ────────────────────────────────────────────────
    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setLevel(level)
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    # ── Console handler ─────────────────────────────────────────────
    ch = logging.StreamHandler()
    ch.setLevel(level)
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    logger.debug(f"Logger initialised — writing to {log_path}")
    return logger


def get_log_info(name: str) -> dict:
    """
    Return metadata about a log file (used by the API health endpoint).
    """
    import datetime
    log_path = _get_log_path(name)
    if not log_path.exists():
        return {"name": name, "exists": False}
    stat = log_path.stat()
    age_hours = (time.time() - stat.st_mtime) / 3600
    return {
        "name":          name,
        "exists":        True,
        "path":          str(log_path),
        "size_kb":       round(stat.st_size / 1024, 2),
        "age_hours":     round(age_hours, 2),
        "will_rotate_in_hours": round(max(0, MAX_AGE_SECS / 3600 - age_hours), 2),
        "last_modified": datetime.datetime.fromtimestamp(stat.st_mtime).strftime(
            "%Y-%m-%d %H:%M:%S"
        ),
    }