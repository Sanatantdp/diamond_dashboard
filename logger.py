import logging
import os
from logging.handlers import RotatingFileHandler

# ── Config ────────────────────────────────────────────────────
LOGS_DIR     = os.path.join(os.getcwd(), "logs")
MAX_BYTES    = 5 * 1024 * 1024   # 5 MB per log file before rotation
BACKUP_COUNT = 3                  # keep 3 rotated backups (.log.1, .log.2, .log.3)
LOG_LEVEL    = logging.DEBUG

# ── Format: timestamp | level | logger name | message ─────────
FORMATTER = logging.Formatter(
    fmt="%(asctime)s | %(levelname)-8s | %(name)-12s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# ── Known logger names → log filenames ────────────────────────
LOGGER_FILES = {
    "luvansh":    "luvansh.log",
    "loosegrown": "loosegrown.log",
    "pc_diamonds": "pc_diamonds.log",
    "comparator": "comparator.log",
}

_loggers: dict[str, logging.Logger] = {}


def get_logger(name: str) -> logging.Logger:
    """
    Return a named logger that writes to:
      - logs/<name>.log     (rotating file)
      - logs/app.log        (shared rotating file with all logs)
      - stdout console      (INFO and above)

    Calling get_logger() with the same name twice returns the same instance.
    """
    if name in _loggers:
        return _loggers[name]

    os.makedirs(LOGS_DIR, exist_ok=True)

    logger = logging.getLogger(name)
    logger.setLevel(LOG_LEVEL)
    logger.propagate = False  # don't bubble up to root logger

    # ── 1. Dedicated rotating file handler ───────────────────
    log_filename = LOGGER_FILES.get(name, f"{name}.log")
    log_filepath = os.path.join(LOGS_DIR, log_filename)

    file_handler = RotatingFileHandler(
        log_filepath,
        maxBytes=MAX_BYTES,
        backupCount=BACKUP_COUNT,
        encoding="utf-8",
    )
    file_handler.setLevel(LOG_LEVEL)
    file_handler.setFormatter(FORMATTER)
    logger.addHandler(file_handler)

    # ── 2. Shared app.log (all scrapers in one place) ─────────
    app_log_path = os.path.join(LOGS_DIR, "app.log")
    app_handler = RotatingFileHandler(
        app_log_path,
        maxBytes=MAX_BYTES,
        backupCount=BACKUP_COUNT,
        encoding="utf-8",
    )
    app_handler.setLevel(LOG_LEVEL)
    app_handler.setFormatter(FORMATTER)
    logger.addHandler(app_handler)

    # ── 3. Console handler (INFO+) ────────────────────────────
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(FORMATTER)
    logger.addHandler(console_handler)

    _loggers[name] = logger
    logger.debug(f"Logger '{name}' initialised → {log_filepath}")
    return logger