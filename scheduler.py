"""
scheduler.py
============

Simple 2-step pipeline:

  1. check_diamond_files()
     Verify the 3 vendor CSVs exist AND were modified within the last 3 days.
     - pc_diamonds.py    -> diamond_files/precious_carbon.csv
     - brilliance        -> diamond_files/brilliance_diamonds.csv
     - loose grown       -> diamond_files/loosegrowndiamond.csv

  2. run_pipeline()
     If all 3 files are fresh, run the comparator (diamond_compare.py) to
     produce compare/*.json, then load the results into the Render
     PostgreSQL database via compare_to_mysql.py.
     (compare_to_mysql.py reads DATABASE_URL from .env / process env.)
"""

from __future__ import annotations

import logging
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

# ── Config ───────────────────────────────────────────────────────────────────
SCRIPT_DIR = Path(__file__).parent.resolve()

# Files we require — must exist AND be no older than MAX_AGE_DAYS
REQUIRED_DIAMOND_FILES = [
    SCRIPT_DIR / "diamond_files" / "precious_carbon.csv",
    SCRIPT_DIR / "diamond_files" / "brilliance_diamonds.csv",
    SCRIPT_DIR / "diamond_files" / "loosegrowndiamond.csv",
]
MAX_AGE_DAYS = 3

# Step scripts
COMPARATOR_SCRIPT = SCRIPT_DIR / "diamond_compare.py"
DB_LOAD_SCRIPT    = SCRIPT_DIR / "compare_to_mysql.py"  # filename kept; now writes to Postgres

STEP_TIMEOUT_SECONDS = 60 * 60 * 4   # 4 h per subprocess


# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(SCRIPT_DIR / "scheduler.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("scheduler")


# ── Step 1: file existence + freshness check ─────────────────────────────────
def check_diamond_files() -> bool:
    """
    Return True only if every file in REQUIRED_DIAMOND_FILES:
      • exists, AND
      • was modified within the last MAX_AGE_DAYS days.
    Otherwise log what's wrong and return False.
    """
    log.info("=" * 72)
    log.info(f"Checking {len(REQUIRED_DIAMOND_FILES)} required diamond file(s)")
    log.info(f"Freshness window: <= {MAX_AGE_DAYS} day(s) old")
    log.info("=" * 72)

    now = datetime.now()
    all_ok = True

    for path in REQUIRED_DIAMOND_FILES:
        if not path.exists():
            log.warning(f"  MISSING  {path}")
            all_ok = False
            continue

        mtime    = datetime.fromtimestamp(path.stat().st_mtime)
        age_days = (now - mtime).total_seconds() / 86400
        size_kb  = path.stat().st_size / 1024

        if age_days <= MAX_AGE_DAYS:
            log.info(
                f"  OK       {path.name}  "
                f"({size_kb:,.1f} KB, age {age_days:.2f}d)"
            )
        else:
            log.warning(
                f"  STALE    {path.name}  "
                f"(age {age_days:.2f}d > {MAX_AGE_DAYS}d) — re-run its scraper"
            )
            all_ok = False

    log.info("-" * 72)
    log.info(f"Files check: {'PASS' if all_ok else 'FAIL'}")
    return all_ok


# ── Step 2: run comparator then load to MySQL ────────────────────────────────
def _run_script(label: str, script_path: Path) -> bool:
    if not script_path.exists():
        log.error(f"[{label}] Script not found: {script_path}")
        return False

    log.info(f"[{label}] Starting {script_path.name} …")
    start = time.time()
    try:
        result = subprocess.run(
            [sys.executable, str(script_path)],
            timeout=STEP_TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired:
        log.error(f"[{label}] TIMEOUT after {STEP_TIMEOUT_SECONDS}s")
        return False
    except Exception as exc:
        log.error(f"[{label}] Error: {exc}")
        return False

    elapsed = round(time.time() - start, 1)
    if result.returncode == 0:
        log.info(f"[{label}] Completed in {elapsed}s")
        return True
    log.error(f"[{label}] Failed (exit code {result.returncode}, {elapsed}s)")
    return False


def run_pipeline() -> None:
    """
    Run the comparator, then load its output into MySQL.
    Called only when check_diamond_files() returned True.
    """
    log.info("=" * 72)
    log.info("PIPELINE STARTED")
    log.info("=" * 72)

    if not _run_script("Comparator", COMPARATOR_SCRIPT):
        log.error("Comparator failed — skipping MySQL load")
        return

    if not _run_script("DB Loader", DB_LOAD_SCRIPT):
        log.error("DB loader failed")
        return

    log.info("=" * 72)
    log.info("PIPELINE COMPLETE")
    log.info("=" * 72)


# ── Entry point ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    if check_diamond_files():
        run_pipeline()
    else:
        log.warning("One or more required CSVs are missing or older than "
                    f"{MAX_AGE_DAYS} days — skipping comparator and MySQL load.")
