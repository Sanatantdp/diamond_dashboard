"""
scheduler.py
============

Daily 2 AM pipeline:

  1. Check precious_carbon.csv — if its modified date equals today, it is
     current and we skip the scrape. Otherwise, run pc_diamonds.py --once
     to download a fresh CSV.
  2. Run diamond_compare.py to (re)build compare/*.json from the CSVs.
  3. Run compare_to_mysql.py to drop & rebuild all tables in the Render
     PostgreSQL DB and bulk-insert the new rows.

Run modes
---------
  python scheduler.py           -> start APScheduler, fire daily at 02:00
  python scheduler.py --once    -> run the pipeline once now (no scheduling)
"""

from __future__ import annotations

import logging
import subprocess
import sys
import time
from datetime import date, datetime
from pathlib import Path

from apscheduler.schedulers.blocking import BlockingScheduler

# ── Config ───────────────────────────────────────────────────────────────────
SCRIPT_DIR = Path(__file__).parent.resolve()

PC_CSV            = SCRIPT_DIR / "diamond_files" / "precious_carbon.csv"
PC_SCRAPER        = SCRIPT_DIR / "pc_diamonds.py"
COMPARATOR_SCRIPT = SCRIPT_DIR / "diamond_compare.py"
DB_LOAD_SCRIPT    = SCRIPT_DIR / "compare_to_mysql.py"  # truncates + reloads Postgres

SCHEDULE_HOUR        = 12
SCHEDULE_MINUTE      = 0
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


# ── PC CSV freshness ─────────────────────────────────────────────────────────
def pc_csv_is_current() -> bool:
    """True if precious_carbon.csv exists AND its mtime is today's date."""
    if not PC_CSV.exists():
        log.warning(f"PC CSV missing: {PC_CSV}")
        return False
    mtime_date = datetime.fromtimestamp(PC_CSV.stat().st_mtime).date()
    today = date.today()
    if mtime_date == today:
        log.info(f"PC CSV is current (mtime: {mtime_date}) — OK, skipping scrape")
        return True
    log.info(f"PC CSV is stale (mtime: {mtime_date}, today: {today}) — re-scrape required")
    return False


# ── Subprocess runner ────────────────────────────────────────────────────────
def _run_script(label: str, script_path: Path, args: list[str] | None = None) -> bool:
    if not script_path.exists():
        log.error(f"[{label}] Script not found: {script_path}")
        return False

    cmd = [sys.executable, str(script_path), *(args or [])]
    log.info(f"[{label}] Starting: {' '.join(cmd)}")
    start = time.time()
    try:
        result = subprocess.run(cmd, timeout=STEP_TIMEOUT_SECONDS)
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


# ── Daily pipeline ───────────────────────────────────────────────────────────
def daily_job() -> None:
    log.info("=" * 72)
    log.info(f"DAILY JOB STARTED @ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log.info("=" * 72)

    if not pc_csv_is_current():
        if not _run_script("PC Scraper", PC_SCRAPER, args=["--once"]):
            log.error("pc_diamonds scrape failed — aborting pipeline")
            return
        if not pc_csv_is_current():
            log.error("PC CSV still not current after scrape — aborting pipeline")
            return

    if not _run_script("Comparator", COMPARATOR_SCRIPT):
        log.error("Comparator failed — skipping DB reload")
        return

    if not _run_script("DB Loader (truncate + reload)", DB_LOAD_SCRIPT):
        log.error("DB loader failed")
        return

    log.info("=" * 72)
    log.info("DAILY JOB COMPLETE")
    log.info("=" * 72)


# ── Entry point ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    if "--once" in sys.argv:
        daily_job()
    else:
        scheduler = BlockingScheduler()
        scheduler.add_job(
            daily_job,
            trigger="cron",
            hour=SCHEDULE_HOUR,
            minute=SCHEDULE_MINUTE,
            id="daily_pipeline",
            misfire_grace_time=60 * 60,  # 1 h grace if host was asleep
        )
        log.info(
            f"Scheduler started — daily job at "
            f"{SCHEDULE_HOUR:02d}:{SCHEDULE_MINUTE:02d} (local time)"
        )
        try:
            scheduler.start()
        except (KeyboardInterrupt, SystemExit):
            log.info("Scheduler stopped")
