"""
scheduler.py
============

Scheduled jobs:

  * 07:00 — loosegrown_diamonds.py if loosegrowndiamond.csv is older than 2 days
  * 12:30 — daily DB pipeline:
      1. If precious_carbon.csv is older than 1 day, delete it and re-scrape
         via pc_diamonds.py --once.
      2. diamond_compare.py to (re)build compare/*.json from the CSVs.
      3. compare_to_mysql.py to drop & rebuild all tables in the Render
         Postgres DB and bulk-insert the new rows.
  * 19:00 — brilliance_diamonds.py if brilliance_diamonds.csv is older than 2 days

Run modes
---------
  python scheduler.py               -> start APScheduler with all jobs
  python scheduler.py --once        -> run the daily DB pipeline once now
  python scheduler.py --loosegrown  -> run the loosegrown scrape job once now
  python scheduler.py --brilliance  -> run the brilliance scrape job once now
"""

from __future__ import annotations

import logging
import platform
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

from apscheduler.schedulers.blocking import BlockingScheduler

# ── Config ───────────────────────────────────────────────────────────────────
SCRIPT_DIR = Path(__file__).parent.resolve()

PC_CSV         = SCRIPT_DIR / "diamond_files" / "precious_carbon.csv"
BRILLIANCE_CSV = SCRIPT_DIR / "diamond_files" / "brilliance_diamonds.csv"
LOOSEGROWN_CSV = SCRIPT_DIR / "diamond_files" / "loosegrowndiamond.csv"

PC_SCRAPER         = SCRIPT_DIR / "pc_diamonds.py"
BRILLIANCE_SCRAPER = SCRIPT_DIR / "brilliance_diamonds.py"
LOOSEGROWN_SCRAPER = SCRIPT_DIR / "loosegrown_diamonds.py"

COMPARATOR_SCRIPT = SCRIPT_DIR / "diamond_compare.py"
DB_LOAD_SCRIPT    = SCRIPT_DIR / "compare_to_mysql.py"  # truncates + reloads Postgres

# Daily DB-reload pipeline (also re-scrapes PC if stale)
SCHEDULE_HOUR   = 16
SCHEDULE_MINUTE = 10

# Standalone scrape jobs
LOOSEGROWN_HOUR   = 7
LOOSEGROWN_MINUTE = 0
BRILLIANCE_HOUR   = 19
BRILLIANCE_MINUTE = 0

# Staleness thresholds — re-scrape when CSV mtime is older than this many days.
PC_STALE_DAYS         = 1
LOOSEGROWN_STALE_DAYS = 2
BRILLIANCE_STALE_DAYS = 2

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


# ── CSV freshness ────────────────────────────────────────────────────────────
def csv_is_fresh(csv_path: Path, max_age_days: float) -> bool:
    """True if csv_path exists AND its mtime is within max_age_days of now."""
    if not csv_path.exists():
        log.warning(f"CSV missing: {csv_path}")
        return False
    mtime = datetime.fromtimestamp(csv_path.stat().st_mtime)
    age_days = (datetime.now() - mtime).total_seconds() / 86400
    if age_days <= max_age_days:
        log.info(f"{csv_path.name} is fresh (age {age_days:.2f}d ≤ {max_age_days}d) — skipping scrape")
        return True
    log.info(f"{csv_path.name} is stale (age {age_days:.2f}d > {max_age_days}d) — re-scrape required")
    return False


# ── VPN cleanup ──────────────────────────────────────────────────────────────
def stop_all_vpns(settle_seconds: int = 5) -> None:
    """Kill any leftover openvpn processes so subsequent network calls go out
    over the real (non-VPN) interface. Render Postgres can reject or drop
    connections from VPN exit IPs, which is why we always run this immediately
    before talking to the database."""
    system = platform.system()
    try:
        if system == "Windows":
            result = subprocess.run(
                ["taskkill", "/F", "/IM", "openvpn.exe"],
                capture_output=True, text=True, timeout=30,
            )
            if result.returncode == 0:
                log.info("[VPN] openvpn.exe terminated")
            elif "not found" in (result.stderr + result.stdout).lower():
                log.info("[VPN] no openvpn.exe running — already clean")
            else:
                log.warning(f"[VPN] taskkill rc={result.returncode}: {result.stderr.strip() or result.stdout.strip()}")
        else:
            result = subprocess.run(
                ["pkill", "-f", "openvpn"],
                capture_output=True, text=True, timeout=30,
            )
            if result.returncode in (0, 1):
                log.info("[VPN] pkill openvpn done" + (" (none running)" if result.returncode == 1 else ""))
            else:
                log.warning(f"[VPN] pkill rc={result.returncode}: {result.stderr.strip()}")
    except FileNotFoundError as exc:
        log.warning(f"[VPN] cleanup tool missing: {exc}")
    except subprocess.TimeoutExpired:
        log.warning("[VPN] cleanup command timed out")
    except Exception as exc:
        log.warning(f"[VPN] cleanup error: {exc}")

    if settle_seconds > 0:
        log.info(f"[VPN] waiting {settle_seconds}s for network to settle…")
        time.sleep(settle_seconds)


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

    if not csv_is_fresh(PC_CSV, PC_STALE_DAYS):
        try:
            PC_CSV.unlink(missing_ok=True)
            log.info(f"Deleted stale PC CSV: {PC_CSV}")
        except Exception as exc:
            log.warning(f"Could not delete PC CSV: {exc}")

        if not _run_script("PC Scraper", PC_SCRAPER, args=["--once"]):
            log.error("pc_diamonds scrape failed — aborting pipeline")
            return
        if not csv_is_fresh(PC_CSV, PC_STALE_DAYS):
            log.error("PC CSV still stale after scrape — aborting pipeline")
            return

    if not csv_is_fresh(LOOSEGROWN_CSV, LOOSEGROWN_STALE_DAYS):
        if not _run_script("Loosegrown Scraper", LOOSEGROWN_SCRAPER):
            log.error("loosegrown_diamonds scrape failed — aborting pipeline")
            return
        stop_all_vpns()
        if not csv_is_fresh(LOOSEGROWN_CSV, LOOSEGROWN_STALE_DAYS):
            log.error("Loosegrown CSV still stale after scrape — aborting pipeline")
            return

    if not csv_is_fresh(BRILLIANCE_CSV, BRILLIANCE_STALE_DAYS):
        if not _run_script("Brilliance Scraper", BRILLIANCE_SCRAPER):
            log.error("brilliance_diamonds scrape failed — aborting pipeline")
            return
        stop_all_vpns()
        if not csv_is_fresh(BRILLIANCE_CSV, BRILLIANCE_STALE_DAYS):
            log.error("Brilliance CSV still stale after scrape — aborting pipeline")
            return

    if not _run_script("Comparator", COMPARATOR_SCRIPT):
        log.error("Comparator failed — skipping DB reload")
        return

    # Ensure no VPN tunnel is up before talking to Render Postgres —
    # the DB rejects/drops connections from VPN exit IPs.
    stop_all_vpns()

    if not _run_script("DB Loader (truncate + reload)", DB_LOAD_SCRIPT):
        log.error("DB loader failed")
        return

    log.info("=" * 72)
    log.info("DAILY JOB COMPLETE")
    log.info("=" * 72)


# ── Standalone scrape jobs ───────────────────────────────────────────────────
def loosegrown_job() -> None:
    log.info("=" * 72)
    log.info(f"LOOSEGROWN JOB STARTED @ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log.info("=" * 72)

    if csv_is_fresh(LOOSEGROWN_CSV, LOOSEGROWN_STALE_DAYS):
        log.info("LOOSEGROWN JOB COMPLETE (no scrape needed)")
        return

    if not _run_script("Loosegrown Scraper", LOOSEGROWN_SCRAPER):
        log.error("loosegrown_diamonds scrape failed")

    # The scraper rotates VPNs internally; make sure we leave none running so
    # the next job (or the daily DB load) starts from a clean network state.
    stop_all_vpns()
    log.info("LOOSEGROWN JOB COMPLETE")


def brilliance_job() -> None:
    log.info("=" * 72)
    log.info(f"BRILLIANCE JOB STARTED @ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log.info("=" * 72)

    if csv_is_fresh(BRILLIANCE_CSV, BRILLIANCE_STALE_DAYS):
        log.info("BRILLIANCE JOB COMPLETE (no scrape needed)")
        return

    if not _run_script("Brilliance Scraper", BRILLIANCE_SCRAPER):
        log.error("brilliance_diamonds scrape failed")

    stop_all_vpns()
    log.info("BRILLIANCE JOB COMPLETE")


# ── Entry point ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    if "--once" in sys.argv:
        daily_job()
    elif "--loosegrown" in sys.argv:
        loosegrown_job()
    elif "--brilliance" in sys.argv:
        brilliance_job()
    else:
        scheduler = BlockingScheduler()
        scheduler.add_job(
            loosegrown_job,
            trigger="cron",
            hour=LOOSEGROWN_HOUR,
            minute=LOOSEGROWN_MINUTE,
            id="loosegrown_scrape",
            misfire_grace_time=60 * 60,
        )
        scheduler.add_job(
            daily_job,
            trigger="cron",
            hour=SCHEDULE_HOUR,
            minute=SCHEDULE_MINUTE,
            id="daily_pipeline",
            misfire_grace_time=60 * 60,  # 1 h grace if host was asleep
        )
        scheduler.add_job(
            brilliance_job,
            trigger="cron",
            hour=BRILLIANCE_HOUR,
            minute=BRILLIANCE_MINUTE,
            id="brilliance_scrape",
            misfire_grace_time=60 * 60,
        )
        log.info(
            f"Scheduler started — loosegrown {LOOSEGROWN_HOUR:02d}:{LOOSEGROWN_MINUTE:02d}, "
            f"daily {SCHEDULE_HOUR:02d}:{SCHEDULE_MINUTE:02d}, "
            f"brilliance {BRILLIANCE_HOUR:02d}:{BRILLIANCE_MINUTE:02d} (local time)"
        )
        try:
            scheduler.start()
        except (KeyboardInterrupt, SystemExit):
            log.info("Scheduler stopped")
