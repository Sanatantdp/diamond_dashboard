import csv
import socket
import paramiko
import logging
import shutil
import subprocess
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

now = datetime.now()
next_run = now + timedelta(minutes=1)

# ── Config ────────────────────────────────────────────────────────────────────
RUN_HOUR = next_run.hour
RUN_MINUTE = next_run.minute
INTERVAL_DAYS           = 3
STALE_THRESHOLD_DAYS    = 3
SCRIPT_DIR              = Path(__file__).parent.resolve()
LOGS_DIR                = SCRIPT_DIR / "logs"
TIMEOUT_SECONDS         = 60 * 60 * 4
BLOCK_WAIT_MINUTES      = 10
INTERNET_CHECK_INTERVAL = BLOCK_WAIT_MINUTES * 60  # 600s

# ── Compare file protection config ───────────────────────────────────────────
COMPARE_DIR        = SCRIPT_DIR / "compare"
COMPARE_BACKUP_DIR = SCRIPT_DIR / "compare_backup"

COMPARE_FILES = [
    COMPARE_DIR / "compare.json",
    COMPARE_DIR / "compare.csv",
    COMPARE_DIR / "compare_partial.json",
    COMPARE_DIR / "compare_all.json",
    COMPARE_DIR / "compare_br_vs_lg.json",
]

# ── SFTP config ───────────────────────────────────────────────────────────────
SFTP_ENABLED    = True
SFTP_HOST       = "122.176.221.68"
SFTP_PORT       = 22
SFTP_USER       = "qnapadmin"
SFTP_PASSWORD   = "Passw0rd@123$"
SFTP_REMOTE_DIR = "/share/CACHEDEV1_DATA/Web/usa/compare"
LOCAL_FOLDER    = COMPARE_DIR

# ── Script definitions ────────────────────────────────────────────────────────
def _find_script(*candidates) -> Path:
    for p in candidates:
        if Path(p).exists():
            return Path(p)
    return Path(candidates[-1])


SCRIPTS = [
    {
        "label":           "PreciousCarbon Scraper",
        "script":          _find_script(SCRIPT_DIR / "pc_diamonds.py"),
        "primary_csv":     SCRIPT_DIR / "diamond_files" / "precious_carbon.csv",
        "folders":         [SCRIPT_DIR / "diamond_files"],
        "max_diamonds":    400_000,
        "resume_divisor":  48,
        "stale_days":      1,      # daily — re-scrape if older than 24h
        "delete_csv_only": True,   # delete CSV only, keep diamond_files/ folder
    },
    {
        "label":           "Brilliance Diamond Scraper",
        "script":          _find_script(SCRIPT_DIR / "brilliance_diamonds.py"),
        "primary_csv":     SCRIPT_DIR / "brilliance_diamonds.csv",
        "folders":         [SCRIPT_DIR / "brilliance"],
        "max_diamonds":    400_000,
        "resume_divisor":  40,
        "stale_days":      2,      # stale if older than 2 days
        "delete_csv_only": True,   # delete CSV only, keep folder
    },
    {
        "label":           "Loosegrown Scraper",
        "script":          SCRIPT_DIR / "loosegrown_diamonds.py",
        "primary_csv":     SCRIPT_DIR / "loosegrown_diamonds" / "loosegrown_diamonds.csv",
        "folders":         [SCRIPT_DIR / "loosegrown"],
        "stale_days":      2,      # stale if older than 2 days
        "delete_csv_only": True,   # delete CSV only, keep folder
    },
    {
        "label":         "Price Comparator",
        "script":        SCRIPT_DIR / "diamond_compare.py",
        "primary_csv":   None,
        "folders":       [],
        "is_comparator": True,
    },
]


# ── Per-run logging setup ─────────────────────────────────────────────────────
def setup_run_logger() -> tuple[logging.Logger, Path]:
    """
    Create a fresh logger for this pipeline run.
    Log file: logs/run_YYYYMMDD_HHMMSS.log
    Also streams to stdout so Windows Task Scheduler captures it too.
    """
    LOGS_DIR.mkdir(exist_ok=True)
    run_id   = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = LOGS_DIR / f"run_{run_id}.log"

    logger = logging.getLogger(f"scheduler.{run_id}")
    logger.setLevel(logging.INFO)
    logger.propagate = False  # don't bubble up to root logger

    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    logger.addHandler(sh)

    return logger, log_file


# ── Startup logger (scheduler lifecycle, not per-run) ─────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(SCRIPT_DIR / "scheduler.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
startup_log = logging.getLogger("scheduler")


# ── Helpers ───────────────────────────────────────────────────────────────────
def file_age_days(path: Path):
    if not path.exists():
        return None
    elapsed = (datetime.now() - datetime.fromtimestamp(path.stat().st_mtime)).total_seconds()
    return elapsed / 86400


def is_stale(primary_csv: Path, log, threshold_days: float = STALE_THRESHOLD_DAYS) -> bool:
    age = file_age_days(primary_csv)
    if age is None:
        log.info(f"  📭 {primary_csv.name} — MISSING → will scrape")
        return True
    if age >= threshold_days:
        log.info(f"  ⏰ {primary_csv.name} — {age:.2f}d old → stale (threshold: {threshold_days}d), re-scraping")
        return True
    log.info(f"  ✅ {primary_csv.name} — {age:.2f}d old → fresh, skipping")
    return False


def delete_folders(folders: list, log):
    for folder in folders:
        if folder.exists():
            try:
                shutil.rmtree(folder)
                log.info(f"  🗑 Deleted folder: {folder}")
            except Exception as e:
                log.warning(f"  ⚠️ Could not delete {folder}: {e}")


def count_existing_diamonds(csv_path: Path) -> int:
    if not csv_path.exists():
        return 0
    try:
        with open(csv_path, newline="", encoding="utf-8") as f:
            return sum(1 for _ in csv.DictReader(f))
    except Exception:
        return 0


def _file_row_count(path: Path, log) -> int:
    if not path.exists():
        return 0
    try:
        if path.suffix == ".csv":
            with open(path, newline="", encoding="utf-8") as f:
                return sum(1 for _ in csv.DictReader(f))
        elif path.suffix == ".json":
            import json
            raw = path.read_text(encoding="utf-8").strip()
            if not raw:
                return 0
            data = json.loads(raw)
            return len(data) if isinstance(data, list) else 0
    except Exception as e:
        log.warning(f"  ⚠️ Could not count rows in {path.name}: {e}")
    return 0


# ══════════════════════════════════════════════════════════════════════════════
# Internet check — block pipeline until online
# ══════════════════════════════════════════════════════════════════════════════

def is_internet_available() -> bool:
    """Check internet using multiple methods — returns True if any succeed."""
    try:
        socket.setdefaulttimeout(5)
        conn = socket.create_connection(("8.8.8.8", 53))
        conn.close()
        return True
    except OSError:
        pass
    try:
        socket.setdefaulttimeout(5)
        conn = socket.create_connection(("1.1.1.1", 53))
        conn.close()
        return True
    except OSError:
        pass
    try:
        import urllib.request
        urllib.request.urlopen("http://www.google.com", timeout=5)
        return True
    except Exception:
        pass
    return False


def wait_for_internet(log, check_interval: int = INTERNET_CHECK_INTERVAL) -> None:
    """Block until internet is available, checking every check_interval seconds."""
    result = is_internet_available()
    log.info(f"🌐 Internet check — {'✅ Online' if result else '❌ Offline'}")
    if result:
        return
    log.warning("⚠️  No internet connection detected — pipeline paused.")
    attempt = 1
    while True:
        log.info(f"  🔄 Attempt {attempt}: Rechecking in {check_interval // 60} min...")
        time.sleep(check_interval)
        result = is_internet_available()
        if result:
            log.info(f"✅ Internet restored (attempt {attempt}) — resuming.")
            return
        log.warning(f"  ❌ Still offline (attempt {attempt}) — waiting again...")
        attempt += 1


# ══════════════════════════════════════════════════════════════════════════════
# Compare file validation — backup → run → validate → restore if empty
# ══════════════════════════════════════════════════════════════════════════════

def backup_compare_files(log):
    if not COMPARE_DIR.exists():
        log.info("  [compare] No compare/ directory yet — nothing to back up")
        return
    COMPARE_BACKUP_DIR.mkdir(exist_ok=True)
    backed_up = 0
    for src in COMPARE_FILES:
        if src.exists():
            dst = COMPARE_BACKUP_DIR / src.name
            shutil.copy2(src, dst)
            log.info(f"  [compare] 💾 Backed up: {src.name} ({src.stat().st_size / 1024:.1f} KB)")
            backed_up += 1
    if backed_up == 0:
        log.info("  [compare] No existing compare files to back up")
    else:
        log.info(f"  [compare] {backed_up} file(s) backed up → {COMPARE_BACKUP_DIR}")


def validate_and_protect_compare_files(log) -> dict:
    log.info("  [compare] Validating compare output files...")
    summary = {}
    for target in COMPARE_FILES:
        fname     = target.name
        backup    = COMPARE_BACKUP_DIR / fname
        row_count = _file_row_count(target, log)
        if row_count > 0:
            log.info(f"  [compare]  ✅ KEEP    {fname}  ({row_count:,} rows)")
            summary[fname] = "kept"
        else:
            if backup.exists():
                backup_rows = _file_row_count(backup, log)
                shutil.copy2(backup, target)
                log.warning(
                    f"  [compare] ♻️  RESTORE {fname}  "
                    f"(new: {row_count} rows → restored backup: {backup_rows:,} rows)"
                )
                summary[fname] = "restored"
            else:
                status = "empty" if target.exists() else "missing"
                log.warning(f"  [compare] ⚠️  {fname} is {status} — no backup available")
                summary[fname] = "no_backup"
    return summary


def cleanup_compare_backup(log):
    if COMPARE_BACKUP_DIR.exists():
        try:
            shutil.rmtree(COMPARE_BACKUP_DIR)
            log.info("  [compare] 🧹 Backup dir cleaned up")
        except Exception as e:
            log.warning(f"  [compare] Could not clean up backup dir: {e}")


# ══════════════════════════════════════════════════════════════════════════════
# SFTP upload
# ══════════════════════════════════════════════════════════════════════════════

def _sftp_mkdir_recursive(sftp: paramiko.SFTPClient, remote_path: str, log):
    parts = remote_path.strip("/").split("/")
    current = ""
    for part in parts:
        current += "/" + part
        try:
            sftp.mkdir(current)
            log.info(f"  [sftp] Created remote dir: {current}")
        except IOError:
            pass


def upload_folder(valid_files: list[Path], log) -> dict:
    if not SFTP_ENABLED:
        log.info("  [sftp] SFTP_ENABLED=False — skipping upload")
        return {}
    if not valid_files:
        log.info("  [sftp] No valid files to upload")
        return {}

    log.info(f"  [sftp] Connecting to {SFTP_HOST}:{SFTP_PORT} as {SFTP_USER}...")
    result = {}
    ssh  = None
    sftp = None

    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(
            hostname=SFTP_HOST,
            port=SFTP_PORT,
            username=SFTP_USER,
            password=SFTP_PASSWORD,
            timeout=15,
        )
        sftp = ssh.open_sftp()
        log.info(f"  [sftp] ✅ Connected — logged in as {SFTP_USER}")

        try:
            sftp.chdir(SFTP_REMOTE_DIR)
            log.info(f"  [sftp] Remote dir ready: {SFTP_REMOTE_DIR}")
        except IOError:
            log.warning(f"  [sftp] Remote dir not found — creating: {SFTP_REMOTE_DIR}")
            _sftp_mkdir_recursive(sftp, SFTP_REMOTE_DIR, log)
            sftp.chdir(SFTP_REMOTE_DIR)

        log.info(f"  [sftp] Uploading {len(valid_files)} file(s) → {SFTP_REMOTE_DIR}")
        for local_path in valid_files:
            fname       = local_path.name
            remote_path = SFTP_REMOTE_DIR.rstrip("/") + "/" + fname
            size_kb     = local_path.stat().st_size / 1024
            try:
                sftp.put(str(local_path), remote_path)
                log.info(f"  [sftp]  ✅ Uploaded: {fname}  ({size_kb:.1f} KB)")
                result[fname] = "uploaded"
            except Exception as e:
                log.error(f"  [sftp]  ❌ Failed:   {fname}  — {e}")
                result[fname] = "failed"

    except paramiko.AuthenticationException:
        log.error(f"  [sftp] ❌ Authentication failed — check {SFTP_USER} / password")
        for p in valid_files:
            result.setdefault(p.name, "failed")
    except paramiko.SSHException as e:
        log.error(f"  [sftp] ❌ SSH error: {e}")
        for p in valid_files:
            result.setdefault(p.name, "failed")
    except Exception as e:
        log.error(f"  [sftp] ❌ Connection error: {e}")
        for p in valid_files:
            result.setdefault(p.name, "failed")
    finally:
        if sftp:
            sftp.close()
        if ssh:
            ssh.close()
            log.info("  [sftp] Connection closed.")

    return result


# ── Run a single script ───────────────────────────────────────────────────────
def run_script(entry: dict, log) -> bool:
    label        = entry["label"]
    script       = entry["script"]
    primary_csv  = entry.get("primary_csv")
    max_diamonds = entry.get("max_diamonds")

    if not script.exists():
        log.error(f"[{label}] Script not found: {script}")
        return False

    if max_diamonds and primary_csv:
        existing = count_existing_diamonds(primary_csv)
        log.info(f"[{label}] Existing diamonds: {existing:,}")
        if existing > max_diamonds:
            log.info(f"[{label}]  Skipping scrape (exceeds {max_diamonds:,} limit)")
            return True
        resume_page = (existing // entry.get("resume_divisor", 40)) + 1
        log.info(f"[{label}] Smart resume → approx from page {resume_page}")

    log.info(f"[{label}] ▶ Starting scraper...")
    start = time.time()

    try:
        result = subprocess.run(
            [sys.executable, str(script)],
            capture_output=False,
            timeout=TIMEOUT_SECONDS,
        )
        elapsed = round(time.time() - start, 1)
        if result.returncode == 0:
            log.info(f"[{label}] ✅ Completed in {elapsed}s")
            return True
        else:
            log.error(f"[{label}] ❌ Failed (exit code {result.returncode})")
            return False

    except subprocess.TimeoutExpired:
        log.error(f"[{label}] ⏰ TIMEOUT — possible block detected")
        log.info(f"Waiting {BLOCK_WAIT_MINUTES} minutes before next task...")
        time.sleep(BLOCK_WAIT_MINUTES * 60)
        return False

    except Exception as exc:
        log.error(f"[{label}] 💥 Error: {exc}")
        return False


# ── Pipeline ──────────────────────────────────────────────────────────────────
def run_pipeline():
    log, log_file = setup_run_logger()

    log.info("=" * 75)
    log.info("🚀 DIAMOND SCRAPING PIPELINE STARTED")
    log.info(f"📄 Log file: {log_file}")
    log.info("=" * 75)

    wait_for_internet(log)

    results = {}

    for s in SCRIPTS:
        label         = s["label"]
        is_comparator = s.get("is_comparator", False)
        log.info(f"── {label} ──")

        if is_comparator:
            backup_compare_files(log)
            ok = run_script(s, log)
            results[label] = "✅ Done" if ok else "❌ Failed"

            validation = validate_and_protect_compare_files(log)
            restored = [f for f, v in validation.items() if v == "restored"]
            kept     = [f for f, v in validation.items() if v == "kept"]

            if restored:
                log.warning(
                    f"[{label}] ⚠️  {len(restored)} file(s) restored from backup "
                    f"(empty output): {', '.join(restored)}"
                )
                results[label] += f" | ⚠️ {len(restored)} file(s) restored"
            else:
                log.info(f"[{label}] ✅ All {len(kept)} compare file(s) valid — keeping new output")
                cleanup_compare_backup(log)

            kept_paths = [
                COMPARE_DIR / fname
                for fname, status in validation.items()
                if status == "kept"
            ]

            if kept_paths:
                log.info(f"  [sftp] {len(kept_paths)} valid file(s) ready for upload")
                sftp_result = upload_folder(kept_paths, log)
                uploaded = sum(1 for v in sftp_result.values() if v == "uploaded")
                failed   = sum(1 for v in sftp_result.values() if v == "failed")
                if failed:
                    log.warning(f"  [sftp] {uploaded} uploaded, {failed} failed")
                    results[label] += f" | SFTP: {uploaded} ✅ {failed} ❌"
                else:
                    log.info(f"  [sftp] All {uploaded} file(s) uploaded successfully")
                    results[label] += f" | SFTP: {uploaded} uploaded"
            else:
                log.info("  [sftp] No newly valid files — skipping upload")

        elif s["primary_csv"] is not None:
            stale_days      = s.get("stale_days", STALE_THRESHOLD_DAYS)
            delete_csv_only = s.get("delete_csv_only", False)

            if is_stale(s["primary_csv"], log, threshold_days=stale_days):
                if delete_csv_only:
                    # Only delete the primary CSV — preserve the rest of the folder
                    csv_path = s["primary_csv"]
                    if csv_path.exists():
                        try:
                            csv_path.unlink()
                            log.info(f"  🗑 Deleted CSV only: {csv_path.name}")
                        except Exception as e:
                            log.warning(f"  ⚠️ Could not delete {csv_path.name}: {e}")
                    else:
                        log.info(f"  📭 {csv_path.name} — already missing")
                else:
                    delete_folders(s["folders"], log)

                ok = run_script(s, log)
                results[label] = "✅ Done" if ok else "❌ Failed"
            else:
                results[label] = "⏭ Skipped (fresh)"

        else:
            ok = run_script(s, log)
            results[label] = "✅ Done" if ok else "❌ Failed"

    log.info("=" * 75)
    log.info("PIPELINE SUMMARY")
    for label, status in results.items():
        log.info(f"  {status:>12} → {label}")
    log.info(f"📄 Full log saved → {log_file}")
    log.info("=" * 75)

    # Close handlers so file is released (important on Windows)
    for handler in log.handlers[:]:
        handler.close()
        log.removeHandler(handler)

    startup_log.info(f"✅ Pipeline run complete — log: {log_file.name}")


# ── Scheduler ─────────────────────────────────────────────────────────────────
def next_run_time(from_dt: datetime) -> datetime:
    candidate = from_dt.replace(hour=RUN_HOUR, minute=RUN_MINUTE, second=0, microsecond=0)
    if candidate <= from_dt:
        candidate += timedelta(days=1)
    return candidate


def main():
    startup_log.info("Diamond Scheduler Started")
    startup_log.info(f"Schedule     : Every {INTERVAL_DAYS} days at {RUN_HOUR:02d}:{RUN_MINUTE:02d}")
    startup_log.info(f"SFTP host    : {SFTP_HOST}:{SFTP_PORT}")
    startup_log.info(f"SFTP remote  : {SFTP_REMOTE_DIR}")
    startup_log.info(f"Block wait   : {BLOCK_WAIT_MINUTES} min on timeout")
    startup_log.info(f"Run logs dir : {LOGS_DIR}")
    startup_log.info(f"Scraper thresholds:")
    for s in SCRIPTS:
        if s.get("primary_csv") is not None:
            mode = "CSV only" if s.get("delete_csv_only") else "full folder"
            startup_log.info(
                f"  {s['label']:<30} stale_days={s.get('stale_days', STALE_THRESHOLD_DAYS)}  delete={mode}"
            )

    next_run = next_run_time(datetime.now())

    while True:
        wait = max(0, (next_run - datetime.now()).total_seconds())
        startup_log.info(f"Sleeping {round(wait/3600, 2)} hours until next run...")
        time.sleep(wait)

        run_pipeline()

        next_run = (next_run + timedelta(days=INTERVAL_DAYS)).replace(
            hour=RUN_HOUR, minute=RUN_MINUTE, second=0, microsecond=0
        )


if __name__ == "__main__":
    main()