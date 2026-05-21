import csv
import socket
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

# ── Render auto-deploy (git push) config ──────────────────────────────────────
# After the comparator finishes and the compare files are validated/uploaded,
# commit them to git and push to origin. Render auto-deploys on push, so the
# dashboard at https://<your-render-service>.onrender.com picks up the new data
# without any manual intervention.
GIT_PUSH_ENABLED  = True
GIT_REMOTE        = "origin"
GIT_BRANCH        = "main"
# Explicit paths — the global .gitignore (*.json*) excludes them otherwise.
# These 5 are already tracked (force-added previously), so plain `git add` works.
GIT_COMMIT_FILES = [
    "compare/compare.json",
    "compare/compare_partial.json",
    "compare/compare_all.json",
    "compare/compare_br_vs_lg.json",
    "compare/diamond_status.json",
]

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
        # Route through run_with_vpn.py — picks a config from vpnbook/brilliance/
        # (pre-vetted by test_vpn_scrapers.py) and auto-rotates on failure.
        "vpn_launcher_key": "brilliance",
    },
    {
        "label":           "Loosegrown Scraper",
        "script":          SCRIPT_DIR / "loosegrown_diamonds.py",
        "primary_csv":     SCRIPT_DIR / "loosegrown_diamonds" / "loosegrown_diamonds.csv",
        "folders":         [SCRIPT_DIR / "loosegrown"],
        "stale_days":      2,      # stale if older than 2 days
        "delete_csv_only": True,   # delete CSV only, keep folder
        # Route through run_with_vpn.py — picks a config from vpnbook/loosegrown/
        # (pre-vetted by test_vpn_scrapers.py) and auto-rotates on failure.
        "vpn_launcher_key": "loosegrown",
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
# Git commit + push (Render auto-deploys on push to GIT_BRANCH)
# ══════════════════════════════════════════════════════════════════════════════

def _run_git(args: list[str]) -> tuple[int, str, str]:
    """Run a git command, return (returncode, stdout, stderr)."""
    proc = subprocess.run(
        ["git", *args],
        cwd=str(SCRIPT_DIR),
        capture_output=True,
        text=True,
        timeout=120,
    )
    return proc.returncode, (proc.stdout or "").strip(), (proc.stderr or "").strip()


def git_commit_and_push(log) -> dict:
    """
    Stage the compare files, create a commit, push to GIT_REMOTE/GIT_BRANCH.

    Returns a small dict describing the outcome — never raises, so a git
    failure (auth expired, network down, etc.) is logged but does not break
    the pipeline. The next scheduled run will retry.
    """
    result = {"status": "skipped", "detail": ""}
    if not GIT_PUSH_ENABLED:
        log.info("  [git] GIT_PUSH_ENABLED=False — skipping commit/push")
        return result

    # Only stage files that actually exist (compare may have run for a subset).
    files_to_add = [p for p in GIT_COMMIT_FILES if (SCRIPT_DIR / p).exists()]
    if not files_to_add:
        log.info("  [git] No compare files exist on disk — nothing to commit")
        return result

    log.info(f"  [git] Staging {len(files_to_add)} compare file(s)")
    rc, _, err = _run_git(["add", "--", *files_to_add])
    if rc != 0:
        log.error(f"  [git]  git add failed: {err}")
        return {"status": "error", "detail": f"git add: {err}"}

    # Anything actually staged?
    rc, _, _ = _run_git(["diff", "--cached", "--quiet"])
    if rc == 0:
        log.info("  [git] Compare files unchanged since last commit — nothing to push")
        return {"status": "no_changes", "detail": ""}

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M")
    msg = f"auto: update compare files {timestamp}"
    log.info(f"  [git] Committing: {msg!r}")
    rc, _, err = _run_git(["commit", "-m", msg])
    if rc != 0:
        log.error(f"  [git]  git commit failed: {err}")
        return {"status": "error", "detail": f"git commit: {err}"}

    log.info(f"  [git] Pushing {GIT_BRANCH} -> {GIT_REMOTE}…")
    rc, out, err = _run_git(["push", GIT_REMOTE, GIT_BRANCH])
    if rc != 0:
        log.error(f"  [git]  git push failed: {err or out}")
        return {"status": "error", "detail": f"git push: {err or out}"}

    log.info(f"  [git]  ✅ Pushed — Render will redeploy automatically")
    return {"status": "pushed", "detail": msg}


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

    # Route through run_with_vpn.py if the entry asks for VPN — otherwise
    # invoke the scraper script directly.
    vpn_key = entry.get("vpn_launcher_key")
    if vpn_key:
        launcher = SCRIPT_DIR / "run_with_vpn.py"
        cmd = [
            sys.executable, str(launcher),
            "--scraper", vpn_key,
            "--rotate-on-fail",
            "--max-attempts", "5",
        ]
        log.info(f"[{label}] ▶ Starting via VPN launcher → {' '.join(cmd[1:])}")
    else:
        cmd = [sys.executable, str(script)]
        log.info(f"[{label}] ▶ Starting scraper (no VPN)...")

    start = time.time()

    try:
        result = subprocess.run(
            cmd,
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

            # Push compare files to GitHub → triggers Render auto-deploy.
            if kept_paths:
                log.info(f"  [git] {len(kept_paths)} valid compare file(s) ready to publish")
            git_result = git_commit_and_push(log)
            if git_result["status"] == "pushed":
                results[label] += " | Render: pushed"
            elif git_result["status"] == "no_changes":
                results[label] += " | Render: no changes"
            elif git_result["status"] == "error":
                results[label] += f" | Render: ❌ {git_result['detail'][:60]}"
            else:
                results[label] += " | Render: skipped"

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
    startup_log.info(f"Publish      : git push {GIT_REMOTE} {GIT_BRANCH} (Render auto-deploys)")
    startup_log.info(f"Push enabled : {GIT_PUSH_ENABLED}")
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