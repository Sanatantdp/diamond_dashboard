import logging
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

# ── Config ────────────────────────────────────────────────────────────────────
SCRIPT_DIR      = Path(__file__).parent.resolve()
LOG_FILE        = SCRIPT_DIR / "scheduler.log"
TIMEOUT_SECONDS = 60 * 60 * 3

# ── Git Config ────────────────────────────────────────────────────────────────
GIT_ENABLED   = True
GIT_REMOTE    = "origin"
GIT_BRANCH    = ""   # empty → auto-detect current branch
GIT_ADD_PATHS = ["."]
GIT_TIMEOUT   = 300

# ── Required diamond files ────────────────────────────────────────────────────
REQUIRED_FILES = [
    SCRIPT_DIR / "diamond_files" / "rouk_diamonds.csv",
    SCRIPT_DIR / "diamond_heaven" / "diamond_heaven.csv",
    SCRIPT_DIR / "abelini" / "abelini.csv",
]

COMPARATOR_SCRIPT = SCRIPT_DIR / "diamond_compare.py"

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("scheduler")

# ── File checks ───────────────────────────────────────────────────────────────

def check_required_files() -> bool:
    log.info("Checking required diamond files...")
    missing = []
    for f in REQUIRED_FILES:
        if f.exists():
            log.info(f"  ✓ {f}")
        else:
            log.error(f"  ✗ MISSING: {f}")
            missing.append(f)
    if missing:
        log.error(f"{len(missing)} file(s) missing — cannot run comparator.")
        return False
    log.info("All required diamond files present.")
    return True

# ── Script runner ─────────────────────────────────────────────────────────────

def run_script(label: str, script: Path) -> bool:
    if not script.exists():
        log.error(f"[{label}] Script not found: {script}")
        return False
    log.info(f"[{label}] ▶ Starting → {script.name}")
    start = time.time()
    try:
        result = subprocess.run(
            [sys.executable, str(script)],
            capture_output=False,
            timeout=TIMEOUT_SECONDS,
        )
        elapsed = round(time.time() - start, 1)
        if result.returncode == 0:
            log.info(f"[{label}] ✅ Done in {elapsed}s")
            return True
        else:
            log.error(f"[{label}] ❌ Exit code {result.returncode} after {elapsed}s")
            return False
    except subprocess.TimeoutExpired:
        log.error(f"[{label}] ⏰ Timed out after {TIMEOUT_SECONDS}s")
        return False
    except Exception as exc:
        log.error(f"[{label}] 💥 Error: {exc}")
        return False

# ── Git auto commit & push ────────────────────────────────────────────────────

def _git(args: list, check: bool = False) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["git", *args],
        cwd=str(SCRIPT_DIR),
        capture_output=True,
        text=True,
        timeout=GIT_TIMEOUT,
        check=check,
    )


def git_commit_and_push() -> None:
    if not GIT_ENABLED:
        return

    log.info("=" * 60)
    log.info("GIT: auto commit & push")

    try:
        inside = _git(["rev-parse", "--is-inside-work-tree"])
        if inside.returncode != 0 or inside.stdout.strip() != "true":
            log.warning("  ⚠️  Not a git repository — skipping git step.")
            log.info("=" * 60)
            return

        branch = GIT_BRANCH or _git(["rev-parse", "--abbrev-ref", "HEAD"]).stdout.strip()
        if not branch or branch == "HEAD":
            log.warning("  ⚠️  Could not resolve current branch — skipping.")
            log.info("=" * 60)
            return

        add_result = _git(["add", "--", *GIT_ADD_PATHS])
        if add_result.returncode != 0:
            log.error(f"  ❌ git add failed: {add_result.stderr.strip()}")
            log.info("=" * 60)
            return

        status = _git(["status", "--porcelain"])
        if not status.stdout.strip():
            log.info("  ℹ️  No changes to commit.")
            log.info("=" * 60)
            return

        message = f"Auto: compare run {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        commit_result = _git(["commit", "-m", message])
        if commit_result.returncode != 0:
            log.error(f"  ❌ git commit failed: {commit_result.stderr.strip() or commit_result.stdout.strip()}")
            log.info("=" * 60)
            return
        log.info(f"  ✓ Committed: {message}")

        push_result = _git(["push", GIT_REMOTE, branch])
        if push_result.returncode != 0:
            log.error(f"  ❌ git push failed: {push_result.stderr.strip() or push_result.stdout.strip()}")
        else:
            log.info(f"  ✓ Pushed to {GIT_REMOTE}/{branch}")

    except subprocess.TimeoutExpired:
        log.error(f"  ⏰ git step timed out after {GIT_TIMEOUT}s")
    except FileNotFoundError:
        log.error("  ❌ git executable not found on PATH")
    except Exception as exc:
        log.error(f"  💥 git step error: {exc}")
    finally:
        log.info("=" * 60)

# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    log.info("=" * 60)
    log.info(f"START — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log.info(f"  Log file : {LOG_FILE}")
    log.info("=" * 60)

    if not check_required_files():
        sys.exit(1)

    ok = run_script("Price comparator", COMPARATOR_SCRIPT)
    if not ok:
        log.error("Comparator failed — skipping git step.")
        sys.exit(1)

    git_commit_and_push()


if __name__ == "__main__":
    main()
