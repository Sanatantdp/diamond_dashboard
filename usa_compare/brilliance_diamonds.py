from curl_cffi import requests
from fake_useragent import UserAgent
import time
import random
import csv
import os
import json
import math
import datetime
import statistics
import sys
import ctypes
import threading
import tempfile
import shutil
import glob
from concurrent.futures import ThreadPoolExecutor
from logger import get_logger

# ── Windows temp fix — redirect %TEMP% away from any RAMDisk ─────────────────
# curl_cffi writes TLS/socket temp files to %TEMP%. If %TEMP% points to a
# RAMDisk (common on Windows), it fills up and causes "insufficient storage".
# Force a real-disk temp folder before any curl_cffi import side-effects run.
if sys.platform == "win32":
    _real_temp = os.path.join(os.environ.get("LOCALAPPDATA", "C:\\Temp"), "curl_tmp")
    os.makedirs(_real_temp, exist_ok=True)
    os.environ["TEMP"] = _real_temp
    os.environ["TMP"]  = _real_temp
    tempfile.tempdir    = _real_temp

def _cleanup_curl_tmp():
    """Delete stale curl_cffi temp files from our dedicated Windows temp folder."""
    try:
        _tmp = os.environ.get("TEMP", "")
        if not _tmp or not os.path.isdir(_tmp):
            return
        deleted = 0
        for f in glob.glob(os.path.join(_tmp, "*")):
            try:
                if os.path.isfile(f):
                    os.remove(f)
                elif os.path.isdir(f):
                    shutil.rmtree(f, ignore_errors=True)
                deleted += 1
            except Exception:
                pass
        if deleted:
            log.debug(f"Cleaned {deleted} stale temp file(s) from {_tmp}")
    except Exception:
        pass

log = get_logger("brilliance")
ua = UserAgent()
# ── CONFIG ───────────────────────────────────────────────────────────────────
URL           = "https://worker.brilliance.com/api/v1/lab-grown-diamond-search"
COUNT_URL     = "https://worker.brilliance.com/api/v1/lab-grown-diamond-search-count"
PER_PAGE      = 48             # diamonds per API page

from pathlib import Path
base_dir = str(Path(__file__).resolve().parent.parent)
os.makedirs(os.path.join(base_dir, "diamond_files", "usa"), exist_ok=True)
os.makedirs(os.path.join(base_dir, "diamond_files_backup"), exist_ok=True)

CSV_FILE          = os.path.join(base_dir, "diamond_files", "usa", "brilliance_diamonds.csv")
CHECKPOINT_FILE   = os.path.join(base_dir, "diamond_files", "usa", "checkpoint.json")
BATCH_REPORT_FILE = os.path.join(base_dir, "diamond_files", "usa", "batch_reports.json")
BACKUP_DIR        = os.path.join(base_dir, "diamond_files_backup")

# ── Backup config ────────────────────────────────────────────────────────────
STALE_THRESHOLD_DAYS = 2     # CSV older than this -> archive + full re-scrape

# ── VPN rotation config ──────────────────────────────────────────────────────
VPN_ROTATE_AFTER_FAILS = 5       # consecutive failed pages -> rotate VPN
VPN_CONFIG_SUBDIR      = "brilliance"  # vpnbook/brilliance/*.ovpn


class VPNRotateNeeded(Exception):
    """Raised by the scraper when consecutive 403s indicate the IP is burned."""
    def __init__(self, page, consecutive_fails):
        self.page = page
        self.consecutive_fails = consecutive_fails
        super().__init__(
            f"VPN rotate signal: {consecutive_fails} consecutive failed page(s) "
            f"ending at page {page}"
        )

SLEEP_MIN       = 0.0          # between normal calls
SLEEP_MAX       = 0.5
SLEEP_EMPTY     = 1.0          # when page returns 0 diamonds
BATCH_SLEEP_MIN = 30          # sleep between every 1000-call batch
BATCH_SLEEP_MAX = 60
RETRY_SLEEP     = 10          # sleep on 403 before retry
PAGE_HARD_TIMEOUT = 45   # seconds — thread-level kill-switch; fires even when curl hangs
MAX_RETRIES     = 3
BATCH_SIZE      = 1000         # calls per batch before report + sleep
CONCURRENT_PAGES = 1          # pages fetched in parallel → effective time = API_time / N
SESSION_RESET_EVERY = 500     # recreate curl session every N pages (prevents /tmp buildup)

FIELDS = [
    "nid", "shape", "price", "color", "carat", "clarity", "cut", "report",
    "polish", "symmetry", "depth", "table", "fluorescence", "list_price",
    "girdle", "culet", "measurement", "url", "diamond_image_flag",
    "reportNumber", "info", "alias", "fast"
]

# ── SESSION (thread-local — each concurrent worker gets its own connection) ───
_thread_local = threading.local()

def _get_session():
    if not hasattr(_thread_local, 'session'):
        _thread_local.session = requests.Session(impersonate="chrome124")
    return _thread_local.session

HEADERS = {
    "accept": "application/json, text/plain, */*",
    "accept-encoding": "gzip, deflate, br",
    "accept-language": "en-GB,en-US;q=0.9,en;q=0.8",
    "cache-control": "no-cache",
    "content-type": "application/json",
    "origin": "https://www.brilliance.com",
    "pragma": "no-cache",
    "priority": "u=1, i",
    "sec-ch-ua": '"Chromium";v="148", "Google Chrome";v="148", "Not/A)Brand";v="99"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-site",
    "user-agent": ua.random
}

BASE_PAYLOAD = {
    "data": {
        "imgOnly": True,
        "view": "grid",
        "priceMin": 750,   "priceMax": 100000,
        "caratMin": 1.5,   "caratMax": 12,
        "colorMin": 6,     "colorMax": 9,
        "clarityMin": 4,   "clarityMax": 9,
        "depthMin": 0,     "depthMax": 90,
        "tableMin": 0,     "tableMax": 90,
        "shapeList": ["0","2","4","6","8","1","3","5","7","9"],
        "certificateList": [],
        "cutMin": 0,   "cutMax": 4,
        "polishMin": 0, "polishMax": 3,
        "symmetryMin": 0, "symmetryMax": 3,
        "fluorMin": 0,  "fluorMax": 3,
        "fastShipping": 0
    }
}

# ── FILE BACKUP / ROTATION ────────────────────────────────────────────────────
def csv_age_days(filepath):
    """Return CSV age in days, or None if missing."""
    if not os.path.exists(filepath):
        return None
    mtime = os.path.getmtime(filepath)
    return (datetime.datetime.now() - datetime.datetime.fromtimestamp(mtime)).total_seconds() / 86400


def archive_to_backup(filepath, threshold_days=STALE_THRESHOLD_DAYS):
    """
    If `filepath` exists AND is older than `threshold_days`, move it into
    BACKUP_DIR with a date suffix and clear checkpoint + batch reports so the
    next scrape restarts from page 1. Returns True if archived, False otherwise.
    """
    if not os.path.exists(filepath):
        log.info(f"No existing CSV at {filepath} — fresh scrape will start from page 1.")
        return False

    mtime    = os.path.getmtime(filepath)
    mod_date = datetime.datetime.fromtimestamp(mtime)
    age      = datetime.datetime.now() - mod_date
    age_days = age.total_seconds() / 86400

    if age_days < threshold_days:
        log.info(
            f"CSV is fresh ({age.days}d {age.seconds // 3600}h old, "
            f"threshold: {threshold_days}d) — skipping backup."
        )
        return False

    os.makedirs(BACKUP_DIR, exist_ok=True)
    date_str  = mod_date.strftime("%Y-%m-%d")
    stem      = os.path.splitext(os.path.basename(filepath))[0]
    ext       = os.path.splitext(filepath)[1]
    dest      = os.path.join(BACKUP_DIR, f"{stem}_{date_str}{ext}")

    counter = 1
    while os.path.exists(dest):
        dest = os.path.join(BACKUP_DIR, f"{stem}_{date_str}_{counter}{ext}")
        counter += 1

    os.rename(filepath, dest)
    log.info(f"Archived old CSV → {dest}  (was {age.days}d {age.seconds // 3600}h old)")

    if os.path.exists(CHECKPOINT_FILE):
        os.remove(CHECKPOINT_FILE)
        log.info("Checkpoint cleared — will start from page 1")

    if os.path.exists(BATCH_REPORT_FILE):
        os.remove(BATCH_REPORT_FILE)
        log.info("Batch report cleared")

    return True


# ── COUNT API → TOTAL PAGES ───────────────────────────────────────────────────
def fetch_total_pages():
    """
    Calls the count endpoint → converts string count to int
    → divides by PER_PAGE (48) → returns ceil total pages.
    """
    log.info("Fetching total diamond count from count API…")

    count_payload = {
        "data": {
            "imgOnly": True,
            "view": "grid",
            "priceMin": 750,   "priceMax": 100000,
            "caratMin": 1.5,   "caratMax": 12,
            "colorMin": 6,     "colorMax": 9,
            "clarityMin": 4,   "clarityMax": 9,
            "depthMin": 0,     "depthMax": 90,
            "tableMin": 0,     "tableMax": 90,
            "shapeList": [],
            "certificateList": [],
            "pager": 0,
            "cutMin": 0,   "cutMax": 4,
            "polishMin": 0, "polishMax": 3,
            "symmetryMin": 0, "symmetryMax": 3,
            "fluorMin": 0,  "fluorMax": 3,
            "fastShipping": 0
        }
    }

    for attempt in range(MAX_RETRIES):
        try:
            r    = _get_session().post(COUNT_URL, headers=HEADERS, json=count_payload, timeout=30)
            data = r.json()                            # {"count": "576099"}

            raw_count   = data["count"]                # "576099"  ← string
            count_int   = int(raw_count)               # 576099    ← int
            total_pages = math.ceil(count_int / PER_PAGE)

            log.info(f"Raw count (API)  : {raw_count}")
            log.info(f"Count (int)      : {count_int:,}")
            log.info(f"Per page         : {PER_PAGE}")
            log.info(f"Total pages      : ceil({count_int:,} / {PER_PAGE}) = {total_pages:,}")

            print(f"\n  {'═'*52}")
            print(f"  📦  COUNT API RESULT")
            print(f"  {'═'*52}")
            print(f"  {'Raw count (API)':<26}: {raw_count}")
            print(f"  {'Count (int)':<26}: {count_int:,}")
            print(f"  {'Per page':<26}: {PER_PAGE}")
            print(f"  {'Total pages (ceil)':<26}: {total_pages:,}")
            print(f"  {'Formula':<26}: ceil({count_int:,} / {PER_PAGE}) = {total_pages:,}")
            print(f"  {'═'*52}\n")

            return total_pages, count_int

        except Exception as e:
            log.warning(f"Count API attempt {attempt+1}/{MAX_RETRIES} failed: {e}")
            time.sleep(5)

    raise Exception("Could not fetch total count after max retries.")


# ── CHECKPOINT ────────────────────────────────────────────────────────────────
def save_checkpoint(page, total_inserted, total_count=None):
    # ✅ FIX: write to a temp file first, then atomically replace — prevents
    #         an empty/corrupt checkpoint.json if the process is killed mid-write
    payload = {
        "last_completed_page": page,
        "total_inserted":      total_inserted,
    }
    if total_count is not None:
        payload["total_count"] = total_count
    elif os.path.exists(CHECKPOINT_FILE):
        # Preserve any previously-stored total_count when caller didn't pass one
        try:
            with open(CHECKPOINT_FILE) as f:
                prev = json.loads(f.read() or "{}")
            if "total_count" in prev:
                payload["total_count"] = prev["total_count"]
        except Exception:
            pass

    tmp = CHECKPOINT_FILE + ".tmp"
    with open(tmp, "w") as f:
        json.dump(payload, f, indent=2)
    os.replace(tmp, CHECKPOINT_FILE)   # atomic on Windows & Linux
    log.debug(f"Checkpoint saved → page={page}, total={total_inserted}")


def load_checkpoint():
    if not os.path.exists(CHECKPOINT_FILE):
        return 1, 0, None

    # ✅ FIX: handle empty or corrupted checkpoint file gracefully
    try:
        with open(CHECKPOINT_FILE, "r") as f:
            content = f.read().strip()
        if not content:
            log.warning("Checkpoint file is empty — starting from page 1")
            return 1, 0, None
        data         = json.loads(content)
        page         = data.get("last_completed_page", 1) + 1
        total        = data.get("total_inserted", 0)
        stored_count = data.get("total_count")
        log.info(f"Resuming from page {page} (previously inserted: {total})")
        return page, total, stored_count
    except json.JSONDecodeError as e:
        log.warning(f"Checkpoint file corrupted ({e}) — deleting and starting from page 1")
        os.remove(CHECKPOINT_FILE)
        return 1, 0, None


def clear_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        os.remove(CHECKPOINT_FILE)
        log.info("Checkpoint cleared — scrape fully complete.")


# ── HELPERS ───────────────────────────────────────────────────────────────────
def normalize_cert(cert):
    if not cert:
        return None
    cert = cert.strip().lower()
    if cert.startswith("lg"):
        cert = cert.replace("lg", "", 1)
    return cert


def load_existing_certs():
    certs = set()
    if not os.path.exists(CSV_FILE):
        return certs
    with open(CSV_FILE, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            cert = row.get("reportNumber")
            if cert:
                certs.add(cert)
    log.info(f"Loaded {len(certs)} existing certificates")
    return certs


def get_writer():
    file_exists = os.path.exists(CSV_FILE)
    f = open(CSV_FILE, "a" if file_exists else "w", newline="", encoding="utf-8")
    writer = csv.DictWriter(f, fieldnames=FIELDS)
    if not file_exists:
        writer.writeheader()
    return f, writer


# ── FETCH ONE PAGE ────────────────────────────────────────────────────────────
def _reset_session():
    """Recreate this thread's curl_cffi session — clears stale keep-alive connections and temp files."""
    try:
        if hasattr(_thread_local, 'session'):
            _thread_local.session.close()
    except Exception:
        pass
    _cleanup_curl_tmp()
    _thread_local.session = requests.Session(impersonate="chrome124")
    log.info("Session recreated.")


def _fetch_page_inner(page, payload, out):
    """Blocking fetch with retry logic — always runs inside a daemon thread."""
    for attempt in range(MAX_RETRIES):
        t0 = time.perf_counter()
        try:
            r = _get_session().post(URL, headers=HEADERS, json=payload, timeout=(10, 30))
            elapsed = time.perf_counter() - t0

            if r.status_code == 200:
                diamonds = r.json().get("diamond", [])
                out.update({"ok": True, "elapsed": elapsed, "status": 200,
                            "count": len(diamonds), "data": diamonds})
                return

            elif r.status_code == 403:
                log.warning(f"[page {page}] 403 Cloudflare – sleeping {RETRY_SLEEP}s "
                            f"(attempt {attempt+1}/{MAX_RETRIES})")
                time.sleep(RETRY_SLEEP)

            else:
                log.warning(f"[page {page}] HTTP {r.status_code} – sleeping 5s")
                time.sleep(5)

        except Exception as e:
            elapsed = time.perf_counter() - t0
            log.error(f"[page {page}] Exception after {elapsed:.1f}s: {e} – sleeping 10s")
            _reset_session()
            time.sleep(10)

    out.update({"ok": False, "elapsed": 0, "status": -1, "count": 0, "data": []})


def fetch_page(page):
    """
    Fetch one page with a hard wall-clock timeout (PAGE_HARD_TIMEOUT seconds).

    The inner request runs in a daemon thread so Python's thread scheduler
    enforces the deadline even when curl/libcurl itself is frozen due to a
    silent VPN tunnel drop or Windows network stack confusion.
    If the thread is still alive after PAGE_HARD_TIMEOUT seconds the session
    is reset so the next call gets a fresh TCP connection.
    """
    payload = {**BASE_PAYLOAD, "data": {**BASE_PAYLOAD["data"], "pager": page}}
    out = {}
    t = threading.Thread(target=_fetch_page_inner, args=(page, payload, out), daemon=True)
    t.start()
    t.join(timeout=PAGE_HARD_TIMEOUT)

    if t.is_alive():
        log.warning(
            f"[page {page}] HARD TIMEOUT ({PAGE_HARD_TIMEOUT}s) — "
            f"network stall detected (VPN drop?). Resetting session."
        )
        _reset_session()
        return {"ok": False, "elapsed": PAGE_HARD_TIMEOUT, "status": -999, "count": 0, "data": []}

    return out or {"ok": False, "elapsed": 0, "status": -1, "count": 0, "data": []}


def write_diamond(writer, d, cert):
    writer.writerow({
        "nid":               d.get("nid"),
        "shape":             d.get("shape"),
        "price":             d.get("price"),
        "color":             d.get("color"),
        "carat":             d.get("carat"),
        "clarity":           d.get("clarity"),
        "cut":               d.get("cut"),
        "report":            d.get("report"),
        "polish":            d.get("polish"),
        "symmetry":          d.get("symmetry"),
        "depth":             d.get("depth"),
        "table":             d.get("table"),
        "fluorescence":      d.get("fluorescence"),
        "list_price":        d.get("list_price"),
        "girdle":            d.get("girdle"),
        "culet":             d.get("culet"),
        "measurement":       d.get("measurement"),
        "url":               d.get("url"),
        "diamond_image_flag": d.get("diamond_image_flag"),
        "reportNumber":      cert,
        "info":              d.get("info"),
        "alias":             d.get("alias"),
        "fast":              d.get("fast"),
    })


# ── BATCH REPORT (every 1000 calls) ──────────────────────────────────────────
def print_batch_report(batch_num, batch_results, batch_wall, all_reports):
    succeeded = [r for r in batch_results if r["ok"]]
    failed    = [r for r in batch_results if not r["ok"]]
    times     = [r["elapsed"] for r in succeeded] or [0]
    diamonds  = sum(r["count"] for r in succeeded)

    status_counts = {}
    for r in batch_results:
        s = r["status"]
        status_counts[s] = status_counts.get(s, 0) + 1

    report = {
        "batch":          batch_num,
        "calls":          len(batch_results),
        "success":        len(succeeded),
        "failed":         len(failed),
        "success_pct":    round(len(succeeded) / len(batch_results) * 100, 1),
        "fail_pct":       round(len(failed)    / len(batch_results) * 100, 1),
        "status_counts":  status_counts,
        "total_diamonds": diamonds,
        "wall_time_s":    round(batch_wall, 2),
        "response_times": {
            "min_s":    round(min(times), 3),
            "max_s":    round(max(times), 3),
            "mean_s":   round(statistics.mean(times), 3),
            "median_s": round(statistics.median(times), 3),
            "p95_s":    round(sorted(times)[int(len(times) * 0.95)], 3) if len(times) >= 20 else "n/a",
        }
    }

    # cumulative across all previous batches + this one
    all_ok  = sum(r["success"] for r in all_reports) + len(succeeded)
    all_bad = sum(r["failed"]  for r in all_reports) + len(failed)
    all_tot = all_ok + all_bad

    h, rem = divmod(int(batch_wall), 3600)
    m, s   = divmod(rem, 60)

    print(f"\n{'═'*64}")
    print(f"  📊  BATCH {batch_num} REPORT  "
          f"(calls {(batch_num-1)*BATCH_SIZE+1} – "
          f"{(batch_num-1)*BATCH_SIZE + len(batch_results)})")
    print(f"{'═'*64}")
    print(f"  {'Metric':<30} {'This Batch':>14}  {'Cumulative':>14}")
    print(f"  {'-'*60}")
    print(f"  {'Total calls':<30} {len(batch_results):>14}  {all_tot:>14,}")
    print(f"  {'  Succeeded':<30} {len(succeeded):>14}  {all_ok:>14,}")
    print(f"  {'  Failed':<30} {len(failed):>14}  {all_bad:>14,}")
    print(f"  {'Success rate':<30} {report['success_pct']:>13}%  "
          f"{round(all_ok/all_tot*100,1) if all_tot else 0:>13}%")
    print(f"  {'Diamonds collected':<30} {diamonds:>14,}")
    print(f"  {'-'*60}")
    print(f"  {'Response time  min':<30} {report['response_times']['min_s']:>13}s")
    print(f"  {'Response time  mean':<30} {report['response_times']['mean_s']:>13}s")
    print(f"  {'Response time  median':<30} {report['response_times']['median_s']:>13}s")
    print(f"  {'Response time  max':<30} {report['response_times']['max_s']:>13}s")
    print(f"  {'Response time  p95':<30} {str(report['response_times']['p95_s']):>13}")
    print(f"  {'-'*60}")
    print(f"  {'Batch wall time':<30} {f'{h}h {m}m {s}s':>14}")

    if status_counts:
        print(f"\n  HTTP status breakdown:")
        for code, cnt in sorted(status_counts.items()):
            bar   = "█" * int(cnt / len(batch_results) * 30)
            pct   = round(cnt / len(batch_results) * 100, 1)
            label = "" if code == 200 else ""
            print(f"    {label} {str(code):<6} {bar:<30}  {cnt:>5} calls  ({pct}%)")

    print(f"{'═'*64}")

    log.info(f"Batch {batch_num} → success={len(succeeded)} fail={len(failed)} "
             f"diamonds={diamonds} wall={batch_wall:.1f}s")

    return report


# ── FINAL SUMMARY ─────────────────────────────────────────────────────────────
def print_final_summary(all_reports, grand_wall, total_available):
    total_calls    = sum(r["calls"]          for r in all_reports)
    total_success  = sum(r["success"]        for r in all_reports)
    total_failed   = sum(r["failed"]         for r in all_reports)
    total_diamonds = sum(r["total_diamonds"] for r in all_reports)

    h, rem = divmod(int(grand_wall), 3600)
    m, s   = divmod(rem, 60)

    print(f"\n{'█'*64}")
    print(f"  🏁  FINAL SUMMARY — {len(all_reports)} batches complete")
    print(f"{'█'*64}")
    print(f"  {'Total API calls':<30}: {total_calls:,}")
    print(f"  {'  Succeeded':<30}: {total_success:,}  "
          f"({round(total_success/total_calls*100,1) if total_calls else 0}%)")
    print(f"  {'  Failed':<30}: {total_failed:,}  "
          f"({round(total_failed/total_calls*100,1) if total_calls else 0}%)")
    print(f"  {'Diamonds collected':<30}: {total_diamonds:,}")
    print(f"  {'Diamonds available (API)':<30}: {total_available:,}")
    print(f"  {'Coverage':<30}: "
          f"{round(total_diamonds/total_available*100,1) if total_available else 0}%")
    print(f"  {'Total wall time':<30}: {h}h {m}m {s}s")
    print(f"  {'Avg per call (wall)':<30}: {grand_wall/total_calls:.3f}s")
    print(f"  {'Effective rate':<30}: {total_calls/grand_wall*60:.1f} calls/min")
    print(f"{'█'*64}")

    print(f"\n  Batch-by-batch success rate:")
    for r in all_reports:
        bar = "█" * int(r["success_pct"] / 5)
        print(f"    Batch {r['batch']:>3}  {bar:<20}  {r['success_pct']:>5}%  "
              f"({r['success']}/{r['calls']} ok)")
    print()


# ── MAIN SCRAPER (no scheduling, no file rotation) ────────────────────────────
def brilliance_diamonds_scraper(rotate_after_fails: int | None = None):
    """
    Pure scraper. Appends to / resumes from the existing CSV + checkpoint.

    Args:
        rotate_after_fails: If set, raise VPNRotateNeeded after N consecutive
            failed pages. Caller should catch and switch VPN before retrying.
            None (default) preserves original behavior — failed pages are
            recorded and the scrape moves on.

    Use `run_with_scheduler()` for the daily-check + auto-backup loop,
    or `run_with_vpn_rotation()` to wrap this in a VPN tunnel with mid-scrape
    IP rotation on persistent blocks.
    """
    # ── Prevent Windows sleep/hibernate while scraping ───────────
    # ES_CONTINUOUS (0x80000000) | ES_SYSTEM_REQUIRED (0x00000001)
    # Tells Windows this thread needs the system awake. Cleared on exit.
    if sys.platform == "win32":
        ctypes.windll.kernel32.SetThreadExecutionState(0x80000001)
        log.info("Windows sleep prevention: ON")

    log.info("=" * 60)
    log.info("Starting Brilliance scraper")
    if rotate_after_fails:
        log.info(f"VPN rotate-on-fail threshold: {rotate_after_fails} consecutive page(s)")

    # ── Step 1: fetch total pages from count API ─────────────────
    try:
        total_pages, total_count = fetch_total_pages()
    except Exception as exc:
        # A blocked exit IP serves a non-JSON challenge page, so the count API
        # fails before the page loop's rotate logic can run. In VPN mode, treat
        # this as a rotate signal so the caller switches exit IP instead of crashing.
        if rotate_after_fails:
            log.warning(f"Count API failed ({exc}) — signalling VPN rotation.")
            raise VPNRotateNeeded(0, rotate_after_fails) from exc
        raise

    # ── Step 2: load existing state ──────────────────────────────
    existing_certs             = load_existing_certs()
    file, writer               = get_writer()
    start_page, total_inserted, _stored_count = load_checkpoint()

    consecutive_fails    = 0
    last_successful_page = start_page - 1

    print(f"\n  {'═'*52}")
    print(f"  🚀  BRILLIANCE SCRAPER")
    print(f"  {'═'*52}")
    print(f"  {'Total pages':<26}: {total_pages:,}")
    print(f"  {'Batch size':<26}: {BATCH_SIZE:,} calls")
    print(f"  {'Total batches':<26}: {math.ceil(total_pages / BATCH_SIZE):,}")
    print(f"  {'Starting from page':<26}: {start_page:,}")
    print(f"  {'Existing certs loaded':<26}: {len(existing_certs):,}")
    print(f"  {'Sleep between batches':<26}: {BATCH_SLEEP_MIN}–{BATCH_SLEEP_MAX}s (random)")
    print(f"  {'═'*52}\n")

    all_reports  = []
    grand_start  = time.perf_counter()

    page      = start_page
    batch_num = math.ceil(start_page / BATCH_SIZE)

    try:
        while page <= total_pages:
            batch_start    = time.perf_counter()
            batch_results  = []
            batch_inserted = 0
            batch_end_page = min(page + BATCH_SIZE - 1, total_pages)

            print(f"\n  ▶  Batch {batch_num}  "
                  f"(pages {page:,} – {batch_end_page:,}  |  "
                  f"{batch_end_page - page + 1} calls)\n")

            while page <= batch_end_page:
                # Fetch CONCURRENT_PAGES pages simultaneously; executor.map
                # preserves input order so checkpoint logic stays correct.
                pages_chunk = list(range(page, min(page + CONCURRENT_PAGES, batch_end_page + 1)))
                with ThreadPoolExecutor(max_workers=len(pages_chunk)) as _exec:
                    chunk_results = list(_exec.map(fetch_page, pages_chunk))

                # Periodically recreate session to free curl_cffi /tmp data
                if page % SESSION_RESET_EVERY == 0:
                    _reset_session()
                    log.debug(f"Session recycled at page {page} (every {SESSION_RESET_EVERY} pages)")

                for result, p in zip(chunk_results, pages_chunk):
                    batch_results.append(result)

                    # write new diamonds to CSV
                    if result["ok"]:
                        for d in result["data"]:
                            cert = normalize_cert(d.get("reportNumber"))
                            if not cert or cert in existing_certs:
                                continue
                            write_diamond(writer, d, cert)
                            existing_certs.add(cert)
                            batch_inserted += 1
                            total_inserted += 1
                        file.flush()
                        consecutive_fails    = 0
                        last_successful_page = p
                    else:
                        consecutive_fails += 1
                        if rotate_after_fails and consecutive_fails >= rotate_after_fails:
                            log.warning(
                                f"VPN rotate needed — {consecutive_fails} consecutive "
                                f"failed pages (last good: {last_successful_page}). "
                                f"Rewinding checkpoint."
                            )
                            save_checkpoint(last_successful_page, total_inserted)
                            file.close()
                            raise VPNRotateNeeded(p, consecutive_fails)

                    # In VPN-rotation mode we only checkpoint up to the last successful
                    # page, so a fresh VPN attempt re-tries the failed pages. In normal
                    # mode we keep the original behavior: advance checkpoint past every
                    # page (success or fail) so the scrape doesn't loop on a dead page.
                    if rotate_after_fails:
                        save_checkpoint(last_successful_page, total_inserted)
                    else:
                        save_checkpoint(p, total_inserted)

                    # ── live progress line ────────────────────────────────────
                    ok_count   = sum(r["ok"]      for r in batch_results)
                    fail_count = sum(not r["ok"]  for r in batch_results)
                    avg_time   = (statistics.mean(r["elapsed"] for r in batch_results if r["ok"])
                                  if ok_count else 0)
                    status_icon = "YES" if result["ok"] else "NO"

                    print(f"  {status_icon} [{p:>6}/{total_pages}]  "
                          f"{result['elapsed']:.3f}s  |  "
                          f"{result['count']:>3} 💎  |  "
                          f"saved: {batch_inserted:<6}  |  "
                          f"avg: {avg_time:.3f}s  |  "
                          f"ok: {ok_count}  fail: {fail_count}")

                page += len(pages_chunk)

                # one sleep per chunk instead of one per page
                all_failed = all(not r["ok"] for r in chunk_results)
                any_empty  = any(r["count"] == 0 for r in chunk_results if r["ok"])
                if all_failed:
                    pass  # retries already included sleep
                elif any_empty:
                    time.sleep(SLEEP_EMPTY)
                else:
                    time.sleep(round(random.uniform(SLEEP_MIN, SLEEP_MAX), 3))

            # ── batch complete ───────────────────────────────────────────
            batch_wall = time.perf_counter() - batch_start
            report     = print_batch_report(batch_num, batch_results, batch_wall, all_reports)
            all_reports.append(report)

            # save running batch report JSON
            with open(BATCH_REPORT_FILE, "w") as bf:
                json.dump(all_reports, bf, indent=2)

            log.info(f"Batch {batch_num} report saved → {BATCH_REPORT_FILE}")

            # ── inter-batch sleep (skip after final batch) ───────────────
            if page <= total_pages:
                sleep_secs = round(random.uniform(BATCH_SLEEP_MIN, BATCH_SLEEP_MAX), 2)
                print(f"\n  💤  Batch {batch_num} complete — "
                      f"sleeping {sleep_secs}s before batch {batch_num + 1}…\n")
                log.info(f"Inter-batch sleep: {sleep_secs}s")
                time.sleep(sleep_secs)

            batch_num += 1

        # ── all batches done ─────────────────────────────────────────────
        clear_checkpoint()

    except VPNRotateNeeded:
        # propagate up to run_with_vpn_rotation() so it can switch tunnels
        raise

    except Exception as e:
        log.error(f"Failed on page {page}: {e}")
        log.info("checkpoint.json saved — re-run to resume from last page")

    finally:
        try:
            file.close()
        except Exception:
            pass
        log.info("CSV saved & closed")
        # Restore normal sleep behaviour
        if sys.platform == "win32":
            ctypes.windll.kernel32.SetThreadExecutionState(0x80000000)
            log.info("Windows sleep prevention: OFF")

    # ── grand final summary ──────────────────────────────────────────────────
    grand_wall = time.perf_counter() - grand_start
    print_final_summary(all_reports, grand_wall, total_count)

    with open(BATCH_REPORT_FILE, "w") as bf:
        json.dump(all_reports, bf, indent=2)

    print(f"    Full batch report → {BATCH_REPORT_FILE}")
    print(f"    Diamond CSV       → {CSV_FILE}\n")

    log.info("=" * 60)

    return {"total_inserted": total_inserted, "total_pages": total_pages}


# ── VPN-WRAPPED RUN ───────────────────────────────────────────────────────────
def run_with_vpn_rotation():
    """
    Run the scraper inside an OpenVPN tunnel, trying EVERY config in
    vpnbook/<VPN_CONFIG_SUBDIR>/ until one completes the scrape.

    Per-config outcome:
      - success           → scrape done, return.
      - VPNRotateNeeded    → tunnel works but the exit IP got blocked (403s);
                             keep the config and rotate to the next one.
      - VPNError (dead)    → the config failed to connect; DELETE the .ovpn file
                             and move on.

    Safety: a missing OpenVPN binary or an auth failure is an *environmental*
    problem (not a dead config), so those abort the run WITHOUT deleting
    anything — otherwise a stale password would wipe the whole pool.

    The scraper resumes from checkpoint, so failed pages are re-tried through
    each new exit IP. Requires Administrator / root.
    """
    from pathlib import Path
    from vpn_rotator import (
        VPNConnection,
        VPNError,
        find_openvpn_executable,
        is_admin,
        parse_credentials_file,
        write_auth_file,
    )

    log.info("=" * 60)
    log.info("Brilliance scraper with VPN rotation")
    log.info(f"Consecutive-fail threshold: {VPN_ROTATE_AFTER_FAILS}")
    log.info("=" * 60)

    if not is_admin():
        log.error(
            "OpenVPN requires Administrator/root. "
            "Re-run from an elevated shell, or call brilliance_diamonds_scraper() directly."
        )
        return

    # Validate the OpenVPN binary once up-front. If it is missing, every connect
    # would fail and we'd delete the entire pool over an environment problem.
    try:
        find_openvpn_executable()
    except VPNError as exc:
        log.error(f"OpenVPN unavailable — aborting without touching configs: {exc}")
        return

    # Anchor to the repo root (one level above usa_compare/) so the VPN path is
    # D:\...\vpnbook\usa\<subdir>, not cwd/vpnbook which breaks when run from usa_compare.
    repo_root = Path(__file__).resolve().parent.parent
    vpn_dir = repo_root / "vpnbook" / "usa" / VPN_CONFIG_SUBDIR
    if not vpn_dir.exists():
        log.error(f"VPN config dir not found: {vpn_dir}")
        return

    # Resolve the shared ProtonVPN credentials relative to this script so the
    # lookup never depends on the process's cwd. The configs in this pool are
    # all ProtonVPN, which vpn_rotator's per-config auto-resolution can miss
    # when cwd != repo root, so pass the auth file in explicitly.
    proton_auth: Path | None = None
    proton_creds = repo_root / "vpnbook" / "proton_credentials.txt"
    if proton_creds.exists():
        user, pwd = parse_credentials_file(proton_creds)
        proton_auth = write_auth_file(user, pwd, proton_creds.parent / "proton_auth.txt")
        log.info(f"Using ProtonVPN credentials from {proton_creds}")
    else:
        log.warning(
            f"ProtonVPN credentials not found at {proton_creds} — "
            f"falling back to vpn_rotator's per-config lookup."
        )

    # OpenVPN's runtime log. vpn_rotator defaults this to cwd/vpnbook/, which is
    # absent when run from anywhere but the repo root — anchor it to the real dir.
    openvpn_log = repo_root / "vpnbook" / "openvpn.log"

    configs = sorted(vpn_dir.glob("*.ovpn"))
    if not configs:
        log.error(f"No .ovpn files in {vpn_dir}")
        return

    random.shuffle(configs)
    total   = len(configs)
    deleted = 0
    log.info(f"VPN pool: {total} config(s) in {vpn_dir} — trying every config")

    for attempt, cfg in enumerate(configs, start=1):
        log.info(f"--- VPN attempt {attempt}/{total}: {cfg.name} ---")

        try:
            with VPNConnection(cfg, auth_file=proton_auth, log_file=openvpn_log) as vpn:
                log.info(f"Tunnel up. Exit IP = {vpn.ip_after}")
                brilliance_diamonds_scraper(rotate_after_fails=VPN_ROTATE_AFTER_FAILS)
            log.info("Scrape completed — VPN torn down.")
            return

        except VPNRotateNeeded as e:
            log.warning(f"Rotating VPN (config works, IP burned): {e}")
            # VPN tunnel already torn down by context manager; keep config, try next

        except VPNError as e:
            # Auth failure = stale credentials, not a dead config. Deleting would
            # wipe configs that are actually fine, so abort the whole run instead.
            if "authentication failed" in str(e).lower():
                log.error(
                    f"VPN auth failed on {cfg.name} — credentials likely stale. "
                    f"Aborting (no configs deleted). Check proton_credentials.txt."
                )
                return
            log.error(f"VPN error on {cfg.name}: {e} — deleting dead config")
            try:
                cfg.unlink()
                deleted += 1
                log.info(f"Deleted non-working VPN config: {cfg.name}")
            except OSError as del_exc:
                log.warning(f"Could not delete {cfg.name}: {del_exc}")

        except KeyboardInterrupt:
            log.warning("Interrupted by user — exiting VPN loop.")
            return

    log.error(
        f"Tried all {total} VPN config(s) — none completed the scrape "
        f"({deleted} dead config(s) deleted)."
    )


# ── RUN ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    # Entry points (scheduling is handled externally by scheduler.py):
    #   run_with_vpn_rotation()        → one-shot VPN-wrapped scrape with mid-run rotation
    brilliance_diamonds_scraper() # → pure scrape, no VPN, no rotation (your real IP)
    #
    # Note: VPN-using entries require Administrator/root (OpenVPN needs TAP/TUN).
    # import sys as _sys
    # try:
    #     run_with_vpn_rotation()
    # except KeyboardInterrupt:
    #     log.warning("Ctrl+C pressed — stopping scraper and tearing down any VPN tunnel.")
    #     try:
    #         from vpn_rotator import kill_openvpn_processes
    #         kill_openvpn_processes()
    #     except Exception as _exc:
    #         log.warning(f"VPN cleanup failed: {_exc}")
    #     _sys.exit(130)