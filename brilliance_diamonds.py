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
from logger import get_logger

log = get_logger("brilliance")
ua = UserAgent()
# ── CONFIG ───────────────────────────────────────────────────────────────────
URL           = "https://worker.brilliance.com/api/v1/lab-grown-diamond-search"
COUNT_URL     = "https://worker.brilliance.com/api/v1/lab-grown-diamond-search-count"
PER_PAGE      = 48             # diamonds per API page

base_dir = os.getcwd()
os.makedirs("diamond_files", exist_ok=True)
os.makedirs("diamond_files_backup", exist_ok=True)

CSV_FILE          = f"{base_dir}/diamond_files/brilliance_diamonds.csv"
CHECKPOINT_FILE   = f"{base_dir}/diamond_files/checkpoint.json"
BATCH_REPORT_FILE = f"{base_dir}/diamond_files/batch_reports.json"
BACKUP_DIR        = f"{base_dir}/diamond_files_backup"

# ── Scheduler config ─────────────────────────────────────────────────────────
SCHEDULE_HOUR        = 16    # 24h clock — daily check time (11 AM)
SCHEDULE_MINUTE      = 0
STALE_THRESHOLD_DAYS = 2     # CSV older than this -> archive + full re-scrape

# ── VPN rotation config ──────────────────────────────────────────────────────
VPN_ROTATE_AFTER_FAILS = 5       # consecutive failed pages -> rotate VPN
MAX_VPN_ROTATIONS      = 10      # max VPN switches per scheduler run
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
SLEEP_MAX       = 1.0
SLEEP_EMPTY     = 2.0          # when page returns 0 diamonds
BATCH_SLEEP_MIN = 100         # sleep between every 1000-call batch
BATCH_SLEEP_MAX = 200
RETRY_SLEEP     = 20
MAX_RETRIES     = 5
BATCH_SIZE      = 1000         # calls per batch before report + sleep

FIELDS = [
    "nid", "shape", "price", "color", "carat", "clarity", "cut", "report",
    "polish", "symmetry", "depth", "table", "fluorescence", "list_price",
    "girdle", "culet", "measurement", "url", "diamond_image_flag",
    "reportNumber", "info", "alias", "fast"
]

# ── SESSION ───────────────────────────────────────────────────────────────────
session = requests.Session(impersonate="chrome124")

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
            r    = session.post(COUNT_URL, headers=HEADERS, json=count_payload, timeout=30)
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
def save_checkpoint(page, total_inserted):
    # ✅ FIX: write to a temp file first, then atomically replace — prevents
    #         an empty/corrupt checkpoint.json if the process is killed mid-write
    tmp = CHECKPOINT_FILE + ".tmp"
    with open(tmp, "w") as f:
        json.dump({"last_completed_page": page, "total_inserted": total_inserted}, f, indent=2)
    os.replace(tmp, CHECKPOINT_FILE)   # atomic on Windows & Linux
    log.debug(f"Checkpoint saved → page={page}, total={total_inserted}")


def load_checkpoint():
    if not os.path.exists(CHECKPOINT_FILE):
        return 1, 0

    # ✅ FIX: handle empty or corrupted checkpoint file gracefully
    try:
        with open(CHECKPOINT_FILE, "r") as f:
            content = f.read().strip()
        if not content:
            log.warning("Checkpoint file is empty — starting from page 1")
            return 1, 0
        data  = json.loads(content)
        page  = data.get("last_completed_page", 1) + 1
        total = data.get("total_inserted", 0)
        log.info(f"Resuming from page {page} (previously inserted: {total})")
        return page, total
    except json.JSONDecodeError as e:
        log.warning(f"Checkpoint file corrupted ({e}) — deleting and starting from page 1")
        os.remove(CHECKPOINT_FILE)
        return 1, 0


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
def fetch_page(page):
    payload = {**BASE_PAYLOAD, "data": {**BASE_PAYLOAD["data"], "pager": page}}

    for attempt in range(MAX_RETRIES):
        try:
            t0 = time.perf_counter()
            r  = session.post(URL, headers=HEADERS, json=payload, timeout=30)
            elapsed = time.perf_counter() - t0

            if r.status_code == 200:
                data     = r.json()
                diamonds = data.get("diamond", [])
                return {
                    "ok":      True,
                    "elapsed": elapsed,
                    "status":  200,
                    "count":   len(diamonds),
                    "data":    diamonds
                }

            elif r.status_code == 403:
                log.warning(f"[page {page}] 403 Cloudflare – sleeping {RETRY_SLEEP}s "
                            f"(attempt {attempt+1}/{MAX_RETRIES})")
                time.sleep(RETRY_SLEEP)

            else:
                log.warning(f"[page {page}] HTTP {r.status_code} – sleeping 5s")
                time.sleep(5)

        except Exception as e:
            log.error(f"[page {page}] Exception: {e} – sleeping 10s")
            time.sleep(10)

    return {"ok": False, "elapsed": 0, "status": -1, "count": 0, "data": []}


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
    log.info("=" * 60)
    log.info("Starting Brilliance scraper")
    if rotate_after_fails:
        log.info(f"VPN rotate-on-fail threshold: {rotate_after_fails} consecutive page(s)")

    # ── Step 1: fetch total pages from count API ─────────────────
    total_pages, total_count = fetch_total_pages()

    # ── Step 2: load existing state ──────────────────────────────
    existing_certs             = load_existing_certs()
    file, writer               = get_writer()
    start_page, total_inserted = load_checkpoint()

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
                result = fetch_page(page)
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
                    last_successful_page = page
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
                        raise VPNRotateNeeded(page, consecutive_fails)

                # In VPN-rotation mode we only checkpoint up to the last successful
                # page, so a fresh VPN attempt re-tries the failed pages. In normal
                # mode we keep the original behavior: advance checkpoint past every
                # page (success or fail) so the scrape doesn't loop on a dead page.
                if rotate_after_fails:
                    save_checkpoint(last_successful_page, total_inserted)
                else:
                    save_checkpoint(page, total_inserted)

                # ── live progress line ────────────────────────────────────
                ok_count   = sum(r["ok"]      for r in batch_results)
                fail_count = sum(not r["ok"]  for r in batch_results)
                avg_time   = (statistics.mean(r["elapsed"] for r in batch_results if r["ok"])
                              if ok_count else 0)
                status_icon = "YES" if result["ok"] else "NO"

                print(f"  {status_icon} [{page:>6}/{total_pages}]  "
                      f"{result['elapsed']:.3f}s  |  "
                      f"{result['count']:>3} 💎  |  "
                      f"saved: {batch_inserted:<6}  |  "
                      f"avg: {avg_time:.3f}s  |  "
                      f"ok: {ok_count}  fail: {fail_count}")

                page += 1

                # per-call sleep
                if not result["ok"]:
                    pass  # already slept during retry
                elif result["count"] == 0:
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
    Run the scraper inside an OpenVPN tunnel; on VPNRotateNeeded (triggered
    after VPN_ROTATE_AFTER_FAILS consecutive 403s), tear down the current
    tunnel and bring up the next config from vpnbook/<VPN_CONFIG_SUBDIR>/.

    The scraper resumes from checkpoint, so the failed pages are re-tried
    through the new exit IP. Requires Administrator / root.
    """
    from pathlib import Path
    from vpn_rotator import VPNConnection, VPNError, is_admin

    log.info("=" * 60)
    log.info("Brilliance scraper with VPN rotation")
    log.info(f"Consecutive-fail threshold: {VPN_ROTATE_AFTER_FAILS}")
    log.info(f"Max VPN rotations         : {MAX_VPN_ROTATIONS}")
    log.info("=" * 60)

    if not is_admin():
        log.error(
            "OpenVPN requires Administrator/root. "
            "Re-run from an elevated shell, or call brilliance_diamonds_scraper() directly."
        )
        return

    vpn_dir = Path(base_dir) / "vpnbook" / VPN_CONFIG_SUBDIR
    if not vpn_dir.exists():
        log.error(f"VPN config dir not found: {vpn_dir}")
        return

    configs = sorted(vpn_dir.glob("*.ovpn"))
    if not configs:
        log.error(f"No .ovpn files in {vpn_dir}")
        return

    random.shuffle(configs)
    log.info(f"VPN pool: {len(configs)} config(s) in {vpn_dir}")

    for attempt in range(1, MAX_VPN_ROTATIONS + 1):
        cfg = configs[(attempt - 1) % len(configs)]
        log.info(f"--- VPN attempt {attempt}/{MAX_VPN_ROTATIONS}: {cfg.name} ---")

        try:
            with VPNConnection(cfg) as vpn:
                log.info(f"Tunnel up. Exit IP = {vpn.ip_after}")
                brilliance_diamonds_scraper(rotate_after_fails=VPN_ROTATE_AFTER_FAILS)
            log.info("Scrape completed — VPN torn down.")
            return

        except VPNRotateNeeded as e:
            log.warning(f"Rotating VPN: {e}")
            # VPN tunnel already torn down by context manager; loop tries next config

        except VPNError as e:
            log.error(f"VPN error on {cfg.name}: {e} — trying next config")

        except KeyboardInterrupt:
            log.warning("Interrupted by user — exiting VPN loop.")
            return

    log.error(f"Exhausted {MAX_VPN_ROTATIONS} VPN rotation(s). Last attempt failed.")


# ── SCHEDULER ─────────────────────────────────────────────────────────────────
def _next_run_time(from_dt: datetime.datetime) -> datetime.datetime:
    candidate = from_dt.replace(
        hour=SCHEDULE_HOUR, minute=SCHEDULE_MINUTE, second=0, microsecond=0
    )
    if candidate <= from_dt:
        candidate += datetime.timedelta(days=1)
    return candidate


def run_with_scheduler():
    """
    Continuous loop. Every day at SCHEDULE_HOUR:SCHEDULE_MINUTE:
      1. Check the CSV's age.
      2. If older than STALE_THRESHOLD_DAYS (or missing), move it into
         diamond_files_backup/, clear checkpoint, and run a full re-scrape
         from page 1 to the last page.
      3. Otherwise, log "fresh" and sleep until the next check.
    """
    log.info("=" * 60)
    log.info("Brilliance scheduler started")
    log.info(f"Daily check       : {SCHEDULE_HOUR:02d}:{SCHEDULE_MINUTE:02d}")
    log.info(f"Stale threshold   : {STALE_THRESHOLD_DAYS} day(s)")
    log.info(f"Backup destination: {BACKUP_DIR}")
    log.info("=" * 60)

    while True:
        next_run  = _next_run_time(datetime.datetime.now())
        wait_secs = max(0, (next_run - datetime.datetime.now()).total_seconds())
        log.info(
            f"Sleeping {wait_secs / 3600:.2f}h until next check "
            f"({next_run.strftime('%Y-%m-%d %H:%M:%S')})"
        )
        time.sleep(wait_secs)

        age = csv_age_days(CSV_FILE)
        if age is None:
            log.info("CSV missing — running fresh scrape.")
            archived = False
        else:
            log.info(f"CSV age: {age:.2f} day(s) (threshold: {STALE_THRESHOLD_DAYS}d)")
            archived = archive_to_backup(CSV_FILE)

        if age is None or archived:
            try:
                run_with_vpn_rotation()
            except Exception as e:
                log.error(f"Scraper crashed: {e}")
        else:
            log.info("Fresh CSV — skipping scrape, waiting for next scheduled check.")


# ── RUN ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    # Entry points:
    #   run_with_scheduler()           → daily check + auto-backup + VPN-wrapped scrape
    #   run_with_vpn_rotation()        → one-shot VPN-wrapped scrape with mid-run rotation
    #   brilliance_diamonds_scraper()  → pure scrape, no VPN, no rotation (your real IP)
    #
    # Note: VPN-using entries require Administrator/root (OpenVPN needs TAP/TUN).
    run_with_scheduler()