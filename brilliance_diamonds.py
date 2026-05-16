from curl_cffi import requests
import time
import random
import csv
import os
import json
import math
import datetime
import statistics
from logger import get_logger
from models import BrillianceLabData, truncate_and_load_csv

log = get_logger("brilliance")

# ── CONFIG ───────────────────────────────────────────────────────────────────
URL           = "https://worker.brilliance.com/api/v1/lab-grown-diamond-search"
COUNT_URL     = "https://worker.brilliance.com/api/v1/lab-grown-diamond-search-count"
PER_PAGE      = 48             # diamonds per API page

base_dir = os.getcwd()
os.makedirs("diamond_files", exist_ok=True)

CSV_FILE        = f"{base_dir}/diamond_files/brilliance_diamonds.csv"
CHECKPOINT_FILE = f"{base_dir}/diamond_files/checkpoint.json"
BATCH_REPORT_FILE = f"{base_dir}/diamond_files/batch_reports.json"

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
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/148.0.0.0 Safari/537.36"
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

# ── FILE ROTATION ─────────────────────────────────────────────────────────────
def rotate_old_file(filepath):
    if not os.path.exists(filepath):
        return False

    mtime    = os.path.getmtime(filepath)
    mod_date = datetime.datetime.fromtimestamp(mtime)
    age      = datetime.datetime.now() - mod_date

    if age.total_seconds() <= 86400:
        log.info(f"File is fresh ({age.seconds // 3600}h {(age.seconds % 3600) // 60}m old) — continuing.")
        return False

    date_str  = mod_date.strftime("%Y-%m-%d")
    stem, ext = os.path.splitext(filepath)
    new_name  = f"{stem}_{date_str}{ext}"

    counter = 1
    while os.path.exists(new_name):
        new_name = f"{stem}_{date_str}_{counter}{ext}"
        counter += 1

    os.rename(filepath, new_name)
    log.info(f"Old file renamed → {os.path.basename(new_name)}  (was {age.days}d {age.seconds // 3600}h old)")

    if os.path.exists(CHECKPOINT_FILE):
        os.remove(CHECKPOINT_FILE)
        log.info("Checkpoint cleared — will start from page 1")

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
    with open(CHECKPOINT_FILE, "w") as f:
        json.dump({"last_completed_page": page, "total_inserted": total_inserted}, f, indent=2)
    log.debug(f"Checkpoint saved → page={page}, total={total_inserted}")


def load_checkpoint():
    if not os.path.exists(CHECKPOINT_FILE):
        return 1, 0
    with open(CHECKPOINT_FILE, "r") as f:
        data = json.load(f)
    page  = data.get("last_completed_page", 1) + 1
    total = data.get("total_inserted", 0)
    log.info(f"Resuming from page {page} (previously inserted: {total})")
    return page, total


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


# ── MAIN SCRAPER ──────────────────────────────────────────────────────────────
def brilliance_diamonds_scraper():
    log.info("=" * 60)
    log.info("Starting Brilliance scraper")

    # ── Step 1: rotate old CSV if older than 1 day ───────────────
    rotated = rotate_old_file(CSV_FILE)
    if rotated:
        log.info("Fresh run — old data archived, starting from page 1.")

    # ── Step 2: fetch total pages from count API ─────────────────
    total_pages, total_count = fetch_total_pages()

    # ── Step 3: load existing state ──────────────────────────────
    existing_certs             = load_existing_certs()
    file, writer               = get_writer()
    start_page, total_inserted = load_checkpoint()

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

    except Exception as e:
        log.error(f"Failed on page {page}: {e}")
        log.info("checkpoint.json saved — re-run to resume from last page")

    finally:
        file.close()
        log.info("CSV saved & closed")

    # ── grand final summary ──────────────────────────────────────────────────
    grand_wall = time.perf_counter() - grand_start
    print_final_summary(all_reports, grand_wall, total_count)

    with open(BATCH_REPORT_FILE, "w") as bf:
        json.dump(all_reports, bf, indent=2)

    print(f"    Full batch report → {BATCH_REPORT_FILE}")
    print(f"    Diamond CSV       → {CSV_FILE}\n")

    # ── Load CSV into DB (TRUNCATE + bulk INSERT) ────────────────────────────
    try:
        db_inserted = truncate_and_load_csv(BrillianceLabData, CSV_FILE, logger=log)
        print(f"    DB rows inserted  → {db_inserted:,} into `brilliance_lab`\n")
    except Exception as e:
        log.error(f"DB load failed: {e}")

    log.info("=" * 60)

    return {"total_inserted": total_inserted, "total_pages": total_pages}


# ── RUN ───────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    brilliance_diamonds_scraper()