import requests
import csv
import time
import os
import json
import math
import random
import statistics
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from logger import get_logger

log = get_logger("precious_carbon")

# ── Config ─────────────────────────────────────────────────────────
URL       = "https://py.preciouscarbon.com/graphql/"
PAGE_SIZE = 2000
WORKERS   = 20
SAVE_EVERY = 100

BATCH_SIZE      = 500          # API calls per batch before sleeping
BATCH_SLEEP_MIN = 120          # 2 min
BATCH_SLEEP_MAX = 300          # 5 min

from pathlib import Path
base_dir   = str(Path(__file__).resolve().parent.parent)
down_files = os.path.join(base_dir, "diamond_files", "usa")
backup_dir = os.path.join(base_dir, "diamond_files_backup")
os.makedirs(down_files, exist_ok=True)
os.makedirs(backup_dir, exist_ok=True)

OUTPUT_CSV        = os.path.join(down_files, "precious_carbon.csv")
PROGRESS_FILE     = os.path.join(down_files, "pc_progress.json")
BATCH_REPORT_FILE = os.path.join(down_files, "pc_batch_reports.json")
BACKUP_DIR        = backup_dir

# ── Backup config ────────────────────────────────────────────────────────────
STALE_THRESHOLD_DAYS = 1     # CSV older than this -> archive + full re-scrape

HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8",
    "Connection": "keep-alive",
    "Content-Type": "application/json",
    "Origin": "https://preciouscarbon.com",
    "Referer": "https://preciouscarbon.com/",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-site",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36",
    "sec-ch-ua": '"Not:A-Brand";v="99", "Google Chrome";v="145", "Chromium";v="145"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
}

CSV_COLS = [
    "diamondId", "shape", "caratWeight", "color", "clarity", "cut",
    "symmetry", "polish", "deptPerc", "tablePerc", "length", "width",
    "depth", "ratio", "girdleMin", "girdleMax", "culet", "fancyColor",
    "flourIntensity", "certificateFile", "finalPriceUsd", "finalPriceEur",
    "finalPriceGbp", "certificateNumber", "stockNumber", "diamondImage",
    "diamondVideo", "diamondType", "lab", "currencyCode", "currencySymbol",
]


# ── File backup / rotation ─────────────────────────────────────────
def csv_age_days(filepath):
    """Return CSV age in days, or None if missing."""
    if not os.path.exists(filepath):
        return None
    mtime = os.path.getmtime(filepath)
    return (datetime.now() - datetime.fromtimestamp(mtime)).total_seconds() / 86400


def archive_to_backup(filepath, threshold_days=STALE_THRESHOLD_DAYS):
    """
    If `filepath` exists AND is older than `threshold_days`, move it into
    BACKUP_DIR with a date suffix and clear progress + batch reports so the
    next scrape restarts from page 1. Returns True if archived, False otherwise.
    """
    if not os.path.exists(filepath):
        log.info(f"No existing CSV at {filepath} — fresh scrape will start from page 1.")
        return False

    mtime    = os.path.getmtime(filepath)
    mod_date = datetime.fromtimestamp(mtime)
    age      = datetime.now() - mod_date
    age_days = age.total_seconds() / 86400

    if age_days < threshold_days:
        log.info(
            f"CSV is fresh ({age.days}d {age.seconds // 3600}h old, "
            f"threshold: {threshold_days}d) — skipping backup."
        )
        return False

    os.makedirs(BACKUP_DIR, exist_ok=True)
    date_str = mod_date.strftime("%Y-%m-%d")
    stem     = os.path.splitext(os.path.basename(filepath))[0]
    ext      = os.path.splitext(filepath)[1]
    dest     = os.path.join(BACKUP_DIR, f"{stem}_{date_str}{ext}")

    counter = 1
    while os.path.exists(dest):
        dest = os.path.join(BACKUP_DIR, f"{stem}_{date_str}_{counter}{ext}")
        counter += 1

    os.rename(filepath, dest)
    log.info(f"Archived old CSV -> {dest}  (was {age.days}d {age.seconds // 3600}h old)")

    if os.path.exists(PROGRESS_FILE):
        os.remove(PROGRESS_FILE)
        log.info("Progress file cleared — will start from page 1")

    if os.path.exists(BATCH_REPORT_FILE):
        os.remove(BATCH_REPORT_FILE)
        log.info("Batch report cleared")

    return True


def make_session() -> requests.Session:
    s = requests.Session()
    adapter = requests.adapters.HTTPAdapter(
        pool_connections=WORKERS + 15,
        pool_maxsize=WORKERS + 15,
        max_retries=requests.adapters.Retry(
            total=6,
            backoff_factor=0.7,
            status_forcelist=[429, 500, 502, 503, 504, 520]
        )
    )
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    s.headers.update(HEADERS)
    return s


def build_query(page: int) -> dict:
    return {
        "query": f"""
        {{
          diamondData(
            page: "{page}",
            shapes:"",
            color:"k,j,i,h,g,f,e,d",
            colorflag:"",
            carat:"0.5,10",
            clarity:"fl,if,vvs1,vvs2,vs1,vs2,si1",
            symmetry:"excellent,very-good,good",
            polish:"excellent,very-good,good",
            price:"80,100000",
            table:"45.00,100.00",
            depthPer:"45.00,100.00",
            cut:"",
            fluoroscence:"none,faint,medium,strong",
            lab:"",
            diamondType:"lab",
            sortBy:"asc",
            orderBy:"price",
            currencyFlag:"USD",
            pageSize:"2000"
          ) {{
            dataCount
            diamondsReturned
            pageNo
            diamond {{
              diamondId shape caratWeight color clarity cut symmetry polish
              deptPerc tablePerc length width depth ratio girdleMin girdleMax
              culet fancyColor flourIntensity certificateFile
              finalPriceUsd finalPriceEur finalPriceGbp
              certificateNumber stockNumber diamondImage diamondVideo
              diamondType lab currencyCode currencySymbol
            }}
          }}
        }}
        """
    }


# ── Progress & Deduplication ───────────────────────────────────────
def load_progress():
    if os.path.exists(PROGRESS_FILE):
        try:
            with open(PROGRESS_FILE, encoding="utf-8") as f:
                data = json.load(f)
            log.info(f"Resuming — {len(data.get('done_pages', [])):,} pages already done")
            return data
        except Exception as e:
            log.warning(f"Could not load progress file: {e}")
    return {"done_pages": [], "total_saved": 0, "total_pages": None}


def save_progress(done_pages: set, total_saved: int, total_pages: int, total_count: int | None = None):
    try:
        payload = {
            "done_pages":  sorted(done_pages),
            "total_saved": total_saved,
            "total_pages": total_pages,
            "updated_at":  datetime.now().isoformat(),
        }
        if total_count is not None:
            payload["total_count"] = total_count
        elif os.path.exists(PROGRESS_FILE):
            # Preserve any previously-stored total_count when caller didn't pass one
            try:
                with open(PROGRESS_FILE, encoding="utf-8") as f:
                    prev = json.load(f)
                if "total_count" in prev:
                    payload["total_count"] = prev["total_count"]
            except Exception:
                pass
        with open(PROGRESS_FILE, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)
    except Exception as e:
        log.warning(f"Failed to save progress: {e}")


def load_seen_certs(csv_path: str) -> set:
    seen = set()
    if not os.path.exists(csv_path) or os.path.getsize(csv_path) == 0:
        return seen
    try:
        with open(csv_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                cert = str(row.get("certificateNumber", "")).strip()
                if cert:
                    seen.add(cert)
        log.info(f"Loaded {len(seen):,} existing certificates")
    except Exception as e:
        log.warning(f"Failed to load existing certificates: {e}")
    return seen


# ── Fetch ──────────────────────────────────────────────────────────
def fetch_page(page: int, session: requests.Session):
    for attempt in range(1, 7):
        try:
            t0   = time.perf_counter()
            resp = session.post(URL, json=build_query(page), timeout=30)
            elapsed = time.perf_counter() - t0
            resp.raise_for_status()
            data     = resp.json()
            diamonds = data.get("data", {}).get("diamondData", {}).get("diamond") or []
            return page, diamonds, elapsed, True
        except requests.exceptions.RequestException as e:
            wait = (2 ** attempt) + 0.5
            log.warning(f"[Page {page}] Attempt {attempt}/6 failed: {e} — retry in {wait:.1f}s")
            time.sleep(wait)
        except Exception as e:
            log.error(f"[Page {page}] Unexpected error: {e}")
            break
    log.error(f"[Page {page}] All retries exhausted")
    return page, None, 0, False


def fetch_total_count(session: requests.Session) -> int:
    """Hit the API once and return the live `dataCount` (total diamonds)."""
    log.info("Fetching total diamond count…")
    resp         = session.post(URL, json=build_query(1), timeout=30)
    diamond_data = resp.json().get("data", {}).get("diamondData", {})
    return int(diamond_data.get("dataCount", 0))


def get_total_pages(session: requests.Session, total_count: int | None = None) -> tuple[int, int]:
    """Return (total_count, total_pages). Reuses a pre-fetched count if given."""
    if total_count is None:
        total_count = fetch_total_count(session)
    total_pages  = math.ceil(total_count / PAGE_SIZE)

    print(f"\n  {'═'*52}")
    print(f"  📦  PC COUNT RESULT")
    print(f"  {'═'*52}")
    print(f"  {'Total diamonds':<26}: {total_count:,}")
    print(f"  {'Per page':<26}: {PAGE_SIZE}")
    print(f"  {'Total pages':<26}: {total_pages:,}")
    print(f"  {'Batch size':<26}: {BATCH_SIZE} calls")
    print(f"  {'Total batches':<26}: {math.ceil(total_pages / BATCH_SIZE):,}")
    print(f"  {'Sleep between batches':<26}: {BATCH_SLEEP_MIN//60}–{BATCH_SLEEP_MAX//60} min (random)")
    print(f"  {'═'*52}\n")

    log.info(f"Total diamonds : {total_count:,}")
    log.info(f"Total pages    : {total_pages:,}")
    return total_count, total_pages


# ── Batch Report ───────────────────────────────────────────────────
def print_batch_report(batch_num, batch_stats, batch_wall, all_reports):
    succeeded  = batch_stats["success"]
    failed     = batch_stats["failed"]
    total      = succeeded + failed
    diamonds   = batch_stats["diamonds_saved"]
    skipped    = batch_stats["skipped"]
    times      = batch_stats["elapsed_times"] or [0]

    cum_ok  = sum(r["success"]        for r in all_reports) + succeeded
    cum_bad = sum(r["failed"]         for r in all_reports) + failed
    cum_tot = cum_ok + cum_bad
    cum_dia = sum(r["diamonds_saved"] for r in all_reports) + diamonds

    h, rem = divmod(int(batch_wall), 3600)
    m, s   = divmod(rem, 60)

    start_page = (batch_num - 1) * BATCH_SIZE + 1
    end_page   = start_page + total - 1

    print(f"\n{'═'*66}")
    print(f"  📊  BATCH {batch_num} REPORT  (pages {start_page:,} – {end_page:,})")
    print(f"{'═'*66}")
    print(f"  {'Metric':<32} {'This Batch':>14}  {'Cumulative':>14}")
    print(f"  {'-'*62}")
    print(f"  {'Total API calls':<32} {total:>14,}  {cum_tot:>14,}")
    print(f"  {'✅  Succeeded':<32} {succeeded:>14,}  {cum_ok:>14,}")
    print(f"  {'❌  Failed':<32} {failed:>14,}  {cum_bad:>14,}")
    print(f"  {'Success rate':<32} {round(succeeded/total*100,1) if total else 0:>13}%  "
          f"{round(cum_ok/cum_tot*100,1) if cum_tot else 0:>13}%")
    print(f"  {'💎 Diamonds saved':<32} {diamonds:>14,}  {cum_dia:>14,}")
    print(f"  {'⏭  Duplicates skipped':<32} {skipped:>14,}")
    print(f"  {'-'*62}")
    print(f"  {'Response time  min':<32} {min(times):.3f}s")
    print(f"  {'Response time  mean':<32} {statistics.mean(times):.3f}s")
    print(f"  {'Response time  median':<32} {statistics.median(times):.3f}s")
    print(f"  {'Response time  max':<32} {max(times):.3f}s")
    if len(times) >= 20:
        p95 = sorted(times)[int(len(times) * 0.95)]
        print(f"  {'Response time  p95':<32} {p95:.3f}s")
    print(f"  {'-'*62}")
    print(f"  {'Batch wall time':<32} {f'{h}h {m}m {s}s':>14}")
    print(f"{'═'*66}")

    report = {
        "batch":          batch_num,
        "pages":          f"{start_page}-{end_page}",
        "success":        succeeded,
        "failed":         failed,
        "success_pct":    round(succeeded / total * 100, 1) if total else 0,
        "diamonds_saved": diamonds,
        "skipped":        skipped,
        "wall_time_s":    round(batch_wall, 2),
        "response_times": {
            "min_s":    round(min(times), 3),
            "max_s":    round(max(times), 3),
            "mean_s":   round(statistics.mean(times), 3),
            "median_s": round(statistics.median(times), 3),
        }
    }

    log.info(f"Batch {batch_num} → success={succeeded} fail={failed} "
             f"diamonds={diamonds} wall={batch_wall:.1f}s")

    return report


# ── Final Summary ──────────────────────────────────────────────────
def print_final_summary(all_reports, grand_wall):
    total_calls   = sum(r["success"] + r["failed"] for r in all_reports)
    total_success = sum(r["success"]        for r in all_reports)
    total_failed  = sum(r["failed"]         for r in all_reports)
    total_dia     = sum(r["diamonds_saved"] for r in all_reports)
    total_skip    = sum(r["skipped"]        for r in all_reports)

    h, rem = divmod(int(grand_wall), 3600)
    m, s   = divmod(rem, 60)

    print(f"\n{'█'*66}")
    print(f"  🏁  FINAL SUMMARY — {len(all_reports)} batches complete")
    print(f"{'█'*66}")
    print(f"  {'Total API calls':<34}: {total_calls:,}")
    print(f"  {'✅  Succeeded':<34}: {total_success:,}  "
          f"({round(total_success/total_calls*100,1) if total_calls else 0}%)")
    print(f"  {'❌  Failed':<34}: {total_failed:,}  "
          f"({round(total_failed/total_calls*100,1) if total_calls else 0}%)")
    print(f"  {'💎 Diamonds saved':<34}: {total_dia:,}")
    print(f"  {'⏭  Duplicates skipped':<34}: {total_skip:,}")
    print(f"  {'Total wall time':<34}: {h}h {m}m {s}s")
    print(f"  {'Effective rate':<34}: "
          f"{total_calls / grand_wall * 60:.1f} calls/min")
    print(f"{'█'*66}")

    print(f"\n  Batch-by-batch success rate:")
    for r in all_reports:
        bar = "█" * int(r["success_pct"] / 5)
        print(f"    Batch {r['batch']:>3}  {bar:<20}  {r['success_pct']:>5}%  "
              f"(pages {r['pages']})")
    print()


# ── Run one batch of 500 pages concurrently ────────────────────────
def run_batch(pages, session, writer, csv_file, seen_certs,
              done_pages, total_saved_ref, total_pages,
              batch_num, total_remaining):
    """
    Submits `pages` concurrently with ThreadPoolExecutor.
    Returns batch_stats dict.
    """
    lock = Lock()

    batch_stats = {
        "success":       0,
        "failed":        0,
        "diamonds_saved": 0,
        "skipped":       0,
        "elapsed_times": [],
    }

    completed_in_batch = [0]

    with ThreadPoolExecutor(max_workers=WORKERS) as executor:
        futures = {executor.submit(fetch_page, p, session): p for p in pages}

        for future in as_completed(futures):
            page, diamonds, elapsed, ok = future.result()
            completed_in_batch[0] += 1

            with lock:
                if not ok or diamonds is None:
                    batch_stats["failed"] += 1
                    done_pages.add(page)
                    log.error(f"[Batch {batch_num}] Page {page:>5} ❌ FAILED")
                    continue

                batch_stats["success"] += 1
                batch_stats["elapsed_times"].append(elapsed)

                new_rows = []
                for d in diamonds:
                    cert = str(d.get("certificateNumber", "")).strip()
                    if cert and cert not in seen_certs:
                        seen_certs.add(cert)
                        new_rows.append(d)
                    else:
                        batch_stats["skipped"] += 1

                if new_rows:
                    writer.writerows(new_rows)
                    csv_file.flush()
                    batch_stats["diamonds_saved"] += len(new_rows)
                    total_saved_ref[0] += len(new_rows)

                done_pages.add(page)

                total_done = len(done_pages)
                pct = total_done / total_pages * 100

                log.info(
                    f"[Batch {batch_num}  {completed_in_batch[0]:>3}/{len(pages)}]  "
                    f"Page {page:>5}  |  "
                    f"+{len(new_rows):>3} new  |  "
                    f"skip {len(diamonds)-len(new_rows):>2}  |  "
                    f"total {total_saved_ref[0]:,}  |  "
                    f"{pct:.1f}%"
                )

                if total_done % SAVE_EVERY == 0:
                    save_progress(done_pages, total_saved_ref[0], total_pages)

    return batch_stats


# ── Main Scraper ───────────────────────────────────────────────────
def scrape():
    log.info("=" * 70)
    log.info("🚀 Starting PreciousCarbon Lab Diamond Scraper")
    log.info(f"Workers      : {WORKERS}")
    log.info(f"Batch size   : {BATCH_SIZE} pages")
    log.info(f"Batch sleep  : {BATCH_SLEEP_MIN//60}–{BATCH_SLEEP_MAX//60} min")
    log.info(f"Output file  : {OUTPUT_CSV}")

    session  = make_session()
    progress = load_progress()

    # ── Total-count gate ───────────────────────────────────────────
    # Compare the live total against what pc_progress.json last saw.
    #   - same count + all pages done   → already scraped, skip entirely
    #   - same count + pages remaining  → resume partial scrape
    #   - count changed                 → inventory shifted, wipe and re-scrape
    #   - no stored count               → first run, just save and proceed
    live_count   = fetch_total_count(session)
    stored_count = progress.get("total_count")

    if stored_count is not None and stored_count == live_count and progress.get("done_pages"):
        expected_pages = math.ceil(live_count / PAGE_SIZE)
        done_count     = len(progress.get("done_pages", []))
        if done_count >= expected_pages:
            log.info(
                f"✅ Diamond count unchanged ({live_count:,}) and all "
                f"{expected_pages:,} pages already scraped — nothing new. Exiting."
            )
            return
        log.info(
            f"⏯  Diamond count unchanged ({live_count:,}) — resuming partial "
            f"scrape ({done_count:,}/{expected_pages:,} pages done)."
        )

    if stored_count is not None and stored_count != live_count:
        log.info(
            f"⚠️  Diamond count changed: {stored_count:,} → {live_count:,}. "
            f"Wiping previous progress and starting fresh."
        )
        for path in (OUTPUT_CSV, PROGRESS_FILE, BATCH_REPORT_FILE):
            if os.path.exists(path):
                try:
                    os.remove(path)
                    log.info(f"  🗑 Deleted {path}")
                except Exception as e:
                    log.warning(f"  ⚠️ Could not delete {path}: {e}")
        progress    = {"done_pages": [], "total_saved": 0, "total_pages": None}

    total_count, total_pages = get_total_pages(session, total_count=live_count)

    done_pages  = set(progress.get("done_pages", []))
    total_saved = progress.get("total_saved", 0)

    seen_certs = load_seen_certs(OUTPUT_CSV)
    remaining  = [p for p in range(1, total_pages + 1) if p not in done_pages]

    log.info(f"Pages done     : {len(done_pages):,}")
    log.info(f"Pages remaining: {len(remaining):,}")

    # Persist the live count up-front so a crash before the first batch save
    # still records what we last saw.
    save_progress(done_pages, total_saved, total_pages, total_count=total_count)

    if not remaining:
        log.info("✅ All pages already scraped. Nothing to do.")
        return

    # ── CSV setup ──────────────────────────────────────────────────
    file_exists = os.path.exists(OUTPUT_CSV) and os.path.getsize(OUTPUT_CSV) > 0
    csv_file = open(OUTPUT_CSV, "a" if file_exists else "w", newline="", encoding="utf-8")
    writer   = csv.DictWriter(csv_file, fieldnames=CSV_COLS, extrasaction="ignore")
    if not file_exists:
        writer.writeheader()

    # ── Split remaining pages into batches of BATCH_SIZE ──────────
    batches     = [remaining[i:i + BATCH_SIZE] for i in range(0, len(remaining), BATCH_SIZE)]
    total_batches = len(batches)

    print(f"\n  {'═'*52}")
    print(f"  🚀  PRECIOUS CARBON SCRAPER")
    print(f"  {'═'*52}")
    print(f"  {'Total pages':<26}: {total_pages:,}")
    print(f"  {'Pages remaining':<26}: {len(remaining):,}")
    print(f"  {'Batch size':<26}: {BATCH_SIZE}")
    print(f"  {'Total batches':<26}: {total_batches}")
    print(f"  {'Workers per batch':<26}: {WORKERS}")
    print(f"  {'Sleep between batches':<26}: {BATCH_SLEEP_MIN//60}–{BATCH_SLEEP_MAX//60} min")
    print(f"  {'═'*52}\n")

    all_reports   = []
    grand_start   = time.perf_counter()
    total_saved_ref = [total_saved]

    try:
        for batch_idx, batch_pages in enumerate(batches, start=1):
            batch_num = batch_idx
            # account for already-done pages: find real batch number from page range
            actual_batch_num = (batch_pages[0] - 1) // BATCH_SIZE + 1

            print(f"\n  ▶  Batch {batch_num}/{total_batches}  "
                  f"(pages {batch_pages[0]:,} – {batch_pages[-1]:,}  |  "
                  f"{len(batch_pages)} calls)\n")
            log.info(f"Starting batch {batch_num}/{total_batches} "
                     f"— pages {batch_pages[0]}–{batch_pages[-1]}")

            batch_start = time.perf_counter()

            batch_stats = run_batch(
                pages          = batch_pages,
                session        = session,
                writer         = writer,
                csv_file       = csv_file,
                seen_certs     = seen_certs,
                done_pages     = done_pages,
                total_saved_ref = total_saved_ref,
                total_pages    = total_pages,
                batch_num      = batch_num,
                total_remaining = len(remaining),
            )

            batch_wall = time.perf_counter() - batch_start

            # ── print + save batch report ──────────────────────────
            report = print_batch_report(batch_num, batch_stats, batch_wall, all_reports)
            all_reports.append(report)

            with open(BATCH_REPORT_FILE, "w") as bf:
                json.dump(all_reports, bf, indent=2)

            save_progress(done_pages, total_saved_ref[0], total_pages)
            log.info(f"Batch {batch_num} report saved → {BATCH_REPORT_FILE}")

            # ── inter-batch sleep (skip after final batch) ─────────
            if batch_num < total_batches:
                sleep_secs = round(random.uniform(BATCH_SLEEP_MIN, BATCH_SLEEP_MAX), 1)
                sleep_min  = sleep_secs / 60

                print(f"\n  💤  Batch {batch_num}/{total_batches} complete — "
                      f"sleeping {sleep_min:.1f} min ({sleep_secs}s) "
                      f"before batch {batch_num + 1}…")
                print(f"  ⏰  Resume at: "
                      f"{(datetime.now() + timedelta(seconds=sleep_secs)).strftime('%H:%M:%S')}\n")

                log.info(f"Inter-batch sleep: {sleep_secs}s ({sleep_min:.1f} min)")
                time.sleep(sleep_secs)

    except KeyboardInterrupt:
        log.warning("Interrupted by user — progress saved, re-run to resume.")
    except Exception as e:
        log.error(f"Unexpected error: {e}")
        log.info("Progress saved — re-run to resume.")
    finally:
        csv_file.close()
        save_progress(done_pages, total_saved_ref[0], total_pages)
        log.info("CSV saved & closed")

    # ── Grand final summary ────────────────────────────────────────
    grand_wall = time.perf_counter() - grand_start
    print_final_summary(all_reports, grand_wall)

    with open(BATCH_REPORT_FILE, "w") as bf:
        json.dump(all_reports, bf, indent=2)

    print(f"  Batch report → {BATCH_REPORT_FILE}")
    print(f"  Diamond CSV  → {OUTPUT_CSV}\n")

    log.info("=" * 70)


if __name__ == "__main__":
    # Entry points (scheduling is handled externally by scheduler.py):
    #   (no args) / --resume  -> scrape() only: resume from progress, no archive
    #   --once                -> archive existing CSV (any age) and run a fresh full scrape
    import sys as _sys

    try:
        if "--once" in _sys.argv:
            archive_to_backup(OUTPUT_CSV, threshold_days=0)
            scrape()
        else:
            scrape()
    except KeyboardInterrupt:
        log.warning("Ctrl+C pressed — stopping PreciousCarbon scraper. Progress saved; re-run to resume.")
        _sys.exit(130)