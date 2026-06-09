from curl_cffi import requests as curl_requests
import csv, os, time, random, json, math, statistics, logging
import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        logger.setLevel(logging.DEBUG)
        fmt = logging.Formatter(
            "%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        ch.setFormatter(fmt)
        logger.addHandler(ch)
        log_dir = os.path.join(os.getcwd(), "logs")
        os.makedirs(log_dir, exist_ok=True)
        fh = logging.FileHandler(
            os.path.join(log_dir, f"{name}.log"), encoding="utf-8"
        )
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(fmt)
        logger.addHandler(fh)
    return logger


log = get_logger("rou")

# ── Config ────────────────────────────────────────────────────────────────────
URL       = "https://py.preciouscarbon.com/rou/graphql"
CSV_FILE  = "rouk_diamonds.csv"
PAGE_SIZE = 1000
WORKERS   = 20
SAVE_EVERY = 100

BATCH_SIZE      = 500    # pages per batch before report + sleep
BATCH_SLEEP_MIN = 120    # 2 min
BATCH_SLEEP_MAX = 300    # 5 min

from pathlib import Path
base_dir      = str(Path(__file__).resolve().parent)                              # uk_scrapping/
diamond_files = str(Path(__file__).resolve().parent.parent / "diamond_files" / "uk")
rouk_path     = os.path.join(base_dir, "rouK")

for d in (rouk_path, diamond_files):
    os.makedirs(d, exist_ok=True)

MAIN_CSV          = os.path.join(rouk_path,     CSV_FILE)
EXTRA_CSV         = os.path.join(diamond_files, CSV_FILE)
PROGRESS_FILE     = os.path.join(diamond_files, "rou_progress.json")
BATCH_REPORT_FILE = os.path.join(diamond_files, "rou_batch_reports.json")


# ── Auto-delete files older than 1 day ───────────────────────────────────────
def delete_if_older_than_1_day(file_path: str):
    if os.path.exists(file_path):
        file_time = datetime.datetime.fromtimestamp(os.path.getmtime(file_path))
        if datetime.datetime.now() - file_time > datetime.timedelta(days=1):
            os.remove(file_path)
            log.info(f"Deleted old file: {file_path}")


for _f in (MAIN_CSV, EXTRA_CSV, PROGRESS_FILE):
    delete_if_older_than_1_day(_f)


# ── Headers ───────────────────────────────────────────────────────────────────
HEADERS = {
    "Accept":             "application/json, text/plain, */*",
    "Accept-Language":    "en-GB,en-US;q=0.9,en;q=0.8",
    "Connection":         "keep-alive",
    "Content-Type":       "application/json",
    "Origin":             "https://preciouscarbon.com",
    "Referer":            "https://preciouscarbon.com/",
    "Sec-Fetch-Dest":     "empty",
    "Sec-Fetch-Mode":     "cors",
    "Sec-Fetch-Site":     "same-site",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/146.0.0.0 Safari/537.36"
    ),
    "sec-ch-ua":          '"Chromium";v="146", "Not-A.Brand";v="24", "Google Chrome";v="146"',
    "sec-ch-ua-mobile":   "?0",
    "sec-ch-ua-platform": '"Windows"',
}

# ── GraphQL query ─────────────────────────────────────────────────────────────
GQL_QUERY = """
    {{
      diamondData(
        page: "{page}",
        shapes:"",
        color:"d,e,f,g,h,i,j,k",
        colorflag:"",
        carat:"1.5,10",
        clarity:"fl,if,vvs1,vvs2,vs1,vs2,si1",
        symmetry:"excellent,very-good,good",
        polish:"excellent,very-good,good",
        price:"1,100000",
        table:"45.00,100.00",
        depthPer:"45.00,100.00",
        cut:"",
        fluoroscence:"none,faint,medium,strong",
        lab:"",
        diamondType:"lab",
        sortBy:"asc",
        orderBy:"price",
        currencyFlag:"GBP",
        pageSize: "{pagesize}"
        
      ) {{
        dataCount
        diamondsReturned
        pageNo
        diamond {{
          diamondId shape caratWeight color clarity cut symmetry polish
          deptPerc tablePerc length width depth ratio girdleMin girdleMax
          culet fancyColor flourIntensity certificateFile
          finalPriceGbp finalPriceUsd finalPricePound finalPriceEur
          certificateNumber stockNumber diamondImage diamondVideo
          diamondType lab currencyCode currencySymbol
        }}
      }}
    }}
"""

# ── CSV columns ───────────────────────────────────────────────────────────────
FIELDS = [
    "diamondId", "shape", "caratWeight", "color", "clarity", "cut",
    "symmetry", "polish", "deptPerc", "tablePerc", "length", "width",
    "depth", "ratio", "girdleMin", "girdleMax", "culet", "fancyColor",
    "flourIntensity", "certificateFile", "finalPriceUsd", "finalPriceEur",
    "finalPriceGbp", "certificateNumber", "stockNumber", "diamondImage",
    "diamondVideo", "diamondType", "lab", "currencyCode", "currencySymbol",
    "status", "page_no",
]


# ══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def make_session() -> curl_requests.Session:
    s = curl_requests.Session(impersonate="chrome")
    s.headers.update(HEADERS)
    return s


def build_payload(page: int) -> dict:
    return {"query": GQL_QUERY.format(page=page,pagesize=PAGE_SIZE)}


# ── Progress ──────────────────────────────────────────────────────────────────
def load_progress() -> dict:
    if os.path.exists(PROGRESS_FILE):
        try:
            with open(PROGRESS_FILE) as f:
                data = json.load(f)
            log.info(
                f"Progress file found — "
                f"done pages: {len(data.get('done_pages', []))}, "
                f"saved: {data.get('total_saved', 0):,}"
            )
            return data
        except Exception as e:
            log.warning(f"Could not read progress file: {e} — starting fresh")
    return {"done_pages": [], "total_saved": 0, "total_pages": None}


def save_progress(done_pages: set, total_saved: int, total_pages: int):
    try:
        with open(PROGRESS_FILE, "w") as f:
            json.dump({
                "done_pages":  sorted(done_pages),
                "total_saved": total_saved,
                "total_pages": total_pages,
                "updated_at":  datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }, f)
    except Exception as e:
        log.warning(f"Could not save progress: {e}")


def load_existing_certs(csv_path: str) -> set:
    seen: set = set()
    if not os.path.exists(csv_path) or os.path.getsize(csv_path) == 0:
        return seen
    try:
        with open(csv_path, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                cert = str(row.get("certificateNumber", "")).strip()
                if cert:
                    seen.add(cert)
        log.info(f"Existing CSV has {len(seen):,} unique diamonds")
    except Exception as e:
        log.warning(f"Could not read existing CSV: {e}")
    return seen


# ── Fetch ─────────────────────────────────────────────────────────────────────
def fetch_page(page: int, session: curl_requests.Session) -> tuple:
    for attempt in range(1, 5):
        try:
            t0           = time.perf_counter()
            resp         = session.post(URL, json=build_payload(page), timeout=30)
            elapsed      = time.perf_counter() - t0
            resp.raise_for_status()
            diamond_data = resp.json().get("data", {}).get("diamondData", {})
            diamonds     = diamond_data.get("diamond") or []
            time.sleep(random.uniform(0, 0.5))
            return page, diamonds, elapsed, True
        except Exception as e:
            wait = 2 ** attempt
            log.warning(f"[page {page}] attempt {attempt}/4 failed: {e} — wait {wait}s")
            time.sleep(wait)
    log.error(f"[page {page}] all retries exhausted — skipping")
    return page, None, 0, False


def get_total_pages(session: curl_requests.Session) -> int:
    log.info("Fetching page 1 to determine total count…")
    for attempt in range(1, 5):
        try:
            resp         = session.post(URL, json=build_payload(1), timeout=30)
            resp.raise_for_status()
            diamond_data = resp.json().get("data", {}).get("diamondData", {})
            total_count  = int(diamond_data.get("dataCount", 0))
            total_pages  = math.ceil(total_count / PAGE_SIZE)

            print(f"\n  {'═'*52}")
            print(f"  📦  ROUK COUNT RESULT")
            print(f"  {'═'*52}")
            print(f"  {'Total diamonds':<26}: {total_count:,}")
            print(f"  {'Per page':<26}: {PAGE_SIZE}")
            print(f"  {'Total pages':<26}: {total_pages:,}")
            print(f"  {'Batch size':<26}: {BATCH_SIZE} calls")
            print(f"  {'Total batches':<26}: {math.ceil(total_pages / BATCH_SIZE):,}")
            print(f"  {'Sleep between batches':<26}: {BATCH_SLEEP_MIN//60}–{BATCH_SLEEP_MAX//60} min")
            print(f"  {'═'*52}\n")

            log.info(f"Total diamonds : {total_count:,}")
            log.info(f"Total pages    : {total_pages:,}")
            return total_pages
        except Exception as e:
            wait = 2 ** attempt
            log.warning(f"get_total_pages attempt {attempt}/4 failed: {e} — wait {wait}s")
            time.sleep(wait)
    raise RuntimeError("Failed to fetch page 1 — cannot determine total pages")


# ── Row builder ───────────────────────────────────────────────────────────────
def make_row(d: dict, status: str, page: int) -> dict:
    return {
        "diamondId":         d.get("diamondId"),
        "shape":             d.get("shape"),
        "caratWeight":       d.get("caratWeight"),
        "color":             d.get("color"),
        "clarity":           d.get("clarity"),
        "cut":               d.get("cut"),
        "symmetry":          d.get("symmetry"),
        "polish":            d.get("polish"),
        "deptPerc":          d.get("deptPerc"),
        "tablePerc":         d.get("tablePerc"),
        "length":            d.get("length"),
        "width":             d.get("width"),
        "depth":             d.get("depth"),
        "ratio":             d.get("ratio"),
        "girdleMin":         d.get("girdleMin"),
        "girdleMax":         d.get("girdleMax"),
        "culet":             d.get("culet"),
        "fancyColor":        d.get("fancyColor"),
        "flourIntensity":    d.get("flourIntensity"),
        "certificateFile":   d.get("certificateFile"),
        "finalPriceUsd":     d.get("finalPriceUsd"),
        "finalPriceEur":     d.get("finalPriceEur"),
        "finalPriceGbp":     d.get("finalPriceGbp"),
        "certificateNumber": d.get("certificateNumber"),
        "stockNumber":       d.get("stockNumber"),
        "diamondImage":      d.get("diamondImage"),
        "diamondVideo":      d.get("diamondVideo"),
        "diamondType":       d.get("diamondType"),
        "lab":               d.get("lab"),
        "currencyCode":      d.get("currencyCode"),
        "currencySymbol":    d.get("currencySymbol"),
        "status":            status,
        "page_no":           page,
    }


# ── Batch Report ──────────────────────────────────────────────────────────────
def print_batch_report(batch_num, batch_stats, batch_wall, all_reports):
    succeeded = batch_stats["success"]
    failed    = batch_stats["failed"]
    total     = succeeded + failed
    diamonds  = batch_stats["diamonds_saved"]
    skipped   = batch_stats["skipped"]
    times     = batch_stats["elapsed_times"] or [0]

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


# ── Final Summary ─────────────────────────────────────────────────────────────
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


# ── Run one batch of 500 pages concurrently ───────────────────────────────────
def run_batch(pages, session, writer_main, writer_extra,
              f_main, f_extra, seen_certs,
              done_pages, total_saved_ref, total_pages, batch_num):

    lock = Lock()
    batch_stats = {
        "success":        0,
        "failed":         0,
        "diamonds_saved": 0,
        "skipped":        0,
        "elapsed_times":  [],
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

                new_rows     = []
                dup_pg_count = 0
                for d in diamonds:
                    cert = str(d.get("certificateNumber", "")).strip()
                    if cert and cert not in seen_certs:
                        seen_certs.add(cert)
                        new_rows.append(make_row(d, "new", page))
                    else:
                        batch_stats["skipped"] += 1
                        dup_pg_count += 1

                if new_rows:
                    writer_main.writerows(new_rows)
                    writer_extra.writerows(new_rows)
                    f_main.flush()
                    f_extra.flush()
                    batch_stats["diamonds_saved"] += len(new_rows)
                    total_saved_ref[0] += len(new_rows)

                done_pages.add(page)
                pct = len(done_pages) / total_pages * 100

                log.info(
                    f"[Batch {batch_num}  {completed_in_batch[0]:>3}/{len(pages)}]  "
                    f"Page {page:>5}  |  "
                    f"+{len(new_rows):>2} new  |  "
                    f"skip {dup_pg_count}  |  "
                    f"total {total_saved_ref[0]:,}  |  "
                    f"{pct:.1f}%"
                )

                if len(done_pages) % SAVE_EVERY == 0:
                    save_progress(done_pages, total_saved_ref[0], total_pages)

    return batch_stats


# ══════════════════════════════════════════════════════════════════════════════
# MAIN SCRAPER
# ══════════════════════════════════════════════════════════════════════════════
def scrape():
    log.info("=" * 55)
    log.info("Starting ROUK scraper")
    log.info(f"Workers      : {WORKERS}")
    log.info(f"Batch size   : {BATCH_SIZE} pages")
    log.info(f"Batch sleep  : {BATCH_SLEEP_MIN//60}–{BATCH_SLEEP_MAX//60} min")
    log.info(f"Output  → {MAIN_CSV}")
    log.info(f"Mirror  → {EXTRA_CSV}")

    session  = make_session()
    progress = load_progress()

    done_pages  = set(progress.get("done_pages", []))
    total_saved = progress.get("total_saved", 0)
    total_pages = progress.get("total_pages") or get_total_pages(session)

    seen_certs = load_existing_certs(MAIN_CSV)
    remaining  = [p for p in range(1, total_pages + 1) if p not in done_pages]

    log.info(f"Pages done     : {len(done_pages):,}")
    log.info(f"Pages remaining: {len(remaining):,}")

    if not remaining:
        log.info("All pages already fetched — nothing to do.")
        return {"total_saved": total_saved, "new_rows": 0}

    # ── Split into batches of BATCH_SIZE ─────────────────────────────────────
    batches       = [remaining[i:i + BATCH_SIZE] for i in range(0, len(remaining), BATCH_SIZE)]
    total_batches = len(batches)

    print(f"\n  {'═'*52}")
    print(f"  🚀  ROUK SCRAPER")
    print(f"  {'═'*52}")
    print(f"  {'Total pages':<26}: {total_pages:,}")
    print(f"  {'Pages remaining':<26}: {len(remaining):,}")
    print(f"  {'Batch size':<26}: {BATCH_SIZE}")
    print(f"  {'Total batches':<26}: {total_batches}")
    print(f"  {'Workers per batch':<26}: {WORKERS}")
    print(f"  {'Sleep between batches':<26}: {BATCH_SLEEP_MIN//60}–{BATCH_SLEEP_MAX//60} min")
    print(f"  {'═'*52}\n")

    # ── CSV setup ─────────────────────────────────────────────────────────────
    file_exists = os.path.exists(MAIN_CSV) and os.path.getsize(MAIN_CSV) > 0
    write_mode  = "a" if file_exists else "w"
    log.info(f"CSV mode: {'APPEND' if write_mode == 'a' else 'NEW'}")

    f_main       = open(MAIN_CSV,  write_mode, newline="", encoding="utf-8")
    f_extra      = open(EXTRA_CSV, write_mode, newline="", encoding="utf-8")
    writer_main  = csv.DictWriter(f_main,  fieldnames=FIELDS, extrasaction="ignore")
    writer_extra = csv.DictWriter(f_extra, fieldnames=FIELDS, extrasaction="ignore")
    if write_mode == "w":
        writer_main.writeheader()
        writer_extra.writeheader()

    all_reports     = []
    grand_start     = time.perf_counter()
    total_saved_ref = [total_saved]

    try:
        for batch_idx, batch_pages in enumerate(batches, start=1):
            print(f"\n  ▶  Batch {batch_idx}/{total_batches}  "
                  f"(pages {batch_pages[0]:,} – {batch_pages[-1]:,}  |  "
                  f"{len(batch_pages)} calls)\n")
            log.info(f"Starting batch {batch_idx}/{total_batches} "
                     f"— pages {batch_pages[0]}–{batch_pages[-1]}")

            batch_start = time.perf_counter()

            batch_stats = run_batch(
                pages           = batch_pages,
                session         = session,
                writer_main     = writer_main,
                writer_extra    = writer_extra,
                f_main          = f_main,
                f_extra         = f_extra,
                seen_certs      = seen_certs,
                done_pages      = done_pages,
                total_saved_ref = total_saved_ref,
                total_pages     = total_pages,
                batch_num       = batch_idx,
            )

            batch_wall = time.perf_counter() - batch_start

            # ── batch report + save ───────────────────────────────────────────
            report = print_batch_report(batch_idx, batch_stats, batch_wall, all_reports)
            all_reports.append(report)

            with open(BATCH_REPORT_FILE, "w") as bf:
                json.dump(all_reports, bf, indent=2)

            save_progress(done_pages, total_saved_ref[0], total_pages)
            log.info(f"Batch {batch_idx} report saved → {BATCH_REPORT_FILE}")

            # ── inter-batch sleep (skip after final batch) ────────────────────
            if batch_idx < total_batches:
                sleep_secs = round(random.uniform(BATCH_SLEEP_MIN, BATCH_SLEEP_MAX), 1)
                sleep_min  = sleep_secs / 60
                resume_at  = (datetime.datetime.now() +
                               datetime.timedelta(seconds=sleep_secs)).strftime("%H:%M:%S")

                print(f"\n  💤  Batch {batch_idx}/{total_batches} complete — "
                      f"sleeping {sleep_min:.1f} min ({sleep_secs}s) "
                      f"before batch {batch_idx + 1}…")
                print(f"  ⏰  Resume at: {resume_at}\n")
                log.info(f"Inter-batch sleep: {sleep_secs}s ({sleep_min:.1f} min)")
                time.sleep(sleep_secs)

    except VPNRotateNeeded:
        # propagate up to run_with_vpn_rotation() so it can switch tunnels
        raise
    except KeyboardInterrupt:
        log.warning("Interrupted — progress saved, re-run to resume.")
    except Exception as e:
        log.error(f"Unexpected error: {e}")
        log.info("Progress saved — re-run to resume.")
    finally:
        f_main.close()
        f_extra.close()
        save_progress(done_pages, total_saved_ref[0], total_pages)
        log.info("CSVs saved & closed")

    # ── Grand final summary ───────────────────────────────────────────────────
    grand_wall = time.perf_counter() - grand_start
    print_final_summary(all_reports, grand_wall)

    with open(BATCH_REPORT_FILE, "w") as bf:
        json.dump(all_reports, bf, indent=2)

    print(f"  📁  Batch report → {BATCH_REPORT_FILE}")
    print(f"  📁  Main CSV     → {MAIN_CSV}")
    print(f"  📁  Mirror CSV   → {EXTRA_CSV}\n")
    log.info("=" * 55)

    return {
        "action":       "scrape_complete",
        "total_saved":  total_saved_ref[0],
        "new_rows":     sum(r["diamonds_saved"] for r in all_reports),
        "duplicates":   sum(r["skipped"]        for r in all_reports),
        "failed_pages": sum(r["failed"]         for r in all_reports),
        "output":       MAIN_CSV,
        "end_time":     datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }


if __name__ == "__main__":
    try:
        scrape()
    except KeyboardInterrupt:
        log.warning("Ctrl+C — stopping scraper.")
