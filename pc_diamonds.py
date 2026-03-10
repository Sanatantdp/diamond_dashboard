import requests
import csv
import time
import os
import json
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from logger import get_logger

log = get_logger("precious_carbon")

# ── Config ─────────────────────────────────────────────────────────
URL        = "https://py.preciouscarbon.com/graphql/"
PAGE_SIZE  = 20        # API returns 20 per page
WORKERS    = 20        # concurrent page fetches
SAVE_EVERY = 100       # flush CSV & progress every N pages

base_dir   = os.getcwd()
down_files = os.path.join(base_dir, "diamond_files")
os.makedirs(down_files, exist_ok=True)

OUTPUT_CSV    = os.path.join(down_files, "precious_carbon.csv")
PROGRESS_FILE = os.path.join(down_files, "pc_progress.json")

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
    "User-Agent":         "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36",
    "sec-ch-ua":          '"Not:A-Brand";v="99", "Google Chrome";v="145", "Chromium";v="145"',
    "sec-ch-ua-mobile":   "?0",
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


# ══════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════

def make_session() -> requests.Session:
    s = requests.Session()
    adapter = requests.adapters.HTTPAdapter(
        pool_connections=WORKERS + 5,
        pool_maxsize=WORKERS + 5,
        max_retries=requests.adapters.Retry(
            total=4, backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504]
        )
    )
    s.mount("https://", adapter)
    s.mount("http://",  adapter)
    s.headers.update(HEADERS)
    return s


def build_query(page: int) -> dict:
    return {
        "query": """
    {
      diamondData(
        page: "%s",
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
        currencyFlag:"USD"
      ) {
        dataCount
        diamondsReturned
        pageNo
        diamond {
          diamondId shape caratWeight color clarity cut symmetry polish
          deptPerc tablePerc length width depth ratio girdleMin girdleMax
          culet fancyColor flourIntensity certificateFile
          finalPriceUsd finalPriceEur finalPriceGbp
          certificateNumber stockNumber diamondImage diamondVideo
          diamondType lab currencyCode currencySymbol
        }
      }
    }""" % page
    }


# ── Progress helpers ────────────────────────────────────────────────

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
                "updated_at":  datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }, f)
    except Exception as e:
        log.warning(f"Could not save progress: {e}")


# ── Load existing cert numbers to deduplicate ───────────────────────

def load_existing_certs(csv_path: str) -> set:
    seen = set()
    if not os.path.exists(csv_path) or os.path.getsize(csv_path) == 0:
        return seen
    try:
        with open(csv_path, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                cert = str(row.get("certificateNumber", "")).strip()
                if cert:
                    seen.add(cert)
        log.info(f"Existing CSV has {len(seen):,} unique certificate numbers — duplicates will be skipped")
    except Exception as e:
        log.warning(f"Could not read existing CSV for deduplication: {e}")
    return seen


# ── Fetch a single page ─────────────────────────────────────────────

def fetch_page(page: int, session: requests.Session) -> tuple[int, list | None]:
    """Returns (page_number, list_of_diamonds or None on failure)."""
    for attempt in range(1, 5):
        try:
            resp = session.post(URL, json=build_query(page), timeout=30)
            resp.raise_for_status()
            data         = resp.json()
            diamond_data = data.get("data", {}).get("diamondData", {})
            diamonds     = diamond_data.get("diamond") or []
            return page, diamonds
        except requests.exceptions.RequestException as e:
            wait = 2 ** attempt   # 2, 4, 8, 16s
            log.warning(f"[page {page}] attempt {attempt}/4 failed: {e} — wait {wait}s")
            time.sleep(wait)
        except (json.JSONDecodeError, KeyError) as e:
            log.error(f"[page {page}] parse error: {e}")
            return page, None
    log.error(f"[page {page}] all retries exhausted — skipping")
    return page, None


# ══════════════════════════════════════════════════════════════
# PHASE 1 — discover total pages (fetch page 1 sequentially)
# ══════════════════════════════════════════════════════════════

def get_total_pages(session: requests.Session) -> int:
    log.info("Fetching page 1 to determine total count…")
    _, diamonds = fetch_page(1, session)
    if diamonds is None:
        raise RuntimeError("Failed to fetch page 1 — cannot determine total pages")

    # Re-fetch with full response to get dataCount
    resp         = session.post(URL, json=build_query(1), timeout=30)
    diamond_data = resp.json().get("data", {}).get("diamondData", {})
    total_count  = int(diamond_data.get("dataCount", 0))
    total_pages  = (total_count + PAGE_SIZE - 1) // PAGE_SIZE

    log.info(f"Total diamonds : {total_count:,}")
    log.info(f"Total pages    : {total_pages:,}")
    return total_pages


# ══════════════════════════════════════════════════════════════
# MAIN SCRAPER
# ══════════════════════════════════════════════════════════════

def scrape():
    log.info("=" * 50)
    log.info("Starting PreciousCarbon scraper (concurrent)")
    log.info(f"Workers : {WORKERS}")
    log.info(f"Output  → {OUTPUT_CSV}")

    session  = make_session()
    progress = load_progress()

    done_pages  = set(progress.get("done_pages", []))
    total_saved = progress.get("total_saved", 0)
    total_pages = progress.get("total_pages") or get_total_pages(session)

    # Load existing certs for dedup
    seen_certs = load_existing_certs(OUTPUT_CSV)

    # Determine remaining pages
    all_pages   = list(range(1, total_pages + 1))
    remaining   = [p for p in all_pages if p not in done_pages]

    log.info(f"Pages done     : {len(done_pages):,}")
    log.info(f"Pages remaining: {len(remaining):,}")

    if not remaining:
        log.info("All pages already fetched — nothing to do.")
        return

    # Open CSV for append (or write if fresh)
    file_exists = os.path.exists(OUTPUT_CSV) and os.path.getsize(OUTPUT_CSV) > 0
    write_mode  = "a" if file_exists and done_pages else "w"
    log.info(f"CSV mode: {'APPEND' if write_mode == 'a' else 'NEW'}")

    csv_file = open(OUTPUT_CSV, write_mode, newline="", encoding="utf-8")
    writer   = csv.DictWriter(csv_file, fieldnames=CSV_COLS, extrasaction="ignore")
    if write_mode == "w":
        writer.writeheader()

    # Shared state
    lock        = Lock()
    saved_count = [0]
    skip_count  = [0]
    fail_count  = [0]
    completed   = [0]
    total_rem   = len(remaining)

    def process_page(page):
        return fetch_page(page, session)

    with ThreadPoolExecutor(max_workers=WORKERS) as executor:
        futures = {executor.submit(process_page, p): p for p in remaining}

        for future in as_completed(futures):
            page, diamonds = future.result()

            with lock:
                completed[0] += 1
                pct = (completed[0] / total_rem * 100)

                if diamonds is None:
                    fail_count[0] += 1
                    log.error(f"[{completed[0]:>6}/{total_rem}] page {page:>5} FAILED")
                    done_pages.add(page)   # mark as done to not retry forever
                    continue

                # ── Deduplicate ───────────────────────────────
                new_rows  = []
                for d in diamonds:
                    cert = str(d.get("certificateNumber", "")).strip()
                    if cert and cert not in seen_certs:
                        seen_certs.add(cert)
                        new_rows.append(d)
                    else:
                        skip_count[0] += 1

                if new_rows:
                    writer.writerows(new_rows)
                    csv_file.flush()
                    saved_count[0] += len(new_rows)

                done_pages.add(page)
                total_saved_now = total_saved + saved_count[0]

                log.info(
                    f"[{completed[0]:>6}/{total_rem}] "
                    f"page {page:>5} | "
                    f"+{len(new_rows):>2} new | "
                    f"skipped {len(diamonds)-len(new_rows)} dupes | "
                    f"total saved: {total_saved_now:,} | "
                    f"{pct:.1f}%"
                )

                # Save progress every SAVE_EVERY pages
                if completed[0] % SAVE_EVERY == 0:
                    save_progress(done_pages, total_saved_now, total_pages)
                    log.debug(f"Progress checkpoint saved at {completed[0]} pages")

    csv_file.close()

    final_saved = total_saved + saved_count[0]
    save_progress(done_pages, final_saved, total_pages)

    log.info("=" * 50)
    log.info(f"DONE")
    log.info(f"  New rows saved  : {saved_count[0]:,}")
    log.info(f"  Duplicates skip : {skip_count[0]:,}")
    log.info(f"  Pages failed    : {fail_count[0]:,}")
    log.info(f"  Total in CSV    : {final_saved:,}")
    log.info(f"  CSV → {OUTPUT_CSV}")
    log.info("=" * 50)

    return {
        "action":      "scrape_complete",
        "total_saved": final_saved,
        "new_rows":    saved_count[0],
        "duplicates":  skip_count[0],
        "failed_pages": fail_count[0],
        "output":      OUTPUT_CSV,
        "end_time":    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }


if __name__ == "__main__":
    scrape()