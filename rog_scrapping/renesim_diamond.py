from curl_cffi import requests as curl_requests
import csv, os, time, random, json, logging
import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock, Event

from status_writer import write_status


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

log = get_logger("renesim")

# ── Config ────────────────────────────────────────────────────────────────────
SCRAPER_NAME = "renesim"
URL          = "https://py.yorxs.de/live/graphql/"
CSV_FILE     = "renesim_eur.csv"          # ← no date in filename
BASE_DIR     = os.getcwd()

PAGE_SIZE  = 20
WORKERS    = 5
TIMEOUT    = 60
SAVE_EVERY = 50

DELAY_MIN       = 1.5
DELAY_MAX       = 3.0
RATELIMIT_PAUSE = 60

renesim_path  = os.path.join(BASE_DIR, "renesim")
diamond_files = os.path.join(BASE_DIR, "diamond_files")

for d in (renesim_path, diamond_files):
    os.makedirs(d, exist_ok=True)

MAIN_CSV      = os.path.join(renesim_path,  CSV_FILE)
EXTRA_CSV     = os.path.join(diamond_files, CSV_FILE)
PROGRESS_FILE = os.path.join(diamond_files, "renesim_progress.json")

pause_event = Event()
pause_event.set()


# ── Headers ───────────────────────────────────────────────────────────────────
HEADERS = {
    "Accept":             "*/*",
    "Accept-Language":    "en-GB,en-US;q=0.9,en;q=0.8",
    "Connection":         "keep-alive",
    "Content-Type":       "application/json",
    "Origin":             "https://www.renesim.com",
    "Referer":            "https://www.renesim.com/",
    "Sec-Fetch-Dest":     "empty",
    "Sec-Fetch-Mode":     "cors",
    "Sec-Fetch-Site":     "cross-site",
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
GQL_QUERY = """{{
  diamondData(
    page: "{page}",
    shapes: "",
    color: "",
    carat: "0.19, 43.12",
    clarity: "",
    symmetry: "",
    polish: "",
    price: "100, 2000000",
    table: "5.1,93",
    depthPer: "5.26,99.10",
    cut: "",
    fluoroscence: "",
    lab: "",
    diamondType: "lab-white",
    currencyFlag: "EUR",
    bulkPageData: "no",
    orderBy: "final_price_eur",
    langCode: "DE",
    taxPercentage: "8",
    sortBy: "asc"
  ) {{
    dataCount
    diamondsReturned
    pageNo
    diamond {{
      diamondId
      fancyColorIntensity
      fancyColorOvertone
      shape
      caratWeight
      color
      fancyColor
      clarity
      cut
      symmetry
      polish
      deptPerc
      tablePerc
      length
      width
      depth
      ratio
      measurement
      girdleMin
      girdleMax
      girdleCondition
      culet
      culetCondition
      flourIntensity
      certificateFile
      lab
      finalPriceEur
      finalPriceEurTax
      finalPriceUsd
      originalTotalPriceEur
      pricePerCtEur
      certificateNumber
      stockNumber
      diamondImage
      diamondVideo
      diamondType
      currencyCode
      currencySymbol
    }}
  }}
}}"""

# ── CSV columns ───────────────────────────────────────────────────────────────
FIELDS = [
    "diamondId", "stockNumber", "certificateNumber",
    "shape", "caratWeight",
    "color", "fancyColor", "fancyColorIntensity", "fancyColorOvertone",
    "clarity", "cut", "symmetry", "polish",
    "deptPerc", "tablePerc",
    "length", "width", "depth", "ratio", "measurement",
    "girdleMin", "girdleMax", "girdleCondition",
    "culet", "culetCondition",
    "flourIntensity",
    "lab", "certificateFile",
    "finalPriceEur", "finalPriceEurTax", "finalPriceUsd",
    "originalTotalPriceEur", "pricePerCtEur",
    "currencyCode", "currencySymbol",
    "diamondImage", "diamondVideo",
    "diamondType",
    "status",
    "page_no",
]


# ══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def make_session() -> curl_requests.Session:
    s = curl_requests.Session(impersonate="chrome")
    s.headers.update(HEADERS)
    return s


def build_payload(page: int) -> dict:
    return {"query": GQL_QUERY.format(page=page)}


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


def fetch_page(page: int, session: curl_requests.Session) -> tuple:
    for attempt in range(1, 6):
        pause_event.wait()
        try:
            resp = session.post(URL, json=build_payload(page), timeout=TIMEOUT)

            if resp.status_code in (429, 503):
                log.warning(
                    f"[page {page}] HTTP {resp.status_code} — rate limited. "
                    f"Pausing all workers for {RATELIMIT_PAUSE}s…"
                )
                pause_event.clear()
                time.sleep(RATELIMIT_PAUSE)
                pause_event.set()
                continue

            resp.raise_for_status()
            diamond_data = resp.json().get("data", {}).get("diamondData", {})
            diamonds     = diamond_data.get("diamond") or []
            time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))
            return page, diamonds

        except Exception as e:
            wait = (2 ** (attempt + 1)) + random.uniform(0, 3)
            log.warning(f"[page {page}] attempt {attempt}/5 failed: {e} — retrying in {wait:.1f}s")
            time.sleep(wait)

    log.error(f"[page {page}] all 5 retries exhausted — skipping")
    return page, None


def get_total_pages(session: curl_requests.Session) -> tuple:
    log.info("Fetching page 1 to determine total count…")
    for attempt in range(1, 5):
        try:
            resp         = session.post(URL, json=build_payload(1), timeout=TIMEOUT)
            resp.raise_for_status()
            diamond_data = resp.json().get("data", {}).get("diamondData", {})
            total_count  = int(diamond_data.get("dataCount", 0))
            total_pages  = (total_count + PAGE_SIZE - 1) // PAGE_SIZE
            log.info(f"Total diamonds : {total_count:,}")
            log.info(f"Total pages    : {total_pages:,}")
            return total_count, total_pages
        except Exception as e:
            wait = (2 ** attempt) + random.uniform(0, 2)
            log.warning(f"get_total_pages attempt {attempt}/4 failed: {e} — wait {wait:.1f}s")
            time.sleep(wait)
    raise RuntimeError("Failed to fetch page 1 — cannot determine total pages")


def make_row(d: dict, status: str, page: int) -> dict:
    return {
        "diamondId":             d.get("diamondId"),
        "stockNumber":           d.get("stockNumber"),
        "certificateNumber":     d.get("certificateNumber"),
        "shape":                 d.get("shape"),
        "caratWeight":           d.get("caratWeight"),
        "color":                 d.get("color"),
        "fancyColor":            d.get("fancyColor"),
        "fancyColorIntensity":   d.get("fancyColorIntensity"),
        "fancyColorOvertone":    d.get("fancyColorOvertone"),
        "clarity":               d.get("clarity"),
        "cut":                   d.get("cut"),
        "symmetry":              d.get("symmetry"),
        "polish":                d.get("polish"),
        "deptPerc":              d.get("deptPerc"),
        "tablePerc":             d.get("tablePerc"),
        "length":                d.get("length"),
        "width":                 d.get("width"),
        "depth":                 d.get("depth"),
        "ratio":                 d.get("ratio"),
        "measurement":           d.get("measurement"),
        "girdleMin":             d.get("girdleMin"),
        "girdleMax":             d.get("girdleMax"),
        "girdleCondition":       d.get("girdleCondition"),
        "culet":                 d.get("culet"),
        "culetCondition":        d.get("culetCondition"),
        "flourIntensity":        d.get("flourIntensity"),
        "lab":                   d.get("lab"),
        "certificateFile":       d.get("certificateFile"),
        "finalPriceEur":         d.get("finalPriceEur"),
        "finalPriceEurTax":      d.get("finalPriceEurTax"),
        "finalPriceUsd":         d.get("finalPriceUsd"),
        "originalTotalPriceEur": d.get("originalTotalPriceEur"),
        "pricePerCtEur":         d.get("pricePerCtEur"),
        "currencyCode":          d.get("currencyCode"),
        "currencySymbol":        d.get("currencySymbol"),
        "diamondImage":          d.get("diamondImage"),
        "diamondVideo":          d.get("diamondVideo"),
        "diamondType":           d.get("diamondType"),
        "status":                status,
        "page_no":               page,
    }


# ══════════════════════════════════════════════════════════════════════════════
# MAIN SCRAPER
# ══════════════════════════════════════════════════════════════════════════════

def scrape():
    log.info("=" * 55)
    log.info("Starting Renesim scraper — SAFE/SLOW mode")
    log.info(f"Workers  : {WORKERS}")
    log.info(f"Timeout  : {TIMEOUT}s")
    log.info(f"Delay    : {DELAY_MIN}–{DELAY_MAX}s per request")
    log.info(f"Output  → {MAIN_CSV}")
    log.info(f"Mirror  → {EXTRA_CSV}")

    # ── Initial status ────────────────────────────────────────
    write_status(SCRAPER_NAME, state="running", message="Starting up…")

    init_session = make_session()
    progress     = load_progress()

    done_pages  = set(progress.get("done_pages", []))
    total_saved = progress.get("total_saved", 0)

    try:
        if progress.get("total_pages"):
            total_pages = progress["total_pages"]
            log.info(f"Resuming — total pages from progress: {total_pages:,}")
        else:
            _, total_pages = get_total_pages(init_session)
    except RuntimeError as e:
        write_status(SCRAPER_NAME, state="failed", message=str(e))
        raise

    seen_certs = load_existing_certs(MAIN_CSV)

    all_pages = list(range(1, total_pages + 1))
    remaining = [p for p in all_pages if p not in done_pages]

    log.info(f"Pages done     : {len(done_pages):,}")
    log.info(f"Pages remaining: {len(remaining):,}")

    write_status(
        SCRAPER_NAME,
        state="running",
        current_page=len(done_pages),
        total_pages=total_pages,
        diamonds_saved=total_saved,
        message=f"Resuming — {len(remaining):,} pages remaining",
    )

    if not remaining:
        log.info("All pages already fetched — nothing to do.")
        write_status(
            SCRAPER_NAME,
            state="done",
            current_page=total_pages,
            total_pages=total_pages,
            diamonds_saved=total_saved,
            message="All pages already fetched",
        )
        return {"total_saved": total_saved, "new_rows": 0}

    file_exists = os.path.exists(MAIN_CSV) and os.path.getsize(MAIN_CSV) > 0
    write_mode  = "a" if file_exists else "w"
    log.info(f"CSV mode: {'APPEND' if write_mode == 'a' else 'NEW'}")

    f_main  = open(MAIN_CSV,  write_mode, newline="", encoding="utf-8")
    f_extra = open(EXTRA_CSV, write_mode, newline="", encoding="utf-8")
    writer_main  = csv.DictWriter(f_main,  fieldnames=FIELDS, extrasaction="ignore")
    writer_extra = csv.DictWriter(f_extra, fieldnames=FIELDS, extrasaction="ignore")
    if write_mode == "w":
        writer_main.writeheader()
        writer_extra.writeheader()

    lock        = Lock()
    saved_count = [0]
    skip_count  = [0]
    fail_count  = [0]
    completed   = [0]
    last_page   = [len(done_pages)]   # tracks highest completed page number
    total_rem   = len(remaining)

    try:
        with ThreadPoolExecutor(max_workers=WORKERS) as executor:
            futures = {
                executor.submit(fetch_page, p, make_session()): p
                for p in remaining
            }

            for future in as_completed(futures):
                page, diamonds = future.result()

                with lock:
                    completed[0] += 1
                    pct = completed[0] / total_rem * 100

                    if diamonds is None:
                        fail_count[0] += 1
                        log.error(
                            f"[{completed[0]:>6}/{total_rem}] page {page:>5} FAILED "
                            f"(will retry on next run)"
                        )
                        write_status(
                            SCRAPER_NAME,
                            state="running",
                            current_page=last_page[0],
                            total_pages=total_pages,
                            diamonds_saved=total_saved + saved_count[0],
                            duplicates=skip_count[0],
                            failed_pages=fail_count[0],
                            message=f"Page {page} failed — will retry",
                        )
                        continue

                    new_rows       = []
                    dup_count_page = 0
                    for d in diamonds:
                        cert = str(d.get("certificateNumber", "")).strip()
                        if cert and cert not in seen_certs:
                            seen_certs.add(cert)
                            new_rows.append(make_row(d, "new", page))
                        else:
                            skip_count[0] += 1
                            dup_count_page += 1

                    if new_rows:
                        writer_main.writerows(new_rows)
                        writer_extra.writerows(new_rows)
                        f_main.flush()
                        f_extra.flush()
                        saved_count[0] += len(new_rows)

                    done_pages.add(page)
                    last_page[0] = max(last_page[0], page)
                    total_saved_now = total_saved + saved_count[0]

                    log.info(
                        f"[{completed[0]:>6}/{total_rem}] "
                        f"page {page:>5} | "
                        f"+{len(new_rows):>2} new | "
                        f"skipped {dup_count_page} dupes | "
                        f"total saved: {total_saved_now:,} | "
                        f"{pct:.1f}%"
                    )

                    # ── Live status update every page ─────────
                    write_status(
                        SCRAPER_NAME,
                        state="running",
                        current_page=last_page[0],
                        total_pages=total_pages,
                        diamonds_saved=total_saved_now,
                        duplicates=skip_count[0],
                        failed_pages=fail_count[0],
                        message=f"Processing… {pct:.1f}% complete",
                        extra={
                            "pages_done":      len(done_pages),
                            "pages_remaining": total_rem - completed[0],
                            "last_page_scraped": last_page[0],
                        },
                    )
                    # ─────────────────────────────────────────

                    if completed[0] % SAVE_EVERY == 0:
                        save_progress(done_pages, total_saved_now, total_pages)

    except Exception as e:
        write_status(
            SCRAPER_NAME,
            state="failed",
            current_page=last_page[0],
            total_pages=total_pages,
            diamonds_saved=total_saved + saved_count[0],
            duplicates=skip_count[0],
            failed_pages=fail_count[0],
            message=f"Crashed: {e}",
        )
        raise
    finally:
        f_main.close()
        f_extra.close()

    final_saved = total_saved + saved_count[0]
    save_progress(done_pages, final_saved, total_pages)

    # ── Final status ──────────────────────────────────────────
    write_status(
        SCRAPER_NAME,
        state="done",
        current_page=total_pages,
        total_pages=total_pages,
        diamonds_saved=final_saved,
        duplicates=skip_count[0],
        failed_pages=fail_count[0],
        message="Scrape complete",
        extra={
            "output_csv":  MAIN_CSV,
            "mirror_csv":  EXTRA_CSV,
        },
    )

    log.info("=" * 55)
    log.info("DONE")
    log.info(f"  New rows saved  : {saved_count[0]:,}")
    log.info(f"  Duplicates skip : {skip_count[0]:,}")
    log.info(f"  Pages failed    : {fail_count[0]:,}")
    log.info(f"  Total in CSV    : {final_saved:,}")
    log.info(f"  CSV    → {MAIN_CSV}")
    log.info(f"  Mirror → {EXTRA_CSV}")
    log.info("=" * 55)

    return {
        "action":       "scrape_complete",
        "total_saved":  final_saved,
        "new_rows":     saved_count[0],
        "duplicates":   skip_count[0],
        "failed_pages": fail_count[0],
        "output":       MAIN_CSV,
        "end_time":     datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }


if __name__ == "__main__":
    scrape()