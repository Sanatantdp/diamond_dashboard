from curl_cffi import requests as curl_requests
import csv, os, time, random, json, logging
import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

from status import write_status


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

log = get_logger("novita")


# ── Config ────────────────────────────────────────────────────────────────────
SCRAPER_NAME = "novita"
URL          = "https://novitadiamonds.de/api/product/diamonds"
CSV_FILE     = "novita_diamonds.csv"     # ← no date in filename
BASE_DIR     = os.getcwd()

PAGE_SIZE  = 1000
WORKERS    = 10
SAVE_EVERY = 50

novita_path   = os.path.join(BASE_DIR, "novita-eur")
diamond_files = os.path.join(BASE_DIR, "diamond_files")

for d in (novita_path, diamond_files):
    os.makedirs(d, exist_ok=True)

MAIN_CSV      = os.path.join(novita_path,   CSV_FILE)
EXTRA_CSV     = os.path.join(diamond_files, CSV_FILE)
PROGRESS_FILE = os.path.join(diamond_files, "novita_progress.json")


# ── Request config ────────────────────────────────────────────────────────────
HEADERS = {
    "accept":           "application/json, text/javascript, */*; q=0.01",
    "accept-language":  "en-GB,en-US;q=0.9,en;q=0.8",
    "content-type":     "application/x-www-form-urlencoded; charset=UTF-8",
    "origin":           "https://novitadiamonds.de",
    "referer":          "https://novitadiamonds.de/en/create/diamanten/buy-lab-grown-diamonds",
    "sec-fetch-dest":   "empty",
    "sec-fetch-mode":   "cors",
    "sec-fetch-site":   "same-origin",
    "user-agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/143.0.0.0 Safari/537.36"
    ),
    "x-requested-with": "XMLHttpRequest",
}

COOKIES = {
    "PHPSESSID":                    "vuaa74cvalc3dsv28s9ru7taqt",
    "intercom-id-gdyrj32x":         "28e8ff81-3c17-4eb9-a7ac-8ec193fc762a",
    "intercom-device-id-gdyrj32x":  "b764acd7-8874-48d1-8491-f2d63c25962a",
    "_ga":                          "GA1.2.2035880902.1772777122",
}

BASE_PAYLOAD = {
    "shapes":                 '["1","4","3","2","6","5","9","8","7","10","11"]',
    "color_from":             "1",
    "color_to":               "16",
    "cut_from":               "1",
    "cut_to":                 "3",
    "clarity_from":           "2",
    "clarity_to":             "8",
    "polish_from":            "1",
    "polish_to":              "2",
    "symmetry_from":          "1",
    "symmetry_to":            "2",
    "carat_from":             "0.3",
    "carat_to":               "20",
    "price_from":             "150",
    "price_to":               "13500",
    "location_description":   "all",
    "certificates":           "[]",
    "ratio_from":             "1",
    "ratio_to":               "3",
    "is_carat_infitinitive":  "1",
    "is_price_infitinitive":  "1",
    "is_ratio_infitinitive":  "1",
    "maxResults":             str(PAGE_SIZE),
    "order_by":               "priceToSellWoLocalTax",
    "order_type":             "ASC",
    "preferred_currency":     "EUR",
    "current_tab":            "result_tab",
    "fetch_count":            "false",
    "token":                  "81f6de78af0d5e3264.cVQUUWY6s1TkJtW6rlwtzN6xOhVw1A6vXIQi5UbUgk8.BhctOEtt3gyIbKH--jNHh7jnVHs7vDnWKcJrvR--sThHFVAyKGjwYIxp4Q",
}

# ── CSV columns ───────────────────────────────────────────────────────────────
FIELDS = [
    "id", "product_id", "stock_status_provider_slug", "shape", "shape_slug",
    "carat", "color", "is_fancy", "fancy_color_slug", "clarity", "cut",
    "polish", "symmetry", "table", "depth", "measurement", "image_url",
    "image_external_url", "video_url", "video_external_url",
    "certificate_laboratory", "certificate_url", "certificate_file_type",
    "is_onshore", "certificate_number", "ratio", "nd_percentage_discount",
    "price", "manual_price", "symbol_currency", "active",
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
                cert = str(row.get("certificate_number", "")).strip()
                if cert:
                    seen.add(cert)
        log.info(f"Existing CSV has {len(seen):,} unique diamonds")
    except Exception as e:
        log.warning(f"Could not read existing CSV: {e}")
    return seen


def fetch_page(page: int, session: curl_requests.Session) -> tuple:
    data = {**BASE_PAYLOAD, "page": str(page)}
    for attempt in range(1, 5):
        try:
            resp = session.post(
                URL, headers=HEADERS, cookies=COOKIES,
                data=data, timeout=30,
            )
            resp.raise_for_status()
            diamonds = resp.json().get("response", {}).get("items", [])
            time.sleep(random.uniform(1, 3))
            return page, diamonds
        except Exception as e:
            wait = 2 ** attempt
            log.warning(f"[page {page}] attempt {attempt}/4 failed: {e} — wait {wait}s")
            time.sleep(wait)
    log.error(f"[page {page}] all retries exhausted — skipping")
    return page, None


def get_total_pages(session: curl_requests.Session) -> int:
    log.info("Fetching page 1 to determine total count…")
    for attempt in range(1, 5):
        try:
            data = {**BASE_PAYLOAD, "page": "1", "fetch_count": "true"}
            resp = session.post(
                URL, headers=HEADERS, cookies=COOKIES,
                data=data, timeout=30,
            )
            resp.raise_for_status()
            body  = resp.json().get("response", {})
            total = int(body.get("total_count") or body.get("count") or 0)
            if total:
                pages = (total + PAGE_SIZE - 1) // PAGE_SIZE
                log.info(f"Total diamonds : {total:,}")
                log.info(f"Total pages    : {pages:,}")
                return pages
            log.info("No total_count in response — will paginate until empty")
            return 9999
        except Exception as e:
            wait = 2 ** attempt
            log.warning(f"get_total_pages attempt {attempt}/4 failed: {e} — wait {wait}s")
            time.sleep(wait)
    raise RuntimeError("Failed to fetch page 1 — cannot determine total pages")


def make_row(d: dict, status: str, page: int) -> dict:
    return {
        "id":                         d.get("id"),
        "product_id":                 d.get("product_id"),
        "stock_status_provider_slug": d.get("stock_status_provider_slug"),
        "shape":                      d.get("shape"),
        "shape_slug":                 d.get("shape_slug"),
        "carat":                      d.get("carat"),
        "color":                      d.get("color"),
        "is_fancy":                   d.get("is_fancy"),
        "fancy_color_slug":           d.get("fancy_color_slug"),
        "clarity":                    d.get("clarity"),
        "cut":                        d.get("cut"),
        "polish":                     d.get("polish"),
        "symmetry":                   d.get("symmetry"),
        "table":                      d.get("table"),
        "depth":                      d.get("depth"),
        "measurement":                d.get("measurement"),
        "image_url":                  d.get("image_url"),
        "image_external_url":         d.get("image_external_url"),
        "video_url":                  d.get("video_url"),
        "video_external_url":         d.get("video_external_url"),
        "certificate_laboratory":     d.get("certificate"),
        "certificate_url":            d.get("certificate_url"),
        "certificate_file_type":      d.get("certificate_file_type"),
        "is_onshore":                 d.get("is_onshore"),
        "certificate_number":         d.get("certificate_number"),
        "ratio":                      d.get("ratio"),
        "nd_percentage_discount":     d.get("nd_percentage_discount"),
        "price":                      d.get("price"),
        "manual_price":               d.get("manual_price"),
        "symbol_currency":            d.get("currency"),
        "active":                     d.get("active"),
        "status":                     status,
        "page_no":                    page,
    }


# ══════════════════════════════════════════════════════════════════════════════
# MAIN SCRAPER
# ══════════════════════════════════════════════════════════════════════════════

def scrape():
    log.info("=" * 55)
    log.info("Starting Novita scraper (concurrent)")
    log.info(f"Workers : {WORKERS}")
    log.info(f"Output  → {MAIN_CSV}")
    log.info(f"Mirror  → {EXTRA_CSV}")

    write_status(SCRAPER_NAME, state="running", message="Starting up…")

    session  = make_session()
    progress = load_progress()

    done_pages  = set(progress.get("done_pages", []))
    total_saved = progress.get("total_saved", 0)

    try:
        if progress.get("total_pages"):
            total_pages = progress["total_pages"]
            log.info(f"Resuming — total pages from progress: {total_pages:,}")
        else:
            total_pages = get_total_pages(session)
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
    empty_pages = [0]
    last_page   = [len(done_pages)]
    total_rem   = len(remaining)

    try:
        with ThreadPoolExecutor(max_workers=WORKERS) as executor:
            futures = {executor.submit(fetch_page, p, session): p for p in remaining}

            for future in as_completed(futures):
                page, diamonds = future.result()

                with lock:
                    completed[0] += 1
                    pct = completed[0] / total_rem * 100

                    if diamonds is None:
                        fail_count[0] += 1
                        log.error(f"[{completed[0]:>6}/{total_rem}] page {page:>5} FAILED")
                        done_pages.add(page)
                        write_status(
                            SCRAPER_NAME,
                            state="running",
                            current_page=last_page[0],
                            total_pages=total_pages,
                            diamonds_saved=total_saved + saved_count[0],
                            duplicates=skip_count[0],
                            failed_pages=fail_count[0],
                            message=f"Page {page} failed",
                        )
                        continue

                    if not diamonds:
                        empty_pages[0] += 1
                        log.info(f"[{completed[0]:>6}/{total_rem}] page {page:>5} EMPTY — stopping")
                        done_pages.add(page)
                        continue

                    empty_pages[0] = 0

                    new_rows       = []
                    dup_count_page = 0
                    for d in diamonds:
                        cert = str(d.get("certificate_number", "")).strip()
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
                        f"+{len(new_rows):>3} new | "
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
                            "pages_done":        len(done_pages),
                            "pages_remaining":   total_rem - completed[0],
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

    write_status(
        SCRAPER_NAME,
        state="done",
        current_page=total_pages,
        total_pages=total_pages,
        diamonds_saved=final_saved,
        duplicates=skip_count[0],
        failed_pages=fail_count[0],
        message="Scrape complete",
        extra={"output_csv": MAIN_CSV, "mirror_csv": EXTRA_CSV},
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