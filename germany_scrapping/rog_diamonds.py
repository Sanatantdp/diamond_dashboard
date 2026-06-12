from curl_cffi import requests as curl_requests
import csv, os, time, random, json, logging
import datetime
from pathlib import Path

def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        logger.setLevel(logging.DEBUG)
        fmt = logging.Formatter(
            "%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        # console
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        ch.setFormatter(fmt)
        logger.addHandler(ch)
        # file
        log_dir = os.path.join(os.getcwd(), "logs")
        os.makedirs(log_dir, exist_ok=True)
        fh = logging.FileHandler(
            os.path.join(log_dir, f"{name}.log"), encoding="utf-8"
        )
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(fmt)
        logger.addHandler(fh)
    return logger

log = get_logger("rog")

# ── Config ────────────────────────────────────────────────────────────────────
URL        = "https://py.preciouscarbon.com/roa/graphql"
DATE_STR   = datetime.datetime.now().strftime("%Y-%m-%d")
CSV_FILE   = f"rog_{DATE_STR}.csv"
PAGE_SIZE  = 20

base_dir      = os.getcwd()
rog_path      = os.path.join(base_dir, "rog")
diamond_files = str(Path(__file__).resolve().parent.parent / "diamond_files" / "germany")

for d in (rog_path, diamond_files):
    os.makedirs(d, exist_ok=True)

MAIN_CSV      = os.path.join(rog_path,      CSV_FILE)
EXTRA_CSV     = os.path.join(diamond_files, CSV_FILE)
PROGRESS_FILE = os.path.join(diamond_files, "rog_progress.json")


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

# ── GraphQL query (kept your original filters) ───────────────────────────────
GQL_QUERY = """{{
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
}}"""

# ── CSV columns ───────────────────────────────────────────────────────────────
FIELDS = [
    "diamondId", "shape", "caratWeight", "color", "clarity", "cut",
    "symmetry", "polish", "deptPerc", "tablePerc", "length", "width",
    "depth", "ratio", "girdleMin", "girdleMax", "culet", "fancyColor",
    "flourIntensity", "certificateFile", "finalPriceUsd", "finalPriceEur",
    "finalPriceGbp", "certificateNumber", "stockNumber", "diamondImage",
    "diamondVideo", "diamondType", "lab", "currencyCode", "currencySymbol",
    "status",   # new | duplicate
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


# ── Progress ──────────────────────────────────────────────────────────────────

def load_progress() -> dict:
    if os.path.exists(PROGRESS_FILE):
        try:
            with open(PROGRESS_FILE) as f:
                data = json.load(f)
            if data.get("status") == "done":
                log.info("Previous run completed — starting fresh")
                return {"done_pages": [], "total_saved": 0, "total_pages": None}
            log.info(
                f"Interrupted run found — "
                f"done pages: {len(data.get('done_pages', []))}, "
                f"saved: {data.get('total_saved', 0):,}"
            )
            return data
        except Exception as e:
            log.warning(f"Could not read progress file: {e} — starting fresh")
    return {"done_pages": [], "total_saved": 0, "total_pages": None}


def save_progress(done_pages: set, total_saved: int, total_pages: int, status: str = "running"):
    try:
        with open(PROGRESS_FILE, "w") as f:
            json.dump({
                "status":      status,
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
            resp         = session.post(URL, json=build_payload(page), timeout=30)
            resp.raise_for_status()
            diamond_data = resp.json().get("data", {}).get("diamondData", {})
            diamonds     = diamond_data.get("diamond") or []
            time.sleep(random.uniform(0, 0.5))   # polite delay
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
            resp         = session.post(URL, json=build_payload(1), timeout=30)
            resp.raise_for_status()
            diamond_data = resp.json().get("data", {}).get("diamondData", {})
            total_count  = int(diamond_data.get("dataCount", 0))
            total_pages  = (total_count + PAGE_SIZE - 1) // PAGE_SIZE
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
        "diamondId":       d.get("diamondId"),
        "shape":           d.get("shape"),
        "caratWeight":     d.get("caratWeight"),
        "color":           d.get("color"),
        "clarity":         d.get("clarity"),
        "cut":             d.get("cut"),
        "symmetry":        d.get("symmetry"),
        "polish":          d.get("polish"),
        "deptPerc":        d.get("deptPerc"),
        "tablePerc":       d.get("tablePerc"),
        "length":          d.get("length"),
        "width":           d.get("width"),
        "depth":           d.get("depth"),
        "ratio":           d.get("ratio"),
        "girdleMin":       d.get("girdleMin"),
        "girdleMax":       d.get("girdleMax"),
        "culet":           d.get("culet"),
        "fancyColor":      d.get("fancyColor"),
        "flourIntensity":  d.get("flourIntensity"),
        "certificateFile": d.get("certificateFile"),
        "finalPriceUsd":   d.get("finalPriceUsd"),
        "finalPriceEur":   d.get("finalPriceEur"),
        "finalPriceGbp":   d.get("finalPriceGbp"),
        "certificateNumber": d.get("certificateNumber"),
        "stockNumber":     d.get("stockNumber"),
        "diamondImage":    d.get("diamondImage"),
        "diamondVideo":    d.get("diamondVideo"),
        "diamondType":     d.get("diamondType"),
        "lab":             d.get("lab"),
        "currencyCode":    d.get("currencyCode"),
        "currencySymbol":  d.get("currencySymbol"),
        "status":          status,
        "page_no":         page,
    }


# ══════════════════════════════════════════════════════════════════════════════
# MAIN SCRAPER
# ══════════════════════════════════════════════════════════════════════════════

def scrape():
    log.info("=" * 55)
    log.info("Starting ROG scraper")
    log.info(f"Output  → {MAIN_CSV}")
    log.info(f"Mirror  → {EXTRA_CSV}")

    session  = make_session()
    progress = load_progress()

    done_pages  = set(progress.get("done_pages", []))
    total_saved = progress.get("total_saved", 0)

    if progress.get("total_pages"):
        total_pages = progress["total_pages"]
        log.info(f"Resuming — total pages from progress: {total_pages:,}")
    else:
        total_pages = get_total_pages(session)

    seen_certs = load_existing_certs(MAIN_CSV)

    all_pages = list(range(1, total_pages + 1))
    remaining = [p for p in all_pages if p not in done_pages]

    log.info(f"Pages done     : {len(done_pages):,}")
    log.info(f"Pages remaining: {len(remaining):,}")

    if not remaining:
        log.info("All pages already fetched — nothing to do.")
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

    saved_count = 0
    skip_count  = 0
    fail_count  = 0
    total_rem   = len(remaining)

    for idx, p in enumerate(remaining, 1):
        page, diamonds = fetch_page(p, session)
        pct = idx / total_rem * 100

        if diamonds is None:
            fail_count += 1
            log.error(f"[{idx:>6}/{total_rem}] page {page:>5} FAILED")
            done_pages.add(page)
            continue

        new_rows = []
        dup_count_page = 0
        for d in diamonds:
            cert = str(d.get("certificateNumber", "")).strip()
            if cert and cert not in seen_certs:
                seen_certs.add(cert)
                new_rows.append(make_row(d, "new", page))
            else:
                skip_count += 1
                dup_count_page += 1

        if new_rows:
            writer_main.writerows(new_rows)
            writer_extra.writerows(new_rows)
            f_main.flush()
            f_extra.flush()
            saved_count += len(new_rows)

        done_pages.add(page)
        total_saved_now = total_saved + saved_count

        log.info(
            f"[{idx:>6}/{total_rem}] "
            f"page {page:>5} | "
            f"+{len(new_rows):>2} new | "
            f"skipped {dup_count_page} dupes | "
            f"total saved: {total_saved_now:,} | "
            f"{pct:.1f}%"
        )

        save_progress(done_pages, total_saved_now, total_pages)

    f_main.close()
    f_extra.close()

    final_saved = total_saved + saved_count
    save_progress(done_pages, final_saved, total_pages, status="done")

    log.info("=" * 55)
    log.info("DONE")
    log.info(f"  New rows saved  : {saved_count:,}")
    log.info(f"  Duplicates skip : {skip_count:,}")
    log.info(f"  Pages failed    : {fail_count:,}")
    log.info(f"  Total in CSV    : {final_saved:,}")
    log.info(f"  CSV    → {MAIN_CSV}")
    log.info(f"  Mirror → {EXTRA_CSV}")
    log.info("=" * 55)

    return {
        "action":       "scrape_complete",
        "total_saved":  final_saved,
        "new_rows":     saved_count,
        "duplicates":   skip_count,
        "failed_pages": fail_count,
        "output":       MAIN_CSV,
        "end_time":     datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }


if __name__ == "__main__":
    scrape()