# -*- coding: utf-8 -*-
"""
brilliant_earth.py — Brilliant Earth lab-grown diamond scraper (USA)

API:  GET https://www.brilliantearth.com/api/v1/plp/products/
Output: diamond_files/usa/brilliant_earth.csv
"""

# ── Bandwidth accounting ─────────────────────────────────────────────────────
# Counts every byte this scraper downloads, per day, into bandwidth/ and then
# the dashboards. Underscore aliases so nothing here shadows the module's own
# imports further down. See bandwidth.py.
import sys as _sys
from pathlib import Path as _Path
_sys.path.insert(0, str(_Path(__file__).resolve().parent.parent))
import bandwidth as _bandwidth
_bandwidth.track('brilliant-earth', 'usa')

from curl_cffi import requests as curl_requests
import csv, os, time, random, json, datetime, shutil
from pathlib import Path
from logger import get_logger

log = get_logger("brilliant_earth")

# ── Config ────────────────────────────────────────────────────────────────────
BASE_URL  = "https://www.brilliantearth.com/api/v1/plp/products/"
PAGE_SIZE = 50          # max the API returns per page
SLEEP_MIN = 0.8         # seconds between pages
SLEEP_MAX = 2.0
RETRY_SLEEP = 12        # on 429 / 5xx
MAX_RETRIES = 3
PAGE_TIMEOUT = 35

STALE_THRESHOLD_DAYS = 2

_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/149.0.0.0 Safari/537.36"
)

# ── Paths ─────────────────────────────────────────────────────────────────────
base_dir      = str(Path(__file__).resolve().parent.parent)
diamond_files = os.path.join(base_dir, "diamond_files", "usa")
BACKUP_DIR    = os.path.join(base_dir, "diamond_files_backup")
os.makedirs(diamond_files, exist_ok=True)
os.makedirs(BACKUP_DIR,    exist_ok=True)

MAIN_CSV      = os.path.join(diamond_files, "brilliant_earth.csv")
PROGRESS_FILE = os.path.join(diamond_files, "brilliant_earth_progress.json")

# ── Request ───────────────────────────────────────────────────────────────────
HEADERS = {
    "accept":             "application/json, text/javascript, */*; q=0.01",
    "accept-language":    "en-GB,en-US;q=0.9,en;q=0.8",
    "origin":             "https://www.brilliantearth.com",
    "referer":            "https://www.brilliantearth.com/loose-diamonds/search/",
    "sec-ch-ua":          '"Google Chrome";v="149", "Chromium";v="149", "Not)A;Brand";v="24"',
    "sec-ch-ua-mobile":   "?0",
    "sec-ch-ua-platform": '"Windows"',
    "sec-fetch-dest":     "empty",
    "sec-fetch-mode":     "cors",
    "sec-fetch-site":     "same-origin",
    "user-agent":         _UA,
    "x-requested-with":   "XMLHttpRequest",
}

BASE_PARAMS = {
    "display":                 PAGE_SIZE,
    "currency":                "USD",
    "product_class":           "Lab Created Colorless Diamonds",
    "cuts":                    "Fair,Good,Very Good,Ideal,Super Ideal",
    "colors":                  "J,I,H,G,F,E,D",
    "clarities":               "SI2,SI1,VS2,VS1,VVS2,VVS1,IF,FL",
    "polishes":                "Good,Very Good,Excellent",
    "symmetries":              "Good,Very Good,Excellent",
    "fluorescences":           "Very Strong,Strong,Medium,Faint,None",
    "real_diamond_view":       "",
    "quick_ship_diamond":      "",
    "hearts_and_arrows_diamonds": "",
    "min_price":  180,       "max_price":  1396180,
    "MIN_PRICE":  180,       "MAX_PRICE":  1396180,
    "min_carat":  0.5,      "max_carat":  10,
    "MIN_CARAT":  0.5,      "MAX_CARAT":  10,
    "order_by":   "carat",
    "order_method": "asc",
}

# ── CSV columns ───────────────────────────────────────────────────────────────
CSV_COLS = [
    "id", "sku", "title", "price", "carat", "shape", "color", "clarity",
    "cut", "polish", "symmetry", "fluorescence",
    "depth", "table", "length", "width", "depth_mm", "ratio", "girdle",
    "certificate_number", "certificate_lab", "certificate_url",
    "image_url", "product_url",
]


# ── Helpers ───────────────────────────────────────────────────────────────────
def _make_session():
    return curl_requests.Session(impersonate="chrome131")


def _csv_age_days(path):
    if not os.path.exists(path):
        return None
    return (datetime.datetime.now() -
            datetime.datetime.fromtimestamp(os.path.getmtime(path))).total_seconds() / 86400


def _archive_old_csv():
    age = _csv_age_days(MAIN_CSV)
    if age is None:
        log.info("No existing CSV — fresh scrape.")
        return
    if age < STALE_THRESHOLD_DAYS:
        log.info(f"CSV is fresh ({age:.1f}d old) — will resume/append.")
        return
    ts   = datetime.datetime.fromtimestamp(os.path.getmtime(MAIN_CSV)).strftime("%Y-%m-%d")
    dest = os.path.join(BACKUP_DIR, f"brilliant_earth_{ts}.csv")
    shutil.move(MAIN_CSV, dest)
    log.info(f"Archived stale CSV → {dest}")
    if os.path.exists(PROGRESS_FILE):
        os.remove(PROGRESS_FILE)
        log.info("Progress cleared — will start from page 1.")


def _load_progress():
    if os.path.exists(PROGRESS_FILE):
        try:
            with open(PROGRESS_FILE) as f:
                return json.load(f)
        except Exception:
            pass
    return {"next_page": 1, "seen_ids": []}


def _save_progress(page, seen_ids):
    with open(PROGRESS_FILE, "w") as f:
        json.dump({"next_page": page, "seen_ids": list(seen_ids)}, f)


def _extract_diamonds(data: dict) -> list[dict]:
    """Pull diamond records out of the API response regardless of key nesting."""
    for key in ("products", "diamonds", "items", "data", "results"):
        v = data.get(key)
        if isinstance(v, list):
            return v
    # Sometimes nested under "response"
    resp = data.get("response") or {}
    for key in ("products", "diamonds", "items", "data"):
        v = resp.get(key) if isinstance(resp, dict) else None
        if isinstance(v, list):
            return v
    if isinstance(data, list):
        return data
    return []


def _extract_total(data: dict) -> int:
    for key in ("total_count", "totalCount", "total", "count"):
        v = data.get(key)
        if isinstance(v, int) and v > 0:
            return v
    resp = data.get("response") or {}
    for key in ("total_count", "totalCount", "total"):
        v = resp.get(key) if isinstance(resp, dict) else None
        if isinstance(v, int) and v > 0:
            return v
    return 0


def _normalise(d: dict) -> dict:
    """Map raw API fields to CSV_COLS. Handles field-name variations."""
    def g(*keys):
        for k in keys:
            v = d.get(k)
            if v not in (None, "", "null"):
                return v
        return ""

    # Price: may be int, float, string like "$1,234"
    raw_price = g("price", "Price", "sell_price", "final_price")
    try:
        price = float(str(raw_price).replace("$", "").replace(",", "").strip())
    except (ValueError, TypeError):
        price = ""

    # Measurements: may be "6.5 x 6.5 x 3.9" or separate l/w/d fields
    measurements = g("measurements", "measurement", "size")
    parts = [p.strip() for p in str(measurements).split("x")] if measurements else []
    length   = parts[0] if len(parts) > 0 else g("length", "Length")
    width    = parts[1] if len(parts) > 1 else g("width", "Width")
    depth_mm = parts[2] if len(parts) > 2 else g("depth_mm", "depth_length")

    # Certificate — strip leading "LG" prefix (e.g. "LG708565" → "708565")
    _raw_cert = str(g("certificate_number", "certNumber", "cert_number",
                      "sku", "report_number", "reportNumber"))
    cert_num  = _raw_cert[2:] if _raw_cert.upper().startswith("LG") else _raw_cert
    cert_lab  = g("certificate_lab", "certLab", "lab", "grading_lab",
                  "certificate_laboratory", "Lab")
    cert_url  = g("certificate_url", "certUrl", "report_url", "cert_file")

    return {
        "id":               g("id", "product_id", "diamond_id"),
        "sku":              g("sku", "SKU", "stock_number"),
        "title":            g("title", "name", "Name"),
        "price":            price,
        "carat":            g("carat", "caratWeight", "carat_weight", "weight"),
        "shape":            g("shape", "Shape"),
        "color":            g("color", "Color"),
        "clarity":          g("clarity", "Clarity"),
        "cut":              g("cut", "Cut"),
        "polish":           g("polish", "Polish"),
        "symmetry":         g("symmetry", "Symmetry"),
        "fluorescence":     g("fluorescence", "Fluorescence", "fluor", "fluorescences"),
        "depth":            g("depth", "depth_percent", "depthPct", "Depth"),
        "table":            g("table", "table_percent", "tablePct", "Table"),
        "length":           length,
        "width":            width,
        "depth_mm":         depth_mm,
        "ratio":            g("ratio", "length_width_ratio", "lwRatio"),
        "girdle":           g("girdle", "Girdle"),
        "certificate_number": cert_num,
        "certificate_lab":    cert_lab,
        "certificate_url":    cert_url,
        "image_url":        g("image", "image_url", "img", "imageUrl", "diamond_image"),
        "product_url":      g("url", "product_url", "link", "productUrl"),
    }


# ── Core fetch ────────────────────────────────────────────────────────────────
def fetch_page(session, page: int) -> dict | None:
    params = {**BASE_PARAMS, "page": page}
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.get(BASE_URL, headers=HEADERS, params=params,
                               timeout=PAGE_TIMEOUT)
            if resp.status_code == 200:
                return resp.json()
            if resp.status_code == 429:
                wait = RETRY_SLEEP * attempt
                log.warning(f"  429 rate-limited — sleeping {wait}s (attempt {attempt})")
                time.sleep(wait)
                continue
            log.warning(f"  HTTP {resp.status_code} on page {page} (attempt {attempt})")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_SLEEP)
        except Exception as exc:
            log.warning(f"  Exception on page {page} attempt {attempt}: {exc}")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_SLEEP)
    return None


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    log.info("=" * 60)
    log.info("Brilliant Earth scraper — START")

    _archive_old_csv()
    progress = _load_progress()
    start_page = progress.get("next_page", 1)
    seen_ids   = set(progress.get("seen_ids", []))

    # Open CSV (append if resuming, write+header if fresh)
    file_exists = os.path.exists(MAIN_CSV)
    csv_mode    = "a" if file_exists and start_page > 1 else "w"
    csv_fh      = open(MAIN_CSV, csv_mode, newline="", encoding="utf-8")
    writer      = csv.DictWriter(csv_fh, fieldnames=CSV_COLS, extrasaction="ignore")
    if csv_mode == "w":
        writer.writeheader()

    session    = _make_session()
    total_pages = None
    total_diamonds = 0
    written    = 0

    try:
        page = start_page
        while True:
            log.info(f"  Page {page}" + (f"/{total_pages}" if total_pages else ""))
            data = fetch_page(session, page)

            if data is None:
                log.error(f"  Failed to fetch page {page} after {MAX_RETRIES} attempts — stopping.")
                break

            diamonds = _extract_diamonds(data)

            # First page — discover total
            if total_pages is None:
                total_count = _extract_total(data)
                if total_count:
                    import math
                    total_pages = math.ceil(total_count / PAGE_SIZE)
                    log.info(f"  Total diamonds: {total_count:,} → {total_pages} pages")
                else:
                    log.warning("  Could not determine total count — will stop when page is empty.")

            if not diamonds:
                log.info(f"  Page {page} returned 0 diamonds — done.")
                break

            new_rows = 0
            for raw in diamonds:
                row = _normalise(raw)
                uid = str(row.get("id") or row.get("sku") or row.get("certificate_number") or "")
                if uid and uid in seen_ids:
                    continue
                if uid:
                    seen_ids.add(uid)
                writer.writerow(row)
                new_rows += 1

            written        += new_rows
            total_diamonds += len(diamonds)
            log.info(f"  → {new_rows} new rows  (total written: {written:,})")

            _save_progress(page + 1, seen_ids)

            if total_pages and page >= total_pages:
                log.info("  All pages fetched.")
                break

            page += 1
            time.sleep(random.uniform(SLEEP_MIN, SLEEP_MAX))

    finally:
        csv_fh.close()
        session.close()

    log.info(f"Brilliant Earth scraper — DONE  ({written:,} rows → {MAIN_CSV})")
    # Clear progress on clean finish
    if os.path.exists(PROGRESS_FILE):
        os.remove(PROGRESS_FILE)


if __name__ == "__main__":
    main()
