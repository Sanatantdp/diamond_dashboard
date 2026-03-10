import requests
import re
import json
import csv
import os
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from logger import get_logger

log = get_logger("luvansh")

# ── Endpoints ──────────────────────────────────────────────────
LIST_URL   = "https://www.luvansh.com/Shop/_ShopDiamondTable"
DETAIL_URL = "https://www.luvansh.com/Shop/_ShopDiamondTableDetail"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.luvansh.com/Shop",
    "X-Requested-With": "XMLHttpRequest",
}

LIST_PARAMS = {
    "IsColor": "false",
    "Color": "",
    "viewDiamond": "true",
    "SearchString": "",
}

LIST_WORKERS   = 10
DETAIL_WORKERS = 15
SAVE_EVERY     = 50

base_dir   = os.getcwd()
down_files = os.path.join(base_dir, "diamond_files")
os.makedirs(down_files, exist_ok=True)

OUTPUT_CSV      = os.path.join(down_files, "luvansh_diamonds.csv")
IDS_PROGRESS    = os.path.join(down_files, "ids_progress.json")
DETAIL_PROGRESS = os.path.join(down_files, "detail_progress.json")
SKIPPED_IDS     = os.path.join(down_files, "skipped_ids.json")

UNAVAILABLE_TITLES = {
    "this diamond is no longer available.",
    "this diamond is no longer available",
}
REQUIRED_FIELDS = ["title"]

CSV_COLUMNS = [
    "product_id", "title", "url", "original_price", "discounted_price",
    "shape", "carat", "color", "clarity", "cut", "polish", "symmetry",
    "measurements", "lw_ratio", "table_pct", "depth_pct",
    "lab", "certificate_number", "certificate_url", "image_url", "ships_by",
]


# ══════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════

def make_session() -> requests.Session:
    s = requests.Session()
    adapter = requests.adapters.HTTPAdapter(
        pool_connections=30, pool_maxsize=30,
        max_retries=requests.adapters.Retry(total=3, backoff_factor=0.5)
    )
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s


def is_unavailable(row: dict) -> bool:
    return row.get("title", "").strip().lower() in UNAVAILABLE_TITLES


def is_complete_row(row: dict) -> bool:
    return all(row.get(f, "").strip() for f in REQUIRED_FIELDS)


def load_ids_from_csv(csv_path: str) -> set:
    saved = set()
    if not os.path.exists(csv_path):
        return saved
    try:
        with open(csv_path, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                pid = row.get("product_id", "").strip()
                if pid.isdigit():
                    saved.add(int(pid))
        log.info(f"CSV already contains {len(saved):,} unique product IDs — will skip these.")
    except Exception as e:
        log.warning(f"Could not read existing CSV: {e}")
    return saved


# ══════════════════════════════════════════════════════════════
# PHASE 1 — Collect all product IDs
# ══════════════════════════════════════════════════════════════

def extract_product_ids(html: str) -> list:
    return [int(x) for x in re.findall(r'displayDetailView\(this,(\d+)\)', html)]


def fetch_list_page(page_no: int, session: requests.Session) -> tuple:
    params = {**LIST_PARAMS, "pageNo": page_no}
    try:
        r = session.get(LIST_URL, params=params, headers=HEADERS, timeout=30)
        r.raise_for_status()
        data = r.json()
        ids  = extract_product_ids(data.get("partialData", ""))
        log.debug(f"List page {page_no}: {len(ids)} IDs found")
        return page_no, ids, data
    except Exception as e:
        log.error(f"List page {page_no} failed: {e}")
        return page_no, None, None


def collect_all_ids(session: requests.Session) -> list:
    log.info("=" * 50)
    log.info("PHASE 1 — Collecting all product IDs (concurrent)")

    if os.path.exists(IDS_PROGRESS):
        with open(IDS_PROGRESS) as f:
            saved = json.load(f)
        all_ids    = set(saved.get("all_ids", []))
        done_pages = set(saved.get("done_pages", []))
        log.info(f"Resuming: {len(all_ids):,} IDs, {len(done_pages):,} pages already done")
    else:
        all_ids    = set()
        done_pages = set()

    _, ids_p1, data1 = fetch_list_page(1, session)
    if data1 is None:
        log.error("Failed to fetch page 1. Aborting phase 1.")
        return []

    total          = data1.get("totalShopDiamond", 0)
    items_per_page = len(ids_p1) if ids_p1 else 100
    est_pages      = (total + items_per_page - 1) // items_per_page

    all_ids.update(ids_p1 or [])
    done_pages.add(1)

    log.info(f"Total diamonds: {total:,} | Est pages: {est_pages:,} | Workers: {LIST_WORKERS}")

    remaining_pages = [p for p in range(2, est_pages + 1) if p not in done_pages]
    lock         = Lock()
    save_counter = [0]

    def _save_progress():
        with open(IDS_PROGRESS, "w") as f:
            json.dump({"all_ids": list(all_ids), "done_pages": list(done_pages)}, f)

    with ThreadPoolExecutor(max_workers=LIST_WORKERS) as executor:
        futures = {executor.submit(fetch_list_page, p, session): p for p in remaining_pages}
        for future in as_completed(futures):
            page_no, page_ids, _ = future.result()
            with lock:
                if page_ids is not None:
                    before = len(all_ids)
                    all_ids.update(page_ids)
                    done_pages.add(page_no)
                    log.debug(f"Page {page_no}: {len(page_ids)} IDs | +{len(all_ids)-before} new | Total: {len(all_ids):,}")
                else:
                    log.warning(f"Page {page_no}: FAILED")

                save_counter[0] += 1
                if save_counter[0] % 20 == 0:
                    _save_progress()

    _save_progress()
    log.info(f"Phase 1 complete. Unique IDs collected: {len(all_ids):,}")
    return sorted(all_ids)


# ══════════════════════════════════════════════════════════════
# PHASE 2 — Fetch + parse details
# ══════════════════════════════════════════════════════════════

def fetch_detail(product_id: int, session: requests.Session) -> str | None:
    try:
        r = session.get(DETAIL_URL, params={"productId": product_id}, headers=HEADERS, timeout=30)
        r.raise_for_status()
        return r.text
    except Exception as e:
        log.error(f"Detail fetch failed for ID {product_id}: {e}")
        return None


def parse_detail(product_id: int, html: str) -> dict:
    soup = BeautifulSoup(html, "html.parser")
    row  = {"product_id": product_id}

    h6 = soup.find("h6", class_="dia-desc-hdn")
    row["title"] = h6.get_text(strip=True) if h6 else ""

    link = soup.find("a", href=re.compile(r"^/product/"))
    row["url"] = ("https://www.luvansh.com" + link["href"]) if link else ""

    img = soup.find("img", class_="diam-img")
    row["image_url"] = img["src"] if img else ""

    price_span = soup.find("span", style=re.compile(r"font-size"))
    if price_span:
        del_tag = price_span.find("del")
        row["original_price"] = del_tag.get_text(strip=True).replace("$", "") if del_tag else ""
        full_text = price_span.get_text(" ", strip=True)
        disc = re.search(r'\$(\d+)', full_text[len(row["original_price"]):])
        row["discounted_price"] = disc.group(1) if disc else ""
    else:
        row["original_price"] = row["discounted_price"] = ""

    ships_span = soup.find("span", class_="text-theme")
    row["ships_by"] = ships_span.get_text(strip=True) if ships_span else ""

    kv_map = {
        "Measurements": "measurements", "L/W Ratio": "lw_ratio",
        "Table": "table_pct", "Depth": "depth_pct", "Cut": "cut",
        "Polish": "polish", "Symmetry": "symmetry", "Shape": "shape",
        "Carat": "carat", "Color": "color", "Clarity": "clarity",
    }
    for div in soup.find_all("div", class_="dia-desc-para"):
        text = div.get_text(" ", strip=True)
        for label, key in kv_map.items():
            if text.startswith(label + ":") or text.startswith(label + " :"):
                span = div.find("span")
                row[key] = span.get_text(strip=True) if span else ""
                break

    for div in soup.find_all("div", class_="dia-desc-para"):
        a_tag = div.find("a", href=re.compile(r"igi|gia|gcal", re.I))
        if a_tag:
            full      = div.get_text(" ", strip=True)
            lab_match = re.match(r'^([A-Z]+)\s*:', full)
            row["lab"]             = lab_match.group(1) if lab_match else ""
            row["certificate_url"] = a_tag.get("href", "")
            cert_match = re.search(r'[?&r=](\d+)', row["certificate_url"])
            row["certificate_number"] = cert_match.group(1) if cert_match else ""
            break

    for col in CSV_COLUMNS:
        row.setdefault(col, "")
    return row


def collect_all_details(all_ids: list, session: requests.Session):
    log.info("=" * 50)
    log.info("PHASE 2 — Fetching diamond details (concurrent)")

    done_ids = set()
    if os.path.exists(DETAIL_PROGRESS):
        with open(DETAIL_PROGRESS) as f:
            done_ids = set(json.load(f).get("done_ids", []))
        log.info(f"detail_progress.json: {len(done_ids):,} IDs already done")

    csv_ids = load_ids_from_csv(OUTPUT_CSV)
    done_ids |= csv_ids

    skipped_ids = set()
    if os.path.exists(SKIPPED_IDS):
        with open(SKIPPED_IDS) as f:
            skipped_ids = set(json.load(f).get("skipped_ids", []))
        log.info(f"skipped_ids.json: {len(skipped_ids):,} unavailable IDs")

    remaining = [pid for pid in all_ids if pid not in done_ids and pid not in skipped_ids]
    log.info(f"Total: {len(all_ids):,} | Done: {len(done_ids):,} | Unavailable: {len(skipped_ids):,} | Remaining: {len(remaining):,}")

    if not remaining:
        log.info("Nothing to do — all IDs already saved!")
        return {"saved": len(done_ids), "skipped": len(skipped_ids)}

    write_header = not os.path.exists(OUTPUT_CSV) or os.path.getsize(OUTPUT_CSV) == 0
    csv_file = open(OUTPUT_CSV, "a", newline="", encoding="utf-8")
    writer   = csv.DictWriter(csv_file, fieldnames=CSV_COLUMNS, extrasaction="ignore")
    if write_header:
        writer.writeheader()

    csv_lock   = Lock()
    done_lock  = Lock()
    counter    = [0]
    skipped    = [0]
    incomplete = [0]

    def _save_progress():
        with open(DETAIL_PROGRESS, "w") as f:
            json.dump({"done_ids": list(done_ids)}, f)

    def _save_skipped():
        with open(SKIPPED_IDS, "w") as f:
            json.dump({"skipped_ids": list(skipped_ids)}, f)

    def fetch_and_parse(pid):
        html = fetch_detail(pid, session)
        if html is None:
            return pid, None
        return pid, parse_detail(pid, html)

    total_remaining = len(remaining)

    with ThreadPoolExecutor(max_workers=DETAIL_WORKERS) as executor:
        futures = {executor.submit(fetch_and_parse, pid): pid for pid in remaining}

        for future in as_completed(futures):
            pid, row = future.result()

            with csv_lock:
                counter[0] += 1
                idx = counter[0]

                if row is None:
                    skipped[0] += 1
                    log.warning(f"[{idx}/{total_remaining}] ID {pid}: FETCH FAILED — skipped")
                    continue

                if is_unavailable(row):
                    with done_lock:
                        skipped_ids.add(pid)
                        if counter[0] % SAVE_EVERY == 0:
                            _save_skipped()
                    log.info(f"[{idx}/{total_remaining}] ID {pid}: no longer available — added to skipped")
                    continue

                if not is_complete_row(row):
                    incomplete[0] += 1
                    log.warning(f"[{idx}/{total_remaining}] ID {pid}: INCOMPLETE — skipped ({row.get('title','')[:40]})")
                    continue

                if pid in done_ids:
                    log.debug(f"[{idx}/{total_remaining}] ID {pid}: already saved — skipped")
                    continue

                writer.writerow(row)
                csv_file.flush()

            with done_lock:
                done_ids.add(pid)
                if counter[0] % SAVE_EVERY == 0:
                    _save_progress()

            log.info(f"[{idx}/{total_remaining}] ID {pid}: ✓ {row.get('title','')[:50]}")

    csv_file.close()
    _save_progress()
    _save_skipped()

    new_saved = len(done_ids) - len(csv_ids)
    log.info(f"Phase 2 complete | Saved: {new_saved:,} | Failed: {skipped[0]:,} | Incomplete: {incomplete[0]:,} | Unavailable: {len(skipped_ids):,}")
    log.info("=" * 50)

    return {
        "new_saved":   new_saved,
        "failed":      skipped[0],
        "incomplete":  incomplete[0],
        "unavailable": len(skipped_ids),
    }


# ══════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════

def main():
    session = make_session()
    all_ids = collect_all_ids(session)

    if not all_ids:
        log.error("No IDs collected. Exiting.")
        return {"error": "No IDs collected"}

    result = collect_all_details(all_ids, session)
    log.info(f"ALL DONE → {os.path.abspath(OUTPUT_CSV)}")
    return result


if __name__ == "__main__":
    main()