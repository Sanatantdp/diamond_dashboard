import requests, os, csv, re, html, datetime
from bs4 import BeautifulSoup
from logger import get_logger

log = get_logger("loosegrown")

URL = "https://www.loosegrowndiamond.com/wp-admin/admin-ajax.php"

HEADERS = {
    "accept": "application/json, text/javascript, */*; q=0.01",
    "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
    "origin": "https://www.loosegrowndiamond.com",
    "referer": "https://www.loosegrowndiamond.com/inventory/",
    "user-agent": "Mozilla/5.0",
    "x-requested-with": "XMLHttpRequest",
}

PAGE_SIZE  = 1000
base_dir   = os.getcwd()
down_files = os.path.join(base_dir, "diamond_files")
os.makedirs(down_files, exist_ok=True)

CSV_FILE   = os.path.join(down_files, "loosegrowndiamond.csv")
STATE_FILE = os.path.join(down_files, "lgd_state.txt")


# ── FILE DATE CHECK ───────────────────────────────────────────────────────────

def rotate_old_file(filepath):
    """
    If `filepath` exists but was last modified MORE than 1 day ago,
    rename it to  <stem>_YYYY-MM-DD.<ext>  (using its modification date)
    and reset the state file so the scraper starts fresh.

    Returns True if the file was rotated (caller should start fresh).
    Returns False if the file is fresh (≤1 day old) or did not exist.
    """
    if not os.path.exists(filepath):
        return False

    mtime      = os.path.getmtime(filepath)
    mod_date   = datetime.datetime.fromtimestamp(mtime)
    age        = datetime.datetime.now() - mod_date

    if age.total_seconds() <= 86400:          # 86 400 s = 1 day
        log.info(
            f"File is fresh ({age.seconds // 3600}h {(age.seconds % 3600) // 60}m old) — continuing."
        )
        return False

    # File is older than 1 day → rename it
    date_str  = mod_date.strftime("%Y-%m-%d")
    stem, ext = os.path.splitext(filepath)
    new_name  = f"{stem}_{date_str}{ext}"

    # If a file with that name already exists, add a counter to avoid collisions
    counter = 1
    while os.path.exists(new_name):
        new_name = f"{stem}_{date_str}_{counter}{ext}"
        counter += 1

    os.rename(filepath, new_name)
    log.info(f"Old file renamed → {os.path.basename(new_name)}  (was {age.days}d {age.seconds // 3600}h old)")

    # Reset state so we start from offset 1
    if os.path.exists(STATE_FILE):
        os.remove(STATE_FILE)
        log.info("State file reset — will start from offset 1")

    return True

# ─────────────────────────────────────────────────────────────────────────────


def get_payload(start):
    return {
        "action": "ls_loadmore_inventory",
        "start": str(start),
        "ls_per_page": str(PAGE_SIZE),
        "price_range": "100.00,100000.00",
        "shape[]": ["Round","Princess","Cushion","Oval","Emerald","Pear","Asscher","Radiant","Marquise","Heart"],
        "carat_range": "0.00,62.96",
        "cut_range": "0.00,4.00",
        "color_range": "1,10.00",
        "clarity_range": "1,11.00",
        "depth_range": "46.00,78.00",
        "table_range": "50.00,80.00",
        "lwratio": "1.00,2.75",
        "heartarrow": "0"
    }


def clean_id(v):
    return v.replace('"', "").strip() if v else ""


def clean_html(v):
    if not v:
        return ""
    v = html.unescape(v)
    v = re.sub(r"<[^>]+>", "", v)
    v = v.replace("\\n", " ").replace("\\t", " ").replace("\\", "")
    return re.sub(r"\s+", " ", v).strip()


def load_existing_skus():
    skus = set()
    if os.path.exists(CSV_FILE):
        with open(CSV_FILE, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                sku = clean_id(row.get("sku"))
                if sku:
                    skus.add(sku)
    log.info(f"Already have {len(skus):,} records in CSV")
    return skus


def get_start_value():
    if not os.path.exists(STATE_FILE):
        with open(STATE_FILE, "w") as f:
            f.write("1")
        log.debug("State file not found — starting from 1")
        return 1
    try:
        val = int(open(STATE_FILE).read().strip())
        log.info(f"Resuming from state: {val}")
        return val
    except Exception as e:
        log.warning(f"Could not read state file ({e}) — resetting to 1")
        with open(STATE_FILE, "w") as f:
            f.write("1")
        return 1


def save_state(start):
    with open(STATE_FILE, "w") as f:
        f.write(str(start))
    log.debug(f"State saved: {start}")


def init_csv():
    if not os.path.exists(CSV_FILE):
        with open(CSV_FILE, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow([
                "shape", "carat", "cut", "color", "clarity",
                "price", "data_iid", "data_id", "sku"
            ])
        log.info(f"CSV initialised: {CSV_FILE}")


def extract_price(td):
    sale = td.select_one(".ls_sprice")
    if sale:
        return clean_html(sale.decode_contents()).replace("$", "")
    raw  = clean_html(td.decode_contents())
    nums = re.findall(r"\d+", raw)
    return min(map(int, nums)) if nums else ""


def fetch_page(start):
    log.debug(f"Fetching page at start={start}")
    try:
        r = requests.post(URL, headers=HEADERS, data=get_payload(start), timeout=60)
        r.raise_for_status()
        data = r.json()
        return data.get("content", ""), data.get("next", "")
    except Exception as e:
        log.error(f"Failed to fetch page at start={start}: {e}")
        return "", ""


def parse_rows(html_block, existing_ids, writer):
    soup = BeautifulSoup(html_block, "lxml")
    trs  = soup.select("tr[data-iid]")
    new_rows = 0

    for tr in trs:
        tds = tr.find_all("td", recursive=False)
        if len(tds) < 6:
            continue

        iid = clean_id(tr.get("data-iid"))
        did = clean_id(tr.get("data-id"))

        sku = ""
        for c in tr.get("class", []):
            if c.startswith("cls"):
                sku = c.replace("cls", "")
                break

        if not sku or sku in existing_ids:
            continue

        price = extract_price(tds[5])

        writer.writerow([
            clean_html(tds[0].decode_contents()).split()[0].lower(),
            clean_html(tds[1].decode_contents()),
            clean_html(tds[2].decode_contents()),
            clean_html(tds[3].decode_contents()),
            clean_html(tds[4].decode_contents()),
            price, iid, did, sku
        ])
        existing_ids.add(sku)
        new_rows += 1

    log.debug(f"Parsed {len(trs)} rows, {new_rows} new written")
    return len(trs)


def loose_grown_diamonds_scrappe():
    log.info("=" * 50)
    log.info("Starting Loose Grown Diamond scrape")

    # ── Rotate CSV if older than 1 day ───────────────────────────
    rotated = rotate_old_file(CSV_FILE)
    if rotated:
        log.info("Fresh run — old data archived, starting from scratch.")
    # ─────────────────────────────────────────────────────────────

    init_csv()
    existing_ids = load_existing_skus()
    start        = get_start_value()
    total_saved  = 0

    log.info(f"Starting from offset: {start}")

    while True:
        html_block, next_start = fetch_page(start)

        if not html_block:
            log.warning("Empty response received — stopping.")
            break

        with open(CSV_FILE, "a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            rows   = parse_rows(html_block, existing_ids, writer)

        if rows == 0:
            log.info("No new rows parsed — all pages complete.")
            break

        total_saved += rows
        start = next_start
        save_state(start)
        log.info(f"Saved up to offset {start} | Session total: {total_saved:,}")

    log.info(f"Loose Grown scrape complete. Total saved this run: {total_saved:,}")
    log.info("=" * 50)

    return {"total_saved": total_saved}


if __name__ == "__main__":
    loose_grown_diamonds_scrappe()