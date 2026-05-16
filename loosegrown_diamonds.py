import requests, os, csv, re, datetime
from logger import get_logger

log = get_logger("loosegrown")

URL = "https://www.loosegrowndiamond.com/wp-json/ls/v1/inventory-filter"

COOKIES = (
    "_gcl_au=1.1.915178826.1775805144; ccid.14761153=358413144.4716173266; "
    "_ga=GA1.1.1114979395.1775805144; _fbp=fb.1.1775805144790.253068608175454441; "
    "_tt_enable_cookie=1; _ttp=01KNV3R94KD71R6GMJFAE5303Y_.tt.1; _cs_c=0; "
    "_hjDonePolls=1822879; _uetvid=a045d2c034ac11f1bfe05df903b29b55; "
    "_rdt_uuid=1775805144168.9f355e9b-f043-4706-9162-70aa306208a4; "
    "ttcsid=1778670534555::6f43lnSvvFGlFFxIntG5.6.1778672397282.0::1.1855543.1859114::1862717.28.511.1455::1861327.142.1000; "
    "ttcsid_D6RQPORC77U3V2TBTL8G=1778670534554::o0TVe7FFEVWqAbuddTrK.6.1778672397282.1; "
    "_ga_2DH2FDW0HL=GS2.1.s1778670527; "
    "sucuri_cloudproxy_uuid_3564cbd26=3ef9a0d17e28805dd104d52439206bcc; "
    "PHPSESSID=m239ano1ec2vvppqk39ioo0b66; "
    "_cs_id=e5882140-3c44-a70f-cc82-b31f9a3cfd7d.1775805148.10.1778822543.1778822490.1.1809969148160.1.x; "
    "_cs_s=4.0.U.9.1778824343971"
)

HEADERS = {
    "accept": "application/json, text/plain, */*",
    "accept-language": "en-GB,en-US;q=0.9,en;q=0.8",
    "cache-control": "no-cache",
    "content-type": "application/json",
    "cookie": COOKIES,
    "origin": "https://www.loosegrowndiamond.com",
    "pragma": "no-cache",
    "priority": "u=1, i",
    "referer": "https://www.loosegrowndiamond.com/lab-diamonds/",
    "sec-ch-ua": '"Chromium";v="148", "Google Chrome";v="148", "Not/A)Brand";v="99"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/148.0.0.0 Safari/537.36",
}

PAGE_SIZE  = 500
base_dir   = os.getcwd()
down_files = os.path.join(base_dir, "diamond_files")
os.makedirs(down_files, exist_ok=True)

CSV_FILE   = os.path.join(down_files, "loosegrowndiamond.csv")
STATE_FILE = os.path.join(down_files, "lgd_state.txt")

CSV_COLUMNS = [
    "shape", "carat", "cut", "color", "clarity",
    "sale_price", "price", "org_price",
    "id", "pid", "sku",
    "certificate", "certificate_no", "certificate_type",
    "video", "lot_number", "mm", "dtype",
    "av_table", "depth", "lwratio",
    "ha", "sale", "express_shipping", "hearts_and_arrows",
    "is_overnight", "note", "created_at", "updated_at",
]


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

    mtime    = os.path.getmtime(filepath)
    mod_date = datetime.datetime.fromtimestamp(mtime)
    age      = datetime.datetime.now() - mod_date

    if age.total_seconds() <= 86400:
        log.info(
            f"File is fresh ({age.seconds // 3600}h {(age.seconds % 3600) // 60}m old) — continuing."
        )
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

    if os.path.exists(STATE_FILE):
        os.remove(STATE_FILE)
        log.info("State file reset — will start from offset 1")

    return True

# ─────────────────────────────────────────────────────────────────────────────
def extract_certificate_no(cert_url):
    """
    Extract the certificate number from certificate URLs.
    
    Supports:
    - GIA:    https://www.gia.edu/report-check?locale=en_US&reportno=2494331013
    - IGI:    https://www.igi.org/verify-your-report/?r=LG660452443  (strips 'LG' prefix)
    - GCAL:   https://www.gcalusa.com/certificate-search.html?certificate_id=311830031
    """
    if not cert_url:
        return ""

    # GIA: ?reportno=XXXXXX
    match = re.search(r"[?&]reportno=(\d+)", cert_url)
    if match:
        return match.group(1)

    # IGI: ?r=XXXXXX or ?r=LGXXXXXX (strip leading LG)
    match = re.search(r"[?&]r=(?:LG)?(\d+)", cert_url, re.IGNORECASE)
    if match:
        return match.group(1)

    # GCAL: ?certificate_id=XXXXXX
    match = re.search(r"[?&]certificate_id=(\d+)", cert_url)
    if match:
        return match.group(1)

    return ""


def create_formula(cert_no, cert_url):
    """
    Return an Excel-style HYPERLINK formula for the certificate URL,
    displaying only the numeric certificate number.
    """
    if not cert_url or not cert_no:
        return ""
    return f'=HYPERLINK("{cert_url}", "{cert_no}")'


def get_payload(start):
    return {
        "ls_start": start,
        "ls_per_page": PAGE_SIZE,
        "shape": [],
    }


def load_existing_skus():
    skus = set()
    if os.path.exists(CSV_FILE):
        with open(CSV_FILE, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                sku = (row.get("sku") or "").strip()
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
            csv.writer(f).writerow(CSV_COLUMNS)
        log.info(f"CSV initialised: {CSV_FILE}")


def fetch_page(start):
    log.debug(f"Fetching page at start={start}")
    try:
        r = requests.post(URL, headers=HEADERS, json=get_payload(start), timeout=60)
        r.raise_for_status()
        data = r.json()
        return data
    except Exception as e:
        log.error(f"Failed to fetch page at start={start}: {e}")
        return {}


def parse_rows(items, existing_ids, writer):
    """Write new diamond records from the JSON `data` list to CSV."""
    new_rows = 0

    for item in items:
        sku = (item.get("sku") or "").strip()
        if not sku or sku in existing_ids:
            continue

        # Skip items with certificate_type == 0
        cert_type = item.get("certificate_type") or ""
        if str(cert_type).strip() == "0":
            continue

        cert_url = item.get("certificate") or ""
        cert_no  = extract_certificate_no(cert_url)

        writer.writerow([
            (item.get("shape")          or "").strip().lower(),
            item.get("carat")           or "",
            item.get("cut")             or "",
            item.get("color")           or "",
            item.get("clarity")         or "",
            item.get("sale_price")      or "",
            item.get("price")           or "",
            item.get("org_price")       or "",
            item.get("id")              or "",
            item.get("pid")             or "",
            sku,
            cert_url,
            cert_no,
            cert_type,
            item.get("video")           or "",
            item.get("lot_number")      or "",
            (item.get("mm")             or "").strip(),
            item.get("dtype")           or "",
            item.get("av_table")        or "",
            item.get("depth")           or "",
            item.get("lwratio")         or "",
            item.get("ha")              or "",
            item.get("sale")            or "",
            item.get("express_shipping") or "",
            item.get("hearts_and_arrows") or "",
            item.get("is_overnight")    or "",
            item.get("note")            or "",
            item.get("created_at")      or "",
            item.get("updated_at")      or "",
        ])
        existing_ids.add(sku)
        new_rows += 1

    log.debug(f"Parsed {len(items)} items, {new_rows} new written")
    return new_rows


def loose_grown_diamonds_scrappe():
    log.info("=" * 50)
    log.info("Starting Loose Grown Diamond scrape")

    rotated = rotate_old_file(CSV_FILE)
    if rotated:
        log.info("Fresh run — old data archived, starting from scratch.")

    init_csv()
    existing_ids = load_existing_skus()
    start        = get_start_value()
    total_saved  = 0

    log.info(f"Starting from offset: {start}")

    while True:
        response = fetch_page(start)

        if not response or not response.get("status"):
            log.warning("Empty or failed response — stopping.")
            break

        items         = response.get("data", [])
        total_avail   = response.get("total", 0)
        current_count = response.get("current_count", 0)

        if not items:
            log.info("No items in response — all pages complete.")
            break

        with open(CSV_FILE, "a", newline="", encoding="utf-8") as f:
            writer    = csv.writer(f)
            new_rows  = parse_rows(items, existing_ids, writer)

        total_saved += new_rows
        next_start   = start + 1
        save_state(next_start)

        log.info(
            f"Offset {start} → {next_start} | "
            f"New this page: {new_rows} | "
            f"Session total: {total_saved:,} / {total_avail:,}"
        )

        if next_start > total_avail:
            log.info("Reached end of inventory.")
            break

        start = next_start

    log.info(f"Loose Grown scrape complete. Total saved this run: {total_saved:,}")
    log.info("=" * 50)

    return {"total_saved": total_saved}


if __name__ == "__main__":
    loose_grown_diamonds_scrappe()