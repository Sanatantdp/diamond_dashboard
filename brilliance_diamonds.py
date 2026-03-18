from curl_cffi import requests
import time
import random
import csv
import os
import json
import datetime
from logger import get_logger

log = get_logger("brilliance")

# ---------------- CONFIG ---------------- #

URL = "https://worker.brilliance.com/api/v1/lab-grown-diamond-search"
base_dir = os.getcwd()
os.makedirs('diamond_files', exist_ok=True)

CSV_FILE        = f"{base_dir}/diamond_files/brilliance_diamonds.csv"
CHECKPOINT_FILE = f"{base_dir}/diamond_files/checkpoint.json"

SLEEP_MIN   = 1.0
SLEEP_MAX   = 3.0
SLEEP_EMPTY = 2.0
RETRY_SLEEP = 20
MAX_RETRIES = 5

FIELDS = [
    "nid","shape","price","color","carat","clarity","cut","report",
    "polish","symmetry","depth","table","fluorescence","list_price",
    "girdle","culet","measurement","url","diamond_image_flag",
    "reportNumber","info","alias","fast"
]


# ── FILE DATE CHECK ───────────────────────────────────────────────────────────

def rotate_old_file(filepath):
    """
    If `filepath` exists but was last modified MORE than 1 day ago,
    rename it to  <stem>_YYYY-MM-DD.<ext>  (using its modification date)
    and clear the checkpoint so the scraper starts fresh.

    Returns True if the file was rotated (caller should start fresh).
    Returns False if the file is fresh (≤1 day old) or did not exist.
    """
    if not os.path.exists(filepath):
        return False

    mtime    = os.path.getmtime(filepath)
    mod_date = datetime.datetime.fromtimestamp(mtime)
    age      = datetime.datetime.now() - mod_date

    if age.total_seconds() <= 86400:          # 86 400 s = 1 day
        log.info(
            f"File is fresh ({age.seconds // 3600}h {(age.seconds % 3600) // 60}m old) — continuing."
        )
        return False

    # File is older than 1 day → rename it with its modification date
    date_str  = mod_date.strftime("%Y-%m-%d")
    stem, ext = os.path.splitext(filepath)
    new_name  = f"{stem}_{date_str}{ext}"

    # Avoid overwriting an existing archive file
    counter = 1
    while os.path.exists(new_name):
        new_name = f"{stem}_{date_str}_{counter}{ext}"
        counter += 1

    os.rename(filepath, new_name)
    log.info(f"Old file renamed → {os.path.basename(new_name)}  (was {age.days}d {age.seconds // 3600}h old)")

    # Clear checkpoint so pagination restarts from page 1
    if os.path.exists(CHECKPOINT_FILE):
        os.remove(CHECKPOINT_FILE)
        log.info("Checkpoint cleared — will start from page 1")

    return True

# ─────────────────────────────────────────────────────────────────────────────


# ---------------- SESSION ---------------- #
session = requests.Session(impersonate="chrome124")

HEADERS = {
    "accept": "application/json, text/plain, */*",
    "accept-encoding": "gzip, deflate, br",
    "accept-language": "en-US,en;q=0.9",
    "content-type": "application/json",
    "origin": "https://www.brilliance.com",
    "referer": "https://www.brilliance.com/lab-grown-diamonds",
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-site",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
}

BASE_PAYLOAD = {
    "data": {
        "imgOnly": True,
        "view": "grid",
        "priceMin": 750,
        "priceMax": 100000,
        "caratMin": 1.5,
        "caratMax": 12,
        "colorMin": 6,
        "colorMax": 9,
        "clarityMin": 4,
        "clarityMax": 9,
        "depthMin": 0,
        "depthMax": 90,
        "tableMin": 0,
        "tableMax": 90,
        "shapeList": ["0","2","4","6","8","1","3","5","7","9"],
        "certificateList": [],
        "cutMin": 0,
        "cutMax": 4,
        "polishMin": 0,
        "polishMax": 3,
        "symmetryMin": 0,
        "symmetryMax": 3,
        "fluorMin": 0,
        "fluorMax": 3,
        "fastShipping": 0
    }
}

# ---------------- CHECKPOINT ---------------- #

def save_checkpoint(page, total_inserted):
    with open(CHECKPOINT_FILE, "w") as f:
        json.dump({"last_completed_page": page, "total_inserted": total_inserted}, f, indent=2)
    log.debug(f"Checkpoint saved → page={page}, total={total_inserted}")


def load_checkpoint():
    if not os.path.exists(CHECKPOINT_FILE):
        return 1, 0
    with open(CHECKPOINT_FILE, "r") as f:
        data = json.load(f)
    page  = data.get("last_completed_page", 1) + 1
    total = data.get("total_inserted", 0)
    log.info(f"Resuming from page {page} (previously inserted: {total})")
    return page, total


def clear_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        os.remove(CHECKPOINT_FILE)
        log.info("Checkpoint cleared — scrape fully complete.")

# ---------------- HELPERS ---------------- #

def normalize_cert(cert):
    if not cert:
        return None
    cert = cert.strip().lower()
    if cert.startswith("lg"):
        cert = cert.replace("lg", "", 1)
    return cert


def load_existing_certs():
    certs = set()
    if not os.path.exists(CSV_FILE):
        return certs
    with open(CSV_FILE, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            cert = row.get("reportNumber")
            if cert:
                certs.add(cert)
    log.info(f"Loaded {len(certs)} existing certificates")
    return certs


def get_writer():
    file_exists = os.path.exists(CSV_FILE)
    f = open(CSV_FILE, "a" if file_exists else "w", newline="", encoding="utf-8")
    writer = csv.DictWriter(f, fieldnames=FIELDS)
    if not file_exists:
        writer.writeheader()
    return f, writer


def fetch_page(page):
    payload = BASE_PAYLOAD.copy()
    payload["data"] = payload["data"].copy()
    payload["data"]["pager"] = page

    for attempt in range(MAX_RETRIES):
        r = session.post(URL, headers=HEADERS, json=payload)
        if r.status_code == 200:
            return r.json()
        if r.status_code == 403:
            log.warning(f"[page {page}] Cloudflare 403 – sleeping {RETRY_SLEEP}s (attempt {attempt+1}/{MAX_RETRIES})…")
            time.sleep(RETRY_SLEEP)
        else:
            log.warning(f"[page {page}] HTTP {r.status_code} – sleeping 5s")
            time.sleep(5)

    raise Exception(f"Max retries exceeded on page {page}")


def write_diamond(writer, d, cert):
    writer.writerow({
        "nid": d.get("nid"),
        "shape": d.get("shape"),
        "price": d.get("price"),
        "color": d.get("color"),
        "carat": d.get("carat"),
        "clarity": d.get("clarity"),
        "cut": d.get("cut"),
        "report": d.get("report"),
        "polish": d.get("polish"),
        "symmetry": d.get("symmetry"),
        "depth": d.get("depth"),
        "table": d.get("table"),
        "fluorescence": d.get("fluorescence"),
        "list_price": d.get("list_price"),
        "girdle": d.get("girdle"),
        "culet": d.get("culet"),
        "measurement": d.get("measurement"),
        "url": d.get("url"),
        "diamond_image_flag": d.get("diamond_image_flag"),
        "reportNumber": cert,
        "info": d.get("info"),
        "alias": d.get("alias"),
        "fast": d.get("fast")
    })

# ---------------- MAIN SCRAPER ---------------- #

def brilliance_diamonds_scraper():
    log.info("=" * 50)
    log.info("Starting Brilliance scraper")

    # ── Rotate CSV if older than 1 day ───────────────────────────
    rotated = rotate_old_file(CSV_FILE)
    if rotated:
        log.info("Fresh run — old data archived, starting from page 1.")
    # ─────────────────────────────────────────────────────────────

    existing_certs             = load_existing_certs()
    file, writer               = get_writer()
    start_page, total_inserted = load_checkpoint()
    page = start_page

    try:
        while True:
            data     = fetch_page(page)
            diamonds = data.get("diamond", [])

            if not diamonds:
                log.info("No more pages — scrape complete.")
                clear_checkpoint()
                break

            inserted = 0
            for d in diamonds:
                cert = normalize_cert(d.get("reportNumber"))
                if not cert or cert in existing_certs:
                    continue
                write_diamond(writer, d, cert)
                existing_certs.add(cert)
                inserted += 1

            file.flush()
            total_inserted += inserted
            log.info(f"Page {page} → {inserted} new rows | total: {total_inserted} (API: {len(diamonds)})")

            save_checkpoint(page, total_inserted)
            page += 1

            if inserted == 0:
                log.debug(f"0 new diamonds on page {page - 1} — sleeping {SLEEP_EMPTY}s")
                time.sleep(SLEEP_EMPTY)
            else:
                delay = round(random.uniform(SLEEP_MIN, SLEEP_MAX), 3)
                log.debug(f"Sleeping {delay}s before next page")
                time.sleep(delay)

    except Exception as e:
        log.error(f"Failed on page {page}: {e}")
        log.info("checkpoint.json saved — re-run the script to resume")

    finally:
        file.close()
        log.info("CSV saved & closed")
        log.info("=" * 50)

    return {"total_inserted": total_inserted, "last_page": page}


# ---------------- RUN ---------------- #

if __name__ == "__main__":
    brilliance_diamonds_scraper()