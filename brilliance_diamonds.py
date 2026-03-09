from curl_cffi import requests
import time
import csv
import os

# ---------------- CONFIG ---------------- #

URL = "https://worker.brilliance.com/api/v1/lab-grown-diamond-search"
base_dir=os.getcwd()
down_files=os.makedirs('diamond_files',exist_ok=True)

CSV_FILE = f"{base_dir}/diamond_files/brilliance_diamonds.csv"

SLEEP_SECONDS = 0.5
RETRY_SLEEP = 20
MAX_RETRIES = 5
MAX_RESULTS = 48

FIELDS = [
    "nid","shape","price","color","carat","clarity","cut","report",
    "polish","symmetry","depth","table","fluorescence","list_price",
    "girdle","culet","measurement","url","diamond_image_flag",
    "reportNumber","info","alias","fast"
]

# ---------------- SESSION ---------------- #

session = requests.Session(impersonate="chrome")

HEADERS = {
    "accept": "application/json",
    "content-type": "application/json",
    "origin": "https://www.brilliance.com",
    "referer": "https://www.brilliance.com/lab-grown-diamonds"
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

    print(f"Loaded {len(certs)} existing certificates")
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
            # print(r.json())
            # exit(0)
            return r.json()

        if r.status_code == 403:
            print("Cloudflare detected – sleeping…")
            time.sleep(RETRY_SLEEP)
        else:
            print("HTTP Error:", r.status_code)
            time.sleep(5)

    raise Exception("Max retries exceeded")


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
    existing_certs = load_existing_certs()
    file, writer = get_writer()

    page = 60

    try:
        while True:
            data = fetch_page(page)
            diamonds = data.get("diamond", [])

            if not diamonds:
                print("No more pages.")
                break

            inserted = 0

            for d in diamonds:
                cert = normalize_cert(d.get("reportNumber"))

                if not cert or cert in existing_certs:
                    continue

                write_diamond(writer, d, cert)
                existing_certs.add(cert)
                inserted += 1

            print(f"Page {page} → {inserted} new rows (API: {len(diamonds)})")

            page += 1
            time.sleep(SLEEP_SECONDS)

    finally:
        file.close()
        print("CSV saved & closed")


# ---------------- RUN ---------------- #

if __name__ == "__main__":
    brilliance_diamonds_scraper()
