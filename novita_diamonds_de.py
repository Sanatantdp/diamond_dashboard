from curl_cffi import requests
import csv, os, time
import datetime

URL = "https://novitadiamonds.de/api/product/diamonds"
CSV_FILE = f"novita_diamonds_eur_{datetime.datetime.now().strftime("%Y-%m-%d")}.csv"
BASE_DIR=os.getcwd()
novita_path=os.path.join(BASE_DIR,'novita-eur')
START_PAGE =1
SLEEP_SECONDS = 1.0
session = requests.Session(impersonate="chrome")

headers = {
    "accept": "application/json, text/javascript, */*; q=0.01",
    "accept-language": "en-GB,en-US;q=0.9,en;q=0.8",
    "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
    "origin": "https://novitadiamonds.de",
    "referer": "https://novitadiamonds.de/en/create/diamanten/buy-lab-grown-diamonds",
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36",
    "x-requested-with": "XMLHttpRequest",
}

cookies = {
    "PHPSESSID": "vuaa74cvalc3dsv28s9ru7taqt",
    "intercom-id-gdyrj32x": "28e8ff81-3c17-4eb9-a7ac-8ec193fc762a",
    "intercom-device-id-gdyrj32x": "b764acd7-8874-48d1-8491-f2d63c25962a",
    "_ga": "GA1.2.2035880902.1772777122"}

payload = {
    "shapes": '["1","4","3","2","6","5","9","8","7","10","11"]',
    "color_from": "1",
    "color_to": "16",
    "cut_from": "1",
    "cut_to": "3",
    "clarity_from": "2",
    "clarity_to": "8",
    "polish_from": "1",
    "polish_to": "2",
    "symmetry_from": "1",
    "symmetry_to": "2",
    "carat_from": "0.3",
    "carat_to": "20",
    "price_from": "150",
    "price_to": "13500",
    "location_description": "all",
    "certificates": "[]",
    "ratio_from": "1",
    "ratio_to": "3",
    "is_carat_infitinitive": "1",
    "is_price_infitinitive": "1",
    "is_ratio_infitinitive": "1",
    "maxResults": "1000",
    
    "order_by": "priceToSellWoLocalTax",
    "order_type": "ASC",
    "preferred_currency": "EUR",
    "current_tab": "result_tab",
    "fetch_count": "false",
    "token": "81f6de78af0d5e3264.cVQUUWY6s1TkJtW6rlwtzN6xOhVw1A6vXIQi5UbUgk8.BhctOEtt3gyIbKH--jNHh7jnVHs7vDnWKcJrvR--sThHFVAyKGjwYIxp4Q"
}

FIELDS = [
    "id","product_id","stock_status_provider_slug","shape","shape_slug","carat",
    "color","is_fancy","fancy_color_slug","clarity","cut","polish","symmetry",
    "table","depth","measurement","image_url","image_external_url","video_url",
    "video_external_url","certificate_laboratory","certificate_url",
    "certificate_file_type","is_onshore","certificate_number","ratio",
    "nd_percentage_discount","price","manual_price","symbol_currency","active"
]

def load_existing_certificates():
    certs = set()
    file_path=os.path.join(novita_path,CSV_FILE)
    if os.path.exists(file_path):
        with open(file_path, "r", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                if row.get("certificate_number"):
                    certs.add(row["certificate_number"])
    print("Loaded", len(certs), "existing certificates")
    return certs


def save_diamonds(diamonds, writer, existing):
    count = 0
    
    for d in diamonds:
        cert = d.get("certificate_number")
        if cert and cert in existing:
            continue

        writer.writerow({
            "id": d.get("id"),
            "product_id": d.get("product_id"),
            "stock_status_provider_slug": d.get("stock_status_provider_slug"),
            "shape": d.get("shape"),
            "shape_slug": d.get("shape_slug"),
            "carat": d.get("carat"),
            "color": d.get("color"),
            "is_fancy": d.get("is_fancy"),
            "fancy_color_slug": d.get("fancy_color_slug"),
            "clarity": d.get("clarity"),
            "cut": d.get("cut"),
            "polish": d.get("polish"),
            "symmetry": d.get("symmetry"),
            "table": d.get("table"),
            "depth": d.get("depth"),
            "measurement": d.get("measurement"),
            "image_url": d.get("image_url"),
            "image_external_url": d.get("image_external_url"),
            "video_url": d.get("video_url"),
            "video_external_url": d.get("video_external_url"),
            "certificate_laboratory": d.get("certificate"),
            "certificate_url": d.get("certificate_url"),
            "certificate_file_type": d.get("certificate_file_type"),
            "is_onshore": d.get("is_onshore"),
            "certificate_number": cert,
            "ratio": d.get("ratio"),
            "nd_percentage_discount": d.get("nd_percentage_discount"),
            "price": d.get("price"),
            "manual_price": d.get("manual_price"),
            "symbol_currency": d.get("currency"),
            "active": d.get("active"),
        })

        if cert:
            existing.add(cert)
        count += 1

    return count


def fetch_novita():
    session = requests.Session()
    existing = load_existing_certificates()
    file_exists = os.path.exists(CSV_FILE)
    file_dir=os.path.join(novita_path,CSV_FILE)
    with open(file_dir, "a" if file_exists else "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)
        if not file_exists:
            writer.writeheader()

        page = START_PAGE

        while True:
            data = payload.copy()
            data["page"] = str(page)

            r =  session.post(URL, headers=headers, cookies=cookies, data=data, timeout=30)
            # print(r.json())

            if r.status_code >= 400:
                print("Status:", r.status_code)
                print("Headers:", r.headers)
                try:
                    print("Body:", r.text[:500])
                except:
                    pass
                time.sleep(2)
                continue

            # print(r.json())

            diamonds = r.json().get("response", {}).get("items", [])

            if not diamonds:
                print("No more pages.")
                break

            inserted = save_diamonds(diamonds, writer, existing)
            print(f"Page {page} → {inserted} new rows")

            page += 1
            # time.sleep(SLEEP_SECONDS)


if __name__ == "__main__":
    fetch_novita()

