from curl_cffi import requests
import csv, os, time, random, json
import datetime

from status import write_status

# ── Config ────────────────────────────────────────────────────────────────────
SCRAPER_NAME     = "77diamonds"
URL              = "https://www.77diamonds.com/api/bff/shop/diamond-list"
CSV_FILE         = "77diamonds.csv"      # ← no date in filename
BASE_DIR         = os.getcwd()

diamonds_path    = os.path.join(BASE_DIR, "77diamonds")
diamond_files    = os.path.join(BASE_DIR, "diamond_files")

for d in (diamonds_path, diamond_files):
    os.makedirs(d, exist_ok=True)

RESULTS_PER_PAGE = 10000
SLEEP_MIN        = 1.0
SLEEP_MAX        = 2.0

MAIN_CSV  = os.path.join(diamonds_path, CSV_FILE)
EXTRA_CSV = os.path.join(diamond_files, CSV_FILE)

# ── Headers ───────────────────────────────────────────────────────────────────
HEADERS = {
    "accept":             "application/json",
    "accept-language":    "en-GB,en-US;q=0.9,en;q=0.8",
    "content-type":       "application/json",
    "domain":             "https://www.77diamonds.com",
    "origin":             "https://www.77diamonds.com",
    "priority":           "u=1, i",
    "referer":            "https://www.77diamonds.com/engagement-rings?item=-1",
    "sec-ch-ua":          '"Chromium";v="146", "Not-A.Brand";v="24", "Google Chrome";v="146"',
    "sec-ch-ua-mobile":   "?0",
    "sec-ch-ua-platform": '"Windows"',
    "sec-fetch-dest":     "empty",
    "sec-fetch-mode":     "cors",
    "sec-fetch-site":     "same-origin",
    "user-agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/146.0.0.0 Safari/537.36"
    ),
}

COOKIES = {
    "session":            "82d3d743-19d8-483c-9a68-5b51a17f171b",
    "currentCountryId":   "756",
    "currentCurrencyId":  "17",
    "addDiscount":        "EARLYSPRING2026",
    "showCookieBar":      "true",
    "h3": (
        "Fe26.2**bda276330a5ed3643beeaa80f18e659bfb56a32cbaf750712cadbd63091d239a"
        "*PZ-dB6vDHFn77VuryN2gvw*pBPBA_0n8tSnIF0Dqbjle7QQLR4RfP2mRdDd3PogJSaMt5npleYkIwq0cxuLo66Y"
        "hXwJdKS1NVw4Y0zFFaKOm2ciPVOLeM5sovDb4TO16NKogZBfJHZYlxERbu5P07hc"
        "**7c2e108ffb4a08b38ce10b8fc8cc33a6cb3e10e090cfe3fcf0f7b258f793d744"
        "*BlzwkuLPoqCat8lMaY2FsckkQy4opYss95qqg8Ku9JE"
    ),
}


BASE_PAYLOAD = {
    "categoryId":       7,
    "clarities":        [],
    "colors":           [],
    "country":          0,
    "currency":         0,
    "cuts":             [],
    "diamondType":      "-1",
    "isGroupedShapes":  False,
    "itemId":           -1,
    "language":         0,
    "maxCarat":         30,
    "maxDepth":         0,
    "maxPrice":         0,
    "maxRatio":         5,
    "maxTable":         0,
    "minCarat":         0.18,
    "minDepth":         0,
    "minPrice":         0,
    "minRatio":         1,
    "minTable":         0,
    "quickShipping":    False,
    "resultsPerPage":   RESULTS_PER_PAGE,
    "searchBlocked":    False,
    "showPairs":        False,
    "stoneType":        3,
    "withMedia":        False,
    "shapes":           [1,2,3,4,5,6,7,8,9,10],
    "Intensities":      [],
    "Certificates":     [],
    "Polishes":         [],
    "Symmetries":       [],
    "Fluorescences":    [],
    "GemClarity":       [],
    "GemIntensity":     [],
    "GemTreatment":     [],
    "GemClarityTreatment": [],
    "WithMedia":        False,
    "QuickShipping":    False,
    "CountryId":        484,
    "CurrencyId":       28,
    "LanguageId":       1,
    "UserPreference": {
        "CountryId":    484,
        "CurrencyId":   28,
        "LanguageId":   1,
        "DiscountCode": "EARLYSPRING2026",
    },
    "Url": "https://www.77diamonds.com/round-cut-diamonds?item=-1&stone=3",
}

# ── CSV columns ───────────────────────────────────────────────────────────────
FIELDS = [
    "Id", "Code", "StockNumber", "CertificateNumber",
    "ShapeId", "ShapeName", "StoneTypeID", "StoneTypeName",
    "Carat", "CaratWeight",
    "ColorId", "Color", "FancyColorId", "IntensityId", "IntensityName",
    "ClarityId", "Clarity",
    "CutId", "Cut",
    "PolishId", "Polish", "SymmetryId", "Symmetry",
    "FlourescenceId", "Flo",
    "Depth", "Width", "Length", "Measurements",
    "DepthPercent", "TablePercent",
    "Ratio", "Lw", "CuletId", "Culet",
    "CertId", "Cert", "CertificateUrl", "ShowCertificate",
    "FinalSalePrice",
    "PriceWithVat", "PriceWithoutVat", "PriceVat",
    "CurrencySymbol",
    "ImageUrl", "ActualPhotoUrl", "HasImage", "HasVideo",
    "ETA", "SupplierId", "QuickShipping",
    "CountryOfOrigin", "IsSold",
    "DateInserted",
    "IsFancy", "DiscountPercent",
    "status",
    "page_no",
]


def load_existing_codes(file_path: str) -> set:
    codes: set = set()
    if os.path.exists(file_path):
        with open(file_path, "r", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                v = row.get("CertificateNumber") or row.get("Code")
                if v:
                    codes.add(v)
    print(f"Loaded {len(codes)} existing records from {file_path}")
    return codes


def _safe(d: dict, *keys):
    for k in keys:
        if not isinstance(d, dict):
            return None
        d = d.get(k)
    return d


def save_diamonds(
    diamonds: list,
    writer: csv.DictWriter,
    existing: set,
    page: int,
) -> tuple[int, int]:
    new_n = dup_n = 0
    for d in diamonds:
        key    = d.get("CertificateNumber") or d.get("Code")
        
        is_dup = bool(key and key in existing)
        status = "duplicate" if is_dup else "new"

        dp        = d.get("DiamondPrice") or {}
        total_num = _safe(dp, "TotalDiamondPrice", "FinalPrice", "NumericPrice") or {}

        writer.writerow({
            "Id":               d.get("Id"),
            "Code":             d.get("Code"),
            "StockNumber":      d.get("StockNumber"),
            "CertificateNumber": d.get("CertificateNumber"),
            "ShapeId":          d.get("ShapeId"),
            "ShapeName":        d.get("ShapeName"),
            "StoneTypeID":      d.get("StoneTypeID"),
            "StoneTypeName":    d.get("StoneTypeName"),
            "Carat":            d.get("Carat"),
            "CaratWeight":      d.get("CaratWeight"),
            "ColorId":          d.get("ColorId"),
            "Color":            d.get("Color"),
            "FancyColorId":     d.get("FancyColorId"),
            "IntensityId":      d.get("IntensityId"),
            "IntensityName":    d.get("IntensityName"),
            "ClarityId":        d.get("ClarityId"),
            "Clarity":          d.get("Clarity"),
            "CutId":            d.get("CutId"),
            "Cut":              d.get("Cut"),
            "PolishId":         d.get("PolishId"),
            "Polish":           d.get("Polish"),
            "SymmetryId":       d.get("SymmetryId"),
            "Symmetry":         d.get("Symmetry"),
            "FlourescenceId":   d.get("FlourescenceId"),
            "Flo":              d.get("Flo"),
            "Depth":            d.get("Depth"),
            "Width":            d.get("Width"),
            "Length":           d.get("Length"),
            "Measurements":     d.get("Measurements"),
            "DepthPercent":     d.get("DepthPercent"),
            "TablePercent":     d.get("TablePercent"),
            "Ratio":            d.get("Ratio"),
            "Lw":               d.get("Lw"),
            "CuletId":          d.get("CuletId"),
            "Culet":            d.get("Culet"),
            "CertId":           d.get("CertId"),
            "Cert":             d.get("Cert"),
            "CertificateUrl":   d.get("CertificateUrl"),
            "ShowCertificate":  d.get("ShowCertificate"),
            "FinalSalePrice":   d.get("FinalSalePrice"),
            "PriceWithVat":     total_num.get("WithVat"),
            "PriceWithoutVat":  total_num.get("WithoutVat"),
            "PriceVat":         total_num.get("Vat"),
            "CurrencySymbol":   _safe(dp, "CurrencySymbol"),
            "ImageUrl":         d.get("ImageUrl"),
            "ActualPhotoUrl":   d.get("ActualPhotoUrl"),
            "HasImage":         d.get("HasImage"),
            "HasVideo":         d.get("HasVideo"),
            "ETA":              d.get("ETA"),
            "SupplierId":       d.get("SupplierId"),
            "QuickShipping":    d.get("QuickShipping"),
            "CountryOfOrigin":  d.get("CountryOfOrigin"),
            "IsSold":           d.get("IsSold"),
            "DateInserted":     d.get("DateInserted"),
            "IsFancy":          d.get("IsFancy"),
            "DiscountPercent":  d.get("DiscountPercent"),
            "status":           status,
            "page_no":          page,
        })

        if not is_dup and key:
            existing.add(key)
        new_n += (0 if is_dup else 1)
        dup_n += (1 if is_dup else 0)

    return new_n, dup_n


def fetch_77diamonds() -> None:
    session = requests.Session(impersonate="chrome")

    existing    = load_existing_codes(MAIN_CSV)
    file_exists = os.path.exists(MAIN_CSV)
    open_mode   = "a" if file_exists else "w"

    write_status(SCRAPER_NAME, state="running", message="Starting up…")

    total_saved = len(existing)
    page        = 1
    total_pages = 1          # updated once first response arrives
    fail_streak = 0

    try:
        with (
            open(MAIN_CSV,  open_mode, newline="", encoding="utf-8") as f_main,
            open(EXTRA_CSV, open_mode, newline="", encoding="utf-8") as f_extra,
        ):
            writer_main  = csv.DictWriter(f_main,  fieldnames=FIELDS)
            writer_extra = csv.DictWriter(f_extra, fieldnames=FIELDS)

            if not file_exists:
                writer_main.writeheader()
                writer_extra.writeheader()

            while True:
                payload = {
                    **BASE_PAYLOAD,
                    "currentPage":       page,
                    "CurrentPageNumber": page,
                    "ResultsPerPage":    RESULTS_PER_PAGE,
                }

                try:
                    r = session.post(
                        URL,
                        headers=HEADERS,
                        cookies=COOKIES,
                        json=payload,
                        timeout=30,
                    )
                except Exception as exc:
                    fail_streak += 1
                    print(f"  Request error on page {page}: {exc}")
                    write_status(
                        SCRAPER_NAME,
                        state="running",
                        current_page=page,
                        total_pages=total_pages,
                        diamonds_saved=total_saved,
                        failed_pages=fail_streak,
                        message=f"Request error page {page}: {exc}",
                    )
                    time.sleep(random.uniform(SLEEP_MIN, SLEEP_MAX))
                    continue

                if r.status_code >= 400:
                    fail_streak += 1
                    print(f"  HTTP {r.status_code} on page {page} — {r.text[:300]}")
                    write_status(
                        SCRAPER_NAME,
                        state="running",
                        current_page=page,
                        total_pages=total_pages,
                        diamonds_saved=total_saved,
                        failed_pages=fail_streak,
                        message=f"HTTP {r.status_code} on page {page}",
                    )
                    time.sleep(random.uniform(SLEEP_MIN, SLEEP_MAX))
                    continue

                fail_streak = 0
                body        = r.json()
                total       = body.get("Total", 0)
                diamonds    = body.get("Diamonds", [])

                if not diamonds:
                    print("No more diamonds — done.")
                    break

                total_pages = max(1, (total + RESULTS_PER_PAGE - 1) // RESULTS_PER_PAGE)

                new_n, dup_n = save_diamonds(diamonds, writer_main,  existing, page)
                save_diamonds(diamonds,               writer_extra, existing, page)
                f_main.flush()
                f_extra.flush()

                total_saved += new_n
                pct          = round(page / total_pages * 100, 1)

                print(
                    f"Page {page:>5}/{total_pages} | "
                    f"items: {len(diamonds):>5} | "
                    f"new: {new_n}, dup: {dup_n} | "
                    f"total API: {total:,}"
                )

                # ── Live status update every page ─────────────
                write_status(
                    SCRAPER_NAME,
                    state="running",
                    current_page=page,
                    total_pages=total_pages,
                    diamonds_saved=total_saved,
                    duplicates=dup_n,
                    failed_pages=fail_streak,
                    message=f"Processing… {pct}% complete",
                    extra={
                        "last_page_scraped": page,
                        "total_api_count":   total,
                    },
                )
                # ─────────────────────────────────────────────

                if len(diamonds) < RESULTS_PER_PAGE:
                    print("Last page reached.")
                    break

                page += 1
                sleep_secs = random.uniform(SLEEP_MIN, SLEEP_MAX)
                print(f"  sleeping {sleep_secs:.2f}s …")
                time.sleep(sleep_secs)

    except Exception as e:
        write_status(
            SCRAPER_NAME,
            state="failed",
            current_page=page,
            total_pages=total_pages,
            diamonds_saved=total_saved,
            failed_pages=fail_streak,
            message=f"Crashed: {e}",
        )
        raise

    # ── Final status ──────────────────────────────────────────
    write_status(
        SCRAPER_NAME,
        state="done",
        current_page=total_pages,
        total_pages=total_pages,
        diamonds_saved=total_saved,
        failed_pages=fail_streak,
        message="Scrape complete",
        extra={"output_csv": MAIN_CSV, "mirror_csv": EXTRA_CSV},
    )

    print(f"\nSaved to:\n  {MAIN_CSV}\n  {EXTRA_CSV}")


if __name__ == "__main__":
    fetch_77diamonds()