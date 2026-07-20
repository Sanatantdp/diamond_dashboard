from curl_cffi import requests
import csv, os, time, random, json
import datetime
from pathlib import Path

from status import write_status

# ── Config ────────────────────────────────────────────────────────────────────
SCRAPER_NAME     = "77diamonds"
URL              = "https://www.77diamonds.com/api/bff/shop/diamond-list"
CSV_FILE         = "77diamonds.csv"      # ← no date in filename
BASE_DIR         = os.getcwd()

diamond_files    = str(Path(__file__).resolve().parent.parent / "diamond_files" / "germany")

os.makedirs(diamond_files, exist_ok=True)

RESULTS_PER_PAGE = 10000
SLEEP_MIN        = 1
SLEEP_MAX        = 3

VPN_ROTATE_AFTER_FAILS = 5
VPN_CONFIG_SUBDIR      = "77diamonds"


class VPNRotateNeeded(Exception):
    def __init__(self, fails):
        super().__init__(f"VPN rotate: {fails} consecutive failed pages")

MAIN_CSV      = os.path.join(diamond_files, CSV_FILE)
PROGRESS_FILE = os.path.join(diamond_files, "77diamonds_progress.json")

# ── Headers ───────────────────────────────────────────────────────────────────
HEADERS = {
    "accept":             "application/json",
    "accept-language":    "en-GB,en-US;q=0.9,en;q=0.8",
    "content-type":       "application/json",
    "domain":             "https://www.77diamonds.com",
    "origin":             "https://www.77diamonds.com",
    "priority":           "u=1, i",
    "referer":            "https://www.77diamonds.com/de/verlobungsringe/brillant?item=-1&stone=3",
    "sec-ch-ua":          '"Google Chrome";v="149", "Chromium";v="149", "Not)A;Brand";v="24"',
    "sec-ch-ua-mobile":   "?0",
    "sec-ch-ua-platform": '"Windows"',
    "sec-fetch-dest":     "empty",
    "sec-fetch-mode":     "cors",
    "sec-fetch-site":     "same-origin",
    "traceparent":        "00-08da4a6f136520f080a7b86eceb7aff1-3a35aaadf612e955-00",
    "user-agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/149.0.0.0 Safari/537.36"
    ),
}

COOKIES = {
    "session":                      "96faf19a-6e2c-4cae-8590-0873f9468382",
    "addDiscount":                  "LATESPRING2026",
    "trustedsite_visit":            "1",
    "sgtm__ga":                     "GA1.1.567279997.1781081617",
    "FPID":                         "FPID2.2.4I3bF9RTegHr7K3dALe7iqCO1GYvL2qoVppJTWGF0mY=.1781081617",
    "_tt_enable_cookie":            "1",
    "_ttp":                         "01KTRBSD4312YA1TVPZ6B8YDRD_.tt.1",
    "_fbp":                         "fb.1.1781081617660.166783200137986487",
    "_clck":                        "1bwp9ew^2^g6s^0^2352",
    "FPLC": (
        "EIVcoRrjK7CIicbkyONslrg8eJUxE3r4bYoUHd3ez18RQ/RdziJ8rhP/mP3Onubv2vyieMEBcCBu11OU89hL"
        "ReXHFUObobEfONPGuIYqDeL/GiG8KSV415uuzlvqLQ=="
    ),
    "FPAU":                         "1.2.1946190473.1781081620",
    "ometria": (
        "2_cid=ATpGYyNkzCk64Fqn&nses=3&osts=1781081616&sid=e966df151o3r382tJ2X8j"
        "&npv=0&tids=&ecamp=&src=google.com|NOT_SUPPLIED|search||Google||20614"
        "&osrc=google.com|NOT_SUPPLIED|search||Google||20614&slt=1781083220"
    ),
    "_uetsid":                      "ded501c064a911f18b0b39e7d6c99a01",
    "_uetvid":                      "ded585a064a911f188e57542db7ab8d1",
    "ttcsid_CEVGTMJC77U3HAH3HV8G":  "1781081617542::F8PzIlv8pie5EnyPxpMU.1.1781083222990.1",
    "ttcsid":                       "1781081617542::NNPYZYF2ghNR7aO172vD.1.1781083222991.0::1.1598838.1605449::1585367.4.430.2004::1613473.170.3680",
    "_clsk":                        "1lvvz2d^1781084500938^10^1^k.clarity.ms/collect",
    "currentCountryId":             "276",
    "currentCurrencyId":            "2",
    "showCookieBar":                "true",
    "__cf_bm": (
        "9rWL0LtllWD5KC7k_1BQXMHR4UPbhGwGPoVxfRThS8w-1781095957.1539342-1.0.1.1"
        "-v6LKC3LFdWhc9b43k7VE3KY4qYx8_upD410WCBBhhoLHwKiFxBuqvicUBPzcNwZFdaaE414dyzOava"
        "WaHtAwf5yAZ2HtKBtlRGbbBvsnpK8PQcaFdQ0BTArI_DK5Ne1f"
    ),
    "viewport":                     "sm",
    "OptanonAlertBoxClosed":        "2026-06-10T12:56:43.171Z",
    "FPGSID":                       "1.1781094536.1781096203.G-ZEY1KN5E2P.OQ5v8cH6hoAmBKt2Aun47g",
    "OptanonConsent": (
        "isGpcEnabled=0&datestamp=Wed+Jun+10+2026+18%3A26%3A44+GMT%2B0530+(India+Standard+Time)"
        "&version=202510.1.0&browserGpcFlag=0&isIABGlobal=false&hosts=&landingPath=NotLandingPage"
        "&groups=C0001%3A1%2CC0002%3A1%2CC0003%3A1%2CC0004%3A1&AwaitingReconsent=false&geolocation=JP%3B13"
    ),
    "sgtm__ga_ZEY1KN5E2P": (
        "GS2.1.s1781086245$o2$g1$t1781096257$j2$l0$h1599193214"
        "$dqzXfxqoFEXDf-wSJb1CaRWEhcnGkG9uk1Q"
    ),
}


BASE_PAYLOAD = {
    "categoryId":       1,
    "clarities":        [],
    "colors":           [],
    "country":          0,
    "currency":         0,
    "cuts":             [],
    "diamondType":      "-1",
    "isGroupedShapes":  False,
    "itemId":           -1,
    "language":         0,
    "maxCarat":         10,
    "maxDepth":         0,
    "maxPrice":         0,
    "maxRatio":         5,
    "maxTable":         0,
    "minCarat":         0.5,
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
    "CountryId":        276,
    "CurrencyId":       2,
    "LanguageId":       2,
    "UserPreference": {
        "CountryId":    276,
        "CurrencyId":   2,
        "LanguageId":   2,
        "DiscountCode": "LATESPRING2026",
    },
    "Url": "https://www.77diamonds.com/de/verlobungsringe/brillant?item=-1&stone=3",
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


def ts() -> str:
    return datetime.datetime.now().strftime("%H:%M:%S")


def load_progress() -> dict:
    """Return saved progress if an interrupted run exists, else {}."""
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        if data.get("status") != "done":
            return data
    return {}


def save_progress(page: int, total_pages: int, total_saved: int) -> None:
    with open(PROGRESS_FILE, "w", encoding="utf-8") as f:
        json.dump(
            {
                "last_completed_page": page,
                "total_pages":         total_pages,
                "total_saved":         total_saved,
                "updated_at":          datetime.datetime.now().isoformat(),
                "status":              "running",
            },
            f,
            indent=2,
        )


def mark_progress_done() -> None:
    with open(PROGRESS_FILE, "w", encoding="utf-8") as f:
        json.dump({"status": "done", "updated_at": datetime.datetime.now().isoformat()}, f, indent=2)


def load_existing_codes(file_path: str) -> set:
    codes: set = set()
    if os.path.exists(file_path):
        with open(file_path, "r", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                v = row.get("CertificateNumber") or row.get("Code")
                if v:
                    codes.add(v)
    print(f"[{ts()}] Loaded {len(codes)} existing records from {file_path}")
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

    # ── Resume logic ──────────────────────────────────────────────────────────
    progress    = load_progress()
    start_page  = progress.get("last_completed_page", 0) + 1
    if start_page > 1:
        print(f"[{ts()}] Resuming from page {start_page} (last run stopped at page {start_page - 1})")
    else:
        print(f"[{ts()}] Starting fresh from page 1")
    # ─────────────────────────────────────────────────────────────────────────

    existing    = load_existing_codes(MAIN_CSV)
    file_exists = os.path.exists(MAIN_CSV)
    open_mode   = "a" if file_exists else "w"

    write_status(SCRAPER_NAME, state="running", message="Starting up…")

    total_saved = len(existing)
    page        = start_page
    total_pages = progress.get("total_pages", 1)   # carry over known total if resuming
    fail_streak = 0

    try:
        with (
            open(MAIN_CSV, open_mode, newline="", encoding="utf-8") as f_main,
        ):
            writer_main = csv.DictWriter(f_main, fieldnames=FIELDS)

            if not file_exists:
                writer_main.writeheader()

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
                        timeout=60,
                    )
                except Exception as exc:
                    fail_streak += 1
                    print(f"[{ts()}]  Request error on page {page} (streak {fail_streak}): {exc}")
                    write_status(
                        SCRAPER_NAME,
                        state="running",
                        current_page=page,
                        total_pages=total_pages,
                        diamonds_saved=total_saved,
                        failed_pages=fail_streak,
                        message=f"Request error page {page}: {exc}",
                    )
                    if fail_streak >= VPN_ROTATE_AFTER_FAILS:
                        raise VPNRotateNeeded(fail_streak)
                    time.sleep(random.uniform(SLEEP_MIN, SLEEP_MAX))
                    continue

                if r.status_code >= 400:
                    fail_streak += 1
                    print(f"[{ts()}]  HTTP {r.status_code} on page {page} (streak {fail_streak})")
                    write_status(
                        SCRAPER_NAME,
                        state="running",
                        current_page=page,
                        total_pages=total_pages,
                        diamonds_saved=total_saved,
                        failed_pages=fail_streak,
                        message=f"HTTP {r.status_code} on page {page}",
                    )
                    if fail_streak >= VPN_ROTATE_AFTER_FAILS:
                        raise VPNRotateNeeded(fail_streak)
                    time.sleep(random.uniform(SLEEP_MIN, SLEEP_MAX))
                    continue

                fail_streak = 0
                body        = r.json()
                total       = body.get("Total", 0)
                diamonds    = body.get("Diamonds", [])

                if not diamonds:
                    print(f"[{ts()}] No more diamonds — done.")
                    break

                total_pages = max(1, (total + RESULTS_PER_PAGE - 1) // RESULTS_PER_PAGE)

                new_n, dup_n = save_diamonds(diamonds, writer_main, existing, page)
                f_main.flush()

                total_saved += new_n
                pct          = round(page / total_pages * 100, 1)

                save_progress(page, total_pages, total_saved)

                print(
                    f"[{ts()}] Page {page:>5}/{total_pages} | "
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
                    print(f"[{ts()}] Last page reached.")
                    break

                page += 1
                sleep_secs = random.uniform(SLEEP_MIN, SLEEP_MAX)
                print(f"[{ts()}]  sleeping {sleep_secs:.2f}s …")
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
        extra={"output_csv": MAIN_CSV},
    )

    mark_progress_done()
    print(f"\n[{ts()}] Saved to:\n  {MAIN_CSV}")


def run_with_vpn_rotation():
    from pathlib import Path as _Path
    from vpn_rotator import (
        VPNConnection,
        VPNError,
        find_openvpn_executable,
        is_admin,
        parse_credentials_file,
        write_auth_file,
    )
    import logging as _logging
    log = _logging.getLogger("77diamonds")

    log.info("=" * 60)
    log.info("77Diamonds scraper with VPN rotation")
    log.info(f"Rotate after {VPN_ROTATE_AFTER_FAILS} consecutive failures")
    log.info("=" * 60)

    if not is_admin():
        log.error("OpenVPN requires Administrator/root — re-run from an elevated shell.")
        return

    try:
        find_openvpn_executable()
    except VPNError as exc:
        log.error("OpenVPN unavailable: %s", exc)
        return

    repo_root = _Path(__file__).resolve().parent.parent
    vpn_dir   = repo_root / "vpnbook" / "germany" / VPN_CONFIG_SUBDIR
    if not vpn_dir.exists():
        log.error("VPN config dir not found: %s", vpn_dir)
        return

    proton_auth  = None
    proton_creds = repo_root / "vpnbook" / "proton_credentials.txt"
    if proton_creds.exists():
        user, pwd   = parse_credentials_file(proton_creds)
        proton_auth = write_auth_file(user, pwd, proton_creds.parent / "proton_auth.txt")
        log.info("Using ProtonVPN credentials from %s", proton_creds)
    else:
        log.warning("proton_credentials.txt not found — falling back to per-config lookup")

    openvpn_log = repo_root / "vpnbook" / "openvpn.log"
    configs     = sorted(vpn_dir.glob("*.ovpn"))
    if not configs:
        log.error("No .ovpn files in %s", vpn_dir)
        return

    random.shuffle(configs)
    total   = len(configs)
    deleted = 0
    log.info("VPN pool: %d config(s) in %s", total, vpn_dir)

    for attempt, cfg in enumerate(configs, start=1):
        log.info("--- VPN attempt %d/%d: %s ---", attempt, total, cfg.name)
        try:
            with VPNConnection(cfg, auth_file=proton_auth, log_file=openvpn_log) as vpn:
                log.info("Tunnel up. Exit IP = %s", vpn.ip_after)
                fetch_77diamonds()
            log.info("Scrape completed — VPN torn down.")
            return

        except VPNRotateNeeded as e:
            log.warning("Rotating VPN (IP burned after %d fails): %s", VPN_ROTATE_AFTER_FAILS, e)

        except VPNError as e:
            if "authentication failed" in str(e).lower():
                log.error("VPN auth failed on %s — aborting. Check proton_credentials.txt.", cfg.name)
                return
            log.error("VPN error on %s: %s — deleting dead config", cfg.name, e)
            try:
                cfg.unlink()
                deleted += 1
            except OSError as del_exc:
                log.warning("Could not delete %s: %s", cfg.name, del_exc)

        except KeyboardInterrupt:
            log.warning("Interrupted — exiting VPN loop.")
            return

    log.error("Tried all %d VPN config(s) — none completed the scrape (%d deleted).", total, deleted)


if __name__ == "__main__":
    import sys
    try:
        # if "--no-vpn" in sys.argv:
        fetch_77diamonds()
    #     else:
    #         run_with_vpn_rotation()
    except KeyboardInterrupt:
        print(f"\n[{ts()}] Ctrl+C — stopping.")
        try:
            from vpn_rotator import kill_openvpn_processes
            kill_openvpn_processes()
        except Exception:
            pass