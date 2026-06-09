from curl_cffi import requests
import csv
import os
import time
import random
import json
import datetime

from status import write_status

# ── Config ────────────────────────────────────────────────────────────────────
SCRAPER_NAME = "qualitydiamonds"
URL = "https://www.qualitydiamonds.co.uk/umbraco/Surface/CategorySurface/GetDiamondListJsonData"

BASE_DIR = os.getcwd()
diamonds_path = os.path.join(BASE_DIR, "qualitydiamonds")
diamond_files = os.path.join(BASE_DIR, "diamond_files")

for d in (diamonds_path, diamond_files):
    os.makedirs(d, exist_ok=True)

CSV_FILE = "qualitydiamonds.csv"
MAIN_CSV = os.path.join(diamonds_path, CSV_FILE)
EXTRA_CSV = os.path.join(diamond_files, CSV_FILE)

RESULTS_PER_PAGE = 1200
SLEEP_MIN = 0.2
SLEEP_MAX = 0.5
MAX_RETRIES = 5

# ── VPN rotation config ───────────────────────────────────────────────────────
VPN_ROTATE_AFTER_FAILS = 5                  # a page that exhausts retries rotates VPN
VPN_CONFIG_SUBDIR      = "qualitydiamonds"  # vpnbook/uk/qualitydiamonds/*.ovpn


class VPNRotateNeeded(Exception):
    """Raised when a page exhausts its retries — the exit IP is likely burned."""
    def __init__(self, page, consecutive_fails):
        self.page = page
        self.consecutive_fails = consecutive_fails
        super().__init__(
            f"VPN rotate signal: page {page} failed after {consecutive_fails} attempts"
        )

# ── Headers ───────────────────────────────────────────────────────────────────
HEADERS = {
    "accept": "application/json, text/javascript, */*; q=0.01",
    "accept-language": "en-GB,en-US;q=0.9,en;q=0.8",
    "content-type": "application/json;charset=UTF-8",
    "domain": "https://www.qualitydiamonds.co.uk",
    "origin": "https://www.qualitydiamonds.co.uk",
    "priority": "u=1, i",
    "referer": "https://www.qualitydiamonds.co.uk/loose-diamonds/buy-lab-grown-diamonds",
    "sec-ch-ua": '"Chromium";v="134", "Not-A.Brand";v="24", "Google Chrome";v="134"',
    "sec-ch-ua-mobile": "?0",
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "user-agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/134.0.0.0 Safari/537.36"
    ),
}

# ── Base Payload ──────────────────────────────────────────────────────────────
BASE_PAYLOAD = {
    "Shape": ["Round Brilliant", "Princess", "Emerald", "Asscher", "Marquise", "Pear", "Oval", "Heart", "Radiant", "Cushion"],
    "DiamondType": "Lab-Grown",
    "MinPrice": 0,
    "MaxPrice": 1000000,
    "MinCarat": 0.2,
    "MaxCarat": 15,
    "SelectedColourType": "WHITE",
    "SelectedFancyColours": [],
    "SelectedFancyIntensities": [],
    "MinGemstonePrice": 0,
    "MaxGemstonePrice": 1000000,
    "MinLength": 0.2,
    "MaxLength": 50,
    "SelectedColourVals": ["2823", "2824", "2825", "2826", "2827", "2828", "2829", "2830"],
    "SelectedGemstoneShapes": [],
    "GemstoneType": "SAPPHIRE",
    "SelectedGemstoneColours": [],
    "SelectedGemstoneClarities": [],
    "SelectedClarityVals": ["2733", "2734", "2735", "2736", "2737", "2738", "2739", "2740"],
    "SelectedCutVals": ["2746", "2799", "2800", "2801"],
    "SelectedPolishVals": ["2808", "2809", "2810", "2811"],
    "SelectedSymmetryVals": ["2832", "2833", "2834", "2835"],
    "SelectedFluorescenceVals": ["2816", "2819", "2902", "2903", "2904"],
    "SelectedCertVals": ["2812", "2814", "2815"],
    "AvailableShapes": ["Round Brilliant", "Princess", "Emerald", "Asscher", "Marquise", "Pear", "Oval", "Heart", "Radiant", "Cushion"],
    "ExchangeRate": "0.82644628",
}

# ── CSV Columns ───────────────────────────────────────────────────────────────
FIELDS = [
    "Name", "FancyName", "Type", "Shape", "Carats", "Colour", "ColourType",
    "GemstoneColour", "FancyColour", "FancyIntensity", "Clarity", "CertBy",
    "CertNumber", "CertUrl", "DiamondScore", "CutGrade", "Table", "Treatment",
    "Depth", "Culet", "Girdle", "Ratio", "Polish", "Symmetry", "Fluorescence",
    "MeasLength", "MeasWidth", "MeasDepth", "UKStones", "DiamondVideo",
    "DiamondPhoto", "SpinUrl", "SpinFrameCount", "SpinTopIndex", "MarkupPrice",
    "Price", "Id", "SupplierID", "NivodaStockID", "Button", "DT_RowId",
    "Source", "StoneType", "PriceValue", "PriceExVatValue", "status", "page_no"
]


def load_existing_codes(file_path: str) -> set:
    """Load all CertNumbers already saved in a CSV file."""
    codes: set = set()
    if os.path.exists(file_path):
        with open(file_path, "r", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                v = row.get("CertNumber") or row.get("Code")
                if v:
                    codes.add(v)
    print(f"Loaded {len(codes)} existing records from {file_path}")
    return codes


def save_diamonds(
    diamonds: list,
    writer: csv.DictWriter,
    existing: set,
    page: int,
    update_existing: bool = True,
) -> tuple[int, int]:
    """
    Write only NEW diamonds to the CSV writer.

    Args:
        diamonds:        List of diamond dicts from the API.
        writer:          CSV writer to write rows into.
        existing:        Set of already-seen CertNumbers.
        page:            Current page number (stored in each row).
        update_existing: If True, add new CertNumbers into `existing`
                         so the second writer doesn't re-insert them.
                         Set False for the mirror (extra) writer.

    Returns:
        (new_count, duplicate_count)
    """
    new_n = dup_n = 0

    for d in diamonds:
        key = d.get("CertNumber") or d.get("Code")
        is_dup = bool(key and key in existing)

        if is_dup:
            dup_n += 1
            continue  # ← skip duplicates entirely; do NOT write them

        # Build the row for new diamonds only
        row = {field: d.get(field) for field in FIELDS[:-2]}  # exclude status & page_no
        row["status"] = "new"
        row["page_no"] = page
        writer.writerow(row)

        if key and update_existing:
            existing.add(key)  # mark as seen so the mirror won't re-add

        new_n += 1

    return new_n, dup_n


def fetch_qualitydiamonds(rotate_after_fails: int | None = None) -> None:
    session = requests.Session(impersonate="chrome")

    existing = load_existing_codes(MAIN_CSV) | load_existing_codes(EXTRA_CSV)

    write_status(SCRAPER_NAME, state="running", message="Starting scraper...")

    total_saved = len(existing)
    page = 1
    total_pages = 1
    fail_streak = 0

    try:
        main_exists = os.path.exists(MAIN_CSV)
        extra_exists = os.path.exists(EXTRA_CSV)

        main_empty = (not main_exists) or os.path.getsize(MAIN_CSV) == 0
        extra_empty = (not extra_exists) or os.path.getsize(EXTRA_CSV) == 0

        open_mode_main = "a" if main_exists else "w"
        open_mode_extra = "a" if extra_exists else "w"

        with (
            open(MAIN_CSV, open_mode_main, newline="", encoding="utf-8") as f_main,
            open(EXTRA_CSV, open_mode_extra, newline="", encoding="utf-8") as f_extra,
        ):
            writer_main = csv.DictWriter(f_main, fieldnames=FIELDS)
            writer_extra = csv.DictWriter(f_extra, fieldnames=FIELDS)

            # Write headers only if the files are empty / new
            if main_empty:
                writer_main.writeheader()
            if extra_empty:
                writer_extra.writeheader()

            while True:
                payload = {**BASE_PAYLOAD, "Page": page, "PageSize": RESULTS_PER_PAGE}

                # ── Fetch with retries ─────────────────────────────────────────
                for attempt in range(1, MAX_RETRIES + 1):
                    try:
                        r = session.post(
                            URL,
                            headers=HEADERS,
                            json=payload,
                            timeout=40,
                        )
                        r.raise_for_status()
                        break
                    except Exception as exc:
                        if attempt == MAX_RETRIES:
                            # A blocked exit IP exhausts every retry. In VPN mode,
                            # signal a rotation so the caller switches tunnels
                            # (resume is automatic — restart from page 1 + dedup).
                            if rotate_after_fails:
                                print(f"  Page {page} exhausted retries — signalling VPN rotation.")
                                raise VPNRotateNeeded(page, MAX_RETRIES) from exc
                            raise
                        wait = min(2 ** attempt, 30) + random.uniform(0, 1)
                        print(f"  Retry {attempt}/{MAX_RETRIES} for page {page} in {wait:.1f}s | Error: {exc}")
                        time.sleep(wait)

                body = r.json()
                total = body.get("recordsTotal", 0)
                diamonds = body.get("data", [])

                if not diamonds:
                    print("No more diamonds — scraping finished.")
                    break

                total_pages = max(1, (total + RESULTS_PER_PAGE - 1) // RESULTS_PER_PAGE)

                # ── Write new records only ─────────────────────────────────────
                # update_existing=True  → adds new CertNumbers to `existing`
                new_n, dup_n = save_diamonds(diamonds, writer_main, existing, page, update_existing=True)

                # update_existing=False → `existing` already updated above;
                #                         mirror just writes the same new rows
                save_diamonds(diamonds, writer_extra, existing, page, update_existing=False)

                f_main.flush()
                f_extra.flush()

                total_saved += new_n
                pct = round(page / total_pages * 100, 1)

                print(
                    f"Page {page:>5}/{total_pages} | "
                    f"items: {len(diamonds):>5} | "
                    f"new: {new_n:>4} | dup: {dup_n:>4} | "
                    f"total in DB: {total:,}"
                )

                write_status(
                    SCRAPER_NAME,
                    state="running",
                    current_page=page,
                    total_pages=total_pages,
                    diamonds_saved=total_saved,
                    duplicates=dup_n,
                    failed_pages=fail_streak,
                    message=f"Progress: {pct}%",
                    extra={
                        "last_page_scraped": page,
                        "total_api_count": total,
                    },
                )

                if len(diamonds) < RESULTS_PER_PAGE:
                    print("Last page reached.")
                    break

                page += 1
                sleep_secs = random.uniform(SLEEP_MIN, SLEEP_MAX)
                print(f"  Sleeping {sleep_secs:.2f}s …")
                time.sleep(sleep_secs)

    except VPNRotateNeeded:
        # propagate up to run_with_vpn_rotation() so it can switch tunnels
        raise
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

    # ── Final status ──────────────────────────────────────────────────────────
    write_status(
        SCRAPER_NAME,
        state="done",
        current_page=total_pages,
        total_pages=total_pages,
        diamonds_saved=total_saved,
        failed_pages=fail_streak,
        message="Scrape completed successfully",
        extra={"output_csv": MAIN_CSV, "mirror_csv": EXTRA_CSV},
    )

    print(f"\nScrape finished!\n   Main CSV: {MAIN_CSV}\n   Extra CSV: {EXTRA_CSV}")


# ── VPN-WRAPPED RUN ───────────────────────────────────────────────────────────
def run_with_vpn_rotation():
    """
    Run the scraper inside an OpenVPN tunnel, trying EVERY config in
    vpnbook/uk/<VPN_CONFIG_SUBDIR>/ until one completes the scrape.

    Per-config outcome:
      - success         → scrape done, return.
      - VPNRotateNeeded → tunnel works but the exit IP got blocked; keep the
                          config and rotate to the next one.
      - VPNError (dead) → the config failed to connect; DELETE the .ovpn file
                          and move on.

    Safety: a missing OpenVPN binary or an auth failure is an *environmental*
    problem (not a dead config), so those abort the run WITHOUT deleting
    anything. The scraper restarts from page 1 and dedups against the CSV, so
    progress is preserved across each new exit IP. Requires Administrator / root.
    """
    from pathlib import Path
    from vpn_rotator import (
        VPNConnection,
        VPNError,
        find_openvpn_executable,
        is_admin,
        parse_credentials_file,
        write_auth_file,
    )

    print("=" * 60)
    print("QualityDiamonds scraper with VPN rotation")
    print(f"Consecutive-fail threshold: {VPN_ROTATE_AFTER_FAILS}")
    print("=" * 60)

    if not is_admin():
        print(
            "OpenVPN requires Administrator/root. "
            "Re-run from an elevated shell, or call fetch_qualitydiamonds() directly."
        )
        return

    try:
        find_openvpn_executable()
    except VPNError as exc:
        print(f"OpenVPN unavailable — aborting without touching configs: {exc}")
        return

    # uk_scrapping/ -> repo root; VPN configs live in vpnbook/uk/<subdir>/.
    repo_root = Path(__file__).resolve().parent.parent
    vpn_dir = repo_root / "vpnbook" / "uk" / VPN_CONFIG_SUBDIR
    if not vpn_dir.exists():
        print(f"VPN config dir not found: {vpn_dir}")
        return

    # Shared ProtonVPN credentials, resolved relative to this script (cwd-independent).
    proton_auth = None
    proton_creds = repo_root / "vpnbook" / "proton_credentials.txt"
    if proton_creds.exists():
        user, pwd = parse_credentials_file(proton_creds)
        proton_auth = write_auth_file(user, pwd, proton_creds.parent / "proton_auth.txt")
        print(f"Using ProtonVPN credentials from {proton_creds}")
    else:
        print(
            f"ProtonVPN credentials not found at {proton_creds} — "
            f"falling back to vpn_rotator's per-config lookup."
        )

    openvpn_log = repo_root / "vpnbook" / "openvpn.log"

    configs = sorted(vpn_dir.glob("*.ovpn"))
    if not configs:
        print(f"No .ovpn files in {vpn_dir}")
        return

    random.shuffle(configs)
    total = len(configs)
    deleted = 0
    print(f"VPN pool: {total} config(s) in {vpn_dir} — trying every config")

    for attempt, cfg in enumerate(configs, start=1):
        print(f"--- VPN attempt {attempt}/{total}: {cfg.name} ---")
        try:
            with VPNConnection(cfg, auth_file=proton_auth, log_file=openvpn_log) as vpn:
                print(f"Tunnel up. Exit IP = {vpn.ip_after}")
                fetch_qualitydiamonds(rotate_after_fails=VPN_ROTATE_AFTER_FAILS)
            print("Scrape completed — VPN torn down.")
            return

        except VPNRotateNeeded as e:
            print(f"Rotating VPN (config works, IP burned): {e}")
            # tunnel already torn down by context manager; keep config, try next

        except VPNError as e:
            if "authentication failed" in str(e).lower():
                print(
                    f"VPN auth failed on {cfg.name} — credentials likely stale. "
                    f"Aborting (no configs deleted). Check proton_credentials.txt."
                )
                return
            print(f"VPN error on {cfg.name}: {e} — deleting dead config")
            try:
                cfg.unlink()
                deleted += 1
                print(f"Deleted non-working VPN config: {cfg.name}")
            except OSError as del_exc:
                print(f"Could not delete {cfg.name}: {del_exc}")

        except KeyboardInterrupt:
            print("Interrupted by user — exiting VPN loop.")
            return

    print(
        f"Tried all {total} VPN config(s) — none completed the scrape "
        f"({deleted} dead config(s) deleted)."
    )


if __name__ == "__main__":
    # run_with_vpn_rotation()    → VPN-wrapped scrape with mid-run IP rotation (needs admin)
    # fetch_qualitydiamonds()    → pure scrape, no VPN (your real IP)
    import sys as _sys
    try:
        run_with_vpn_rotation()
    except KeyboardInterrupt:
        print("Ctrl+C — stopping scraper and tearing down any VPN tunnel.")
        try:
            from vpn_rotator import kill_openvpn_processes
            kill_openvpn_processes()
        except Exception as _exc:
            print(f"VPN cleanup failed: {_exc}")
        _sys.exit(130)
