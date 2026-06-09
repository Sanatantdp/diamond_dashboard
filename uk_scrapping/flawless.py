import csv
import json
import logging
import os
import random
import time
from threading import Event, Lock
from concurrent.futures import ThreadPoolExecutor, Future, as_completed

from curl_cffi import requests as cffi_requests

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
SCRAPER_NAME      = "flawless"
API_URL           = "https://stoneapi.octahedral.co.uk/api/v1/data/getlabdiamonds"
PAGE_LIMIT        = 40
MAX_WORKERS       = 4
SLEEP_MIN         = 0.3
SLEEP_MAX         = 0.8
MAX_RETRIES       = 5
RETRY_BACKOFF     = 1.5
CHROME_VERSION    = "chrome120"

BASE_DIR          = os.getcwd()
diamonds_path     = os.path.join(BASE_DIR, "flawless")
diamond_files     = os.path.join(BASE_DIR, "diamond_files")

for d in (diamonds_path, diamond_files):
    os.makedirs(d, exist_ok=True)

CSV_FILE  = "flawless.csv"
MAIN_CSV  = os.path.join(diamonds_path, CSV_FILE)
EXTRA_CSV = os.path.join(diamond_files, CSV_FILE)
STATUS_FILE = os.path.join(diamonds_path, "status.json")

# ── VPN rotation config ───────────────────────────────────────────────────────
VPN_ROTATE_AFTER_FAILS = 5          # consecutive page failures -> rotate VPN
VPN_CONFIG_SUBDIR      = "flawless" # vpnbook/uk/flawless/*.ovpn


class VPNRotateNeeded(Exception):
    """Raised when consecutive page failures indicate the exit IP is burned."""
    def __init__(self, page, consecutive_fails):
        self.page = page
        self.consecutive_fails = consecutive_fails
        super().__init__(
            f"VPN rotate signal: {consecutive_fails} consecutive failed page(s) "
            f"ending at page {page}"
        )

# ── CSV columns (mirrors the API response fields) ─────────────────────────────
FIELDS = [
    "_id", "stock_num", "shape", "carat", "clarity", "color", "cut",
    "polish", "symmetry", "fluorescence", "depth_percent", "table_percent",
    "length", "width", "depth", "l_w_ratio", "girdle", "culet_size",
    "certificate_num", "lab", "location", "image_url", "video_url",
    "our_price", "stone_type",
    # meta
    "status", "page_no",
]

# ── Request headers ───────────────────────────────────────────────────────────
HEADERS = {
    "Accept": "*/*",
    "Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8",
    "Connection": "keep-alive",
    "Origin": "https://www.flawlessfinejewelry.com",
    "Referer": "https://www.flawlessfinejewelry.com/",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "cross-site",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/146.0.0.0 Safari/537.36"
    ),
    "content-type": "application/json",
    "sec-ch-ua": '"Chromium";v="146", "Not-A.Brand";v="24", "Google Chrome";v="146"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
}

BASE_PAYLOAD = {
    "shape": [],
    "filter_price_min": 260,
    "filter_price_max": 191531,
    "filter_carat_min": 0.3,
    "filter_carat_max": 15,
    "filter_cut_min": 0,
    "filter_cut_max": 4,
    "filter_color_min": 0,
    "filter_color_max": 7,
    "filter_clarity_min": 0,
    "filter_clarity_max": 7,
    "filter_l_w_ratio_min": 1,
    "filter_l_w_ratio_max": 2.2,
    "filter_fluorescence_min": 1,
    "filter_fluorescence_max": 4,
    "filter_symmerty_min": 3,
    "filter_symmerty_max": 4,
    "filter_polish_min": 3,
    "filter_polish_max": 4,
    "filter_table_min": 50,
    "filter_table_max": 90,
    "filter_depth_min": 40,
    "filter_depth_max": 90,
    "lab": [],
    "sorting_order": "ASC",
    "sort_by": "our_price",
    "search_ID": "",
    "limit": PAGE_LIMIT,
    "defaultFilters": {
        "filter_carat_max": 15,
        "filter_carat_min": 0.3,
        "filter_depth_max": 90,
        "filter_depth_min": 40,
        "filter_l_w_ratio_max": 2.2,
        "filter_l_w_ratio_min": 1,
        "filter_price_max": 275804,
        "filter_price_min": 374,
        "filter_table_max": 90,
        "filter_table_min": 50,
        "filter_price_min_c": 374,
        "filter_price_max_c": 275804,
    },
}

# ── Status helpers ────────────────────────────────────────────────────────────

def write_status(state: str, **kwargs) -> None:
    data = {"scraper": SCRAPER_NAME, "state": state, "updated_at": time.strftime("%Y-%m-%dT%H:%M:%S"), **kwargs}
    try:
        with open(STATUS_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
    except Exception as e:
        log.warning("Could not write status file: %s", e)


# ── Load existing stock numbers (resume support) ──────────────────────────────

def load_existing_stock_nums(path: str) -> set:
    codes: set = set()
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                v = row.get("stock_num") or row.get("certificate_num")
                if v:
                    codes.add(v)
    log.info("Loaded %d existing records from %s", len(codes), path)
    return codes


# ── Row builder ───────────────────────────────────────────────────────────────

def build_rows(diamonds: list, existing: set, page: int) -> tuple[list[dict], int, int]:
    """Returns (rows_to_write, new_count, dup_count)."""
    rows, new_n, dup_n = [], 0, 0
    for d in diamonds:
        key = d.get("stock_num") or d.get("certificate_num") or d.get("_id")
        is_dup = bool(key and key in existing)

        row = {field: d.get(field) for field in FIELDS[:-2]}
        row["status"] = "duplicate" if is_dup else "new"
        row["page_no"] = page
        rows.append(row)

        if not is_dup and key:
            existing.add(key)

        if is_dup:
            dup_n += 1
        else:
            new_n += 1

    return rows, new_n, dup_n


# ── Single page fetch (with retry) ────────────────────────────────────────────

def fetch_page(page: int) -> list[dict]:
    payload = {**BASE_PAYLOAD, "page_offset": page}
    time.sleep(random.uniform(SLEEP_MIN, SLEEP_MAX))

    last_exc = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            with cffi_requests.Session(impersonate=CHROME_VERSION) as session:
                resp = session.post(API_URL, headers=HEADERS, json=payload, timeout=30)

            if resp.status_code == 400:
                raise ValueError(f"400 Bad Request page {page}: {resp.text[:300]}")

            resp.raise_for_status()
            body = resp.json()

            if not body.get("success"):
                raise ValueError(f"API success=false page {page}: {body}")

            return body["result"]["data"]  # empty list = end of pages

        except ValueError:
            raise
        except Exception as exc:
            last_exc = exc
            wait = min(RETRY_BACKOFF ** attempt, 30) + random.uniform(0, 1)
            log.warning("Page %d | attempt %d/%d | %s — retry in %.1fs",
                        page, attempt, MAX_RETRIES, exc, wait)
            time.sleep(wait)

    raise RuntimeError(f"Page {page} failed after {MAX_RETRIES} attempts") from last_exc


# ── Main scraper ──────────────────────────────────────────────────────────────

def fetch_flawless(rotate_after_fails: int | None = None) -> None:
    existing    = load_existing_stock_nums(MAIN_CSV)
    file_exists = os.path.exists(MAIN_CSV)
    open_mode   = "a" if file_exists else "w"

    total_saved = len(existing)
    total_dups  = 0
    fail_streak = 0

    write_status("running", message="Starting scraper...", diamonds_saved=total_saved)

    BATCH = MAX_WORKERS * 2   # pages per submission wave
    done  = Event()
    lock  = Lock()
    page  = 1

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

            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
                while not done.is_set():
                    batch_pages = list(range(page, page + BATCH))
                    page += BATCH

                    future_to_page: dict[Future, int] = {
                        pool.submit(fetch_page, p): p for p in batch_pages
                    }

                    # Collect this batch; keep results in page order
                    batch_results: dict[int, list[dict]] = {}
                    for future in as_completed(future_to_page):
                        p = future_to_page[future]
                        try:
                            records = future.result()
                            batch_results[p] = records
                            if not records:
                                log.info("Page %d returned 0 records — end of data.", p)
                                done.set()
                                for f in future_to_page:
                                    f.cancel()
                                break
                            fail_streak = 0
                        except Exception as exc:
                            log.error("FAIL page %d: %s", p, exc)
                            batch_results[p] = []
                            fail_streak += 1

                    # Write batch in page order (thread-safe via lock)
                    for p in sorted(batch_results):
                        recs = batch_results[p]
                        if not recs:
                            continue

                        rows, new_n, dup_n = build_rows(recs, existing, p)

                        with lock:
                            writer_main.writerows(rows)
                            writer_extra.writerows(rows)
                            f_main.flush()
                            f_extra.flush()
                            total_saved += new_n
                            total_dups  += dup_n

                        log.info(
                            "Page %4d | items: %3d | new: %3d | dup: %3d | "
                            "total saved: %d",
                            p, len(recs), new_n, dup_n, total_saved,
                        )

                        write_status(
                            "running",
                            current_page=p,
                            diamonds_saved=total_saved,
                            duplicates=total_dups,
                            failed_pages=fail_streak,
                            message=f"Scraping page {p}...",
                        )

                    # Whole batch failing → exit IP likely blocked. In VPN mode,
                    # signal a rotation; resume is automatic (fetch_flawless
                    # restarts from page 1 and dedups against the CSV).
                    if rotate_after_fails and fail_streak >= rotate_after_fails:
                        log.warning(
                            "%d consecutive page failures — signalling VPN rotation.",
                            fail_streak,
                        )
                        raise VPNRotateNeeded(page, fail_streak)

            log.info("--- Scrape loop finished ---")

    except VPNRotateNeeded:
        # propagate up to run_with_vpn_rotation() so it can switch tunnels
        raise
    except Exception as exc:
        write_status(
            "failed",
            diamonds_saved=total_saved,
            duplicates=total_dups,
            failed_pages=fail_streak,
            message=f"Crashed: {exc}",
        )
        raise

    write_status(
        "done",
        diamonds_saved=total_saved,
        duplicates=total_dups,
        failed_pages=fail_streak,
        message="Scrape completed successfully",
        extra={"main_csv": MAIN_CSV, "mirror_csv": EXTRA_CSV},
    )

    log.info("=== Done | %d new diamonds saved ===", total_saved)
    log.info("    Main  CSV : %s", MAIN_CSV)
    log.info("    Mirror CSV: %s", EXTRA_CSV)
    log.info("    Status    : %s", STATUS_FILE)


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

    log.info("=" * 60)
    log.info("Flawless scraper with VPN rotation")
    log.info("Consecutive-fail threshold: %d", VPN_ROTATE_AFTER_FAILS)
    log.info("=" * 60)

    if not is_admin():
        log.error(
            "OpenVPN requires Administrator/root. "
            "Re-run from an elevated shell, or call fetch_flawless() directly."
        )
        return

    try:
        find_openvpn_executable()
    except VPNError as exc:
        log.error("OpenVPN unavailable — aborting without touching configs: %s", exc)
        return

    # uk_scrapping/ -> repo root; VPN configs live in vpnbook/uk/<subdir>/.
    repo_root = Path(__file__).resolve().parent.parent
    vpn_dir = repo_root / "vpnbook" / "uk" / VPN_CONFIG_SUBDIR
    if not vpn_dir.exists():
        log.error("VPN config dir not found: %s", vpn_dir)
        return

    # Shared ProtonVPN credentials, resolved relative to this script (cwd-independent).
    proton_auth = None
    proton_creds = repo_root / "vpnbook" / "proton_credentials.txt"
    if proton_creds.exists():
        user, pwd = parse_credentials_file(proton_creds)
        proton_auth = write_auth_file(user, pwd, proton_creds.parent / "proton_auth.txt")
        log.info("Using ProtonVPN credentials from %s", proton_creds)
    else:
        log.warning(
            "ProtonVPN credentials not found at %s — "
            "falling back to vpn_rotator's per-config lookup.", proton_creds
        )

    openvpn_log = repo_root / "vpnbook" / "openvpn.log"

    configs = sorted(vpn_dir.glob("*.ovpn"))
    if not configs:
        log.error("No .ovpn files in %s", vpn_dir)
        return

    random.shuffle(configs)
    total   = len(configs)
    deleted = 0
    log.info("VPN pool: %d config(s) in %s — trying every config", total, vpn_dir)

    for attempt, cfg in enumerate(configs, start=1):
        log.info("--- VPN attempt %d/%d: %s ---", attempt, total, cfg.name)
        try:
            with VPNConnection(cfg, auth_file=proton_auth, log_file=openvpn_log) as vpn:
                log.info("Tunnel up. Exit IP = %s", vpn.ip_after)
                fetch_flawless(rotate_after_fails=VPN_ROTATE_AFTER_FAILS)
            log.info("Scrape completed — VPN torn down.")
            return

        except VPNRotateNeeded as e:
            log.warning("Rotating VPN (config works, IP burned): %s", e)
            # tunnel already torn down by context manager; keep config, try next

        except VPNError as e:
            if "authentication failed" in str(e).lower():
                log.error(
                    "VPN auth failed on %s — credentials likely stale. "
                    "Aborting (no configs deleted). Check proton_credentials.txt.", cfg.name
                )
                return
            log.error("VPN error on %s: %s — deleting dead config", cfg.name, e)
            try:
                cfg.unlink()
                deleted += 1
                log.info("Deleted non-working VPN config: %s", cfg.name)
            except OSError as del_exc:
                log.warning("Could not delete %s: %s", cfg.name, del_exc)

        except KeyboardInterrupt:
            log.warning("Interrupted by user — exiting VPN loop.")
            return

    log.error(
        "Tried all %d VPN config(s) — none completed the scrape (%d dead config(s) deleted).",
        total, deleted
    )


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    # run_with_vpn_rotation()  → VPN-wrapped scrape with mid-run IP rotation (needs admin)
    # fetch_flawless()         → pure scrape, no VPN (your real IP)
    import sys as _sys
    try:
        run_with_vpn_rotation()
    except KeyboardInterrupt:
        log.warning("Ctrl+C — stopping scraper and tearing down any VPN tunnel.")
        try:
            from vpn_rotator import kill_openvpn_processes
            kill_openvpn_processes()
        except Exception as _exc:
            log.warning("VPN cleanup failed: %s", _exc)
        _sys.exit(130)