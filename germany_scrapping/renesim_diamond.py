from curl_cffi import requests as curl_requests
import csv, os, time, random, json, logging
import datetime
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from status import write_status


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        logger.setLevel(logging.DEBUG)
        fmt = logging.Formatter(
            "%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        ch.setFormatter(fmt)
        logger.addHandler(ch)
        log_dir = os.path.join(os.getcwd(), "logs")
        os.makedirs(log_dir, exist_ok=True)
        fh = logging.FileHandler(
            os.path.join(log_dir, f"{name}.log"), encoding="utf-8"
        )
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(fmt)
        logger.addHandler(fh)
    return logger

log = get_logger("renesim")

# ── Config ────────────────────────────────────────────────────────────────────
SCRAPER_NAME = "renesim"
URL          = "https://py.yorxs.de/live/graphql/"
CSV_FILE     = "renesim_eur.csv"          # ← no date in filename
BASE_DIR     = os.getcwd()

PAGE_SIZE  = 20
TIMEOUT    = 45       # increased from 30 — give slow responses more room
WORKERS    = 5        # concurrent page fetchers; raise to 8-10 if the server allows it

VPN_ROTATE_AFTER_FAILS = 3          # rotate sooner (was 5) — don't burn the IP
VPN_CONFIG_SUBDIR      = "renesim"


class VPNRotateNeeded(Exception):
    def __init__(self, fails):
        super().__init__(f"VPN rotate: {fails} consecutive failed pages")

DELAY_MIN       = 0.2   # was 0.2 — space out requests
DELAY_MAX       = 0.5   # was 0.5
RATELIMIT_PAUSE = 60    # was 30 — longer pause on 429/503

renesim_path  = os.path.join(BASE_DIR, "renesim")
diamond_files = str(Path(__file__).resolve().parent.parent / "diamond_files" / "germany")

for d in (renesim_path, diamond_files):
    os.makedirs(d, exist_ok=True)

MAIN_CSV      = os.path.join(renesim_path,  CSV_FILE)
EXTRA_CSV     = os.path.join(diamond_files, CSV_FILE)
PROGRESS_FILE = os.path.join(diamond_files, "renesim_progress.json")



# ── Headers ───────────────────────────────────────────────────────────────────
HEADERS = {
    "Accept":             "*/*",
    "Accept-Language":    "en-GB,en-US;q=0.9,en;q=0.8",
    "Connection":         "keep-alive",
    "Content-Type":       "application/json",
    "Origin":             "https://www.renesim.com",
    "Referer":            "https://www.renesim.com/",
    "Sec-Fetch-Dest":     "empty",
    "Sec-Fetch-Mode":     "cors",
    "Sec-Fetch-Site":     "cross-site",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/146.0.0.0 Safari/537.36"
    ),
    "sec-ch-ua":          '"Chromium";v="146", "Not-A.Brand";v="24", "Google Chrome";v="146"',
    "sec-ch-ua-mobile":   "?0",
    "sec-ch-ua-platform": '"Windows"',
}

# ── GraphQL query ─────────────────────────────────────────────────────────────
GQL_QUERY = """{{
  diamondData(
    page: "{page}",
    shapes: "",
    color: "",
    carat: "0.50, 15",
    clarity: "",
    symmetry: "",
    polish: "",
    price: "100, 2000000",
    table: "5.1,93",
    depthPer: "5.26,99.10",
    cut: "",
    fluoroscence: "",
    lab: "",
    diamondType: "lab-white",
    currencyFlag: "EUR",
    bulkPageData: "no",
    orderBy: "final_price_eur",
    langCode: "DE",
    taxPercentage: "8",
    sortBy: "asc"
  ) {{
    dataCount
    diamondsReturned
    pageNo
    diamond {{
      diamondId
      fancyColorIntensity
      fancyColorOvertone
      shape
      caratWeight
      color
      fancyColor
      clarity
      cut
      symmetry
      polish
      deptPerc
      tablePerc
      length
      width
      depth
      ratio
      measurement
      girdleMin
      girdleMax
      girdleCondition
      culet
      culetCondition
      flourIntensity
      certificateFile
      lab
      finalPriceEur
      finalPriceEurTax
      finalPriceUsd
      originalTotalPriceEur
      pricePerCtEur
      certificateNumber
      stockNumber
      diamondImage
      diamondVideo
      diamondType
      currencyCode
      currencySymbol
    }}
  }}
}}"""

# ── CSV columns ───────────────────────────────────────────────────────────────
FIELDS = [
    "diamondId", "stockNumber", "certificateNumber",
    "shape", "caratWeight",
    "color", "fancyColor", "fancyColorIntensity", "fancyColorOvertone",
    "clarity", "cut", "symmetry", "polish",
    "deptPerc", "tablePerc",
    "length", "width", "depth", "ratio", "measurement",
    "girdleMin", "girdleMax", "girdleCondition",
    "culet", "culetCondition",
    "flourIntensity",
    "lab", "certificateFile",
    "finalPriceEur", "finalPriceEurTax", "finalPriceUsd",
    "originalTotalPriceEur", "pricePerCtEur",
    "currencyCode", "currencySymbol",
    "diamondImage", "diamondVideo",
    "diamondType",
    "status",
    "page_no",
]


# ══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def make_session() -> curl_requests.Session:
    s = curl_requests.Session(impersonate="chrome")
    s.headers.update(HEADERS)
    return s


def build_payload(page: int) -> dict:
    return {"query": GQL_QUERY.format(page=page)}


def load_progress() -> dict:
    if os.path.exists(PROGRESS_FILE):
        try:
            with open(PROGRESS_FILE) as f:
                data = json.load(f)
            if data.get("status") == "done":
                log.info("Previous run completed successfully — starting fresh")
                return {"done_pages": [], "total_saved": 0, "total_pages": None}
            last_page   = data.get("last_page", 0)
            done_count  = len(data.get("done_pages", []))
            total_pages = data.get("total_pages") or "?"
            log.info(
                f"Interrupted run found — "
                f"stopped at page {last_page} / {total_pages} | "
                f"pages done: {done_count} | "
                f"saved: {data.get('total_saved', 0):,} | "
                f"resuming from page {last_page + 1}"
            )
            return data
        except Exception as e:
            log.warning(f"Could not read progress file: {e} — starting fresh")
    return {"done_pages": [], "total_saved": 0, "total_pages": None}


def save_progress(done_pages: set, total_saved: int, total_pages: int, status: str = "running"):
    last_page = max(done_pages) if done_pages else 0
    try:
        with open(PROGRESS_FILE, "w") as f:
            json.dump({
                "status":      status,
                "last_page":   last_page,
                "done_pages":  sorted(done_pages),
                "total_saved": total_saved,
                "total_pages": total_pages,
                "updated_at":  datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }, f, indent=2)
    except Exception as e:
        log.warning(f"Could not save progress: {e}")


def load_existing_certs(csv_path: str) -> set:
    seen: set = set()
    if not os.path.exists(csv_path) or os.path.getsize(csv_path) == 0:
        return seen
    try:
        with open(csv_path, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                cert = str(row.get("certificateNumber", "")).strip()
                if cert:
                    seen.add(cert)
        log.info(f"Existing CSV has {len(seen):,} unique diamonds")
    except Exception as e:
        log.warning(f"Could not read existing CSV: {e}")
    return seen


def fetch_page(page: int, session: curl_requests.Session) -> tuple:
    for attempt in range(1, 6):
        try:
            resp = session.post(URL, json=build_payload(page), timeout=TIMEOUT)

            if resp.status_code in (429, 503):
                log.warning(
                    f"[page {page}] HTTP {resp.status_code} — rate limited. "
                    f"Pausing for {RATELIMIT_PAUSE}s…"
                )
                time.sleep(RATELIMIT_PAUSE)
                continue

            resp.raise_for_status()
            diamond_data = resp.json().get("data", {}).get("diamondData", {})
            diamonds     = diamond_data.get("diamond") or []
            time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))
            return page, diamonds

        except Exception as e:
            is_timeout = "timed out" in str(e).lower() or "curl: (28)" in str(e).lower()
            base = (3 ** attempt) if is_timeout else (2 ** attempt)
            wait = base + random.uniform(0, 2)
            log.warning(f"[page {page}] attempt {attempt}/5 failed: {e} — retrying in {wait:.1f}s")
            time.sleep(wait)

    log.error(f"[page {page}] all 5 retries exhausted — skipping")
    return page, None


def get_total_pages(session: curl_requests.Session) -> tuple:
    log.info("Fetching page 1 to determine total count…")
    for attempt in range(1, 5):
        try:
            resp         = session.post(URL, json=build_payload(1), timeout=TIMEOUT)
            resp.raise_for_status()
            diamond_data = resp.json().get("data", {}).get("diamondData", {})
            total_count  = int(diamond_data.get("dataCount", 0))
            total_pages  = (total_count + PAGE_SIZE - 1) // PAGE_SIZE
            log.info(f"Total diamonds : {total_count:,}")
            log.info(f"Total pages    : {total_pages:,}")
            return total_count, total_pages
        except Exception as e:
            wait = (2 ** attempt) + random.uniform(0, 2)
            log.warning(f"get_total_pages attempt {attempt}/4 failed: {e} — wait {wait:.1f}s")
            time.sleep(wait)
    raise RuntimeError("Failed to fetch page 1 — cannot determine total pages")


def make_row(d: dict, status: str, page: int) -> dict:
    return {
        "diamondId":             d.get("diamondId"),
        "stockNumber":           d.get("stockNumber"),
        "certificateNumber":     d.get("certificateNumber"),
        "shape":                 d.get("shape"),
        "caratWeight":           d.get("caratWeight"),
        "color":                 d.get("color"),
        "fancyColor":            d.get("fancyColor"),
        "fancyColorIntensity":   d.get("fancyColorIntensity"),
        "fancyColorOvertone":    d.get("fancyColorOvertone"),
        "clarity":               d.get("clarity"),
        "cut":                   d.get("cut"),
        "symmetry":              d.get("symmetry"),
        "polish":                d.get("polish"),
        "deptPerc":              d.get("deptPerc"),
        "tablePerc":             d.get("tablePerc"),
        "length":                d.get("length"),
        "width":                 d.get("width"),
        "depth":                 d.get("depth"),
        "ratio":                 d.get("ratio"),
        "measurement":           d.get("measurement"),
        "girdleMin":             d.get("girdleMin"),
        "girdleMax":             d.get("girdleMax"),
        "girdleCondition":       d.get("girdleCondition"),
        "culet":                 d.get("culet"),
        "culetCondition":        d.get("culetCondition"),
        "flourIntensity":        d.get("flourIntensity"),
        "lab":                   d.get("lab"),
        "certificateFile":       d.get("certificateFile"),
        "finalPriceEur":         d.get("finalPriceEur"),
        "finalPriceEurTax":      d.get("finalPriceEurTax"),
        "finalPriceUsd":         d.get("finalPriceUsd"),
        "originalTotalPriceEur": d.get("originalTotalPriceEur"),
        "pricePerCtEur":         d.get("pricePerCtEur"),
        "currencyCode":          d.get("currencyCode"),
        "currencySymbol":        d.get("currencySymbol"),
        "diamondImage":          d.get("diamondImage"),
        "diamondVideo":          d.get("diamondVideo"),
        "diamondType":           d.get("diamondType"),
        "status":                status,
        "page_no":               page,
    }


# ══════════════════════════════════════════════════════════════════════════════
# MAIN SCRAPER
# ══════════════════════════════════════════════════════════════════════════════

def scrape():
    log.info("=" * 55)
    log.info("Starting Renesim scraper")
    log.info(f"Timeout  : {TIMEOUT}s")
    log.info(f"Delay    : {DELAY_MIN}–{DELAY_MAX}s per request")
    log.info(f"Output  → {MAIN_CSV}")
    log.info(f"Mirror  → {EXTRA_CSV}")

    # ── Initial status ────────────────────────────────────────
    write_status(SCRAPER_NAME, state="running", message="Starting up…")

    init_session = make_session()
    progress     = load_progress()

    done_pages  = set(progress.get("done_pages", []))
    total_saved = progress.get("total_saved", 0)
    last_page   = progress.get("last_page", 0)

    try:
        if progress.get("total_pages"):
            total_pages = progress["total_pages"]
            log.info(f"Resuming — total pages from progress: {total_pages:,}")
        else:
            _, total_pages = get_total_pages(init_session)
    except RuntimeError as e:
        write_status(SCRAPER_NAME, state="failed", message=str(e))
        raise

    seen_certs = load_existing_certs(MAIN_CSV)

    # Start from last_page + 1 so we continue sequentially from where we stopped.
    # Also skip any pages already done (handles out-of-order completions from parallel workers).
    resume_from = last_page + 1 if last_page else 1
    all_pages   = list(range(resume_from, total_pages + 1))
    remaining   = [p for p in all_pages if p not in done_pages]

    log.info(f"Pages done     : {len(done_pages):,}")
    log.info(f"Resume from    : page {resume_from}")
    log.info(f"Pages remaining: {len(remaining):,}")

    write_status(
        SCRAPER_NAME,
        state="running",
        current_page=len(done_pages),
        total_pages=total_pages,
        diamonds_saved=total_saved,
        message=f"Resuming from page {resume_from} — {len(remaining):,} pages remaining",
    )

    if not remaining:
        log.info("All pages already fetched — nothing to do.")
        write_status(
            SCRAPER_NAME,
            state="done",
            current_page=total_pages,
            total_pages=total_pages,
            diamonds_saved=total_saved,
            message="All pages already fetched",
        )
        return {"total_saved": total_saved, "new_rows": 0}

    file_exists = os.path.exists(MAIN_CSV) and os.path.getsize(MAIN_CSV) > 0
    write_mode  = "a" if file_exists else "w"
    log.info(f"CSV mode: {'APPEND' if write_mode == 'a' else 'NEW'}")

    f_main  = open(MAIN_CSV,  write_mode, newline="", encoding="utf-8")
    f_extra = open(EXTRA_CSV, write_mode, newline="", encoding="utf-8")
    writer_main  = csv.DictWriter(f_main,  fieldnames=FIELDS, extrasaction="ignore")
    writer_extra = csv.DictWriter(f_extra, fieldnames=FIELDS, extrasaction="ignore")
    if write_mode == "w":
        writer_main.writeheader()
        writer_extra.writeheader()

    saved_count = 0
    skip_count  = 0
    fail_count  = 0
    fail_streak = 0
    last_page   = len(done_pages)
    total_rem   = len(remaining)

    # Thread-safety primitives
    csv_lock      = threading.Lock()
    state_lock    = threading.Lock()
    completed_idx = [0]  # mutable counter shared across threads

    def _process_page(p: int) -> None:
        nonlocal saved_count, skip_count, fail_count, fail_streak, last_page

        session = make_session()
        page, diamonds = fetch_page(p, session)

        with state_lock:
            completed_idx[0] += 1
            idx = completed_idx[0]
            pct = idx / total_rem * 100

            if diamonds is None:
                fail_count  += 1
                fail_streak += 1
                log.error(
                    f"[{idx:>6}/{total_rem}] page {page:>5} FAILED "
                    f"(streak {fail_streak})"
                )
                write_status(
                    SCRAPER_NAME,
                    state="running",
                    current_page=last_page,
                    total_pages=total_pages,
                    diamonds_saved=total_saved + saved_count,
                    duplicates=skip_count,
                    failed_pages=fail_count,
                    message=f"Page {page} failed — will retry",
                )
                if fail_streak >= VPN_ROTATE_AFTER_FAILS:
                    raise VPNRotateNeeded(fail_streak)
                return

            fail_streak = 0

        # Build new rows outside the lock (no shared state touched yet)
        new_rows: list = []
        dup_count_page = 0
        with csv_lock:
            for d in diamonds:
                cert = str(d.get("certificateNumber", "")).strip()
                if cert and cert not in seen_certs:
                    seen_certs.add(cert)
                    new_rows.append(make_row(d, "new", page))
                else:
                    dup_count_page += 1

            if new_rows:
                writer_main.writerows(new_rows)
                writer_extra.writerows(new_rows)
                f_main.flush()
                f_extra.flush()

        with state_lock:
            skip_count  += dup_count_page
            saved_count += len(new_rows)
            done_pages.add(page)
            last_page = max(last_page, page)
            total_saved_now = total_saved + saved_count

            log.info(
                f"[{idx:>6}/{total_rem}] "
                f"page {page:>5} | "
                f"+{len(new_rows):>2} new | "
                f"skipped {dup_count_page} dupes | "
                f"total saved: {total_saved_now:,} | "
                f"{pct:.1f}%"
            )

            write_status(
                SCRAPER_NAME,
                state="running",
                current_page=last_page,
                total_pages=total_pages,
                diamonds_saved=total_saved_now,
                duplicates=skip_count,
                failed_pages=fail_count,
                message=f"Processing… {pct:.1f}% complete",
                extra={
                    "pages_done":        len(done_pages),
                    "pages_remaining":   total_rem - idx,
                    "last_page_scraped": last_page,
                },
            )

            save_progress(done_pages, total_saved_now, total_pages)

    try:
        with ThreadPoolExecutor(max_workers=WORKERS) as pool:
            futures = {pool.submit(_process_page, p): p for p in remaining}
            for fut in as_completed(futures):
                exc = fut.exception()
                if exc is not None:
                    if isinstance(exc, VPNRotateNeeded):
                        pool.shutdown(wait=False, cancel_futures=True)
                        raise exc
                    log.error(f"Unexpected error in worker: {exc}")

    except KeyboardInterrupt:
        log.warning("Ctrl+C received — saving progress…")
        save_progress(done_pages, total_saved + saved_count, total_pages)
        write_status(
            SCRAPER_NAME,
            state="interrupted",
            current_page=last_page,
            total_pages=total_pages,
            diamonds_saved=total_saved + saved_count,
            duplicates=skip_count,
            failed_pages=fail_count,
            message="Stopped by Ctrl+C — progress saved",
        )
        raise
    except Exception as e:
        write_status(
            SCRAPER_NAME,
            state="failed",
            current_page=last_page,
            total_pages=total_pages,
            diamonds_saved=total_saved + saved_count,
            duplicates=skip_count,
            failed_pages=fail_count,
            message=f"Crashed: {e}",
        )
        raise
    finally:
        f_main.close()
        f_extra.close()

    final_saved = total_saved + saved_count
    save_progress(done_pages, final_saved, total_pages, status="done")

    write_status(
        SCRAPER_NAME,
        state="done",
        current_page=total_pages,
        total_pages=total_pages,
        diamonds_saved=final_saved,
        duplicates=skip_count,
        failed_pages=fail_count,
        message="Scrape complete",
        extra={"output_csv": MAIN_CSV, "mirror_csv": EXTRA_CSV},
    )

    log.info("=" * 55)
    log.info("DONE")
    log.info(f"  New rows saved  : {saved_count:,}")
    log.info(f"  Duplicates skip : {skip_count:,}")
    log.info(f"  Pages failed    : {fail_count:,}")
    log.info(f"  Total in CSV    : {final_saved:,}")
    log.info(f"  CSV    → {MAIN_CSV}")
    log.info(f"  Mirror → {EXTRA_CSV}")
    log.info("=" * 55)

    return {
        "action":       "scrape_complete",
        "total_saved":  final_saved,
        "new_rows":     saved_count,
        "duplicates":   skip_count,
        "failed_pages": fail_count,
        "output":       MAIN_CSV,
        "end_time":     datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }


# ══════════════════════════════════════════════════════════════════════════════
# VPN ROTATION
# ══════════════════════════════════════════════════════════════════════════════

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

    log.info("=" * 60)
    log.info("Renesim scraper — VPN rotation enabled")
    log.info(f"  Rotate after fails   : {VPN_ROTATE_AFTER_FAILS}")
    log.info(f"  Delay per request    : {DELAY_MIN}–{DELAY_MAX}s")
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

    import random as _random
    _random.shuffle(configs)
    total   = len(configs)
    deleted = 0
    log.info("VPN pool: %d config(s) in %s", total, vpn_dir)

    for attempt, cfg in enumerate(configs, start=1):
        log.info("--- VPN attempt %d/%d: %s ---", attempt, total, cfg.name)
        try:
            with VPNConnection(cfg, auth_file=proton_auth, log_file=openvpn_log) as vpn:
                log.info("Tunnel up. Exit IP = %s", vpn.ip_after)
                scrape()
            log.info("Scrape completed — VPN torn down.")
            return

        except VPNRotateNeeded as e:
            log.warning(
                "IP burned (%d consecutive fails) — rotating VPN: %s",
                VPN_ROTATE_AFTER_FAILS, e,
            )
            log.info("Cooling down 15s before next VPN…")
            time.sleep(15)

        except VPNError as e:
            if "authentication failed" in str(e).lower():
                log.error("VPN auth failed on %s — aborting. Check proton_credentials.txt.", cfg.name)
                return
            log.error("VPN error on %s: %s — skipping config", cfg.name, e)
            # try:
            #     cfg.unlink()
            #     deleted += 1
            # except OSError as del_exc:
            #     log.warning("Could not delete %s: %s", cfg.name, del_exc)

        except KeyboardInterrupt:
            log.warning("Ctrl+C — exiting VPN loop.")
            return

    log.error(
        "Tried all %d VPN config(s) — none completed the scrape (%d deleted).",
        total, deleted,
    )


if __name__ == "__main__":
    import sys
    try:
        if "--no-vpn" in sys.argv:
            scrape()
        else:
            run_with_vpn_rotation()
    except KeyboardInterrupt:
        log.warning("Ctrl+C — stopped.")
    except Exception as e:
        log.error("Fatal error: %s", e)
        raise