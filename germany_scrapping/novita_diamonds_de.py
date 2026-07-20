from curl_cffi import requests as curl_requests
import csv, os, time, random, json, logging, re
import datetime
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from status import write_status


# ── VPN rotation config ───────────────────────────────────────────────────────
VPN_ROTATE_AFTER_FAILS = 3       # consecutive failed pages before rotating
VPN_CONFIG_SUBDIR      = "novita"
# Server is slow under concurrency (15-20s/req at 5 workers) and starts returning
# HTTP 405 (soft block) once request volume/rate crosses some threshold. Keep
# concurrency modest — this is not a fast site to hammer.
WORKERS         = int(os.environ.get("NOVITA_WORKERS", "3"))
REQUEST_TIMEOUT = int(os.environ.get("NOVITA_REQUEST_TIMEOUT", "45"))


class VPNRotateNeeded(Exception):
    def __init__(self, fails: int):
        super().__init__(f"VPN rotate: {fails} consecutive failed pages")


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


log = get_logger("novita")


# ── Config ────────────────────────────────────────────────────────────────────
SCRAPER_NAME = "novita"

BASE_URL      = "https://novitadiamonds.de"
PAGE_URL      = "/en/create/diamanten/buy-lab-grown-diamonds"
API_URL       = "https://novitadiamonds.de/en/api/product/diamonds"
COOKIE_URL    = "https://novitadiamonds.de/save-cookie"

CSV_FILE  = "novita_diamonds.csv"
BASE_DIR  = os.getcwd()

# Server hard-caps maxResults at 100 — 128+ returns empty
PAGE_SIZE = 100

diamond_files = str(Path(__file__).resolve().parent.parent / "diamond_files" / "germany")

os.makedirs(diamond_files, exist_ok=True)

MAIN_CSV      = os.path.join(diamond_files, CSV_FILE)
PROGRESS_FILE = os.path.join(diamond_files, "novita_progress.json")


# ── Live session state (populated by refresh_session before scraping) ─────────
_SESSION_COOKIES: dict = {}
_SESSION_TOKEN:   str  = ""


# ── Static headers ────────────────────────────────────────────────────────────
_BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/149.0.0.0 Safari/537.36"
)

API_HEADERS = {
    "accept":           "application/json, text/javascript, */*; q=0.01",
    "accept-language":  "en-GB,en-US;q=0.9,en;q=0.8",
    "content-type":     "application/json",
    "origin":           BASE_URL,
    "referer":          BASE_URL + PAGE_URL,
    "sec-ch-ua":        '"Google Chrome";v="149", "Chromium";v="149", "Not)A;Brand";v="24"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "sec-fetch-dest":   "empty",
    "sec-fetch-mode":   "cors",
    "sec-fetch-site":   "same-origin",
    "user-agent":       _BROWSER_UA,
    "x-requested-with": "XMLHttpRequest",
}


# ── Base payload — token injected fresh per-request from _SESSION_TOKEN ───────
BASE_PAYLOAD = {
    "shapes":                ["1", "4", "3", "2", "6", "5", "9", "8", "7", "10", "11"],
    "color_from":            1,
    "color_to":              16,
    "cut_from":              1,
    "cut_to":                3,
    "clarity_from":          2,
    "clarity_to":            8,
    "polish_from":           1,
    "polish_to":             2,
    "symmetry_from":         1,
    "symmetry_to":           2,
    "carat_from":            0.5,
    "carat_to":              10,
    "price_from":            150,
    "price_to":              13500,
    "location_description":  "all",
    "certificates":          [],
    "ratio_from":            1,
    "ratio_to":              3,
    "is_carat_infitinitive": 0,
    "is_price_infitinitive": 1,
    "is_ratio_infitinitive": 1,
    "maxResults":            PAGE_SIZE,
    "order_by":              "priceToSellWoLocalTax",
    "order_type":            "ASC",
    "preferred_currency":    "EUR",
    "current_tab":           "result_tab",
    "fetch_count":           False,
    # "token" always injected at call time — never hardcoded
}


# ── CSV columns ───────────────────────────────────────────────────────────────
FIELDS = [
    "id", "product_id", "stock_status_provider_slug", "shape", "shape_slug",
    "carat", "color", "is_fancy_color", "fancy_color_slug", "clarity", "cut",
    "polish", "symmetry", "table", "depth", "measurement",
    "image_url", "image_external_url",
    "video_url", "video_external_url", "video_file_type",
    "certificate_laboratory", "certificate_url", "certificate_file_type",
    "certificate_external_url", "is_onshore", "country_slug",
    "is_onUkShore", "certificate_number", "ratio",
    "only_for_special_engrings", "manual_price", "nd_percentage_discount",
    "price", "symbol_currency", "active", "recommended",
    "local_diamonds_code_uk", "includes_sample_video",
    "there_is_diamond_overseas", "provider_slug", "type",
    "status", "page_no",
]


# ══════════════════════════════════════════════════════════════════════════════
# SESSION REFRESH  — acquires live PHPSESSID + cookie_preferences + CSRF token
# ══════════════════════════════════════════════════════════════════════════════

def refresh_session() -> tuple:
    """
    Acquire live session credentials from novitadiamonds.de before scraping.
    No hardcoded cookies or tokens needed — fetches fresh ones every run.

    Flow:
      1. GET  /en/create/diamanten/buy-lab-grown-diamonds
             → server sets PHPSESSID cookie via Set-Cookie
             → HTML contains product_diamonds_token hidden input
      2. POST /save-cookie  {cookie_type=all}
             → server sets cookie_preferences cookie (required for data API)
      3. GET  page again
             → re-parse token (now bound to a cookie-consented session)

    Returns:
      (cookies_dict, csrf_token)
    """
    global _SESSION_COOKIES, _SESSION_TOKEN

    log.info("=" * 55)
    log.info("refresh_session() — acquiring live credentials")

    page_headers = {
        "accept":                  "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "accept-language":         "en-GB,en;q=0.9,de;q=0.8",
        "accept-encoding":         "gzip, deflate, br",
        "cache-control":           "max-age=0",
        "sec-ch-ua":               '"Google Chrome";v="149", "Chromium";v="149", "Not)A;Brand";v="24"',
        "sec-ch-ua-mobile":        "?0",
        "sec-ch-ua-platform":      '"Windows"',
        "sec-fetch-dest":          "document",
        "sec-fetch-mode":          "navigate",
        "sec-fetch-site":          "none",
        "sec-fetch-user":          "?1",
        "upgrade-insecure-requests": "1",
        "user-agent":              _BROWSER_UA,
    }

    s = curl_requests.Session(impersonate="chrome131")

    # ── Step 1: load diamond page → get PHPSESSID ─────────────────────────────
    log.info(f"  [1/3] GET {BASE_URL + PAGE_URL}")
    try:
        r1 = s.get(BASE_URL + PAGE_URL, headers=page_headers, timeout=30)
        if r1.status_code != 200:
            raise RuntimeError(f"page returned HTTP {r1.status_code}")
        log.info(f"  [1/3] OK — cookies so far: {list(dict(s.cookies).keys())}")
    except Exception as e:
        raise RuntimeError(f"refresh_session step 1 failed: {e}")

    # ── Step 2: accept cookie consent → get cookie_preferences ───────────────
    log.info(f"  [2/3] POST {COOKIE_URL}")
    try:
        cr = s.post(
            COOKIE_URL,
            data={"cookie_type": "all"},
            headers={
                "accept":           "*/*",
                "content-type":     "application/x-www-form-urlencoded; charset=UTF-8",
                "origin":           BASE_URL,
                "referer":          BASE_URL + PAGE_URL,
                "user-agent":       _BROWSER_UA,
                "x-requested-with": "XMLHttpRequest",
            },
            timeout=15,
        )
        log.info(f"  [2/3] {cr.status_code} — {cr.text.strip()[:60]}")
    except Exception as e:
        log.warning(f"  [2/3] /save-cookie failed (non-fatal): {e}")

    # ── Step 3: reload page → parse fresh token ───────────────────────────────
    log.info(f"  [3/3] GET {BASE_URL + PAGE_URL} (token refresh)")
    try:
        r2 = s.get(
            BASE_URL + PAGE_URL,
            headers={**page_headers, "referer": BASE_URL},
            timeout=30,
        )
        html = r2.text
    except Exception as e:
        raise RuntimeError(f"refresh_session step 3 failed: {e}")

    # Parse CSRF token from hidden input
    m = (
        re.search(r'id=["\']product_diamonds_token["\'][^>]*value=["\']([^"\']+)["\']', html)
        or re.search(r'value=["\']([^"\']+)["\'][^>]*id=["\']product_diamonds_token["\']', html)
    )
    if not m:
        raise RuntimeError(
            "product_diamonds_token not found — check if site HTML changed"
        )

    token        = m.group(1)
    cookies_dict = dict(s.cookies)

    if "PHPSESSID" not in cookies_dict:
        raise RuntimeError("PHPSESSID not set — session warmup failed")

    log.info(f"  PHPSESSID          : {cookies_dict['PHPSESSID']}")
    log.info(f"  cookie_preferences : {cookies_dict.get('cookie_preferences', 'MISSING')}")
    log.info(f"  token              : {token[:40]}...")
    log.info("refresh_session() — done ✓")
    log.info("=" * 55)

    _SESSION_COOKIES = cookies_dict
    _SESSION_TOKEN   = token
    return cookies_dict, token


# ══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def make_session() -> curl_requests.Session:
    s = curl_requests.Session(impersonate="chrome131")
    s.headers.update(API_HEADERS)
    s.verify = False
    for name, value in _SESSION_COOKIES.items():
        s.cookies.set(name, value, domain="novitadiamonds.de")
    return s


# ── Thread-safe session refresh (multiple workers can hit 405 at once —
#    only one of them should actually pay for a fresh token/cookies) ──────────
_refresh_lock       = threading.Lock()
_last_refresh_time  = 0.0

def maybe_refresh_session(cooldown: float = 15.0) -> None:
    global _last_refresh_time
    with _refresh_lock:
        if time.time() - _last_refresh_time < cooldown:
            return  # another worker refreshed moments ago — reuse it
        try:
            refresh_session()
        except Exception as e:
            log.warning(f"maybe_refresh_session() failed: {e}")
        finally:
            _last_refresh_time = time.time()


def load_progress() -> dict:
    if os.path.exists(PROGRESS_FILE):
        try:
            with open(PROGRESS_FILE) as f:
                data = json.load(f)
            if data.get("status") == "done":
                log.info("Previous run completed — starting fresh")
                return {"done_pages": [], "total_saved": 0, "total_pages": None}
            log.info(
                f"Interrupted run found — "
                f"done pages: {len(data.get('done_pages', []))}, "
                f"saved: {data.get('total_saved', 0):,}"
            )
            return data
        except Exception as e:
            log.warning(f"Could not read progress file: {e} — starting fresh")
    return {"done_pages": [], "total_saved": 0, "total_pages": None}


def save_progress(done_pages: set, total_saved: int, total_pages: int, status: str = "running"):
    try:
        with open(PROGRESS_FILE, "w") as f:
            json.dump({
                "status":      status,
                "done_pages":  sorted(done_pages),
                "total_saved": total_saved,
                "total_pages": total_pages,
                "updated_at":  datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }, f)
    except Exception as e:
        log.warning(f"Could not save progress: {e}")


def load_existing_certs(csv_path: str) -> set:
    seen: set = set()
    if not os.path.exists(csv_path) or os.path.getsize(csv_path) == 0:
        return seen
    try:
        with open(csv_path, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                cert = str(row.get("certificate_number", "")).strip()
                if cert:
                    seen.add(cert)
        log.info(f"Existing CSV has {len(seen):,} unique diamonds")
    except Exception as e:
        log.warning(f"Could not read existing CSV: {e}")
    return seen


def fetch_page(page: int, session: curl_requests.Session) -> tuple:
    for attempt in range(1, 5):
        # re-read the live token/cookies each attempt — another worker may have
        # refreshed them since this page started retrying
        payload = {
            **BASE_PAYLOAD,
            "page":  page,
            "token": _SESSION_TOKEN,  # always live — never hardcoded
        }
        try:
            resp = session.post(
                API_URL,
                headers=API_HEADERS,
                cookies=_SESSION_COOKIES,
                data=json.dumps(payload),
                timeout=REQUEST_TIMEOUT,
                verify=False,
            )

            if resp.status_code == 405:
                # Server-side soft block (WAF/rate-limit) — same token/cookies
                # won't recover on their own. Refresh and retry.
                log.warning(f"[page {page}] 405 — session looks blocked, "
                            f"refreshing session/token (attempt {attempt}/4)")
                time.sleep(random.uniform(5, 10))
                maybe_refresh_session()
                continue

            resp.raise_for_status()
            diamonds = resp.json().get("response", {}).get("items", [])
            time.sleep(random.uniform(1.0, 2.0))
            return page, diamonds
        except Exception as e:
            wait = 2 ** attempt
            log.warning(f"[page {page}] attempt {attempt}/4 failed: {e} — retry in {wait}s")
            time.sleep(wait)

    log.error(f"[page {page}] all retries exhausted — skipping")
    return page, None


def get_total_pages(session: curl_requests.Session) -> int:
    """
    Calls API with fetch_count=true on page 1.
    Server returns both total count AND items in the same response.
    """
    log.info("Fetching page 1 with fetch_count=true to get total…")
    for attempt in range(1, 5):
        try:
            payload = {
                **BASE_PAYLOAD,
                "page":        1,
                "fetch_count": True,
                "token":       _SESSION_TOKEN,
            }
            resp = session.post(
                API_URL,
                headers=API_HEADERS,
                cookies=_SESSION_COOKIES,
                data=json.dumps(payload),
                timeout=REQUEST_TIMEOUT,
            )
            resp.raise_for_status()
            body  = resp.json().get("response", {})
            total = int(
                body.get("total")
                or body.get("total_count")
                or body.get("count")
                or 0
            )
            if total:
                pages = (total + PAGE_SIZE - 1) // PAGE_SIZE
                log.info(f"Total diamonds : {total:,}")
                log.info(f"Total pages    : {pages:,}  (PAGE_SIZE={PAGE_SIZE})")
                return pages
            log.warning("No total in fetch_count response — paginating until empty")
            return 9999
        except Exception as e:
            wait = 2 ** attempt
            log.warning(f"get_total_pages attempt {attempt}/4 failed: {e} — wait {wait}s")
            time.sleep(wait)
    raise RuntimeError("Failed to fetch page 1 — cannot determine total pages")


def make_row(d: dict, status: str, page: int) -> dict:
    return {f: d.get(f, "") for f in FIELDS} | {"status": status, "page_no": page}


# ══════════════════════════════════════════════════════════════════════════════
# MAIN SCRAPER
# ══════════════════════════════════════════════════════════════════════════════

def scrape():
    log.info("=" * 55)
    log.info("Starting Novita scraper")
    log.info(f"Page size : {PAGE_SIZE}")
    log.info(f"Output    → {MAIN_CSV}")

    write_status(SCRAPER_NAME, state="running", message="Starting up…")

    # ── Step 1: live session acquisition ─────────────────────────────────────
    write_status(SCRAPER_NAME, state="running", message="Acquiring live session cookies + token…")
    try:
        refresh_session()
    except Exception as e:
        log.error(f"refresh_session() failed: {e}")
        write_status(SCRAPER_NAME, state="failed", message=f"Session failed: {e}")
        raise

    # ── Step 2: progress + total pages ───────────────────────────────────────
    session  = make_session()
    progress = load_progress()

    done_pages  = set(progress.get("done_pages", []))
    total_saved = progress.get("total_saved", 0)

    try:
        if progress.get("total_pages"):
            total_pages = progress["total_pages"]
            log.info(f"Resuming — total pages from progress: {total_pages:,}")
        else:
            total_pages = get_total_pages(session)
    except RuntimeError as e:
        write_status(SCRAPER_NAME, state="failed", message=str(e))
        raise

    seen_certs = load_existing_certs(MAIN_CSV)

    all_pages = list(range(1, total_pages + 1))
    remaining = [p for p in all_pages if p not in done_pages]

    log.info(f"Pages done     : {len(done_pages):,}")
    log.info(f"Pages remaining: {len(remaining):,}")

    write_status(
        SCRAPER_NAME,
        state="running",
        current_page=len(done_pages),
        total_pages=total_pages,
        diamonds_saved=total_saved,
        message=f"Resuming — {len(remaining):,} pages remaining",
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

    # ── Step 3: open CSV files ────────────────────────────────────────────────
    file_exists = os.path.exists(MAIN_CSV) and os.path.getsize(MAIN_CSV) > 0
    write_mode  = "a" if file_exists else "w"
    log.info(f"CSV mode: {'APPEND' if write_mode == 'a' else 'NEW'}")

    f_main      = open(MAIN_CSV, write_mode, newline="", encoding="utf-8")
    writer_main = csv.DictWriter(f_main, fieldnames=FIELDS, extrasaction="ignore")
    if write_mode == "w":
        writer_main.writeheader()

    saved_count = 0
    skip_count  = 0
    fail_count  = 0
    fail_streak = 0
    last_page   = len(done_pages)
    total_rem   = len(remaining)

    # Thread-safety primitives
    csv_lock      = threading.Lock()
    state_lock    = threading.Lock()
    completed_idx = [0]

    def _process_page(p: int) -> None:
        nonlocal saved_count, skip_count, fail_count, fail_streak, last_page

        page_session = make_session()
        page, diamonds = fetch_page(p, page_session)

        with state_lock:
            completed_idx[0] += 1
            idx = completed_idx[0]
            pct = idx / total_rem * 100

            if diamonds is None:
                fail_count  += 1
                fail_streak += 1
                log.error(f"[{idx:>6}/{total_rem}] page {page:>5} FAILED (streak {fail_streak})")
                write_status(
                    SCRAPER_NAME, state="running",
                    current_page=last_page, total_pages=total_pages,
                    diamonds_saved=total_saved + saved_count,
                    duplicates=skip_count, failed_pages=fail_count,
                    message=f"Page {page} failed after retries",
                )
                if fail_streak >= VPN_ROTATE_AFTER_FAILS:
                    raise VPNRotateNeeded(fail_streak)
                return

            if not diamonds:
                log.info(f"[{idx:>6}/{total_rem}] page {page:>5} EMPTY")
                done_pages.add(page)
                return

            fail_streak = 0

        new_rows, dup_this_page = [], 0
        with csv_lock:
            for d in diamonds:
                cert = str(d.get("certificate_number", "")).strip()
                if cert and cert not in seen_certs:
                    seen_certs.add(cert)
                    new_rows.append(make_row(d, "new", page))
                else:
                    dup_this_page += 1

            if new_rows:
                writer_main.writerows(new_rows)
                f_main.flush()

        with state_lock:
            skip_count  += dup_this_page
            saved_count += len(new_rows)
            done_pages.add(page)
            last_page = max(last_page, page)
            total_saved_now = total_saved + saved_count

            log.info(
                f"[{idx:>6}/{total_rem}] "
                f"page {page:>5} | "
                f"+{len(new_rows):>3} new | "
                f"dup {dup_this_page:>3} | "
                f"total {total_saved_now:,} | "
                f"{pct:.1f}%"
            )

            write_status(
                SCRAPER_NAME, state="running",
                current_page=last_page, total_pages=total_pages,
                diamonds_saved=total_saved_now,
                duplicates=skip_count, failed_pages=fail_count,
                message=f"Processing… {pct:.1f}% complete",
                extra={
                    "pages_done":        len(done_pages),
                    "pages_remaining":   total_rem - idx,
                    "last_page_scraped": last_page,
                },
            )

            save_progress(done_pages, total_saved_now, total_pages)

    # ── Step 4: concurrent page fetching ──────────────────────────────────────
    try:
        with ThreadPoolExecutor(max_workers=WORKERS) as pool:
            futures = {pool.submit(_process_page, p): p for p in remaining}
            for fut in as_completed(futures):
                exc = fut.exception()
                if exc is not None:
                    if isinstance(exc, VPNRotateNeeded):
                        pool.shutdown(wait=False, cancel_futures=True)
                        raise exc
                    log.error(f"Unexpected worker error: {exc}")

    except Exception as e:
        write_status(
            SCRAPER_NAME, state="failed",
            current_page=last_page, total_pages=total_pages,
            diamonds_saved=total_saved + saved_count,
            duplicates=skip_count, failed_pages=fail_count,
            message=f"Crashed: {e}",
        )
        raise
    finally:
        f_main.close()

    # ── Step 5: finalise ──────────────────────────────────────────────────────
    final_saved = total_saved + saved_count
    save_progress(done_pages, final_saved, total_pages, status="done")

    write_status(
        SCRAPER_NAME, state="done",
        current_page=total_pages, total_pages=total_pages,
        diamonds_saved=final_saved,
        duplicates=skip_count, failed_pages=fail_count,
        message="Scrape complete",
        extra={"output_csv": MAIN_CSV},
    )

    log.info("=" * 55)
    log.info("DONE")
    log.info(f"  New rows saved  : {saved_count:,}")
    log.info(f"  Duplicates skip : {skip_count:,}")
    log.info(f"  Pages failed    : {fail_count:,}")
    log.info(f"  Total in CSV    : {final_saved:,}")
    log.info(f"  CSV    → {MAIN_CSV}")
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


def run_with_vpn_rotation():
    from vpn_rotator import (
        VPNConnection,
        VPNError,
        find_openvpn_executable,
        is_admin,
        parse_credentials_file,
        write_auth_file,
    )

    log.info("=" * 60)
    log.info("Novita scraper with VPN rotation")
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

    repo_root = Path(__file__).resolve().parent.parent
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
                scrape()
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

    log.error(
        "Tried all %d VPN config(s) — none completed the scrape (%d deleted).",
        total, deleted,
    )


def run_forever_with_recovery(max_consecutive_crashes: int = 10):
    """
    Same-IP alternative to VPN rotation: scrape() checkpoints progress after
    every page (novita_progress.json), so a VPNRotateNeeded (repeated 405s
    the in-page refresh couldn't clear) doesn't need a new exit IP — just a
    cool-down + fresh session, then resume from where it left off.
    """
    crashes = 0
    while True:
        try:
            return scrape()
        except VPNRotateNeeded as e:
            crashes += 1
            if crashes > max_consecutive_crashes:
                log.error(f"Gave up after {crashes} consecutive blocks — aborting.")
                raise
            cooldown = min(60 * crashes, 300)
            log.warning(f"Blocked ({e}) — cooling down {cooldown}s then resuming (attempt {crashes}/{max_consecutive_crashes})")
            time.sleep(cooldown)
        except KeyboardInterrupt:
            log.warning("Interrupted by user — stopping.")
            return


if __name__ == "__main__":
    run_forever_with_recovery()
    # run_with_vpn_rotation()   → rotate exit IP via OpenVPN configs (needs admin)