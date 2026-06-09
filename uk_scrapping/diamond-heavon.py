"""
diamond_heaven.py — Fast, anti-block scraper for diamond-heaven.co.uk lab-grown diamonds.

Columns saved (trimmed set — matches abelini output):
  certificate, cert_no_clean, lab, shape, carat, colour, clarity, cut,
  price, price_formatted, cert_url, available, type, status, page_no

cert_no_clean  = certificate number with leading "LG" / "lg" stripped
                 e.g.  "LG12345678"  →  "12345678"
                        "12345678"   →  "12345678"  (unchanged if no prefix)
"""

import csv
import json
import logging
import os
import random
import re
import shutil
import time
from urllib.parse import unquote

from curl_cffi import requests as cffi_requests

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
SCRAPER_NAME   = "diamond_heaven"
API_URL        = "https://www.diamond-heaven.co.uk/diamonds/index/listt"
ITEMS_PER_PAGE = 10000
MAX_RETRIES    = 5
RETRY_BACKOFF  = 1.5
SLEEP_MIN      = 1.5
SLEEP_MAX      = 3.0
CHROME_VERSION = "chrome120"

from pathlib import Path
_SCRIPT_DIR   = Path(__file__).resolve().parent                          # uk_scrapping/
_REPO_ROOT    = _SCRIPT_DIR.parent                                       # repo root
BASE_DIR      = str(_SCRIPT_DIR)
diamonds_path = os.path.join(BASE_DIR, "diamond_heaven")
diamond_files = str(_REPO_ROOT / "diamond_files" / "uk")
backup_dir    = str(_REPO_ROOT / "diamond_files_backup")

for d in (diamonds_path, diamond_files):
    os.makedirs(d, exist_ok=True)

CSV_FILE    = "diamond_heaven.csv"
MAIN_CSV    = os.path.join(diamonds_path, CSV_FILE)
EXTRA_CSV   = os.path.join(diamond_files, CSV_FILE)
STATUS_FILE = os.path.join(diamonds_path, "status.json")

STALE_DAYS = 2

# ── VPN Config ────────────────────────────────────────────────────────────────
VPN_ROTATE_AFTER_FAILS = 5          # consecutive blocked pages → rotate VPN
VPN_CONFIG_SUBDIR      = "diamond-heavon"  # vpnbook/uk/diamond-heavon/*.ovpn


class PageBlockedError(Exception):
    """Raised when a request is blocked (429 / 403) — caller may rotate VPN and retry."""


class VPNRotateNeeded(Exception):
    """Raised when consecutive 403/blocks indicate this exit IP is burned."""
    def __init__(self, page: int, consecutive_fails: int) -> None:
        self.page             = page
        self.consecutive_fails = consecutive_fails
        super().__init__(
            f"VPN rotate signal: {consecutive_fails} consecutive blocked page(s) "
            f"starting at page {page}"
        )


# ── Rotating User-Agents ──────────────────────────────────────────────────────
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
]

def pick_ua() -> str:
    return random.choice(USER_AGENTS)

def make_headers() -> dict:
    ua = pick_ua()
    is_firefox = "Firefox" in ua
    is_safari  = "Safari" in ua and "Chrome" not in ua
    if is_firefox:
        sec_ch_ua = '"Firefox";v="124", "Not-A.Brand";v="8"'
    elif is_safari:
        sec_ch_ua = '"Safari";v="17", "Not-A.Brand";v="8"'
    else:
        sec_ch_ua = '"Chromium";v="146", "Not-A.Brand";v="24", "Google Chrome";v="146"'
    return {
        "accept":                      "application/json, text/javascript, */*; q=0.01",
        "accept-language":             "en-GB,en-US;q=0.9,en;q=0.8",
        "content-type":                "application/x-www-form-urlencoded; charset=UTF-8",
        "origin":                      "https://www.diamond-heaven.co.uk",
        "referer":                     "https://www.diamond-heaven.co.uk/build-your-ring/by-stone/lab-grown-diamond",
        "x-requested-with":            "XMLHttpRequest",
        "user-agent":                  ua,
        "sec-ch-ua":                   sec_ch_ua,
        "sec-ch-ua-arch":              '"x86"',
        "sec-ch-ua-bitness":           '"64"',
        "sec-ch-ua-full-version":      '"146.0.7680.165"',
        "sec-ch-ua-full-version-list": '"Chromium";v="146.0.7680.165", "Not-A.Brand";v="24.0.0.0", "Google Chrome";v="146.0.7680.165"',
        "sec-ch-ua-mobile":            "?0",
        "sec-ch-ua-model":             '""',
        "sec-ch-ua-platform":          '"Windows"',
        "sec-ch-ua-platform-version":  '"15.0.0"',
        "sec-fetch-dest":              "empty",
        "sec-fetch-mode":              "cors",
        "sec-fetch-site":              "same-origin",
        "priority":                    "u=1, i",
        "cache-control":               "no-cache",
        "pragma":                      "no-cache",
    }

# ── Cookie auto-loader ────────────────────────────────────────────────────────
CURL_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "curl.txt")

TRACKED_COOKIE_KEYS = {
    "cf_clearance", "private_content_version", "PHPSESSID", "form_key",
    "_ga", "_ga_ZVLH87X2L7", "_gid", "inova_p_sid",
    "_cc_id", "_uetsid", "_uetvid", "panoramaId", "panoramaId_expiry",
    "panoramaIdType", "mage-cache-sessid", "mage-messages", "region_override",
    # "section_data_ids" deliberately omitted — handled separately
}

def _clean_curl(raw: str) -> str:
    raw = raw.replace("^\n", " ").replace("^ \n", " ")
    for a, b in [("^|", "|"), ("^{", "{"), ("^}", "}"), ("^$", "$"), ('^"', '"'), ("^&", "&")]:
        raw = raw.replace(a, b)
    return re.sub(r"\^(?=[^\n])", "", raw)

def _parse_cookies(s: str) -> dict:
    out = {}
    for part in s.split(";"):
        part = part.strip()
        if "=" in part:
            k, _, v = part.partition("=")
            out[k.strip()] = unquote(v.strip())
    return out

def _parse_section_data_ids(cookie_str: str):
    for part in cookie_str.split(";"):
        part = part.strip()
        if part.startswith("section_data_ids="):
            return unquote(part[len("section_data_ids="):])
    return None

def _load_cookies_from_curl():
    if not os.path.exists(CURL_FILE):
        log.warning("curl.txt not found — using hardcoded COOKIES.")
        return None
    with open(CURL_FILE, "r", encoding="utf-8") as f:
        raw = f.read()
    cleaned    = _clean_curl(raw)
    cookie_str = None
    for pat in [r'-b\s+"((?:[^"\\]|\\.)*)"', r"-b\s+'((?:[^'\\]|\\.)*)'",
                r'--cookie\s+"((?:[^"\\]|\\.)*)"']:
        m = re.search(pat, cleaned, re.DOTALL)
        if m:
            cookie_str = m.group(1)
            break
    if not cookie_str:
        log.warning("Could not parse cookies from curl.txt — using hardcoded COOKIES.")
        return None
    all_cookies = _parse_cookies(cookie_str)
    tracked = {k: v for k, v in all_cookies.items() if k in TRACKED_COOKIE_KEYS}
    sdi = _parse_section_data_ids(cookie_str)
    if sdi:
        tracked["section_data_ids"] = sdi
        log.info("section_data_ids loaded from curl.txt: %s...", sdi[:60])
    _self_patch_safe_cookies(tracked)
    return tracked

def _self_patch_safe_cookies(tracked: dict) -> None:
    safe_keys = {k: v for k, v in tracked.items() if k != "section_data_ids"}
    if not safe_keys:
        return
    try:
        this_file = os.path.abspath(__file__)
        with open(this_file, "r", encoding="utf-8") as f:
            src = f.read()
        changed = []
        for key, new_val in safe_keys.items():
            pat = '"' + re.escape(key) + r'"(\s*:\s*)"([^"]*)"'
            mm  = re.search(pat, src)
            if mm and mm.group(2) != new_val:
                escaped = new_val.replace("\\", "\\\\").replace('"', '\\"')
                src = src[:mm.start()] + '"' + key + '"' + mm.group(1) + '"' + escaped + '"' + src[mm.end():]
                changed.append(key)
        if changed:
            with open(this_file, "w", encoding="utf-8") as f:
                f.write(src)
            log.info("Self-patched %d cookie(s): %s", len(changed), ", ".join(changed))
        else:
            log.info("Safe cookies already up-to-date.")
    except Exception as e:
        log.warning("Self-patch failed: %s", e)

# ── Hardcoded COOKIES fallback ────────────────────────────────────────────────
_SECTION_DATA_IDS_DICT = {"messages": 1775042802}

COOKIES = {
    "_gcl_au":               "1.1.411579683.1774357159",
    "mage-cache-sessid":     "true",
    "mage-messages":         "",
    "PHPSESSID":             "9584c430c6622bcc61e0d80bd48a4ad0",
    "form_key":              "vAZMjATzJczejlbt",
    "inova_p_sid":           "3eifjfq_260405231923",
    "region_override":       "true",
    "_cc_id":                "4883d51dad3e98471b35f2d06f5737fd",
    "_gid":                  "GA1.3.90311475.1775731794",
    "_uetsid":               "5081fc10340211f1824783f3f4295e20|1f4yu06|2|g52|0|2290",
    "_uetvid":               "50825970340211f19fb725b81358ac91",
    "panoramaId_expiry":     "1775116493461",
    "panoramaId":            "06bd167ff09ee9acdffeff87e2a8a9fb927a7c8599b4ca8faecf6de1103a5e25",
    "panoramaIdType":        "panoDevice",
    "cf_clearance":          "eUcNjtzTyBlvLSsutVcePRc_78FMXXPBfY8CQb5UK4I-1775733881-1.2.1.1-L3odvc9AdW6gNlvcqRV3hXicGo_.6KLRn0DaACg2_yEE4iu3IadZKCpgh.fPlNPGnmHt4_sPEKopNy.PknF6IznQLOXeGqJryByv91N4wC_hS2wmzlVRjPKfdez9AzL9RHsYktFvCV5IUcmWLQs55wZJyW8czVII.Xx_A5kf2j9hQm5NOxWIPQzqBaSy_uDWikpK4EcCLyCMRKotbqGtAliGNrdkVJKtKIOic3.dmjIlZ6ODBopAdWAzbvD5h_JMzZNhcLs8_6XROrw7ZRgiEeR1SpignVvJ1O6y1itklMHmQnwhJVyaQ0YBCaWlcnCttgqeFkJ8Hqdjjdw4eWMNTg",
    "private_content_version": "acded08f15281272e00ae3eb74049697",
    "_ga":                   "GA1.3.101399263.1775456360",
    "_ga_ZVLH87X2L7":        "GS2.1.s1775731802$o2$g1$t1775733876$j58$l0$h2020236009$dMu__nWOx_nf8YI6Iz_scSn6OgDvJlleCrg",
}

def _build_cookies() -> dict:
    cookies = dict(COOKIES)
    cookies["section_data_ids"] = json.dumps(_SECTION_DATA_IDS_DICT, separators=(",", ":"))
    return cookies

# ── Base form payload ─────────────────────────────────────────────────────────
BASE_FORM = {
    "ajax":              "1",
    "is_product_page":   "1",
    "diamond_type":      "labgrown",
    "clarity":           "SI2,SI1,VS2,VS1,VVS2,VVS1,IF,FL",
    "colour":            "J,I,H,G,F,E,D",
    "cut":               "Good,Very Good,Excellent,Ideal",
    "itemsPerPage":      str(ITEMS_PER_PAGE),
    "viewtype":          "list",
    "carat":             "0.3-10",
    "price":             "100-100000",
    "polish":            "Good,Very Good,Excellent",
    "symmetry":          "Good,Very Good,Excellent",
    "flou":              "Very Strong,Strong,Medium,Faint,None",
    "withimagevideo":    "false",
    "available_instore": "false",
    "search":            "",
    "gemtype":           "",
    "ring_builder":      "1",
    "pagetype":          "ringbuilder",
}

# ── CSV columns ───────────────────────────────────────────────────────────────
# cert_no_clean = certificate with leading "LG"/"lg" stripped
# Matches abelini column layout so both CSVs can be merged/compared easily
FIELDS = [
    "certificate", "cert_no_clean", "lab",
    "shape", "carat", "colour", "clarity", "cut",
    "price", "price_formatted",
    "cert_url", "available", "type",
    "status", "page_no",
]

def strip_lg(value: str) -> str:
    """Remove leading 'LG' or 'lg' prefix from a certificate number."""
    if value and re.match(r'^[Ll][Gg]', value):
        return value[2:]
    return value or ""

# ── Stale-file backup ─────────────────────────────────────────────────────────

def backup_stale_files() -> bool:
    """
    Move output files older than STALE_DAYS to diamond_files_backup/.
    Returns True if any file was moved (caller should expect a fresh full scrape).
    """
    candidates = [MAIN_CSV, EXTRA_CSV, STATUS_FILE]
    threshold  = STALE_DAYS * 86400
    now_ts     = time.time()
    moved      = []

    for path in candidates:
        if not os.path.exists(path):
            continue
        age = now_ts - os.path.getmtime(path)
        if age < threshold:
            log.info("Fresh (%.2fd): %s", age / 86400, path)
            continue

        os.makedirs(backup_dir, exist_ok=True)
        stamp    = time.strftime("%Y%m%d_%H%M%S")
        base     = os.path.basename(path)
        dest     = os.path.join(backup_dir, f"{stamp}_{base}")
        try:
            shutil.move(path, dest)
            log.info("Stale (%.2fd) — moved: %s → %s", age / 86400, path, dest)
            moved.append(base)
        except Exception as e:
            log.warning("Could not move stale file %s: %s", path, e)

    if moved:
        log.info("Backup complete (%d file(s)) — scraper will restart from page 1.", len(moved))
        return True
    log.info("No stale files (>= %d day(s)). Continuing with existing state.", STALE_DAYS)
    return False

# ── Status helpers ────────────────────────────────────────────────────────────

def write_status(state: str, **kwargs) -> None:
    data = {"scraper": SCRAPER_NAME, "state": state,
            "updated_at": time.strftime("%Y-%m-%dT%H:%M:%S"), **kwargs}
    try:
        with open(STATUS_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
    except Exception as e:
        log.warning("Could not write status: %s", e)

# ── Load existing records ─────────────────────────────────────────────────────

def load_existing_ids(path: str) -> set:
    """Return the set of cert_no_clean values already saved (unique key)."""
    ids: set = set()
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                raw = row.get("cert_no_clean") or row.get("certificate") or ""
                v   = strip_lg(str(raw).strip()) if raw else ""
                if v:
                    ids.add(v)
    log.info("Loaded %d existing records from %s", len(ids), path)
    return ids

# ── Flatten one diamond record ────────────────────────────────────────────────

def flatten(d: dict) -> dict:
    raw_cert = (d.get("certificate") or d.get("cert_num") or
                d.get("certificate_number") or "")
    return {
        "certificate":     raw_cert,
        "cert_no_clean":   strip_lg(str(raw_cert)),
        "lab":             d.get("lab") or d.get("certby"),
        "shape":           d.get("shape"),
        "carat":           d.get("carat") or d.get("weight"),
        "colour":          d.get("colour") or d.get("color"),
        "clarity":         d.get("clarity"),
        "cut":             d.get("cut") or d.get("cut_grade"),
        "price":           d.get("price") or d.get("price_value"),
        "price_formatted": d.get("price_formatted") or d.get("formatted_price"),
        "cert_url":        d.get("cert_url") or d.get("certificate_url"),
        "available":       d.get("available") or d.get("in_stock"),
        "type":            d.get("type") or d.get("diamond_type"),
    }

# ── Row builder ───────────────────────────────────────────────────────────────

def build_rows(diamonds: list, existing: set, page: int):
    rows, new_n, dup_n = [], 0, 0
    for d in diamonds:
        flat   = flatten(d)
        raw    = str(flat.get("cert_no_clean") or flat.get("certificate") or "")
        key    = strip_lg(raw.strip())              # canonical unique key
        is_dup = bool(key and key in existing)
        row    = {f: flat.get(f) for f in FIELDS[:-2]}
        row["status"]  = "duplicate" if is_dup else "new"
        row["page_no"] = page
        rows.append(row)
        if not is_dup and key:
            existing.add(key)
            new_n += 1
        else:
            dup_n += 1
    return rows, new_n, dup_n

# ── Single page fetch ─────────────────────────────────────────────────────────

def fetch_page(session: cffi_requests.Session, page: int, cookies: dict):
    form = {**BASE_FORM, "p": str(page)}
    time.sleep(random.uniform(SLEEP_MIN, SLEEP_MAX))
    last_exc = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.post(API_URL, headers=make_headers(),
                                cookies=cookies, data=form, timeout=40)
            if resp.status_code == 429:
                wait = min(15 * attempt, 90) + random.uniform(0, 5)
                log.warning("Page %d — 429 → sleeping %.1fs", page, wait)
                time.sleep(wait)
                continue
            if resp.status_code == 403:
                raise PageBlockedError(f"403 Forbidden on page {page}")
            if resp.status_code == 400:
                raise ValueError(f"400 Bad Request p{page}: {resp.text[:300]}")
            resp.raise_for_status()
            try:
                body = resp.json()
            except Exception:
                raise ValueError(f"Non-JSON response p{page}: {resp.text[:200]}")
            diamonds = (body.get("items") or body.get("diamonds") or
                        body.get("data") or body.get("products") or
                        (body if isinstance(body, list) else []))
            total = int(body.get("total_count") or body.get("totalCount") or
                        body.get("total") or body.get("count")  or (body.get("pagination") or {}).get("totalItems") or 0)
            return diamonds, total
        except PageBlockedError:
            raise
        except ValueError:
            raise
        except Exception as exc:
            last_exc = exc
            wait = min(RETRY_BACKOFF ** attempt, 30) + random.uniform(0, 1)
            log.warning("Page %d | attempt %d/%d | %s — retry %.1fs",
                        page, attempt, MAX_RETRIES, exc, wait)
            time.sleep(wait)
    if last_exc is None:
        raise PageBlockedError(f"Page {page} blocked (429) after {MAX_RETRIES} attempts")
    raise RuntimeError(f"Page {page} failed after {MAX_RETRIES} attempts") from last_exc

# ── Main scraper ──────────────────────────────────────────────────────────────

def fetch_diamond_heaven(rotate_after_fails: int | None = None) -> None:
    """
    Scrape diamond-heaven.co.uk lab-grown diamonds.

    rotate_after_fails: If set, raise VPNRotateNeeded after N consecutive
        403/block errors so the caller (run_with_vpn_rotation) can switch
        to a fresh exit IP.  If None, just log and retry on blocks.
    """
    global _SECTION_DATA_IDS_DICT

    if rotate_after_fails:
        log.info("VPN rotate-on-fail threshold: %d consecutive blocked page(s)", rotate_after_fails)

    backup_stale_files()

    existing    = load_existing_ids(MAIN_CSV)
    file_exists = os.path.exists(MAIN_CSV)
    open_mode   = "a" if file_exists else "w"
    total_saved = len(existing)
    total_dups  = total_count = 0

    # Resume — vpn_rotate state means a previous run saved the blocked page;
    # all other states resume from the last successfully written page + 1.
    start_page = 1
    if os.path.exists(STATUS_FILE):
        try:
            with open(STATUS_FILE, "r", encoding="utf-8") as f:
                prev = json.load(f)
            last  = prev.get("current_page", 0)
            state = prev.get("state", "")
            if isinstance(last, int) and last >= 1:
                start_page = last + 1
                log.info("Resuming from page %d (prev state: %s)", start_page, state)
        except Exception as e:
            log.warning("Status read failed: %s — starting p1", e)

    write_status("running", current_page=max(0, start_page - 1),
                 message=f"Starting from page {start_page}",
                 diamonds_saved=total_saved)

    # Load cookies
    loaded = _load_cookies_from_curl()
    if loaded:
        COOKIES.update({k: v for k, v in loaded.items() if k != "section_data_ids"})
        if "section_data_ids" in loaded:
            try:
                parsed = json.loads(loaded["section_data_ids"])
                if isinstance(parsed, dict):
                    _SECTION_DATA_IDS_DICT = parsed
            except json.JSONDecodeError:
                pass

    cookies = _build_cookies()
    log.info("section_data_ids → %s", cookies["section_data_ids"])

    session = cffi_requests.Session(impersonate=CHROME_VERSION)

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

            page              = start_page
            consecutive_fails = 0
            total_pages       = 0   # set once from API total on first page
            while True:
                log.info("Fetching page %d ...", page)
                try:
                    diamonds, api_total = fetch_page(session, page, cookies)
                    consecutive_fails = 0
                except PageBlockedError as e:
                    consecutive_fails += 1
                    threshold = rotate_after_fails or VPN_ROTATE_AFTER_FAILS
                    log.warning("🚫 Blocked: %s  (streak %d/%d)",
                                e, consecutive_fails, threshold)
                    if rotate_after_fails and consecutive_fails >= rotate_after_fails:
                        log.warning(
                            "VPN rotate needed — %d consecutive blocked page(s) at p%d",
                            consecutive_fails, page,
                        )
                        # Save progress so the next VPN attempt resumes from this page,
                        # not from page 1. current_page = page - 1 because resume logic
                        # does start_page = last + 1.
                        write_status(
                            "vpn_rotate",
                            current_page=max(0, page - 1),
                            diamonds_saved=total_saved,
                            message=f"VPN rotate at page {page} — will resume here",
                        )
                        raise VPNRotateNeeded(page, consecutive_fails)
                    continue  # retry the same page
                except ValueError as e:
                    log.error("Fatal: %s", e)
                    write_status("failed", message=str(e), diamonds_saved=total_saved)
                    raise

                if api_total and total_count == 0:
                    total_count = api_total
                    total_pages = max(1, -(-total_count // ITEMS_PER_PAGE))
                    log.info("API total: %d → ~%d pages", total_count, total_pages)

                if not diamonds:
                    log.info("Page %d: 0 items — done.", page)
                    break

                rows, new_n, dup_n = build_rows(diamonds, existing, page)
                writer_main.writerows(rows)
                writer_extra.writerows(rows)
                f_main.flush()
                f_extra.flush()

                total_saved += new_n
                total_dups  += dup_n
                pct = round(page / total_pages * 100, 1) if total_count else "?"
                log.info("Page %4d/%s | new:%3d dup:%3d | saved:%d | %s%%",
                         page, total_pages if total_count else "?",
                         new_n, dup_n, total_saved, pct)

                write_status("running", current_page=page,
                             diamonds_saved=total_saved, duplicates=total_dups,
                             message=f"Progress: {pct}%")

                if len(diamonds) < ITEMS_PER_PAGE:
                    log.info("Last page (got %d < %d).", len(diamonds), ITEMS_PER_PAGE)
                    break
                page += 1

    except VPNRotateNeeded:
        raise  # status already saved above — don't overwrite with "failed"
    except Exception as exc:
        write_status("failed", diamonds_saved=total_saved, message=f"Crashed: {exc}")
        raise
    finally:
        session.close()

    write_status("done", diamonds_saved=total_saved, duplicates=total_dups,
                 message="Completed")
    log.info("=== Done | %d saved | Main: %s | Mirror: %s ===",
             total_saved, MAIN_CSV, EXTRA_CSV)


# ── VPN-WRAPPED RUN ───────────────────────────────────────────────────────────
def run_with_vpn_rotation() -> None:
    """
    Run the scraper inside an OpenVPN tunnel, trying EVERY config in
    vpnbook/uk/<VPN_CONFIG_SUBDIR>/ until one completes the scrape.

    Per-config outcome:
      - success           → scrape done, return.
      - VPNRotateNeeded   → N consecutive 403s burned the exit IP; keep the
                            config and rotate to the next one.
      - VPNError (dead)   → the config failed to connect; DELETE the .ovpn file
                            and move on.

    Safety: a missing OpenVPN binary or stale credentials abort the run
    WITHOUT deleting any configs. Requires Administrator / root.
    """
    from vpn_rotator import (
        VPNConnection,
        VPNError,
        find_openvpn_executable,
        is_admin,
        parse_credentials_file,
        write_auth_file,
    )

    log.info("=" * 60)
    log.info("Diamond-Heaven scraper with VPN rotation")
    log.info("Consecutive-fail threshold: %d", VPN_ROTATE_AFTER_FAILS)
    log.info("=" * 60)

    if not is_admin():
        log.error(
            "OpenVPN requires Administrator/root. "
            "Re-run from an elevated shell, or call fetch_diamond_heaven() directly."
        )
        return

    try:
        find_openvpn_executable()
    except VPNError as exc:
        log.error("OpenVPN unavailable — aborting without touching configs: %s", exc)
        return

    # uk_scrapping/ -> repo root; VPN configs live in vpnbook/uk/<subdir>/.
    repo_root = Path(__file__).resolve().parent.parent
    vpn_dir   = repo_root / "vpnbook" / "uk" / VPN_CONFIG_SUBDIR
    if not vpn_dir.exists():
        log.error("VPN config dir not found: %s", vpn_dir)
        return

    # Shared ProtonVPN credentials, resolved relative to this script (cwd-independent).
    proton_auth  = None
    proton_creds = repo_root / "vpnbook" / "proton_credentials.txt"
    if proton_creds.exists():
        user, pwd   = parse_credentials_file(proton_creds)
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
                fetch_diamond_heaven(rotate_after_fails=VPN_ROTATE_AFTER_FAILS)
            log.info("Scrape completed — VPN torn down.")
            return

        except VPNRotateNeeded as exc:
            log.warning("Rotating VPN (IP burned after %d blocks): %s", exc.consecutive_fails, exc)

        except VPNError as exc:
            if "authentication failed" in str(exc).lower():
                log.error(
                    "VPN auth failed on %s — credentials likely stale. "
                    "Aborting (no configs deleted). Check proton_credentials.txt.", cfg.name
                )
                return
            log.error("VPN error on %s: %s — deleting dead config", cfg.name, exc)
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
        total, deleted,
    )


if __name__ == "__main__":
    # run_with_vpn_rotation()   → VPN-wrapped scrape, rotates after 5 consecutive blocks (needs admin)
    fetch_diamond_heaven()   # → pure scrape, no VPN (your real IP)
    # import sys as _sys
    # try:
    #     run_with_vpn_rotation()
    # except KeyboardInterrupt:
    #     log.warning("Ctrl+C — stopping scraper and tearing down any VPN tunnel.")
    #     try:
    #         from vpn_rotator import kill_openvpn_processes
    #         kill_openvpn_processes()
    #     except Exception as _exc:
    #         log.warning("VPN cleanup failed: %s", _exc)
    #     _sys.exit(130)
    