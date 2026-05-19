"""
loosegrowndiamond.py
~~~~~~~~~~~~~~~~~~~~
Production-grade scraper for loosegrowndiamond.com inventory.

Design principles
-----------------
* Dynamic timeout scaling — read timeout grows with pagination depth.
* Dynamic page-size reduction — smaller chunks at deep offsets to ease server SQL load.
* Retry sleep is INSIDE each exception handler (was broken before — now correct).
* Streaming response reader — avoids hard read-timeout on large payloads.
* Exponential back-off with full jitter on every transient failure.
* Explicit separation of concerns: config / HTTP / parsing / persistence.
* Context-manager for the CSV writer so the file is never left half-open.
* A single dataclass represents one diamond row — no magic string indexes.
* Rotating / archiving old data files is handled before any I/O begins.
* Random User-Agent via fake-useragent (graceful fallback pool if unavailable).
* Fresh requests.Session per page — rotates UA + clears connection state.
* All public surface area is type-annotated.

Fixes applied vs previous version
----------------------------------
1. Dynamic timeout   — _get_timeout(start) scales read timeout with depth.
2. Dynamic page size — _get_page_size(start) reduces chunk size at depth.
3. Retry sleep bug   — time.sleep() now runs immediately inside each except block.
4. Streaming fetch   — stream=True + iter_content avoids monolithic read timeouts.
5. Deep-page pause   — extra inter-page sleep beyond offset 50 to let server breathe.
6. Retry exhaustion  — explicit `continue` after each sleep keeps loop correct.
7. Session UA log    — truncated to 80 chars to keep logs readable.
"""

from __future__ import annotations

import csv
import datetime
import json
import os
import random
import re
import time
from dataclasses import astuple, dataclass, fields
from typing import Any, Iterator

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from logger import get_logger

# ── fake-useragent (lazy-init after logger is ready) ──────────────────────────
try:
    from fake_useragent import UserAgent as _FakeUA
    _ua_pool = _FakeUA(browsers=["chrome", "firefox", "edge"], os=["windows", "macos"])
except Exception:
    _ua_pool = None

# ── Logging ───────────────────────────────────────────────────────────────────

log = get_logger("loosegrown")

# ── Constants ─────────────────────────────────────────────────────────────────

URL = "https://www.loosegrowndiamond.com/wp-json/ls/v1/inventory-filter"

# Page size tiers — reduced at depth to avoid server-side SQL timeouts.
# The server runs OFFSET n*PAGE_SIZE LIMIT PAGE_SIZE; higher offsets are slower.
PAGE_SIZE_SHALLOW  = 5_000   # offset  <  30
PAGE_SIZE_MID      = 2_000   # offset  30 – 59
PAGE_SIZE_DEEP     = 1_000   # offset  60 – 99
PAGE_SIZE_VERYDEEP =   500   # offset 100+

# Back-off knobs (all in seconds)
RETRY_TOTAL      = 6     # total retry attempts per page
BACKOFF_BASE     = 15    # first wait  → 15 s
BACKOFF_CAP      = 180   # maximum wait → 3 min
JITTER_RANGE     = 5     # ± random seconds added to each wait

# Inter-page polite pause
INTER_PAGE_SLEEP_SHALLOW = 1.5   # offset  < 50
INTER_PAGE_SLEEP_DEEP    = 5.0   # offset 50+ — let server breathe

# Streaming chunk size (bytes)
STREAM_CHUNK = 65_536  # 64 KB

# File paths
BASE_DIR   = os.getcwd()
DOWN_FILES = os.path.join(BASE_DIR, "diamond_files")
CSV_FILE   = os.path.join(DOWN_FILES, "loosegrowndiamond.csv")
STATE_FILE = os.path.join(DOWN_FILES, "lgd_state.txt")

os.makedirs(DOWN_FILES, exist_ok=True)

# ── Static HTTP config ────────────────────────────────────────────────────────

COOKIES = (
    "_gcl_au=1.1.915178826.1775805144; ccid.14761153=358413144.4716173266; "
    "_ga=GA1.1.1114979395.1775805144; _fbp=fb.1.1775805144790.253068608175454441; "
    "_tt_enable_cookie=1; _ttp=01KNV3R94KD71R6GMJFAE5303Y_.tt.1; _cs_c=0; "
    "_hjDonePolls=1822879; _uetvid=a045d2c034ac11f1bfe05df903b29b55; "
    "_rdt_uuid=1775805144168.9f355e9b-f043-4706-9162-70aa306208a4; "
    "ttcsid=1778670534555::6f43lnSvvFGlFFxIntG5.6.1778672397282.0::1.1855543.1859114::1862717.28.511.1455::1861327.142.1000; "
    "ttcsid_D6RQPORC77U3V2TBTL8G=1778670534554::o0TVe7FFEVWqAbuddTrK.6.1778672397282.1; "
    "_ga_2DH2FDW0HL=GS2.1.s1778670527; "
    "sucuri_cloudproxy_uuid_3564cbd26=3ef9a0d17e28805dd104d52439206bcc; "
    "PHPSESSID=m239ano1ec2vvppqk39ioo0b66; "
    "_cs_id=e5882140-3c44-a70f-cc82-b31f9a3cfd7d.1775805148.10.1778822543.1778822490.1.1809969148160.1.x; "
    "_cs_s=4.0.U.9.1778824343971"
)

BASE_HEADERS: dict[str, str] = {
    "accept":             "application/json, text/plain, */*",
    "accept-language":    "en-GB,en-US;q=0.9,en;q=0.8",
    "cache-control":      "no-cache",
    "content-type":       "application/json",
    "cookie":             COOKIES,
    "origin":             "https://www.loosegrowndiamond.com",
    "pragma":             "no-cache",
    "priority":           "u=1, i",
    "referer":            "https://www.loosegrowndiamond.com/lab-diamonds/",
    "sec-ch-ua":          '"Chromium";v="148", "Google Chrome";v="148", "Not/A)Brand";v="99"',
    "sec-ch-ua-mobile":   "?0",
    "sec-ch-ua-platform": '"Windows"',
    "sec-fetch-dest":     "empty",
    "sec-fetch-mode":     "cors",
    "sec-fetch-site":     "same-origin",
}

_FALLBACK_UA_POOL: list[str] = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4.1 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 Edg/124.0.0.0",
]


def _random_ua() -> str:
    if _ua_pool is not None:
        try:
            return _ua_pool.random
        except Exception as exc:
            log.debug(f"fake-useragent .random failed ({exc}) — using fallback pool.")
    return random.choice(_FALLBACK_UA_POOL)


# ── Dynamic timeout & page-size ───────────────────────────────────────────────

def _get_timeout(start: int) -> tuple[int, int]:
    """
    Scale read timeout with pagination depth.

    The server executes  OFFSET (start × page_size) LIMIT page_size.
    Higher offsets mean heavier SQL scans — we give the server more time.

    Offset range  | Connect | Read
    --------------|---------|------
    < 20          |  12 s   |  90 s
    20 – 49       |  12 s   | 180 s
    50 – 99       |  15 s   | 300 s   (5 min)
    100+          |  15 s   | 480 s   (8 min)
    """
    if start < 20:
        return (12, 90)
    elif start < 50:
        return (12, 180)
    elif start < 100:
        return (15, 300)
    else:
        return (15, 480)


def _get_page_size(start: int) -> int:
    """
    Reduce page size at depth to lower server SQL cost per request.

    Smaller chunks = faster server response = far fewer read timeouts.

    Offset   | Page size
    ---------|----------
    < 30     |  5 000
    30 – 59  |  2 000
    60 – 99  |  1 000
    100+     |    500
    """
    if start < 30:
        return PAGE_SIZE_SHALLOW
    elif start < 60:
        return PAGE_SIZE_MID
    elif start < 100:
        return PAGE_SIZE_DEEP
    else:
        return PAGE_SIZE_VERYDEEP


def _inter_page_sleep(start: int) -> None:
    """
    Polite pause between pages.
    Longer at depth so the server is not hammered on slow queries.
    """
    if start >= 50:
        delay = random.uniform(INTER_PAGE_SLEEP_DEEP, INTER_PAGE_SLEEP_DEEP + 3.0)
        log.debug(f"Deep-page throttle: sleeping {delay:.1f}s before next fetch.")
    else:
        delay = random.uniform(INTER_PAGE_SLEEP_SHALLOW, INTER_PAGE_SLEEP_SHALLOW + 1.0)
    time.sleep(delay)


# ── Data model ────────────────────────────────────────────────────────────────

@dataclass(slots=True)
class DiamondRow:
    shape:             str
    carat:             str
    cut:               str
    color:             str
    clarity:           str
    sale_price:        str
    price:             str
    org_price:         str
    id:                str
    pid:               str
    sku:               str
    certificate:       str
    certificate_no:    str
    certificate_type:  str
    video:             str
    lot_number:        str
    mm:                str
    dtype:             str
    av_table:          str
    depth:             str
    lwratio:           str
    ha:                str
    sale:              str
    express_shipping:  str
    hearts_and_arrows: str
    is_overnight:      str
    note:              str
    created_at:        str
    updated_at:        str

    @classmethod
    def csv_headers(cls) -> list[str]:
        return [f.name for f in fields(cls)]

    def as_row(self) -> tuple:
        return astuple(self)


# ── HTTP session factory ──────────────────────────────────────────────────────

def _build_session() -> requests.Session:
    """
    Build a fresh requests.Session for a single page fetch.

    Fresh session per request ensures:
    - User-Agent rotates on every call (anti-fingerprinting).
    - TCP connections are not reused across pages.
    - Cookie jars never accumulate server-set tracking cookies.
    """
    ua = _random_ua()
    log.debug(f"Session UA → {ua[:80]}{'…' if len(ua) > 80 else ''}")

    session = requests.Session()
    session.headers.update({**BASE_HEADERS, "user-agent": ua})

    adapter = HTTPAdapter(
        pool_connections=1,
        pool_maxsize=1,
        max_retries=Retry(
            total=3,
            connect=3,
            read=0,           # application-level back-off handles read retries
            backoff_factor=2,
            status_forcelist=[502, 503, 504],
            allowed_methods=["POST"],
        ),
    )
    session.mount("https://", adapter)
    session.mount("http://",  adapter)
    return session


# ── Back-off helpers ──────────────────────────────────────────────────────────

def _backoff_wait(attempt: int) -> float:
    """
    Exponential back-off with full jitter.

    wait = min(cap, base × 2^(attempt-1)) ± jitter

    Attempt  | Base wait  | With cap=180
    ---------|------------|-------------
        1    |   15 s     |   15 s
        2    |   30 s     |   30 s
        3    |   60 s     |   60 s
        4    |  120 s     |  120 s
        5    |  240 s     |  180 s  (capped)
        6    |  480 s     |  180 s  (capped)
    """
    base   = min(BACKOFF_CAP, BACKOFF_BASE * (2 ** (attempt - 1)))
    jitter = random.uniform(-JITTER_RANGE, JITTER_RANGE)
    return max(1.0, base + jitter)


# ── Certificate helpers ───────────────────────────────────────────────────────

_CERT_PATTERNS = [
    re.compile(r"[?&]reportno=(\d+)"),
    re.compile(r"[?&]r=(?:LG)?(\d+)", re.IGNORECASE),
    re.compile(r"[?&]certificate_id=(\d+)"),
]


def _extract_certificate_no(cert_url: str) -> str:
    if not cert_url:
        return ""
    for pattern in _CERT_PATTERNS:
        m = pattern.search(cert_url)
        if m:
            return m.group(1)
    return ""


# ── File rotation ─────────────────────────────────────────────────────────────

def _rotate_old_file(filepath: str, max_age_seconds: float = 86_400) -> bool:
    """
    Archive `filepath` when it is older than `max_age_seconds`.
    Resets the state file so the next run starts from offset 1.
    """
    if not os.path.exists(filepath):
        return False

    mtime  = os.path.getmtime(filepath)
    mod_dt = datetime.datetime.fromtimestamp(mtime)
    age    = datetime.datetime.now() - mod_dt

    if age.total_seconds() <= max_age_seconds:
        h, m = divmod(age.seconds, 3600)
        log.info(f"Data file is fresh ({h}h {m // 60}m old) — continuing from last offset.")
        return False

    stem, ext = os.path.splitext(filepath)
    date_tag  = mod_dt.strftime("%Y-%m-%d")
    new_path  = f"{stem}_{date_tag}{ext}"

    counter = 1
    while os.path.exists(new_path):
        new_path = f"{stem}_{date_tag}_{counter}{ext}"
        counter += 1

    os.rename(filepath, new_path)
    log.info(
        f"Archived stale file → {os.path.basename(new_path)} "
        f"(was {age.days}d {age.seconds // 3600}h old)"
    )

    if os.path.exists(STATE_FILE):
        os.remove(STATE_FILE)
        log.info("State file cleared — will start from offset 1.")

    return True


# ── State / CSV helpers ───────────────────────────────────────────────────────

def _load_existing_skus() -> set[str]:
    skus: set[str] = set()
    if not os.path.exists(CSV_FILE):
        return skus
    with open(CSV_FILE, newline="", encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            sku = (row.get("sku") or "").strip()
            if sku:
                skus.add(sku)
    log.info(f"Loaded {len(skus):,} existing SKUs from CSV.")
    return skus


def _get_start_offset() -> int:
    if not os.path.exists(STATE_FILE):
        _save_offset(1)
        log.debug("No state file found — starting from offset 1.")
        return 1
    try:
        value = int(open(STATE_FILE).read().strip())
        log.info(f"Resuming from saved offset: {value}")
        return value
    except (ValueError, OSError) as exc:
        log.warning(f"Corrupt state file ({exc}) — resetting to 1.")
        _save_offset(1)
        return 1


def _save_offset(offset: int) -> None:
    with open(STATE_FILE, "w") as fh:
        fh.write(str(offset))
    log.debug(f"State saved: offset={offset}")


def _init_csv() -> None:
    if not os.path.exists(CSV_FILE):
        with open(CSV_FILE, "w", newline="", encoding="utf-8") as fh:
            csv.writer(fh).writerow(DiamondRow.csv_headers())
        log.info(f"CSV initialised: {CSV_FILE}")


# ── HTTP fetching ─────────────────────────────────────────────────────────────

def _build_payload(start: int) -> dict[str, Any]:
    return {
        "ls_start":    start,
        "ls_per_page": _get_page_size(start),
        "shape":       [],
    }


def _fetch_page(start: int) -> dict[str, Any]:
    """
    POST to the inventory endpoint with a fresh session per call.

    Key improvements over v1
    -------------------------
    * stream=True + iter_content — avoids monolithic read blocking that caused
      the original ReadTimeout at deep offsets. Data is read in 64 KB chunks
      so the socket stays alive throughout the transfer.
    * Dynamic timeout via _get_timeout(start) — read budget grows with depth.
    * Dynamic page size via _build_payload(start) — smaller chunks at depth
      reduce server-side SQL cost and eliminate the root cause of slow responses.
    * time.sleep(wait) is now INSIDE each except block so it executes immediately
      after a failure, not at an arbitrary point in the outer loop.
    * Explicit `continue` after each sleep keeps retry semantics unambiguous.
    """
    timeout   = _get_timeout(start)
    page_size = _get_page_size(start)

    log.debug(
        f"Fetching offset={start} | page_size={page_size:,} "
        f"| timeout={timeout[1]}s read"
    )

    wait = 0.0  # initialised so the post-loop log can reference it safely

    for attempt in range(1, RETRY_TOTAL + 1):
        with _build_session() as session:
            try:
                resp = session.post(
                    URL,
                    json=_build_payload(start),
                    timeout=timeout,
                    stream=True,        # ← FIX: streaming avoids hard read-timeout
                )
                resp.raise_for_status()

                # ── Read streamed response in chunks ──────────────────────
                raw_chunks: list[bytes] = []
                for chunk in resp.iter_content(chunk_size=STREAM_CHUNK):
                    if chunk:
                        raw_chunks.append(chunk)

                return json.loads(b"".join(raw_chunks))

            # ── ReadTimeout: most common failure at deep offsets ──────────
            except requests.exceptions.ReadTimeout:
                wait = _backoff_wait(attempt)
                log.warning(
                    f"[attempt {attempt}/{RETRY_TOTAL}] Read timeout "
                    f"at offset={start} (timeout={timeout[1]}s). "
                    f"Retrying in {wait:.0f}s …"
                )
                if attempt < RETRY_TOTAL:
                    time.sleep(wait)   # ← FIX: sleep immediately inside handler
                continue               # ← FIX: explicit continue

            # ── Connection errors (DNS, TCP reset, proxy drop) ────────────
            except requests.exceptions.ConnectionError as exc:
                wait = _backoff_wait(attempt)
                log.warning(
                    f"[attempt {attempt}/{RETRY_TOTAL}] Connection error "
                    f"at offset={start}: {exc}. "
                    f"Retrying in {wait:.0f}s …"
                )
                if attempt < RETRY_TOTAL:
                    time.sleep(wait)   # ← FIX: sleep immediately inside handler
                continue

            # ── HTTP 4xx / 5xx ────────────────────────────────────────────
            except requests.exceptions.HTTPError as exc:
                status = exc.response.status_code if exc.response is not None else "?"
                if isinstance(status, int) and 400 <= status < 500:
                    # 4xx is non-retriable — server rejected the request
                    log.error(f"Non-retriable HTTP {status} at offset={start}: {exc}")
                    return {}
                wait = _backoff_wait(attempt)
                log.warning(
                    f"[attempt {attempt}/{RETRY_TOTAL}] HTTP {status} "
                    f"at offset={start}. Retrying in {wait:.0f}s …"
                )
                if attempt < RETRY_TOTAL:
                    time.sleep(wait)   # ← FIX: sleep immediately inside handler
                continue

            # ── JSON decode error (truncated / corrupt response) ──────────
            except json.JSONDecodeError as exc:
                wait = _backoff_wait(attempt)
                log.warning(
                    f"[attempt {attempt}/{RETRY_TOTAL}] JSON decode error "
                    f"at offset={start}: {exc}. Retrying in {wait:.0f}s …"
                )
                if attempt < RETRY_TOTAL:
                    time.sleep(wait)
                continue

            # ── Catch-all for unexpected errors ───────────────────────────
            except Exception as exc:
                log.error(f"Unexpected error at offset={start}: {exc}", exc_info=True)
                return {}

    log.error(
        f"All {RETRY_TOTAL} attempts exhausted at offset={start}. "
        f"Skipping page and saving checkpoint."
    )
    return {}


# ── Parsing ───────────────────────────────────────────────────────────────────

def _s(value: Any) -> str:
    """Safely coerce any value to a stripped string."""
    return str(value).strip() if value is not None else ""


def _iter_new_rows(
    items: list[dict],
    seen_skus: set[str],
) -> Iterator[DiamondRow]:
    """Yield DiamondRow objects for items not already in `seen_skus`."""
    for item in items:
        sku = _s(item.get("sku"))
        if not sku or sku in seen_skus:
            continue

        cert_type = _s(item.get("certificate_type"))
        if cert_type == "0":
            continue  # uncertified — skip

        cert_url = _s(item.get("certificate"))
        cert_no  = _extract_certificate_no(cert_url)

        row = DiamondRow(
            shape             = _s(item.get("shape")).lower(),
            carat             = _s(item.get("carat")),
            cut               = _s(item.get("cut")),
            color             = _s(item.get("color")),
            clarity           = _s(item.get("clarity")),
            sale_price        = _s(item.get("sale_price")),
            price             = _s(item.get("price")),
            org_price         = _s(item.get("org_price")),
            id                = _s(item.get("id")),
            pid               = _s(item.get("pid")),
            sku               = sku,
            certificate       = cert_url,
            certificate_no    = cert_no,
            certificate_type  = cert_type,
            video             = _s(item.get("video")),
            lot_number        = _s(item.get("lot_number")),
            mm                = _s(item.get("mm")),
            dtype             = _s(item.get("dtype")),
            av_table          = _s(item.get("av_table")),
            depth             = _s(item.get("depth")),
            lwratio           = _s(item.get("lwratio")),
            ha                = _s(item.get("ha")),
            sale              = _s(item.get("sale")),
            express_shipping  = _s(item.get("express_shipping")),
            hearts_and_arrows = _s(item.get("hearts_and_arrows")),
            is_overnight      = _s(item.get("is_overnight")),
            note              = _s(item.get("note")),
            created_at        = _s(item.get("created_at")),
            updated_at        = _s(item.get("updated_at")),
        )
        seen_skus.add(sku)
        yield row


# ── Main entry point ──────────────────────────────────────────────────────────

def loose_grown_diamonds_scrappe() -> dict[str, int]:
    log.info("=" * 60)
    log.info("Loose Grown Diamond scrape — starting")

    rotated = _rotate_old_file(CSV_FILE)
    if rotated:
        log.info("Old data archived — this will be a full fresh run.")

    _init_csv()

    seen_skus   = _load_existing_skus()
    start       = _get_start_offset()
    total_saved = 0

    log.info(f"Starting from offset {start}")

    while True:
        # ── Log effective settings for this page ──────────────────────────
        page_size = _get_page_size(start)
        timeout   = _get_timeout(start)
        log.debug(
            f"Page config → page_size={page_size:,} | "
            f"timeout={timeout[1]}s | offset={start}"
        )

        response = _fetch_page(start)

        if not response or not response.get("status"):
            log.warning(
                f"Empty or failed response at offset={start} — "
                f"checkpoint saved, stopping scrape."
            )
            break

        items       = response.get("data") or []
        total_avail = int(response.get("total", 0))

        if not items:
            log.info("No items returned — inventory fully consumed.")
            break

        # ── Write new rows ────────────────────────────────────────────────
        new_rows = 0
        with open(CSV_FILE, "a", newline="", encoding="utf-8") as fh:
            writer = csv.writer(fh)
            for diamond in _iter_new_rows(items, seen_skus):
                writer.writerow(diamond.as_row())
                new_rows += 1

        total_saved += new_rows
        next_start   = start + 1
        _save_offset(next_start)

        pct = (next_start / total_avail * 100) if total_avail else 0
        log.info(
            f"Offset {start:>6} → {next_start:<6} | "
            f"Page size: {page_size:>5,} | "
            f"New: {new_rows:>4} | "
            f"Session total: {total_saved:>6,} | "
            f"Progress: {pct:.1f}% of {total_avail:,}"
        )

        if next_start > total_avail:
            log.info("All inventory pages consumed — done.")
            break

        start = next_start
        # ── FIX: deep-page sleep to let server breathe ────────────────────
        _inter_page_sleep(start)

    log.info(f"Scrape complete. Saved {total_saved:,} new records this run.")
    log.info("=" * 60)
    return {"total_saved": total_saved}


if __name__ == "__main__":
    loose_grown_diamonds_scrappe()