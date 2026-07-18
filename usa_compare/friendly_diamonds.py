from __future__ import annotations

import csv
import datetime
import json
import os
import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import astuple, dataclass, fields
from pathlib import Path
from typing import Any, Iterator

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from logger import get_logger

# ── fake-useragent (lazy-init after logger is ready) ──────────────────────────
try:
    from fake_useragent import UserAgent as _FakeUA
    _ua_pool = _FakeUA(browsers=["chrome", "firefox", "edge"], os=["windows", "macos"])
except Exception:          # network unavailable, cache missing, etc.
    _ua_pool = None

# ── Logging ───────────────────────────────────────────────────────────────────

log = get_logger("friendly")

# ── Constants ─────────────────────────────────────────────────────────────────

BASE_URL   = "https://friendlydiamonds.com/api/us/diamond/filter/loose"

# Fixed filters applied to every request (cut/color/clarity cover the full range).
FIXED_PARAMS = (
    "Cut=FR,GD,VG,EX,ID"
    "&Color=M,L,K,J,I,H,G,F,E,D"
    "&Clarity=I1,SI2,SI1,VS2,VS1,VVS2,VVS1,IF,FL"
)

# The vendor's full-range (1-99 carat, all shapes) query drifts/duplicates deep
# into pagination (thousands of pages), so results are collected shape-by-shape
# in narrow carat bands instead — each combo has a small, stable result set.
SHAPES = [
    "Round", "Oval", "Cushion", "Princess", "Emerald",
    "Pear", "Radiant", "Asscher", "Heart", "Marquise",
]

CARAT_FROM  = 0.5
CARAT_TO    = 10.0
CARAT_STEP  = 0.01


def _fmt_carat(value: float) -> str:
    """Render a carat boundary the way the API expects (e.g. 2.0 -> '2', 2.5 -> '2.5')."""
    return f"{value:g}"


def _generate_carat_bands(start: float, end: float, step: float) -> list[tuple[float, float]]:
    bands = []
    c = round(start, 2)
    while c < end - 1e-9:
        nxt = round(min(c + step, end), 2)
        bands.append((c, nxt))
        c = nxt
    return bands


CARAT_BANDS = _generate_carat_bands(CARAT_FROM, CARAT_TO, CARAT_STEP)

# Shapes are pulled from a shared queue by a fixed-size pool of worker
# threads, so only SHAPE_WORKERS shapes are ever in flight at once — the
# rest wait their turn. Each worker walks a shape's carat bands sequentially.
SHAPE_WORKERS = 1

# Back-off knobs (all in seconds)
RETRY_TOTAL       = 3
BACKOFF_BASE      = 8
BACKOFF_CAP       = 60
JITTER_RANGE      = 3
INTER_PAGE_SLEEP  = 0.5

# Consecutive zero-new-diamond pages before a combo is considered exhausted
# (the vendor's API repeats the same ~600-item window past this depth).
ZERO_NEW_STREAK_LIMIT = 2

REQUEST_TIMEOUT   = (12, 45)

# File paths
BASE_DIR    = str(Path(__file__).resolve().parent.parent)
DOWN_FILES  = os.path.join(BASE_DIR, "diamond_files", "usa")
CSV_FILE    = os.path.join(DOWN_FILES, "friendlydiamond.csv")
STATE_FILE  = os.path.join(DOWN_FILES, "friendly_state.txt")

os.makedirs(DOWN_FILES, exist_ok=True)

# ── Backup config ────────────────────────────────────────────────────────────
STALE_THRESHOLD_DAYS = 2    # CSV older than this -> archive + full re-scrape

# ── Static HTTP config ────────────────────────────────────────────────────────

BASE_HEADERS: dict[str, str] = {
    "accept":             "application/json, text/plain, */*",
    "accept-language":    "en-GB,en-US;q=0.9,en;q=0.8",
    "cache-control":      "no-cache",
    "origin":             "https://friendlydiamonds.com",
    "pragma":             "no-cache",
    "priority":           "u=1, i",
    "referer":            "https://friendlydiamonds.com/loose-diamonds",
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

# ── Data model ────────────────────────────────────────────────────────────────

@dataclass(slots=True)
class DiamondRow:
    id:               str
    shape:            str
    carat:            str
    weight:           str
    color:            str
    clarity:          str
    cut:              str
    cut_grade:        str
    polish:           str
    symmetry:         str
    fluorescence:     str
    lab:              str
    report:           str
    certificate_no:   str
    price:            str
    mrp:              str
    discount_label:   str
    actual_ppc:       str
    length:           str
    width:            str
    depth_percent:    str
    table_percent:    str
    lw_ratio:         str
    growth_process:   str
    country:          str
    shade:            str
    lot:              str
    title:            str
    slug:             str
    thumb_image:      str
    video:            str

    @classmethod
    def csv_headers(cls) -> list[str]:
        return [f.name for f in fields(cls)]

    def as_row(self) -> tuple:
        return astuple(self)


# ── HTTP session factory ──────────────────────────────────────────────────────

def _build_session() -> requests.Session:
    """
    Build a fresh requests.Session for a single page fetch.

    A new session is created per request so the User-Agent rotates on every
    call and no keep-alive connection or cookie jar carries over between pages.
    """
    ua = _random_ua()
    log.debug(f"Session UA -> {ua[:80]}{'...' if len(ua) > 80 else ''}")

    session = requests.Session()
    session.headers.update({**BASE_HEADERS, "user-agent": ua})

    adapter = HTTPAdapter(
        pool_connections=1,
        pool_maxsize=1,
        max_retries=Retry(
            total=3,
            connect=3,
            read=0,
            backoff_factor=2,
            status_forcelist=[502, 503, 504],
            allowed_methods=["GET"],
        ),
    )
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session

# ── Back-off helpers ──────────────────────────────────────────────────────────

def _backoff_wait(attempt: int) -> float:
    base = min(BACKOFF_CAP, BACKOFF_BASE * (2 ** (attempt - 1)))
    jitter = random.uniform(-JITTER_RANGE, JITTER_RANGE)
    return max(1.0, base + jitter)


# ── File rotation ─────────────────────────────────────────────────────────────

def _rotate_old_file(filepath: str, max_age_seconds: float = STALE_THRESHOLD_DAYS * 86_400) -> bool:
    """
    Archive `filepath` when it is older than `max_age_seconds`.
    Resets the state file so the next run starts from page 1.
    """
    if not os.path.exists(filepath):
        return False

    mtime  = os.path.getmtime(filepath)
    mod_dt = datetime.datetime.fromtimestamp(mtime)
    age    = datetime.datetime.now() - mod_dt

    if age.total_seconds() <= max_age_seconds:
        h, m = divmod(age.seconds, 3600)
        log.info(f"Data file is fresh ({h}h {m // 60}m old) — continuing from last page.")
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
        f"Archived stale file -> {os.path.basename(new_path)} "
        f"(was {age.days}d {age.seconds // 3600}h old)"
    )

    if os.path.exists(STATE_FILE):
        os.remove(STATE_FILE)
        log.info("State file cleared — will start from page 1.")

    return True


# ── State / CSV helpers ───────────────────────────────────────────────────────

def _load_existing_certs() -> set[str]:
    certs: set[str] = set()
    if not os.path.exists(CSV_FILE):
        return certs
    with open(CSV_FILE, newline="", encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            cert = (row.get("certificate_no") or "").strip()
            if cert:
                certs.add(cert)
    log.info(f"Loaded {len(certs):,} existing certificate numbers from CSV.")
    return certs


def _load_all_state() -> dict[str, dict[str, int]]:
    """Load the {shape: {band_index, page}} progress map (one entry per shape)."""
    if not os.path.exists(STATE_FILE):
        return {}
    try:
        data = json.loads(open(STATE_FILE).read().strip() or "{}")
        if not isinstance(data, dict):
            raise ValueError("state file is not a JSON object")
        return data
    except (ValueError, OSError, json.JSONDecodeError) as exc:
        log.warning(f"Corrupt state file ({exc}) — resetting all shape progress.")
        return {}


def _get_start_state(shape: str, state: dict) -> tuple[int, int]:
    """Return (band_index, page) to resume `shape` from."""
    entry = state.get(shape)
    if not entry:
        return 0, 1
    try:
        return int(entry["band_index"]), int(entry["page"])
    except (KeyError, ValueError, TypeError):
        return 0, 1


def _update_state(
    shape: str, band_index: int, page: int, state: dict, state_lock: threading.Lock
) -> None:
    """Update `shape`'s progress in the shared state map and persist it."""
    with state_lock:
        state[shape] = {"band_index": band_index, "page": page}
        with open(STATE_FILE, "w") as fh:
            json.dump(state, fh)
    log.debug(f"State saved: shape={shape}, band_index={band_index}, page={page}")


def _init_csv() -> None:
    if not os.path.exists(CSV_FILE):
        with open(CSV_FILE, "w", newline="", encoding="utf-8") as fh:
            csv.writer(fh).writerow(DiamondRow.csv_headers())
        log.info(f"CSV initialised: {CSV_FILE}")


# ── HTTP fetching ─────────────────────────────────────────────────────────────

def _fetch_page(shape: str, carat_from: float, carat_to: float, page: int) -> dict[str, Any]:
    """
    GET the loose-diamond filter endpoint for a single (shape, carat band, page)
    combo with a fresh session per call. Retries up to RETRY_TOTAL times on
    transient failures using exponential back-off with jitter. Non-retriable
    errors (4xx) are logged and an empty dict returned immediately.
    """
    url = (
        f"{BASE_URL}?Shape={shape}"
        f"&CaratFrom={_fmt_carat(carat_from)}&CaratTo={_fmt_carat(carat_to)}"
        f"&{FIXED_PARAMS}&page={page}"
    )
    label = f"{shape} {carat_from:g}-{carat_to:g}ct page={page}"

    for attempt in range(1, RETRY_TOTAL + 1):
        with _build_session() as session:
            try:
                resp = session.get(url, timeout=REQUEST_TIMEOUT)
                resp.raise_for_status()
                return resp.json()

            except requests.exceptions.ReadTimeout:
                wait = _backoff_wait(attempt)
                log.warning(
                    f"[attempt {attempt}/{RETRY_TOTAL}] Read timeout at {label}. "
                    f"Retrying in {wait:.0f}s ..."
                )

            except requests.exceptions.ConnectionError as exc:
                wait = _backoff_wait(attempt)
                log.warning(
                    f"[attempt {attempt}/{RETRY_TOTAL}] Connection error at {label}: {exc}. "
                    f"Retrying in {wait:.0f}s ..."
                )

            except requests.exceptions.HTTPError as exc:
                status = exc.response.status_code if exc.response is not None else "?"
                if isinstance(status, int) and status < 500:
                    log.error(f"Non-retriable HTTP {status} at {label}: {exc}")
                    return {}
                wait = _backoff_wait(attempt)
                log.warning(
                    f"[attempt {attempt}/{RETRY_TOTAL}] HTTP {status} at {label}. "
                    f"Retrying in {wait:.0f}s ..."
                )

            except Exception as exc:
                log.error(f"Unexpected error at {label}: {exc}")
                return {}

        if attempt < RETRY_TOTAL:
            time.sleep(wait)

    log.error(f"All {RETRY_TOTAL} attempts exhausted at {label}. Skipping page.")
    return {}


# ── Parsing ───────────────────────────────────────────────────────────────────

def _s(value: Any) -> str:
    """Safely coerce any value to a stripped string."""
    return str(value).strip() if value is not None else ""


def _iter_new_rows(items: list[dict], seen_certs: set[str]) -> Iterator[DiamondRow]:
    """Yield DiamondRow objects for items not already in `seen_certs`."""
    for item in items:
        cert = _s(item.get("certificate_no"))
        if not cert or cert in seen_certs:
            continue

        row = DiamondRow(
            id               = _s(item.get("id")),
            shape            = _s(item.get("shape")),
            carat            = _s(item.get("carat")),
            weight           = _s(item.get("weight")),
            color            = _s(item.get("color")),
            clarity          = _s(item.get("clarity")),
            cut              = _s(item.get("cut")),
            cut_grade        = _s(item.get("cut_grade")),
            polish           = _s(item.get("polish")),
            symmetry         = _s(item.get("symmetry")),
            fluorescence     = _s(item.get("fluorescence")),
            lab              = _s(item.get("lab")),
            report           = _s(item.get("report")),
            certificate_no   = _s(item.get("certificate_no")),
            price            = _s(item.get("price")),
            mrp              = _s(item.get("mrp")),
            discount_label   = _s(item.get("discount_label")),
            actual_ppc       = _s(item.get("actual_ppc")),
            length           = _s(item.get("length")),
            width            = _s(item.get("width")),
            depth_percent    = _s(item.get("depth_percent")),
            table_percent    = _s(item.get("table_percent")),
            lw_ratio         = _s(item.get("lw_ratio")),
            growth_process   = _s(item.get("growth_process")),
            country          = _s(item.get("country")),
            shade            = _s(item.get("shade")),
            lot              = _s(item.get("lot")),
            title            = _s(item.get("title")),
            slug             = _s(item.get("slug")),
            thumb_image      = _s(item.get("thumb_image")),
            video            = _s(item.get("video")),
        )
        seen_certs.add(cert)
        yield row


# ── Per-shape worker ──────────────────────────────────────────────────────────

def _scrape_shape(
    shape: str,
    seen_certs: set[str],
    certs_lock: threading.Lock,
    state: dict,
    state_lock: threading.Lock,
    totals: dict[str, int],
) -> None:
    """
    Walk every carat band for a single shape, paginating each to completion
    before moving to the next band. Runs on its own thread — one per shape —
    so all shapes are scraped concurrently instead of one after another.
    """
    band_index, page = _get_start_state(shape, state)

    while band_index < len(CARAT_BANDS):
        carat_from, carat_to = CARAT_BANDS[band_index]
        combo_label = (
            f"[{shape} band {band_index + 1}/{len(CARAT_BANDS)}] {carat_from:g}-{carat_to:g}ct"
        )
        log.info(f"{combo_label} — starting from page {page}")

        total_pages: int | None = None
        total_avail  = 0
        band_saved   = 0
        zero_streak  = 0

        while True:
            response = _fetch_page(shape, carat_from, carat_to, page)
            payload  = response.get("data") if response else None

            if not payload:
                log.warning(f"{combo_label} — empty/failed response at page={page}. Moving to next band.")
                break

            items = payload.get("data") or []
            if total_pages is None:
                total_pages = int(payload.get("total_page") or 0) or None
                total_avail = int(payload.get("total") or 0)
                log.info(f"{combo_label} — total pages: {total_pages}  |  available: {total_avail:,}")

            if not items:
                log.info(f"{combo_label} — no items returned, band consumed.")
                break

            with certs_lock:
                new_rows = 0
                with open(CSV_FILE, "a", newline="", encoding="utf-8") as fh:
                    writer = csv.writer(fh)
                    for diamond in _iter_new_rows(items, seen_certs):
                        writer.writerow(diamond.as_row())
                        new_rows += 1
                totals["saved"] += new_rows
                session_total = totals["saved"]

            band_saved  += new_rows
            next_page    = page + 1
            _update_state(shape, band_index, next_page, state, state_lock)

            pct = (page / total_pages * 100) if total_pages else 0
            log.info(
                f"{combo_label} — page {page:>5} -> {next_page:<5} | "
                f"New: {new_rows:>4} | "
                f"Session total: {session_total:>6,} | "
                f"Progress: {pct:.1f}% of {total_pages or '?'} pages"
            )

            # The vendor's API only serves a fixed-size result window (~600 items,
            # 10 pages of 60) per query — pages beyond it repeat the same items
            # forever regardless of the reported `total`. A run of empty pages
            # means that window is exhausted, so bail out to the next carat band
            # instead of looping on duplicates.
            zero_streak = 0 if new_rows else zero_streak + 1
            if zero_streak >= ZERO_NEW_STREAK_LIMIT:
                log.warning(
                    f"{combo_label} — {zero_streak} consecutive pages with no new diamonds "
                    f"at page {page} (API result window exhausted; {total_avail:,} reported "
                    f"but only ~{band_saved:,} unique served this band). Moving to next band."
                )
                break

            if total_pages and page >= total_pages:
                log.info(f"{combo_label} — all pages consumed.")
                break

            page = next_page
            time.sleep(INTER_PAGE_SLEEP)

        band_index += 1
        page = 1
        _update_state(shape, band_index, page, state, state_lock)

    log.info(f"[{shape}] all carat bands scraped.")


# ── Main entry point ──────────────────────────────────────────────────────────

def friendly_diamonds_scrape() -> dict[str, int]:
    """
    Pure scraper. Appends to / resumes from the existing CSV + state file.

    Scrapes all shapes concurrently (one worker thread per shape, up to
    SHAPE_WORKERS at a time); each worker walks its own carat bands
    sequentially. Splitting the 1-10 carat range into bands keeps each
    query's result set small and stable — a single unfiltered 1-99 carat
    sweep drifts/duplicates thousands of pages deep.
    """
    log.info("=" * 60)
    log.info("Friendly Diamonds scrape — starting")
    log.info(
        f"{len(SHAPES)} shapes x {len(CARAT_BANDS)} carat bands "
        f"({SHAPE_WORKERS} shapes running concurrently)"
    )

    rotated = _rotate_old_file(CSV_FILE)
    if rotated:
        log.info("Old data archived — this will be a full fresh run.")

    _init_csv()

    seen_certs = _load_existing_certs()
    state      = _load_all_state()
    certs_lock = threading.Lock()
    state_lock = threading.Lock()
    totals     = {"saved": 0}

    with ThreadPoolExecutor(max_workers=SHAPE_WORKERS) as executor:
        futures = {
            executor.submit(_scrape_shape, shape, seen_certs, certs_lock, state, state_lock, totals): shape
            for shape in SHAPES
        }
        for future in as_completed(futures):
            shape = futures[future]
            try:
                future.result()
            except Exception as exc:
                log.error(f"[{shape}] worker crashed: {exc}")

    log.info(f"Scrape complete. Saved {totals['saved']:,} new records this run.")
    log.info("=" * 60)
    return {"total_saved": totals["saved"]}


if __name__ == "__main__":
    friendly_diamonds_scrape()
