import csv
import json
import logging
import os
import random
import re
import shutil
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from curl_cffi import requests as cffi_requests

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Config ───────────────────────────────────────────────────────────────────
SCRAPER_NAME   = "abelini"
BASE_URL       = "https://www.abelini.com/api/diamond-search"
ITEMS_PER_PAGE = 24                    # server-enforced, do not change
MAX_RETRIES    = 4      # was 6
CHROME_VERSION = "chrome131"

# ── Concurrency knobs (env-overridable) ──────────────────────────────────────
PAGE_WORKERS       = int(os.environ.get("ABELINI_PAGE_WORKERS",       "4"))
LARGE_PAGE_WORKERS = int(os.environ.get("ABELINI_LARGE_PAGE_WORKERS", "12"))
MAX_RPS            = float(os.environ.get("ABELINI_MAX_RPS",          "12.0"))
MAX_CONCURRENT     = int(os.environ.get("ABELINI_MAX_CONCURRENT",     "15"))
FLUSH_EVERY        = int(os.environ.get("ABELINI_FLUSH_EVERY",        "500"))
FAST_MODE          = bool(int(os.environ.get("ABELINI_FAST_MODE",     "0")))

# ── Timing knobs ─────────────────────────────────────────────────────────────
REQUEST_TIMEOUT       = 45
WORKER_DELAY_MIN      = 0.1
WORKER_DELAY_MAX      = 0.4
BACKOFF_429_BASE      = 15.0   # was 30.0 — shorter first 429 backoff
BACKOFF_429_MAX       = 90.0   # was 300.0 (5 min) — capped at 90s
LARGE_SHARD_PAGES     = 200            # if shard claims this many pages, use big pool
INTER_SHAPE_SLEEP_MIN = 5      # was 10s — sleep between shapes
INTER_SHAPE_SLEEP_MAX = 10     # was 20s
INTER_SHARD_SLEEP_MIN = 1      # was 2s
INTER_SHARD_SLEEP_MAX = 3      # was 5s
DUP_PAGE_LIMIT        = 2              # consecutive all-dup pages → bail shard
STATE_PERSIST_EVERY   = 10             # persist status.json every N shard completions
MAX_PAGES_PER_SHARD   = 600            # hard safety cap per (shape, carat-band)

# ── Shapes ───────────────────────────────────────────────────────────────────
ALL_SHAPES = ["RND", "PRN", "OVL", "EMR", "CUS", "MQS", "PER", "ASC", "RDT", "HRT"]

# ── Carat bands ──────────────────────────────────────────────────────────────
def _build_carat_bands(fast: bool):
    """Return list of (carat_min, carat_max) tuples. Fast mode = engagement range."""
    bands = []
    if fast:
        # 0.50 → 1.00 in 0.20 steps
        v = 0.50
        while v < 1.00 - 1e-9:
            bands.append((round(v, 2), round(v + 0.20, 2))); v += 0.20
        # 1.00 → 2.00 in 0.20
        v = 1.00
        while v < 2.00 - 1e-9:
            bands.append((round(v, 2), round(v + 0.20, 2))); v += 0.20
        # 2.00 → 3.00 in 0.25
        v = 2.00
        while v < 3.00 - 1e-9:
            bands.append((round(v, 2), round(v + 0.25, 2))); v += 0.25
    else:
        # Full range matching your old 0.20-10.00 coverage
        v = 0.20
        while v < 1.00 - 1e-9:
            bands.append((round(v, 2), round(v + 0.20, 2))); v += 0.20
        v = 1.00
        while v < 3.00 - 1e-9:
            bands.append((round(v, 2), round(v + 0.20, 2))); v += 0.20
        v = 3.00
        while v < 5.00 - 1e-9:
            bands.append((round(v, 2), round(v + 0.50, 2))); v += 0.50
        bands.append((5.00, 10.00))
    return bands

CARAT_BANDS = _build_carat_bands(FAST_MODE)

# ── Paths ─────────────────────────────────────────────────────────────────────
BASE_DIR      = str(Path(__file__).resolve().parent)
diamond_files = str(Path(__file__).resolve().parent.parent / "diamond_files" / "uk")

os.makedirs(diamond_files, exist_ok=True)

MAIN_CSV    = os.path.join(diamond_files, "abelini.csv")
STATUS_FILE = os.path.join(diamond_files, "abelini_status.json")

# ── VPN ──────────────────────────────────────────────────────────────────────
VPN_ROTATE_AFTER_FAILS = 5
VPN_CONFIG_SUBDIR      = "abelini"

class VPNRotateNeeded(Exception):
    """Raised when a 403/Cloudflare block trips the hard-stop — exit IP is burned."""
    def __init__(self, reason: str = "403 / Cloudflare block"):
        self.reason = reason
        super().__init__(f"VPN rotate signal: {reason}")

# ── User-Agents ──────────────────────────────────────────────────────────────
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.6 Safari/605.1.15",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_2) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:131.0) Gecko/20100101 Firefox/131.0",
    "Mozilla/5.0 (X11; Linux x86_64; rv:130.0) Gecko/20100101 Firefox/130.0",
]
def pick_ua() -> str: return random.choice(USER_AGENTS)

def make_headers() -> dict:
    return {
        "accept":             "*/*",
        "accept-language":    "en-GB,en-US;q=0.9,en;q=0.8",
        "content-type":       "text/plain;charset=UTF-8",
        "origin":             "https://www.abelini.com",
        "referer":            "https://www.abelini.com/choose-diamond",
        "user-agent":         pick_ua(),
        "sec-ch-ua":          '"Google Chrome";v="131", "Chromium";v="131", "Not-A.Brand";v="99"',
        "sec-ch-ua-mobile":   "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-site":     "same-origin",
        "sec-fetch-mode":     "cors",
        "sec-fetch-dest":     "empty",
    }

# ── Base params (carat range is now per-shard, supplied at call time) ────────
BASE_PARAMS = {
    "stone_type":      "LBG",
    "product_id":      "1240",
    "order":           "asc",
    "search_code":     "",
    "category_id":     "1",
    "limit":           str(ITEMS_PER_PAGE),
    "stone_price_min": "100",
    "stone_price_max": "500000",
    "markup":          "25",
    "discount":        "22",
}

FIELDS = [
    "stone_price_id", "rapnet_id", "stone_type", "stone_name",
    "shape", "shape_icon", "weight", "weight_display",
    "color", "clarity", "lab", "cut", "polish", "intensity",
    "symmetry", "fluorescence", "meas",
    "cert_no", "cert_no_clean", "depth", "table",
    "cert_url", "image_url", "video_url", "carat_price", "classname",
    "status", "page_no",
]

def strip_lg(value: str) -> str:
    if value and re.match(r'^[Ll][Gg]', value):
        return value[2:]
    return value or ""

# ══════════════════════════════════════════════════════════════════════════════
# RATE LIMITER + SEMAPHORE
# ══════════════════════════════════════════════════════════════════════════════
class TokenBucket:
    def __init__(self, rate: float):
        self._rate     = rate
        self._capacity = max(1.0, rate * 0.5)
        self._tokens   = self._capacity
        self._last     = time.monotonic()
        self._lock     = threading.Lock()

    def acquire(self) -> None:
        while True:
            with self._lock:
                now = time.monotonic()
                self._tokens = min(self._capacity, self._tokens + (now - self._last) * self._rate)
                self._last = now
                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return
                wait = (1.0 - self._tokens) / self._rate
            time.sleep(wait)

_bucket         = TokenBucket(MAX_RPS)
_hard_stop      = threading.Event()
_http_semaphore = threading.Semaphore(MAX_CONCURRENT)

_global_backoff_until = 0.0
_backoff_lock         = threading.Lock()

def _trigger_global_backoff(seconds: float) -> None:
    global _global_backoff_until
    with _backoff_lock:
        target = time.monotonic() + seconds
        if target > _global_backoff_until:
            _global_backoff_until = target
            log.warning("⚠ Global back-off %.0fs — all workers pausing", seconds)

def _wait_global_backoff() -> None:
    remaining = _global_backoff_until - time.monotonic()
    if remaining > 0:
        time.sleep(remaining)

# ══════════════════════════════════════════════════════════════════════════════
# STATUS + CSV
# ══════════════════════════════════════════════════════════════════════════════
_status_lock   = threading.Lock()
_csv_lock      = threading.Lock()
_flush_lock    = threading.Lock()
_flush_counter = 0
_state_persist_counter = 0

def _shard_key(shape: str, c_min: float, c_max: float) -> str:
    """Stable string key for status.json (JSON can't have tuple keys)."""
    return f"{shape}|{c_min:.2f}|{c_max:.2f}"

def load_status() -> dict:
    """
    status.json schema (new):
      {
        "done_shapes":     [shape, ...],         # fully complete shapes
        "done_shards":     [shard_key, ...],     # fully complete (shape,c_min,c_max)
        "shard_progress":  {shard_key: last_completed_page, ...},
        "updated_at":      iso8601
      }
    Old-format files (done_shapes + shape_pages) are upgraded silently:
    shape_pages is dropped, done_shapes is honored so completed shapes don't redo.
    """
    if not os.path.exists(STATUS_FILE):
        return {"done_shapes": [], "done_shards": [], "shard_progress": {}}
    try:
        with open(STATUS_FILE) as f:
            data = json.load(f)
    except Exception:
        return {"done_shapes": [], "done_shards": [], "shard_progress": {}}
    return {
        "done_shapes":    list(data.get("done_shapes", [])),
        "done_shards":    list(data.get("done_shards", [])),
        "shard_progress": dict(data.get("shard_progress", {})),
    }

def save_status(done_shapes, done_shards, shard_progress, force=False):
    global _state_persist_counter
    _state_persist_counter += 1
    if not force and _state_persist_counter < STATE_PERSIST_EVERY:
        return
    _state_persist_counter = 0
    with _status_lock:
        with open(STATUS_FILE, "w") as f:
            json.dump({
                "scraper":        SCRAPER_NAME,
                "done_shapes":    done_shapes,
                "done_shards":    done_shards,
                "shard_progress": shard_progress,
                "updated_at":     time.strftime("%Y-%m-%dT%H:%M:%S"),
            }, f, indent=2)

def migrate_schema_if_needed() -> None:
    """
    The API moved from HTML-scraped fields (certno, caratweight, price, certurl...)
    to the JSON /api/diamond-search fields (cert_no, weight, carat_price...).
    If a CSV from the old schema is on disk, appending new-schema rows to it would
    corrupt the file. Archive the stale CSV + status.json instead, so a fresh,
    full re-scrape populates the new schema cleanly.
    """
    if not os.path.exists(MAIN_CSV):
        return
    with open(MAIN_CSV, encoding="utf-8") as f:
        header = f.readline().strip()
    if header == ",".join(FIELDS):
        return
    stamp         = time.strftime("%Y%m%d_%H%M%S")
    archived_csv  = MAIN_CSV.replace(".csv", f"_old_schema_{stamp}.csv")
    shutil.move(MAIN_CSV, archived_csv)
    log.warning("CSV schema changed — archived old file to %s", archived_csv)
    if os.path.exists(STATUS_FILE):
        archived_status = STATUS_FILE.replace(".json", f"_old_schema_{stamp}.json")
        shutil.move(STATUS_FILE, archived_status)
        log.warning("Archived stale status.json to %s — full re-scrape will run", archived_status)

def load_existing_ids() -> set:
    """Set of cert_no_clean values already in MAIN_CSV. Used for cross-run dedup."""
    ids: set = set()
    if os.path.exists(MAIN_CSV):
        with open(MAIN_CSV, encoding="utf-8") as f:
            for row in csv.DictReader(f):
                raw = row.get("cert_no_clean") or row.get("cert_no") or ""
                v   = strip_lg(str(raw).strip()) if raw else ""
                if v:
                    ids.add(v)
    log.info("Loaded %d existing unique cert_no IDs from previous run", len(ids))
    return ids

def _maybe_flush(new_records: int, force: bool = False) -> None:
    global _flush_counter
    with _flush_lock:
        _flush_counter += new_records
        if force or _flush_counter >= FLUSH_EVERY:
            _flush_counter = 0

# ── Shared dedupe set ────────────────────────────────────────────────────────
_existing_ids: set            = set()
_ids_lock:     threading.Lock = threading.Lock()

def _is_dup_and_register(key: str) -> bool:
    with _ids_lock:
        if key in _existing_ids:
            return True
        _existing_ids.add(key)
        return False

# ══════════════════════════════════════════════════════════════════════════════
# PARSING (JSON stones array from /api/diamond-search)
# ══════════════════════════════════════════════════════════════════════════════
def parse_diamonds(stones: list) -> list:
    diamonds = []
    for s in stones:
        raw_cert_no = str(s.get("cert_no") or "")
        diamonds.append({
            "stone_price_id": s.get("stone_price_id", ""),
            "rapnet_id":      s.get("rapnet_id", ""),
            "stone_type":     s.get("stone_type", ""),
            "stone_name":     s.get("stone_name", ""),
            "shape":          s.get("shape", ""),
            "shape_icon":     s.get("shape_icon", ""),
            "weight":         s.get("weight", ""),
            "weight_display": s.get("weight_display", ""),
            "color":          s.get("color", ""),
            "clarity":        s.get("clarity", ""),
            "lab":            s.get("lab", ""),
            "cut":            s.get("cut", ""),
            "polish":         s.get("polish", ""),
            "intensity":      s.get("intensity", ""),
            "symmetry":       s.get("symmetry", ""),
            "fluorescence":   s.get("fluorescence", ""),
            "meas":           s.get("meas", ""),
            "cert_no":        raw_cert_no,
            "cert_no_clean":  strip_lg(raw_cert_no.strip()),
            "depth":          s.get("depth", ""),
            "table":          s.get("table", ""),
            "cert_url":       s.get("cert_url", ""),
            "image_url":      s.get("image_url", ""),
            "video_url":      s.get("video_url", ""),
            "carat_price":    s.get("carat_price", ""),
            "classname":      s.get("classname", ""),
        })
    return diamonds

# ══════════════════════════════════════════════════════════════════════════════
# SESSIONS (thread-local)
# ══════════════════════════════════════════════════════════════════════════════
_tl = threading.local()

def _get_session() -> cffi_requests.Session:
    if not hasattr(_tl, "session"):
        _tl.session = cffi_requests.Session(impersonate=CHROME_VERSION)
        try:
            _tl.session.get("https://www.abelini.com/choose-diamond", headers=make_headers(), timeout=20)
            time.sleep(random.uniform(0.5, 1.2))
        except Exception as exc:
            log.debug("Warm-up failed (non-fatal): %s", exc)
    return _tl.session

def _recycle_session() -> cffi_requests.Session:
    if hasattr(_tl, "session"):
        try: _tl.session.close()
        except Exception: pass
        del _tl.session
    return _get_session()

# ══════════════════════════════════════════════════════════════════════════════
# FETCH ONE PAGE (semaphore-gated, identical safety model to your old code)
# ══════════════════════════════════════════════════════════════════════════════
def fetch_page(shape: str, c_min: float, c_max: float, page: int) -> tuple:
    """Returns (page, diamonds_list, stone_total)."""
    if _hard_stop.is_set():
        return page, [], 0

    payload = {
        **BASE_PARAMS,
        "stone_shape":     shape,
        "stone_carat_min": f"{c_min:.2f}",
        "stone_carat_max": f"{c_max:.2f}",
        "page":            str(page),
    }
    body            = json.dumps(payload)
    consecutive_429 = 0

    for attempt in range(1, MAX_RETRIES + 1):
        if _hard_stop.is_set():
            return page, [], 0

        _wait_global_backoff()
        time.sleep(random.uniform(WORKER_DELAY_MIN, WORKER_DELAY_MAX))

        _bucket.acquire()
        _http_semaphore.acquire()
        try:
            session = _get_session()
            resp    = session.post(BASE_URL, headers=make_headers(), data=body, timeout=REQUEST_TIMEOUT)

            if resp.status_code == 429:
                consecutive_429 += 1
                backoff = min(BACKOFF_429_BASE * (2 ** (consecutive_429 - 1)), BACKOFF_429_MAX)
                backoff += random.uniform(0, backoff * 0.3)
                log.warning("[%s %.2f-%.2f] p%d 429 (#%d) → back-off %.0fs",
                            shape, c_min, c_max, page, consecutive_429, backoff)
                _trigger_global_backoff(backoff)
                if consecutive_429 >= 2:
                    _recycle_session()
                    consecutive_429 = 0
                continue

            if resp.status_code == 403:
                log.error("[%s %.2f-%.2f] p%d 403 — Cloudflare block. Hard stop.",
                          shape, c_min, c_max, page)
                _hard_stop.set()
                return page, [], 0

            resp.raise_for_status()
            consecutive_429 = 0

            payload_resp = resp.json()
            inner        = payload_resp.get("data", {}) or {}
            stones       = inner.get("stones") or []
            if not stones:
                return page, [], 0

            diamonds = parse_diamonds(stones)
            total    = int(inner.get("stone_total") or 0)
            return page, diamonds, total

        except (ValueError, KeyError) as exc:
            log.error("[%s %.2f-%.2f] p%d parse error: %s",
                      shape, c_min, c_max, page, exc)
            return page, [], 0

        except Exception as exc:
            is_timeout = "28" in str(exc) or "timed out" in str(exc).lower()
            wait = random.uniform(15.0, 30.0) if is_timeout else min(2 ** attempt, 15) + random.uniform(0, 2)
            log.warning("[%s %.2f-%.2f] p%d attempt %d/%d → %s  retry in %.0fs",
                        shape, c_min, c_max, page, attempt, MAX_RETRIES,
                        "TIMEOUT (curl:28)" if is_timeout else exc, wait)
            time.sleep(wait)
            _recycle_session()

        finally:
            _http_semaphore.release()

    log.error("[%s %.2f-%.2f] p%d gave up after %d attempts",
              shape, c_min, c_max, page, MAX_RETRIES)
    return page, [], 0

# ══════════════════════════════════════════════════════════════════════════════
# SCRAPE ONE SHARD  (NEW: shape × carat-band)
# ══════════════════════════════════════════════════════════════════════════════
def scrape_shard(shape: str, c_min: float, c_max: float,
                 start_page: int,
                 writer, f_handle,
                 status_state: dict) -> tuple:
    """
    Scrape one (shape, carat_min, carat_max) shard.
    Returns (new_count, dup_count, last_page_scraped).
    """
    sk = _shard_key(shape, c_min, c_max)

    # probe page 1 (or resume page) to learn stone_total
    _, probe, stone_total = fetch_page(shape, c_min, c_max, start_page)
    if not probe and start_page == 1:
        log.info("[%s %.2f-%.2f] ✖ No data — empty shard", shape, c_min, c_max)
        return 0, 0, 0

    # cap implied page count — stone_total is inflated, real shards terminate earlier
    implied_pages = max(1, -(-stone_total // ITEMS_PER_PAGE)) if stone_total else 1
    implied_pages = min(implied_pages, MAX_PAGES_PER_SHARD)
    workers       = LARGE_PAGE_WORKERS if implied_pages >= LARGE_SHARD_PAGES else PAGE_WORKERS

    log.info("[%s %.2f-%.2f] claimed_total=%d  pages_cap=%d  workers=%d  start_page=%d",
             shape, c_min, c_max, stone_total, implied_pages, workers, start_page)

    new_count = dup_count = 0
    dup_streak = 0

    def _flush_rows(diamonds, page):
        nonlocal new_count, dup_count
        rows = []
        batch_new = 0
        page_dups = 0
        for d in diamonds:
            raw_certno = str(d.get("cert_no_clean") or d.get("cert_no") or "")
            key        = strip_lg(raw_certno.strip())
            is_dup     = _is_dup_and_register(key) if key else True
            row    = {f: d.get(f, "") for f in FIELDS[:-2]}
            row["status"]  = "duplicate" if is_dup else "new"
            row["page_no"] = page
            rows.append(row)
            if is_dup:
                dup_count += 1; page_dups += 1
            else:
                new_count += 1; batch_new += 1
        with _csv_lock:
            writer.writerows(rows)
            f_handle.flush()
        _maybe_flush(batch_new)
        # was this page all-dup?
        return batch_new == 0 and page_dups > 0

    page_was_all_dup = _flush_rows(probe, start_page)
    if page_was_all_dup:
        dup_streak = 1

    status_state["shard_progress"][sk] = start_page
    save_status(status_state["done_shapes"], status_state["done_shards"],
                status_state["shard_progress"])

    # batched parallel page fetches for the rest of the shard
    remaining  = list(range(start_page + 1, implied_pages + 1))
    batch_size = workers * 4
    last_page  = start_page
    t0         = time.monotonic()

    with ThreadPoolExecutor(max_workers=workers,
                            thread_name_prefix=f"pg-{shape}-{c_min:.1f}") as pool:
        i = 0
        while i < len(remaining) and not _hard_stop.is_set():
            if dup_streak >= DUP_PAGE_LIMIT:
                log.info("[%s %.2f-%.2f] dup-streak %d pages → bail shard at p%d",
                         shape, c_min, c_max, dup_streak, last_page)
                break

            batch   = remaining[i: i + batch_size]
            i      += batch_size
            futures = {pool.submit(fetch_page, shape, c_min, c_max, p): p for p in batch}
            results = {}
            empty_pages = 0

            for fut in as_completed(futures):
                pg, diamonds, _ = fut.result()
                results[pg] = diamonds
                if not diamonds:
                    empty_pages += 1

            for pg in sorted(results):
                if results[pg]:
                    all_dup = _flush_rows(results[pg], pg)
                    dup_streak = dup_streak + 1 if all_dup else 0
                else:
                    # empty page = end of pagination for this shard
                    dup_streak = DUP_PAGE_LIMIT  # force bail
                last_page = max(last_page, pg)
                if dup_streak >= DUP_PAGE_LIMIT:
                    break

            status_state["shard_progress"][sk] = last_page
            save_status(status_state["done_shapes"], status_state["done_shards"],
                        status_state["shard_progress"])

            elapsed = time.monotonic() - t0
            pps     = (last_page - start_page) / elapsed if elapsed > 0 else 0
            log.info("[%s %.2f-%.2f] p%d/%d  %.1f p/s  new=%d dup=%d  dup_streak=%d",
                     shape, c_min, c_max, last_page, implied_pages, pps,
                     new_count, dup_count, dup_streak)

            # bail if every page in the batch was empty
            if empty_pages == len(batch):
                break

    return new_count, dup_count, last_page

# ══════════════════════════════════════════════════════════════════════════════
# SCRAPE ONE SHAPE  (iterates all carat bands for that shape)
# ══════════════════════════════════════════════════════════════════════════════
def scrape_shape(shape: str, writer, f_handle, status_state: dict) -> tuple:
    """Returns (total_new, total_dup) for this shape."""
    log.info("=" * 60)
    log.info("[%s] ▶ Starting shape — %d carat bands", shape, len(CARAT_BANDS))
    log.info("=" * 60)

    shape_new = shape_dup = 0

    for band_idx, (c_min, c_max) in enumerate(CARAT_BANDS, 1):
        if _hard_stop.is_set():
            break

        sk = _shard_key(shape, c_min, c_max)
        if sk in status_state["done_shards"]:
            log.info("[%s %.2f-%.2f] ✓ already done (resume)", shape, c_min, c_max)
            continue

        start_page = status_state["shard_progress"].get(sk, 0) + 1
        if start_page > 1:
            log.info("[%s %.2f-%.2f] resuming from page %d",
                     shape, c_min, c_max, start_page)

        try:
            new_n, dup_n, _ = scrape_shard(
                shape, c_min, c_max,
                start_page=start_page,
                writer=writer, f_handle=f_handle,
                status_state=status_state,
            )
            shape_new += new_n
            shape_dup += dup_n

            if not _hard_stop.is_set():
                with _status_lock:
                    if sk not in status_state["done_shards"]:
                        status_state["done_shards"].append(sk)
                save_status(status_state["done_shapes"],
                            status_state["done_shards"],
                            status_state["shard_progress"])
                log.info("[%s %.2f-%.2f] ✅ shard done  band %d/%d  new=%d dup=%d",
                         shape, c_min, c_max, band_idx, len(CARAT_BANDS), new_n, dup_n)
        except Exception as exc:
            log.error("[%s %.2f-%.2f] ❌ shard crashed: %s", shape, c_min, c_max, exc)

        # short rest between shards within the same shape
        if band_idx < len(CARAT_BANDS) and not _hard_stop.is_set():
            time.sleep(random.uniform(INTER_SHARD_SLEEP_MIN, INTER_SHARD_SLEEP_MAX))

    return shape_new, shape_dup

# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════
def fetch_abelini(rotate_after_fails: int | None = None) -> None:
    global _existing_ids

    _hard_stop.clear()
    migrate_schema_if_needed()

    raw_status     = load_status()
    done_shapes    = list(raw_status["done_shapes"])
    done_shards    = list(raw_status["done_shards"])
    shard_progress = dict(raw_status["shard_progress"])
    _existing_ids  = load_existing_ids()

    status_state = {
        "done_shapes":    done_shapes,
        "done_shards":    done_shards,
        "shard_progress": shard_progress,
    }

    shapes_to_run = [s for s in ALL_SHAPES if s not in done_shapes]
    if not shapes_to_run:
        log.info("All shapes already complete. Delete status.json to re-run.")
        return

    log.info("=" * 60)
    log.info("Abelini scraper  |  fast_mode=%s", FAST_MODE)
    log.info("Shapes to run    : %d / %d", len(shapes_to_run), len(ALL_SHAPES))
    log.info("Carat bands/shape: %d  (shards total = %d)",
             len(CARAT_BANDS), len(shapes_to_run) * len(CARAT_BANDS))
    log.info("Done shards      : %d", len(done_shards))
    log.info("Workers          : %d small / %d large", PAGE_WORKERS, LARGE_PAGE_WORKERS)
    log.info("MAX_RPS          : %.1f", MAX_RPS)
    log.info("MAX_CONCURRENT   : %d", MAX_CONCURRENT)
    log.info("Request delay    : %.1f–%.1fs", WORKER_DELAY_MIN, WORKER_DELAY_MAX)
    log.info("Rest between shapes : %d–%ds", INTER_SHAPE_SLEEP_MIN, INTER_SHAPE_SLEEP_MAX)
    log.info("Chrome version   : %s", CHROME_VERSION)
    log.info("=" * 60)

    file_exists = os.path.exists(MAIN_CSV)
    grand_new = grand_dup = 0
    run_start = time.monotonic()

    with open(MAIN_CSV, "a" if file_exists else "w",
              newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)
        if not file_exists:
            writer.writeheader()

        for idx, shape in enumerate(shapes_to_run):
            if _hard_stop.is_set():
                break

            try:
                new_n, dup_n = scrape_shape(shape, writer, f, status_state)
                grand_new += new_n
                grand_dup += dup_n

                if not _hard_stop.is_set():
                    # shape is "done" when all its carat bands are in done_shards
                    expected_shards = {
                        _shard_key(shape, c_min, c_max) for c_min, c_max in CARAT_BANDS
                    }
                    if expected_shards.issubset(set(status_state["done_shards"])):
                        with _status_lock:
                            if shape not in status_state["done_shapes"]:
                                status_state["done_shapes"].append(shape)
                        save_status(status_state["done_shapes"],
                                    status_state["done_shards"],
                                    status_state["shard_progress"],
                                    force=True)
                        log.info("[%s] ✅ SHAPE COMPLETE — new=%d dup=%d",
                                 shape, new_n, dup_n)
            except Exception as exc:
                log.error("[%s] ❌ Shape crashed: %s", shape, exc)

            if idx < len(shapes_to_run) - 1 and not _hard_stop.is_set():
                rest = random.uniform(INTER_SHAPE_SLEEP_MIN, INTER_SHAPE_SLEEP_MAX)
                log.info("Resting %.0fs before next shape...", rest)
                _recycle_session()
                time.sleep(rest)

    save_status(status_state["done_shapes"], status_state["done_shards"],
                status_state["shard_progress"], force=True)
    _maybe_flush(0, force=True)

    elapsed_min = (time.monotonic() - run_start) / 60
    log.info("=" * 60)
    log.info("ALL DONE in %.1f min", elapsed_min)
    log.info("   New    : %d", grand_new)
    log.info("   Dup    : %d", grand_dup)
    log.info("   Total  : %d", len(_existing_ids))
    log.info("=" * 60)

    if _hard_stop.is_set():
        log.error("Ended early due to 403 block.")
        if rotate_after_fails:
            raise VPNRotateNeeded("403 / Cloudflare block")


# ══════════════════════════════════════════════════════════════════════════════
# VPN-WRAPPED RUN  (your existing logic, kept verbatim)
# ══════════════════════════════════════════════════════════════════════════════
def run_with_vpn_rotation():
    """
    Run the scraper inside an OpenVPN tunnel, trying EVERY config in
    vpnbook/uk/<VPN_CONFIG_SUBDIR>/ until one completes the scrape.

    Per-config outcome:
      - success         → scrape done, return.
      - VPNRotateNeeded → a 403/Cloudflare block burned the exit IP; keep the
                          config and rotate to the next one.
      - VPNError (dead) → the config failed to connect; DELETE the .ovpn file
                          and move on.

    Safety: a missing OpenVPN binary or an auth failure is an *environmental*
    problem (not a dead config), so those abort the run WITHOUT deleting
    anything. The scraper resumes from status.json, so progress is preserved
    across each new exit IP. Requires Administrator / root.
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
    log.info("Abelini scraper with VPN rotation")
    log.info("Rotate trigger: 403 / Cloudflare block")
    log.info("=" * 60)

    if not is_admin():
        log.error(
            "OpenVPN requires Administrator/root. "
            "Re-run from an elevated shell, or call fetch_abelini() directly."
        )
        return

    try:
        find_openvpn_executable()
    except VPNError as exc:
        log.error("OpenVPN unavailable — aborting without touching configs: %s", exc)
        return

    repo_root = Path(__file__).resolve().parent.parent
    vpn_dir = repo_root / "vpnbook" / "uk" / VPN_CONFIG_SUBDIR
    if not vpn_dir.exists():
        log.error("VPN config dir not found: %s", vpn_dir)
        return

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
                fetch_abelini(rotate_after_fails=VPN_ROTATE_AFTER_FAILS)
            log.info("Scrape completed — VPN torn down.")
            return

        except VPNRotateNeeded as e:
            log.warning("Rotating VPN (config works, IP burned): %s", e)

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


if __name__ == "__main__":
    # run_with_vpn_rotation()  → VPN-wrapped scrape (needs admin)
    fetch_abelini() #          → pure scrape, no VPN
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