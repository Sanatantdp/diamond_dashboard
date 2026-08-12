
from __future__ import annotations

# ── Bandwidth accounting ─────────────────────────────────────────────────────
# Counts every byte this scraper downloads, per day, into bandwidth/ and then
# the dashboards. Underscore aliases so nothing here shadows the module's own
# imports further down. See bandwidth.py.
import sys as _sys
from pathlib import Path as _Path
_sys.path.insert(0, str(_Path(__file__).resolve().parent.parent))
import bandwidth as _bandwidth
_bandwidth.track('loose-grown', 'usa')

import csv
import datetime
import os
import random
import re
import time
from dataclasses import astuple, dataclass, fields
from pathlib import Path
from typing import Any, Iterator

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from logger import get_logger

# ── fake-useragent (lazy-init after logger is ready) ──────────────────────────
# Imported here so the warning uses `log`. The pool is module-level so it is
# created once and reused across every _random_ua() call.
try:
    from fake_useragent import UserAgent as _FakeUA
    _ua_pool = _FakeUA(browsers=["chrome", "firefox", "edge"], os=["windows", "macos"])
except Exception:          # network unavailable, cache missing, etc.
    _ua_pool = None

# ── Logging ───────────────────────────────────────────────────────────────────

log = get_logger("loosegrown")

# ── Constants ─────────────────────────────────────────────────────────────────

URL       = "https://www.loosegrowndiamond.com/wp-json/ls/v1/inventory-filter"
PAGE_SIZE = 5_000

# Back-off knobs (all in seconds)
RETRY_TOTAL       = 3       # was 6 — fewer retries means less stalling
BACKOFF_BASE      = 8       # was 15s — shorter first wait
BACKOFF_CAP       = 60      # was 180s (3 min) — max backoff capped at 60s
JITTER_RANGE      = 3       # ± random seconds added to each wait
INTER_PAGE_SLEEP  = 0.5     # was 1.5s — reduced inter-page pause

REQUEST_TIMEOUT   = (12, 45)    # was (12, 120) — 2 min read timeout caused long stalls

# File paths
BASE_DIR    = str(Path(__file__).resolve().parent.parent)
DOWN_FILES  = os.path.join(BASE_DIR, "diamond_files", "usa")
CSV_FILE    = os.path.join(DOWN_FILES, "loosegrowndiamond.csv")
STATE_FILE  = os.path.join(DOWN_FILES, "lgd_state.txt")

os.makedirs(DOWN_FILES, exist_ok=True)

# ── VPN rotation config ───────────────────────────────────────────────────────
VPN_ROTATE_AFTER_FAILS = 5        # consecutive failed pages -> rotate VPN
VPN_CONFIG_SUBDIR      = "loosegrown"   # vpnbook/loosegrown/*.ovpn
RETRY_SLEEP_BEFORE_ROTATE = 2     # pause between same-offset retries in VPN mode

# ── Backup config ────────────────────────────────────────────────────────────
STALE_THRESHOLD_DAYS = 2    # CSV older than this -> archive + full re-scrape


class VPNRotateNeeded(Exception):
    """Raised by the scraper when consecutive failures indicate the IP is burned."""
    def __init__(self, offset: int, consecutive_fails: int) -> None:
        self.offset = offset
        self.consecutive_fails = consecutive_fails
        super().__init__(
            f"VPN rotate signal: {consecutive_fails} consecutive failed fetch(es) "
            f"at offset {offset}"
        )

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

# Static headers shared by every request.
# user-agent is intentionally absent here — it is injected per-session
# by _random_ua() so every page fetch looks like a different browser.
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

# Hardcoded fallback pool — used only when fake-useragent is unavailable.
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
    """
    Return a random User-Agent string.

    Priority:
      1. fake-useragent library  (real-world, frequently updated)
      2. Hardcoded fallback pool (always available, updated manually)
    """
    if _ua_pool is not None:
        try:
            return _ua_pool.random
        except Exception as exc:
            log.debug(f"fake-useragent .random failed ({exc}) — using fallback pool.")
    return random.choice(_FALLBACK_UA_POOL)

# ── Data model ────────────────────────────────────────────────────────────────

@dataclass(slots=True)
class DiamondRow:
    shape:            str
    carat:            str
    cut:              str
    color:            str
    clarity:          str
    sale_price:       str
    price:            str
    org_price:        str
    id:               str
    pid:              str
    sku:              str
    certificate:      str
    certificate_no:   str
    certificate_type: str
    video:            str
    lot_number:       str
    mm:               str
    dtype:            str
    av_table:         str
    depth:            str
    lwratio:          str
    ha:               str
    sale:             str
    express_shipping: str
    hearts_and_arrows: str
    is_overnight:     str
    note:             str
    created_at:       str
    updated_at:       str

    @classmethod
    def csv_headers(cls) -> list[str]:
        return [f.name for f in fields(cls)]

    def as_row(self) -> tuple:
        return astuple(self)


# ── HTTP session factory ──────────────────────────────────────────────────────

def _build_session() -> requests.Session:
    """
    Build a fresh requests.Session for a single page fetch.

    A new session is created per request so that:
    - The User-Agent rotates on every call (anti-fingerprinting).
    - TCP connections are not reused across pages (avoids sticky routing
      to the same CDN edge node that might be rate-limiting us).
    - Cookie jars never accumulate server-set tracking cookies between pages.

    Connection pooling within a single fetch still benefits from the
    HTTPAdapter (keep-alive for retry attempts on the *same* page).
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
            read=0,           
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

    wait = min(cap, base * 2^(attempt-1))  ±  jitter

    Attempt  | Base wait  | With cap=180
    ---------|------------|-------------
        1    |   15 s     |   15 s
        2    |   30 s     |   30 s
        3    |   60 s     |   60 s
        4    |  120 s     |  120 s
        5    |  240 s     |  180 s  (capped)
        6    |  480 s     |  180 s  (capped)
    """
    base = min(BACKOFF_CAP, BACKOFF_BASE * (2 ** (attempt - 1)))
    jitter = random.uniform(-JITTER_RANGE, JITTER_RANGE)
    return max(1.0, base + jitter)


# ── Certificate helpers ───────────────────────────────────────────────────────

_CERT_PATTERNS = [
    re.compile(r"[?&]reportno=(\d+)"),                      # GIA
    re.compile(r"[?&]r=(?:LG)?(\d+)", re.IGNORECASE),       # IGI
    re.compile(r"[?&]certificate_id=(\d+)"),                 # GCAL
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

def _rotate_old_file(filepath: str, max_age_seconds: float = STALE_THRESHOLD_DAYS * 86_400) -> bool:
    """
    Archive `filepath` when it is older than `max_age_seconds`.
    Resets the state file so the next run starts from offset 1.

    Returns True  → file was rotated (fresh run needed).
    Returns False → file is current or did not exist.
    """
    if not os.path.exists(filepath):
        return False

    mtime    = os.path.getmtime(filepath)
    mod_dt   = datetime.datetime.fromtimestamp(mtime)
    age      = datetime.datetime.now() - mod_dt

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
        "ls_per_page": PAGE_SIZE,
        "shape":       [],
    }


def _fetch_page(start: int) -> dict[str, Any]:
    """
    POST to the inventory endpoint with a **fresh session per call**.

    Each call gets a new random User-Agent and a clean TCP connection so the
    server cannot fingerprint us across pages.  Retries up to RETRY_TOTAL
    times on transient failures using exponential back-off with jitter.
    Non-retriable errors (4xx) are logged and an empty dict returned immediately.
    """
    for attempt in range(1, RETRY_TOTAL + 1):
        # Fresh session = new UA + new connection + clean cookie jar
        with _build_session() as session:
            try:
                resp = session.post(
                    URL,
                    json=_build_payload(start),
                    timeout=REQUEST_TIMEOUT,
                )
                resp.raise_for_status()
                return resp.json()

            # ── Retriable ──────────────────────────────────────────────────
            except requests.exceptions.ReadTimeout:
                wait = _backoff_wait(attempt)
                log.warning(
                    f"[attempt {attempt}/{RETRY_TOTAL}] Read timeout at offset={start}. "
                    f"Retrying in {wait:.0f}s …"
                )

            except requests.exceptions.ConnectionError as exc:
                wait = _backoff_wait(attempt)
                log.warning(
                    f"[attempt {attempt}/{RETRY_TOTAL}] Connection error at offset={start}: {exc}. "
                    f"Retrying in {wait:.0f}s …"
                )

            except requests.exceptions.HTTPError as exc:
                status = exc.response.status_code if exc.response is not None else "?"
                if isinstance(status, int) and status < 500:
                    log.error(f"Non-retriable HTTP {status} at offset={start}: {exc}")
                    return {}
                wait = _backoff_wait(attempt)
                log.warning(
                    f"[attempt {attempt}/{RETRY_TOTAL}] HTTP {status} at offset={start}. "
                    f"Retrying in {wait:.0f}s …"
                )

            except Exception as exc:
                log.error(f"Unexpected error at offset={start}: {exc}")
                return {}

        if attempt < RETRY_TOTAL:
            time.sleep(wait)

    log.error(f"All {RETRY_TOTAL} attempts exhausted at offset={start}. Skipping page.")
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
            shape            = _s(item.get("shape")).lower(),
            carat            = _s(item.get("carat")),
            cut              = _s(item.get("cut")),
            color            = _s(item.get("color")),
            clarity          = _s(item.get("clarity")),
            sale_price       = _s(item.get("sale_price")),
            price            = _s(item.get("price")),
            org_price        = _s(item.get("org_price")),
            id               = _s(item.get("id")),
            pid              = _s(item.get("pid")),
            sku              = sku,
            certificate      = cert_url,
            certificate_no   = cert_no,
            certificate_type = cert_type,
            video            = _s(item.get("video")),
            lot_number       = _s(item.get("lot_number")),
            mm               = _s(item.get("mm")),
            dtype            = _s(item.get("dtype")),
            av_table         = _s(item.get("av_table")),
            depth            = _s(item.get("depth")),
            lwratio          = _s(item.get("lwratio")),
            ha               = _s(item.get("ha")),
            sale             = _s(item.get("sale")),
            express_shipping = _s(item.get("express_shipping")),
            hearts_and_arrows= _s(item.get("hearts_and_arrows")),
            is_overnight     = _s(item.get("is_overnight")),
            note             = _s(item.get("note")),
            created_at       = _s(item.get("created_at")),
            updated_at       = _s(item.get("updated_at")),
        )
        seen_skus.add(sku)
        yield row


# ── Main entry point ──────────────────────────────────────────────────────────

def loose_grown_diamonds_scrappe(rotate_after_fails: int | None = None) -> dict[str, int]:
    """
    Pure scraper. Appends to / resumes from the existing CSV + state file.

    Args:
        rotate_after_fails: If set, raise VPNRotateNeeded after N consecutive
            failed fetches. Caller should catch and switch VPN before retrying.
            None (default) preserves original behavior — a failed fetch stops
            the run.
    """
    log.info("=" * 60)
    log.info("Loose Grown Diamond scrape — starting")
    if rotate_after_fails:
        log.info(f"VPN rotate-on-fail threshold: {rotate_after_fails} consecutive fetch(es)")

    rotated = _rotate_old_file(CSV_FILE)
    if rotated:
        log.info("Old data archived — this will be a full fresh run.")

    _init_csv()

    seen_skus   = _load_existing_skus()
    start       = _get_start_offset()
    total_saved = 0
    consecutive_fails = 0

    log.info(f"Starting from offset {start}  |  PAGE_SIZE={PAGE_SIZE:,}")

    while True:
        response = _fetch_page(start)

        if not response or not response.get("status"):
            consecutive_fails += 1
            if rotate_after_fails:
                if consecutive_fails >= rotate_after_fails:
                    log.warning(
                        f"VPN rotate needed — {consecutive_fails} consecutive "
                        f"failed fetch(es) at offset {start}. State file retains "
                        f"offset so next VPN attempt resumes here."
                    )
                    raise VPNRotateNeeded(start, consecutive_fails)
                log.warning(
                    f"Failed fetch at offset={start} "
                    f"({consecutive_fails}/{rotate_after_fails}) — retrying same offset."
                )
                time.sleep(RETRY_SLEEP_BEFORE_ROTATE)
                continue
            log.warning("Empty or failed response — stopping scrape.")
            break

        # Success — reset block counter
        consecutive_fails = 0

        items        = response.get("data") or []
        total_avail  = int(response.get("total", 0))

        if not items:
            log.info("No items returned — inventory fully consumed.")
            break

        # Write new rows using a context manager so the file is never
        # left open on exception.
        new_rows = 0
        with open(CSV_FILE, "a", newline="", encoding="utf-8") as fh:
            writer = csv.writer(fh)
            for diamond in _iter_new_rows(items, seen_skus):
                writer.writerow(diamond.as_row())
                new_rows += 1

        total_saved += new_rows
        next_start   = start + 1
        _save_offset(next_start)

        pct = (total_saved / total_avail * 100) if total_avail else 0
        log.info(
            f"Offset {start:>6} → {next_start:<6} | "
            f"New: {new_rows:>4} | "
            f"Session total: {total_saved:>6,} | "
            f"Progress: {pct:.1f}% of {total_avail:,}"
        )

        if start * PAGE_SIZE >= total_avail:
            log.info("All inventory pages consumed — done.")
            break

        start = next_start
        time.sleep(INTER_PAGE_SLEEP)

    log.info(f"Scrape complete. Saved {total_saved:,} new records this run.")
    log.info("=" * 60)
    return {"total_saved": total_saved}


# ── VPN-WRAPPED RUN ───────────────────────────────────────────────────────────
def run_with_vpn_rotation() -> None:
    """
    Run the scraper inside an OpenVPN tunnel, trying EVERY config in
    vpnbook/<VPN_CONFIG_SUBDIR>/ until one completes the scrape.

    Per-config outcome:
      - success           → scrape done, return.
      - VPNRotateNeeded    → tunnel works but the exit IP got blocked mid-scrape;
                             keep the config and rotate to the next one.
      - VPNError (dead)    → the config failed to connect; DELETE the .ovpn file
                             and move on.

    Safety: a missing OpenVPN binary or an auth failure is an *environmental*
    problem (not a dead config), so those abort the run WITHOUT deleting
    anything — otherwise a stale password would wipe the whole pool.

    The scraper resumes from the state file, so a failed offset is re-tried
    through each new exit IP. Requires Administrator / root.
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
    log.info("Loose Grown scraper with VPN rotation")
    log.info(f"Consecutive-fail threshold: {VPN_ROTATE_AFTER_FAILS}")
    log.info("=" * 60)

    if not is_admin():
        log.error(
            "OpenVPN requires Administrator/root. "
            "Re-run from an elevated shell, or call loose_grown_diamonds_scrappe() directly."
        )
        return

    # Validate the OpenVPN binary once up-front. If it is missing, every connect
    # would fail and we'd delete the entire pool over an environment problem.
    try:
        find_openvpn_executable()
    except VPNError as exc:
        log.error(f"OpenVPN unavailable — aborting without touching configs: {exc}")
        return

    repo_root = Path(__file__).resolve().parent.parent
    vpn_dir = repo_root / "vpnbook" / "usa" / VPN_CONFIG_SUBDIR
    if not vpn_dir.exists():
        log.error(f"VPN config dir not found: {vpn_dir}")
        return

    # Resolve the shared ProtonVPN credentials relative to this script so the
    # lookup never depends on the process's cwd. The configs in this pool are
    # all ProtonVPN, which vpn_rotator's per-config auto-resolution can miss
    # when cwd != repo root, so pass the auth file in explicitly.
    proton_auth: Path | None = None
    proton_creds = repo_root / "vpnbook" / "proton_credentials.txt"
    if proton_creds.exists():
        user, pwd = parse_credentials_file(proton_creds)
        proton_auth = write_auth_file(user, pwd, proton_creds.parent / "proton_auth.txt")
        log.info(f"Using ProtonVPN credentials from {proton_creds}")
    else:
        log.warning(
            f"ProtonVPN credentials not found at {proton_creds} — "
            f"falling back to vpn_rotator's per-config lookup."
        )

    # OpenVPN's runtime log. vpn_rotator defaults this to cwd/vpnbook/, which is
    # absent when run from anywhere but the repo root — anchor it to the real dir.
    openvpn_log = repo_root / "vpnbook" / "openvpn.log"

    configs = sorted(vpn_dir.glob("*.ovpn"))
    if not configs:
        log.error(f"No .ovpn files in {vpn_dir}")
        return

    random.shuffle(configs)
    total   = len(configs)
    deleted = 0
    log.info(f"VPN pool: {total} config(s) in {vpn_dir} — trying every config")

    for attempt, cfg in enumerate(configs, start=1):
        log.info(f"--- VPN attempt {attempt}/{total}: {cfg.name} ---")

        try:
            with VPNConnection(cfg, auth_file=proton_auth, log_file=openvpn_log) as vpn:
                log.info(f"Tunnel up. Exit IP = {vpn.ip_after}")
                loose_grown_diamonds_scrappe(rotate_after_fails=VPN_ROTATE_AFTER_FAILS)
            log.info("Scrape completed — VPN torn down.")
            return

        except VPNRotateNeeded as exc:
            log.warning(f"Rotating VPN (config works, IP burned): {exc}")
            # tunnel already torn down by context manager; keep config, try next

        except VPNError as exc:
            # Auth failure = stale credentials, not a dead config. Abort without touching files.
            if "authentication failed" in str(exc).lower():
                log.error(
                    f"VPN auth failed on {cfg.name} — credentials likely stale. "
                    f"Aborting (no configs deleted). Check proton_credentials.txt."
                )
                return
            log.warning(f"VPN error on {cfg.name}: {exc} — skipping to next config (file kept)")
            # VPN file deletion disabled — only 403 block triggers a rotate,
            # failed connections just skip to the next config without removing the file.
            # try:
            #     cfg.unlink()
            #     deleted += 1
            #     log.info(f"Deleted non-working VPN config: {cfg.name}")
            # except OSError as del_exc:
            #     log.warning(f"Could not delete {cfg.name}: {del_exc}")

        except KeyboardInterrupt:
            log.warning("Interrupted by user — exiting VPN loop.")
            return

    log.error(
        f"Tried all {total} VPN config(s) — none completed the scrape "
        f"(no files deleted — VPN file removal is disabled)."
    )


if __name__ == "__main__":
    # Entry points (scheduling is handled externally by scheduler.py):
    #   run_with_vpn_rotation()           → one-shot VPN-wrapped scrape with mid-run rotation
    loose_grown_diamonds_scrappe()    #→ pure scrape, no VPN (your real IP)
    #
    # Note: VPN-using entries require Administrator/root (OpenVPN needs TAP/TUN).
    # import sys as _sys
    # try:
    #     run_with_vpn_rotation()
    # except KeyboardInterrupt:
    #     log.warning("Ctrl+C pressed — stopping scraper and tearing down any VPN tunnel.")
        # try:
        #     from vpn_rotator import kill_openvpn_processes
        #     kill_openvpn_processes()
        # except Exception as _exc:
        #     log.warning(f"VPN cleanup failed: {_exc}")
        # _sys.exit(130)