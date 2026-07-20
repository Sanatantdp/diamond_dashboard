"""
GS Diamonds scraper — Playwright edition (v2)

Uses a real Chromium browser so Cloudflare challenges are solved by the
actual JS engine.  All AJAX requests are made via the browser's own fetch()
so every cookie (cf_clearance, XSRF-TOKEN, session) is always live.

No curl_gs.txt required.

Usage:
    python gs_diamond_v2.py              # headless (default)
    python gs_diamond_v2.py --headed     # visible browser window
"""

import argparse, csv, logging, os, re, time
from pathlib import Path
from urllib.parse import urlencode

from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

# ─────────────────────────────────────────────────────────────
#  LOGGER
# ─────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  [%(levelname)-8s]  gs2 — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("gs2")

# ─────────────────────────────────────────────────────────────
#  CONFIG
# ─────────────────────────────────────────────────────────────
_REPO       = Path(__file__).resolve().parent.parent
OUTPUT_DIR  = str(_REPO / "diamond_files" / "aus")
CSV_FILE    = os.path.join(OUTPUT_DIR, "gs_diamonds.csv")

BASE_URL = "https://www.gsdiamonds.com.au/lab-diamonds"

SHAPES = [
    "Round", "Oval", "Cushion", "Princess", "Emerald",
    "Pear", "Radiant", "Asscher", "Heart", "Marquise",
]

FILTERS = {
    "creation_method":     "Lab-White",
    "min-carat":           "0.5",
    "max-carat":           "10",
    "min-clarity_numeric": "0",
    "max-clarity_numeric": "75.5",
    "min-color_numeric":   "0",
    "max-color_numeric":   "30",
}

FIELDS = [
    "shape", "cert_no", "carat", "color", "clarity",
    "cut", "polish", "symmetry", "price_aud", "url",
]

PAGE_DELAY   = 1.2   # seconds between AJAX calls
FETCH_TIMEOUT = 30_000  # ms for page.evaluate fetch

# ─────────────────────────────────────────────────────────────
#  CSV HELPERS
# ─────────────────────────────────────────────────────────────

def ensure_csv():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    if not os.path.exists(CSV_FILE):
        with open(CSV_FILE, "w", newline="", encoding="utf-8") as f:
            csv.DictWriter(f, fieldnames=FIELDS).writeheader()


def load_existing_certs():
    certs = set()
    if os.path.exists(CSV_FILE):
        with open(CSV_FILE, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                if row.get("cert_no"):
                    certs.add(row["cert_no"])
    log.info(f"Existing certificates in CSV: {len(certs)}")
    return certs


# ─────────────────────────────────────────────────────────────
#  HTML PARSER  (same logic as v1)
# ─────────────────────────────────────────────────────────────

def parse_html_to_rows(html, shape, existing_certs):
    soup = BeautifulSoup(html, "lxml")
    grid = soup.select_one("div.product-list")
    if not grid:
        return [], False

    cards = grid.select("div.bg-white")
    if not cards:
        return [], False

    rows = []
    for card in cards:
        a = card.select_one("a.font-semibold")
        if not a:
            continue

        url = a.get("href", "")
        cert_no = ""
        m = re.search(r'gs(\d+)', url)
        if m:
            cert_no = m.group(1)
        elif "gs" in url:
            cert_no = url.split("gs")[-1].split("_")[0]

        if cert_no and cert_no in existing_certs:
            continue
        if cert_no:
            existing_certs.add(cert_no)

        data = {
            "shape": shape, "cert_no": cert_no, "url": url,
            "carat": "", "color": "", "clarity": "",
            "cut": "", "polish": "", "symmetry": "", "price_aud": "",
        }

        spans = a.find_all("span")
        if len(spans) >= 1:
            data["carat"] = spans[0].get_text(strip=True).replace("ct", "").strip()
        if len(spans) >= 2:
            cc = spans[1].get_text(strip=True)
            if "/" in cc:
                data["color"], data["clarity"] = [x.strip() for x in cc.split("/", 1)]
        if len(spans) >= 3:
            data["cut"] = spans[2].get_text(strip=True)
        if len(spans) >= 4:
            data["polish"] = spans[3].get_text(strip=True)
        if len(spans) >= 5:
            data["symmetry"] = spans[4].get_text(strip=True)

        price_el = card.select_one(".text-primary-dark")
        if price_el:
            data["price_aud"] = (
                price_el.get_text(strip=True)
                .replace("A$", "").replace(",", "").strip()
            )

        rows.append(data)

    return rows, True


# ─────────────────────────────────────────────────────────────
#  BROWSER FETCH HELPER
# ─────────────────────────────────────────────────────────────

_FETCH_JS = """
async (url) => {
    // Read the XSRF token from cookies (Laravel CSRF)
    const raw = document.cookie
        .split('; ')
        .find(c => c.startsWith('XSRF-TOKEN='));
    const xsrf = raw ? decodeURIComponent(raw.split('=').slice(1).join('=')) : '';

    const r = await fetch(url, {
        method: 'GET',
        headers: {
            'accept':           'application/json, text/plain, */*',
            'accept-language':  'en-GB,en-US;q=0.9,en;q=0.8',
            'sec-fetch-dest':   'empty',
            'sec-fetch-mode':   'cors',
            'sec-fetch-site':   'same-origin',
            'x-xsrf-token':     xsrf,
        },
        credentials: 'include',
    });

    if (!r.ok) return { __status: r.status };
    return await r.json();
}
"""


def browser_fetch(page, url, retries=2):
    """
    Run a fetch() inside the live browser page.
    Returns the parsed JSON dict, or {"__status": <code>} on HTTP error,
    or {"__error": <msg>} on JS exception.
    """
    for attempt in range(retries + 1):
        try:
            result = page.evaluate(_FETCH_JS, url)
            return result
        except PWTimeout:
            if attempt < retries:
                log.warning(f"  fetch timeout — retrying ({attempt+1}/{retries})")
                time.sleep(3)
            else:
                return {"__error": "timeout"}
        except Exception as e:
            return {"__error": str(e)}


# ─────────────────────────────────────────────────────────────
#  CLOUDFLARE WAIT
# ─────────────────────────────────────────────────────────────

def wait_for_cloudflare(page, timeout_ms=30_000):
    """Block until Cloudflare 'Just a moment' challenge is gone."""
    try:
        page.wait_for_function(
            "() => !document.title.toLowerCase().includes('just a moment')"
            "   && !document.title.toLowerCase().includes('checking')",
            timeout=timeout_ms,
        )
    except PWTimeout:
        log.warning("Cloudflare challenge did not clear in time — continuing anyway")


# ─────────────────────────────────────────────────────────────
#  MAIN SCRAPE
# ─────────────────────────────────────────────────────────────

# Patches applied before any page script runs — hides Playwright/headless signals
_STEALTH_JS = """
() => {
    Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
    Object.defineProperty(navigator, 'plugins',   { get: () => [1, 2, 3, 4, 5] });
    Object.defineProperty(navigator, 'languages', { get: () => ['en-GB', 'en-US', 'en'] });
    window.chrome = { runtime: {}, loadTimes: () => {}, csi: () => {}, app: {} };
    const orig = navigator.permissions.query.bind(navigator.permissions);
    navigator.permissions.query = (p) =>
        p.name === 'notifications'
            ? Promise.resolve({ state: Notification.permission })
            : orig(p);
}
"""


def scrape(headless=True):
    log.info("=" * 60)
    log.info("  GS DIAMONDS SCRAPER  v2  (Playwright)")
    log.info(f"  headless={headless}  channel=chrome")
    log.info("=" * 60)

    ensure_csv()
    existing_certs = load_existing_certs()

    with sync_playwright() as pw:
        # Use system Chrome (channel="chrome") — real Chrome binary is far less
        # detectable than Playwright's bundled headless Chromium.
        # Falls back to bundled Chromium if Chrome isn't installed.
        try:
            browser = pw.chromium.launch(
                channel="chrome",
                headless=headless,
                args=["--no-sandbox", "--disable-blink-features=AutomationControlled"],
            )
            log.info("Using system Chrome")
        except Exception:
            log.warning("System Chrome not found — falling back to bundled Chromium")
            browser = pw.chromium.launch(
                headless=headless,
                args=["--no-sandbox", "--disable-blink-features=AutomationControlled"],
            )

        context = browser.new_context(
            viewport={"width": 1920, "height": 1080},
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/149.0.0.0 Safari/537.36"
            ),
            locale="en-GB",
            # Mimic normal browser accept headers
            extra_http_headers={"accept-language": "en-GB,en-US;q=0.9,en;q=0.8"},
        )
        page = context.new_page()

        # Inject stealth patches before any page script can check for automation
        page.add_init_script(_STEALTH_JS)

        # ── Initial navigation — real browser solves Cloudflare ──
        log.info("Navigating to GS Diamonds (Cloudflare handshake)...")
        try:
            page.goto(BASE_URL, wait_until="domcontentloaded", timeout=60_000)
        except PWTimeout:
            log.warning("Initial page load timed out — continuing")

        wait_for_cloudflare(page, timeout_ms=45_000)
        log.info(f"Page ready: {page.title()!r}")
        time.sleep(2)

        total_rows = 0

        with open(CSV_FILE, "a", newline="", encoding="utf-8") as csv_fh:
            writer = csv.DictWriter(csv_fh, fieldnames=FIELDS)

            for shape in SHAPES:
                log.info(f"[{shape}]")
                page_num = 1
                shape_rows = 0

                while True:
                    params = {**FILTERS, "shape": shape, "page": str(page_num)}
                    url = BASE_URL + "?" + urlencode(params)

                    result = browser_fetch(page, url)

                    # ── Error handling ──
                    if "__error" in result:
                        log.warning(f"  Page {page_num:>3}  -> JS error: {result['__error']}")
                        break

                    status = result.get("__status")
                    if status == 403:
                        log.warning(f"  Page {page_num:>3}  -> 403 — trying page reload to refresh session")
                        try:
                            page.reload(wait_until="domcontentloaded", timeout=30_000)
                            wait_for_cloudflare(page)
                            time.sleep(3)
                            result = browser_fetch(page, url)
                            if result.get("__status") == 403:
                                log.warning(f"  Still 403 after reload — skipping {shape}")
                                break
                        except Exception as e:
                            log.warning(f"  Reload failed: {e}")
                            break
                    elif status:
                        log.warning(f"  Page {page_num:>3}  -> HTTP {status}, end of {shape}")
                        break

                    # ── Parse Turbo Streams response ──
                    commands = result.get("commands", [])
                    if not commands:
                        log.info(f"  Page {page_num:>3}  -> no commands, end of {shape}")
                        break

                    html = commands[0].get("html", "")
                    if not html:
                        log.info(f"  Page {page_num:>3}  -> empty html, end of {shape}")
                        break

                    rows, has_products = parse_html_to_rows(html, shape, existing_certs)

                    if not has_products:
                        log.info(f"  Page {page_num:>3}  -> no products, end of {shape}")
                        break

                    writer.writerows(rows)
                    csv_fh.flush()
                    shape_rows += len(rows)
                    log.info(f"  Page {page_num:>3}  -> {len(rows):>3} new rows")
                    page_num += 1
                    time.sleep(PAGE_DELAY)

                log.info(f"  [{shape}] total: {shape_rows} rows")
                total_rows += shape_rows

        browser.close()

    log.info("=" * 60)
    log.info(f"  DONE — {total_rows} rows -> {CSV_FILE}")
    log.info("=" * 60)


# ─────────────────────────────────────────────────────────────
#  ENTRY POINT
# ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="GS Diamonds Playwright scraper")
    parser.add_argument(
        "--headed", action="store_true",
        help="Show the browser window (useful for debugging Cloudflare)"
    )
    args = parser.parse_args()

    try:
        scrape(headless=not args.headed)
    except KeyboardInterrupt:
        log.warning("Ctrl+C — stopping.")
