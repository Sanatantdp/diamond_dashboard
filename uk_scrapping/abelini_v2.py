"""
Abelini scraper — Selenium edition (v2)

abelini.py's curl_cffi transport stopped passing Cloudflare on this endpoint —
even a cf_clearance cookie captured seconds earlier from a real, working
browser session gets served the "Just a moment..." JS challenge when replayed
through curl_cffi's impersonation. Cloudflare is evidently checking something
(TLS/HTTP2 fingerprint) that no amount of header/cookie copying reproduces.

This version sidesteps the problem entirely: a real Chrome (driven by
undetected_chromedriver, which patches the automation signals Cloudflare
looks for) navigates to the site once so ITS OWN JS engine solves the
challenge, then every paginated /api/diamond-search call is made via
`fetch()` executed inside that same live page. The request is indistinguishable
from the site's own AJAX calls because it genuinely is one.

Shares abelini.py's output CSV (diamond_files/uk/abelini.csv) and dedup logic,
so this can be run interchangeably with abelini.py — same schema, same
cert_no-based dedup. Does NOT share abelini.py's status.json shard-resume
state (that's curl_cffi-run-specific); instead every run walks all shapes and
carat bands again, relying on the CSV dedup set + dup-streak bail to skip
already-scraped shards cheaply (matches gs_diamond_v2.py's convention).

Usage:
    python abelini_v2.py             # visible browser window (default — confirmed
                                      # necessary: headless gets stuck on Cloudflare's
                                      # "Just a moment..." challenge and 403s)
    python abelini_v2.py --headless  # headless Chrome (currently does NOT pass
                                      # Cloudflare on this site — kept as an option
                                      # in case that changes, not recommended today)
"""

import argparse
import csv
import logging
import os
import random
import time

from selenium.common.exceptions import TimeoutException, WebDriverException
import undetected_chromedriver as uc

from abelini import (
    ALL_SHAPES,
    BASE_PARAMS,
    CARAT_BANDS,
    DUP_PAGE_LIMIT,
    FIELDS,
    MAIN_CSV,
    MAX_PAGES_PER_SHARD,
    load_existing_ids,
    migrate_schema_if_needed,
    parse_diamonds,
    strip_lg,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] abelini2 — %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("abelini_v2")

API_URL     = "https://www.abelini.com/api/diamond-search"
WARMUP_URL  = "https://www.abelini.com/choose-diamond"

# undetected_chromedriver otherwise grabs the newest chromedriver release,
# which can outrun the locally installed Chrome (e.g. driver 151 vs. browser
# 150) and fail with SessionNotCreatedException. Bump this to match whatever
# `chrome://version` reports if that error resurfaces after a Chrome update.
CHROME_MAJOR_VERSION = 150

PAGE_DELAY_MIN        = 0.4
PAGE_DELAY_MAX        = 1.0
SCRIPT_TIMEOUT        = 30
CHALLENGE_WAIT_TIMEOUT = 45

# Runs inside the live page via execute_async_script — the last argument
# Selenium appends is the async callback used to return the result.
_FETCH_JS = """
const callback = arguments[arguments.length - 1];
const payload  = arguments[0];
const url      = arguments[1];
fetch(url, {
    method: 'POST',
    headers: {'content-type': 'text/plain;charset=UTF-8'},
    body: JSON.stringify(payload),
    credentials: 'include',
}).then(r => r.json()
              .then(data => callback({status: r.status, data: data}))
              .catch(err => callback({status: r.status, error: 'bad json: ' + String(err)})))
  .catch(err => callback({status: 0, error: String(err)}));
"""


def make_driver(headless: bool) -> uc.Chrome:
    options = uc.ChromeOptions()
    if headless:
        options.add_argument("--headless=new")
    options.add_argument("--window-size=1366,900")
    driver = uc.Chrome(options=options, headless=headless, version_main=CHROME_MAJOR_VERSION)
    driver.set_script_timeout(SCRIPT_TIMEOUT)
    return driver


def wait_for_cloudflare(driver, timeout: int = CHALLENGE_WAIT_TIMEOUT) -> bool:
    """Block until Cloudflare's 'Just a moment...' interstitial is gone."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        title = (driver.title or "").lower()
        if "just a moment" not in title and "checking" not in title:
            return True
        time.sleep(1)
    log.warning("Cloudflare challenge did not clear within %ss — continuing anyway", timeout)
    return False


def browser_fetch(driver, payload: dict, retries: int = 2) -> dict:
    """Run the POST inside the live page. Returns {status, data} or {status, error}."""
    for attempt in range(retries + 1):
        try:
            return driver.execute_async_script(_FETCH_JS, payload, API_URL)
        except TimeoutException:
            log.warning("fetch script timed out (attempt %d/%d)", attempt + 1, retries + 1)
            time.sleep(2)
        except WebDriverException as exc:
            log.warning("fetch webdriver error: %s (attempt %d/%d)", exc, attempt + 1, retries + 1)
            time.sleep(2)
    return {"status": 0, "error": "gave up after retries"}


def scrape(headless: bool) -> None:
    log.info("=" * 60)
    log.info("Abelini scraper v2 (Selenium/undetected_chromedriver)  headless=%s", headless)
    log.info("=" * 60)

    migrate_schema_if_needed()
    existing_ids = load_existing_ids()

    driver = make_driver(headless)
    grand_new = grand_dup = 0

    try:
        log.info("Navigating to %s (Cloudflare handshake)...", WARMUP_URL)
        driver.get(WARMUP_URL)
        wait_for_cloudflare(driver)
        log.info("Page ready: %r", driver.title)
        time.sleep(random.uniform(1.0, 2.0))

        file_exists = os.path.exists(MAIN_CSV)
        with open(MAIN_CSV, "a" if file_exists else "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=FIELDS)
            if not file_exists:
                writer.writeheader()

            for shape in ALL_SHAPES:
                log.info("-" * 60)
                log.info("[%s] starting shape — %d carat bands", shape, len(CARAT_BANDS))
                shape_new = shape_dup = 0

                for c_min, c_max in CARAT_BANDS:
                    dup_streak = 0
                    page = 1

                    while page <= MAX_PAGES_PER_SHARD:
                        payload = {
                            **BASE_PARAMS,
                            "stone_shape":     shape,
                            "stone_carat_min": f"{c_min:.2f}",
                            "stone_carat_max": f"{c_max:.2f}",
                            "page":            str(page),
                        }
                        result = browser_fetch(driver, payload)

                        if "error" in result:
                            log.warning("[%s %.2f-%.2f] p%d fetch error: %s",
                                        shape, c_min, c_max, page, result["error"])
                            break

                        status = result.get("status")
                        if status == 403:
                            log.warning("[%s %.2f-%.2f] p%d 403 — reloading to refresh session",
                                        shape, c_min, c_max, page)
                            driver.get(WARMUP_URL)
                            wait_for_cloudflare(driver)
                            time.sleep(2)
                            result = browser_fetch(driver, payload)
                            if result.get("status") == 403:
                                log.error("[%s %.2f-%.2f] p%d still 403 after reload — bailing shard",
                                          shape, c_min, c_max, page)
                                break
                        elif status and status != 200:
                            log.warning("[%s %.2f-%.2f] p%d HTTP %d — end of shard",
                                        shape, c_min, c_max, page, status)
                            break

                        payload_resp = result.get("data") or {}
                        inner  = payload_resp.get("data", {}) or {}
                        stones = inner.get("stones") or []
                        if not stones:
                            log.info("[%s %.2f-%.2f] p%d empty — end of shard", shape, c_min, c_max, page)
                            break

                        diamonds  = parse_diamonds(stones)
                        rows      = []
                        batch_new = 0
                        page_dups = 0
                        for d in diamonds:
                            raw_certno = str(d.get("cert_no_clean") or d.get("cert_no") or "")
                            key        = strip_lg(raw_certno.strip())
                            is_dup     = key in existing_ids if key else True
                            if key:
                                existing_ids.add(key)
                            row = {fld: d.get(fld, "") for fld in FIELDS[:-2]}
                            row["status"]  = "duplicate" if is_dup else "new"
                            row["page_no"] = page
                            rows.append(row)
                            if is_dup:
                                page_dups += 1
                            else:
                                batch_new += 1

                        writer.writerows(rows)
                        f.flush()
                        shape_new += batch_new
                        shape_dup += page_dups

                        dup_streak = dup_streak + 1 if (batch_new == 0 and page_dups > 0) else 0
                        log.info("[%s %.2f-%.2f] p%d -> %d new, %d dup (streak=%d)",
                                 shape, c_min, c_max, page, batch_new, page_dups, dup_streak)

                        if dup_streak >= DUP_PAGE_LIMIT:
                            log.info("[%s %.2f-%.2f] dup-streak %d -> bail shard at p%d",
                                     shape, c_min, c_max, dup_streak, page)
                            break

                        page += 1
                        time.sleep(random.uniform(PAGE_DELAY_MIN, PAGE_DELAY_MAX))

                log.info("[%s] shape done — new=%d dup=%d", shape, shape_new, shape_dup)
                grand_new += shape_new
                grand_dup += shape_dup

        log.info("=" * 60)
        log.info("ALL DONE — new=%d dup=%d total=%d", grand_new, grand_dup, len(existing_ids))
        log.info("=" * 60)

    finally:
        driver.quit()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Abelini Selenium scraper")
    parser.add_argument(
        "--headless", action="store_true",
        help="Run headless — NOT currently recommended, gets stuck on Cloudflare's challenge on this site"
    )
    args = parser.parse_args()

    try:
        scrape(headless=args.headless)
    except KeyboardInterrupt:
        log.warning("Ctrl+C — stopping.")
