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
cert_no-based dedup. Keeps its OWN shard-resume state in
abelini_v2_status.json (separate from abelini.py's abelini_status.json,
since the two scrapers can be run interchangeably and shouldn't clobber
each other's progress): done_shapes/done_shards skip whatever already
finished, and shard_progress resumes an in-progress carat band from its
last completed page instead of restarting that band from page 1.

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
import json
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

# ── Shard-resume state (own status file — see module docstring) ─────────────
STATUS_FILE = os.path.join(os.path.dirname(MAIN_CSV), "abelini_v2_status.json")

def _shard_key(shape: str, c_min: float, c_max: float) -> str:
    """Stable string key for status.json (JSON can't have tuple keys)."""
    return f"{shape}|{c_min:.2f}|{c_max:.2f}"

def load_status() -> dict:
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

def save_status(done_shapes, done_shards, shard_progress) -> None:
    with open(STATUS_FILE, "w") as f:
        json.dump({
            "scraper":        "abelini_v2",
            "done_shapes":    done_shapes,
            "done_shards":    done_shards,
            "shard_progress": shard_progress,
            "updated_at":     time.strftime("%Y-%m-%dT%H:%M:%S"),
        }, f, indent=2)

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

    raw_status     = load_status()
    done_shapes    = raw_status["done_shapes"]
    done_shards    = raw_status["done_shards"]
    shard_progress = raw_status["shard_progress"]

    shapes_to_run = [s for s in ALL_SHAPES if s not in done_shapes]
    log.info("Shapes to run: %d / %d  (done: %s)", len(shapes_to_run), len(ALL_SHAPES), done_shapes or "none")
    if not shapes_to_run:
        log.info("All shapes already complete. Delete %s to re-run.", STATUS_FILE)
        return

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

            for shape in shapes_to_run:
                log.info("-" * 60)
                log.info("[%s] starting shape — %d carat bands", shape, len(CARAT_BANDS))
                shape_new = shape_dup = 0

                for c_min, c_max in CARAT_BANDS:
                    sk = _shard_key(shape, c_min, c_max)
                    if sk in done_shards:
                        log.info("[%s %.2f-%.2f] already done (resume) — skipping", shape, c_min, c_max)
                        continue

                    start_page = shard_progress.get(sk, 0) + 1
                    if start_page > 1:
                        log.info("[%s %.2f-%.2f] resuming from page %d", shape, c_min, c_max, start_page)

                    try:
                        dup_streak   = 0
                        page         = start_page
                        shard_done   = True   # flips False on a transient/unresolved error — retry same page next run

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
                                log.warning("[%s %.2f-%.2f] p%d fetch error: %s — will retry from here next run",
                                            shape, c_min, c_max, page, result["error"])
                                shard_done = False
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
                                    log.error("[%s %.2f-%.2f] p%d still 403 after reload — will retry from here next run",
                                              shape, c_min, c_max, page)
                                    shard_done = False
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

                            shard_progress[sk] = page
                            save_status(done_shapes, done_shards, shard_progress)

                            dup_streak = dup_streak + 1 if (batch_new == 0 and page_dups > 0) else 0
                            log.info("[%s %.2f-%.2f] p%d -> %d new, %d dup (streak=%d)",
                                     shape, c_min, c_max, page, batch_new, page_dups, dup_streak)

                            if dup_streak >= DUP_PAGE_LIMIT:
                                log.info("[%s %.2f-%.2f] dup-streak %d -> bail shard at p%d",
                                         shape, c_min, c_max, dup_streak, page)
                                break

                            page += 1
                            time.sleep(random.uniform(PAGE_DELAY_MIN, PAGE_DELAY_MAX))

                        if shard_done:
                            if sk not in done_shards:
                                done_shards.append(sk)
                            save_status(done_shapes, done_shards, shard_progress)
                            log.info("[%s %.2f-%.2f] shard done", shape, c_min, c_max)
                    except Exception as exc:
                        log.error("[%s %.2f-%.2f] shard crashed: %s — will retry from last saved page next run",
                                  shape, c_min, c_max, exc)

                expected_shards = {_shard_key(shape, cm, cx) for cm, cx in CARAT_BANDS}
                if expected_shards.issubset(set(done_shards)):
                    if shape not in done_shapes:
                        done_shapes.append(shape)
                    save_status(done_shapes, done_shards, shard_progress)
                    log.info("[%s] ✅ SHAPE COMPLETE — new=%d dup=%d", shape, shape_new, shape_dup)
                else:
                    log.info("[%s] shape incomplete this run — new=%d dup=%d", shape, shape_new, shape_dup)
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
