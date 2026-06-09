from curl_cffi import requests as cf_requests
import json, os, csv, sys, re, time, random, logging
from pathlib import Path
from bs4 import BeautifulSoup
from urllib.parse import unquote, urlencode

# ─────────────────────────────────────────────────────────────
#  LOGGER
# ─────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  [%(levelname)-8s]  gs — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("gs")

# ─────────────────────────────────────────────────────────────
#  CONFIG — edit here only
# ─────────────────────────────────────────────────────────────

_REPO          = Path(__file__).resolve().parent.parent
CURL_FILE      = str(Path(__file__).resolve().parent / "curl_gs.txt")
VPN_DIR        = _REPO / "vpnbook" / "roa" / "gs"
PROTON_CREDS   = _REPO / "vpnbook" / "proton_credentials.txt"
OPENVPN_LOG    = _REPO / "vpnbook" / "openvpn.log"
OUTPUT_DIR      = str(_REPO / "diamond_files" / "aus")
VPN_ROTATE_AFTER_FAILS = 3
JSON_DIR        = os.path.join(OUTPUT_DIR, "gs_pages")
CSV_FILE        = os.path.join(OUTPUT_DIR, "gs_diamonds.csv")
CHECKPOINT_FILE = os.path.join(OUTPUT_DIR, "gs_checkpoint.json")
SLEEP_SEC       = 2.0
CHECKPOINT_MAX_DAYS = 2

import datetime

# ── Checkpoint helpers ────────────────────────────────────────
def load_checkpoint():
    """Return (shape, page) to resume from, or (None, 1) for fresh start."""
    if not os.path.exists(CHECKPOINT_FILE):
        return None, 1
    try:
        with open(CHECKPOINT_FILE) as f:
            cp = json.load(f)
        saved = datetime.datetime.fromisoformat(cp["timestamp"])
        age = (datetime.datetime.now() - saved).days
        if age >= CHECKPOINT_MAX_DAYS:
            os.remove(CHECKPOINT_FILE)
            log.info(f"Checkpoint is {age} day(s) old (>= {CHECKPOINT_MAX_DAYS}) — starting from beginning.")
            return None, 1
        log.info(f"Resuming from shape={cp['shape']} page={cp['page']} (checkpoint age: {age} day(s))")
        return cp["shape"], cp["page"]
    except Exception as e:
        log.warning(f"Checkpoint read error: {e} — starting from beginning.")
        return None, 1

def save_checkpoint(shape, page):
    with open(CHECKPOINT_FILE, "w") as f:
        json.dump({"shape": shape, "page": page,
                   "timestamp": datetime.datetime.now().isoformat()}, f)

def clear_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        os.remove(CHECKPOINT_FILE)
        log.info("Checkpoint cleared (scrape complete).")

BASE_URL = "https://www.gsdiamonds.com.au/lab-diamonds"

SHAPES = [
    "Round", "Oval", "Cushion", "Princess", "Emerald",
    "Pear", "Radiant", "Asscher", "Heart", "Marquise",
]

FILTERS = {
    "creation_method":     "Lab-White",
    "min-carat":           "0.3",
    "max-carat":           "20.1",
    "min-clarity_numeric": "0",
    "max-clarity_numeric": "75.5",
    "min-color_numeric":   "0",
    "max-color_numeric":   "30",
}

FIELDS = [
    "shape", "cert_no", "carat", "color", "clarity",
    "cut", "polish", "symmetry", "price_aud", "url",
]

# ─────────────────────────────────────────────────────────────
#  CURL.TXT PARSER
# ─────────────────────────────────────────────────────────────

class GSBlockedException(Exception):
    """Raised after N consecutive 403s — signals VPN rotation."""
    def __init__(self, page: int, consecutive: int):
        super().__init__(
            f"VPN rotate signal: {consecutive} consecutive 403(s) ending at page {page}"
        )
        self.page = page
        self.consecutive = consecutive


def load_curl(path):
    if not os.path.exists(path):
        print(f"\n[FATAL] '{path}' not found.")
        print("  Steps to create it:")
        print("  1. Open Chrome -> gsdiamonds.com.au/lab-diamonds")
        print("  2. DevTools (F12) -> Network tab")
        print("  3. Find any lab-diamonds GET request")
        print("  4. Right-click -> Copy as cURL")
        print("  5. Paste and save as curl_gs.txt")
        sys.exit(1)

    with open(path, "r", encoding="utf-8") as f:
        raw = f.read()

    # Normalise both Unix (\) and Windows CMD (^) line continuations
    raw = raw.replace("^\r\n", " ").replace("^\n", " ").replace("\\\n", " ")
    # Decode Windows CMD escape sequences before parsing
    raw = re.sub(r'\^%\^', '%', raw)   # ^%^ → %  (encoded URL chars)
    raw = raw.replace('^$', '$')        # ^$ → $
    raw = raw.replace('^&', '&')        # ^& → &  (URL ampersands)
    raw = raw.replace(r'^\^"', '"')     # ^\^" → " (quoted value inside -H)
    raw = raw.replace('^"', '"')        # ^"  → "  (outer delimiters)
    raw = raw.replace('^^', '^')        # ^^ → ^   (literal caret)
    raw = re.sub(r"\s+", " ", raw).strip()

    q = "'" if raw.lstrip().startswith("curl '") else '"'
    Q = re.escape(q)

    result = {}

    # Cookies
    cookie_m = re.search(rf'-b\s+{Q}([^{q}]+){Q}', raw)
    cookies = {}
    if cookie_m:
        raw_c = cookie_m.group(1)
        try:
            raw_c = unquote(raw_c)
        except Exception:
            pass
        for part in raw_c.split(";"):
            part = part.strip()
            if "=" in part:
                k, _, v = part.partition("=")
                cookies[k.strip()] = v.strip()
    result["cookies"] = cookies

    # Headers
    headers = {}
    for hm in re.finditer(rf'-H\s+{Q}([^{q}]+){Q}', raw):
        hdr = hm.group(1)
        if ":" in hdr:
            k, _, v = hdr.partition(":")
            k = k.strip().lower()
            if k not in ("host", "content-length"):
                headers[k] = v.strip()
    headers["accept"] = "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
    result["headers"] = headers

    return result


def validate_curl(c):
    print(f"  Cookies  : {len(c['cookies'])} found")
    if not c["cookies"]:
        print("  [WARN] No cookies — Cloudflare may block requests.")
    else:
        for k in list(c["cookies"].keys())[:3]:
            print(f"    {k}: {c['cookies'][k][:40]}")
    print()


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
    print(f"  Existing : {len(certs)} certificates in CSV")
    return certs


# ─────────────────────────────────────────────────────────────
#  HTML PARSER
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

        # Extract cert_no from URL
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
            "shape":     shape,
            "cert_no":   cert_no,
            "url":       url,
            "carat":     "",
            "color":     "",
            "clarity":   "",
            "cut":       "",
            "polish":    "",
            "symmetry":  "",
            "price_aud": "",
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
#  STEP 1 — FETCH: live HTML pages -> save as JSON
# ─────────────────────────────────────────────────────────────

def fetch_all_pages(curl, rotate_after_fails=None):
    os.makedirs(JSON_DIR, exist_ok=True)
    session = cf_requests.Session(impersonate="chrome")

    resume_shape, resume_page = load_checkpoint()
    total_saved = 0
    consecutive_fails = 0
    reached_resume = (resume_shape is None)  # True = no checkpoint, start normally

    for shape in SHAPES:
        # Skip shapes before the checkpoint shape
        if not reached_resume:
            if shape != resume_shape:
                log.info(f"[{shape}] skipping (before checkpoint)")
                continue
            reached_resume = True

        log.info(f"[{shape}]")
        page = resume_page if shape == resume_shape else 1
        resume_page = 1  # reset for subsequent shapes

        while True:
            params = {**FILTERS, "shape": shape, "page": str(page)}
            url = BASE_URL + "?" + urlencode(params)

            json_filename = f"{shape.lower()}_page{page:03d}.json"
            json_path = os.path.join(JSON_DIR, json_filename)

            if os.path.exists(json_path):
                log.info(f"  Page {page:>3}  -> cached, skip")
                page += 1
                consecutive_fails = 0
                continue

            try:
                r = session.get(
                    url,
                    headers=curl["headers"],
                    cookies=curl["cookies"],
                    timeout=30,
                )
            except Exception as e:
                log.warning(f"  [ERROR] Page {page}: {e}")
                time.sleep(3)
                break

            # 403 = Cloudflare blocked — count consecutive fails then rotate
            if r.status_code == 403:
                consecutive_fails += 1
                log.warning(
                    f"  [403] {shape} page {page} "
                    f"(consecutive: {consecutive_fails}/{rotate_after_fails or '∞'})"
                )
                if rotate_after_fails and consecutive_fails >= rotate_after_fails:
                    raise GSBlockedException(page, consecutive_fails)
                time.sleep(5)
                continue

            if r.status_code == 404:
                log.info(f"  Page {page:>3}  -> 404, no more pages for {shape}")
                break

            if r.status_code >= 400:
                log.warning(f"  [WARN] HTTP {r.status_code} on page {page}")
                time.sleep(2)
                break

            consecutive_fails = 0
            html = r.text

            # Check for products before saving
            soup = BeautifulSoup(html, "lxml")
            grid = soup.select_one("div.product-list")
            cards = grid.select("div.bg-white") if grid else []

            if not cards:
                log.info(f"  Page {page:>3}  -> no products, end of {shape}")
                break

            # Save HTML wrapped in same JSON format as your existing gs_pages
            with open(json_path, "w", encoding="utf-8") as jf:
                json.dump({"commands": [{"html": html}]}, jf)

            log.info(f"  Page {page:>3}  -> {len(cards)} cards  -> {json_filename}")
            total_saved += 1
            save_checkpoint(shape, page + 1)
            page += 1
            time.sleep(SLEEP_SEC)

    clear_checkpoint()
    log.info(f"Fetch done. {total_saved} new pages saved.")


# ─────────────────────────────────────────────────────────────
#  STEP 2 — PARSE: JSON files -> CSV rows
# ─────────────────────────────────────────────────────────────

def parse_all_json(existing_certs):
    files = sorted(f for f in os.listdir(JSON_DIR) if f.endswith(".json"))

    if not files:
        print("  No JSON files to parse.")
        return []

    parsed_files = []
    total_rows = 0

    with open(CSV_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)

        for filename in files:
            path = os.path.join(JSON_DIR, filename)

            # Shape from filename: round_page001.json -> Round
            shape_slug = filename.split("_page")[0]
            shape = next(
                (s for s in SHAPES if s.lower() == shape_slug.lower()),
                shape_slug.capitalize()
            )

            try:
                with open(path, "r", encoding="utf-8") as jf:
                    data = json.load(jf)
            except Exception as e:
                print(f"  [ERROR] {filename}: {e}")
                continue

            commands = data.get("commands", [])
            if not commands:
                print(f"  [SKIP]  {filename} — no commands")
                parsed_files.append(path)
                continue

            html = commands[0].get("html", "")
            if not html:
                print(f"  [SKIP]  {filename} — no html")
                parsed_files.append(path)
                continue

            rows, has_products = parse_html_to_rows(html, shape, existing_certs)

            if not has_products:
                print(f"  {filename:<35}  -> no product grid")
            else:
                writer.writerows(rows)
                print(f"  {filename:<35}  -> {len(rows):>3} new rows  [{shape}]")
                total_rows += len(rows)

            parsed_files.append(path)

    print()
    print(f"  Parse done. {total_rows} new rows -> {CSV_FILE}")
    print()
    return parsed_files


# ─────────────────────────────────────────────────────────────
#  STEP 3 — CLEANUP: delete JSON files
# ─────────────────────────────────────────────────────────────

def delete_json_files(file_paths):
    deleted = 0
    for path in file_paths:
        try:
            os.remove(path)
            deleted += 1
        except Exception as e:
            print(f"  [WARN] Could not delete {path}: {e}")

    try:
        if os.path.exists(JSON_DIR) and not os.listdir(JSON_DIR):
            os.rmdir(JSON_DIR)
            print(f"  Removed empty folder: {JSON_DIR}/")
    except Exception:
        pass

    print(f"  Deleted {deleted} JSON files.")
    print()


# ─────────────────────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────────────────────

def scrape(rotate_after_fails=None):
    print()
    print("=" * 60)
    print("  GS DIAMONDS SCRAPER")
    print("=" * 60)
    print()

    print("[1/4] Reading curl_gs.txt ...")
    curl = load_curl(CURL_FILE)
    validate_curl(curl)

    print("[2/4] Preparing CSV ...")
    ensure_csv()
    existing_certs = load_existing_certs()
    print()

    log.info("[3/4] Fetching pages (shape by shape) ...")
    fetch_all_pages(curl, rotate_after_fails=rotate_after_fails)

    print("[4/4] Parsing JSON -> CSV ...")
    print()
    parsed_files = parse_all_json(existing_certs)

    print("      Cleaning up JSON files ...")
    delete_json_files(parsed_files)

    print("=" * 60)
    print(f"  DONE  ->  {CSV_FILE}")
    print("=" * 60)
    print()


def run_with_vpn_rotation():
    from vpn_rotator import (
        VPNConnection, VPNError,
        find_openvpn_executable, is_admin,
        parse_credentials_file, write_auth_file,
    )

    log.info("=" * 60)
    log.info("GS Diamonds scraper with VPN rotation")
    log.info(f"Consecutive-fail threshold: {VPN_ROTATE_AFTER_FAILS}")
    log.info("=" * 60)

    if not is_admin():
        log.error(
            "OpenVPN requires Administrator/root. "
            "Re-run from an elevated shell, or use --no-vpn."
        )
        return

    try:
        find_openvpn_executable()
    except VPNError as exc:
        log.error(f"OpenVPN unavailable — aborting: {exc}")
        return

    if not VPN_DIR.exists():
        log.error(f"VPN config dir not found: {VPN_DIR}")
        return

    # ProtonVPN credentials
    proton_auth = None
    if PROTON_CREDS.exists():
        user, pwd = parse_credentials_file(PROTON_CREDS)
        proton_auth = write_auth_file(user, pwd, PROTON_CREDS.parent / "proton_auth.txt")
        log.info(f"Using ProtonVPN credentials from {PROTON_CREDS}")
    else:
        log.warning(f"ProtonVPN credentials not found at {PROTON_CREDS} — falling back to per-config lookup")

    configs = sorted(VPN_DIR.glob("*.ovpn"))
    if not configs:
        log.error(f"No .ovpn files in {VPN_DIR}")
        return

    random.shuffle(configs)
    total, deleted = len(configs), 0
    log.info(f"VPN pool: {total} config(s) in {VPN_DIR} — trying every config")

    for attempt, cfg in enumerate(configs, start=1):
        log.info(f"--- VPN attempt {attempt}/{total}: {cfg.name} ---")
        try:
            with VPNConnection(cfg, auth_file=proton_auth, log_file=OPENVPN_LOG) as vpn:
                log.info(f"Tunnel up. Exit IP = {vpn.ip_after}")
                scrape(rotate_after_fails=VPN_ROTATE_AFTER_FAILS)
            log.info("Scrape completed — VPN torn down.")
            return

        except GSBlockedException as e:
            log.warning(f"Rotating VPN (IP burned): {e}")

        except VPNError as e:
            if "authentication failed" in str(e).lower():
                log.error(
                    f"VPN auth failed on {cfg.name} — credentials likely stale. "
                    f"Aborting. Check proton_credentials.txt."
                )
                return
            log.error(f"VPN error on {cfg.name}: {e} — deleting dead config")
            try:
                cfg.unlink()
                deleted += 1
                log.info(f"Deleted non-working config: {cfg.name}")
            except OSError as del_exc:
                log.warning(f"Could not delete {cfg.name}: {del_exc}")

        except KeyboardInterrupt:
            log.warning("Interrupted — exiting VPN loop.")
            return

    log.error(
        f"Tried all {total} VPN config(s) — none completed the scrape "
        f"({deleted} dead config(s) deleted)."
    )


if __name__ == "__main__":
    try:
        if "--no-vpn" in sys.argv:
            scrape()
        else:
            run_with_vpn_rotation()
    except KeyboardInterrupt:
        log.warning("Ctrl+C — stopping GS scraper.")
        try:
            from vpn_rotator import kill_openvpn_processes
            kill_openvpn_processes()
        except Exception:
            pass