import csv, os, time, json, datetime, random, sys
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

import cv2, numpy as np, pandas as pd, psutil
import requests
from fake_useragent import UserAgent
from requests.adapters import HTTPAdapter

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from vpn_rotator import VPNConnection, VPNError, kill_openvpn_processes

# ── Config ────────────────────────────────────────────────────────
_AUS_DIR = str(Path(__file__).resolve().parent.parent / "diamond_files" / "aus")
os.makedirs(_AUS_DIR, exist_ok=True)
CSV_FILE            = os.path.join(_AUS_DIR, "cullen_diamonds.csv")
CHECKPOINT_FILE     = os.path.join(_AUS_DIR, "cullen_checkpoint.json")
API_LOG_FILE        = os.path.join(_AUS_DIR, "cullen_api_log.jsonl")
MAX_WORKERS         = 10
CPU_LIMIT           = 80
CHECKPOINT_MAX_DAYS = 2
CERT_BATCH_SIZE     = 100   # process certs in batches instead of per page
VPN_LOG_FILE        = Path(__file__).resolve().parent.parent / "vpnbook" / "openvpn.log"
BLOCK_STATUS_CODES  = {403, 429, 503}
MAX_CONSEC_ERRORS   = 3

# ── Shared session (connection pool reused across all requests) ───
_session = requests.Session()
_session.mount("https://", HTTPAdapter(pool_connections=MAX_WORKERS, pool_maxsize=MAX_WORKERS))

URL = "https://api.cullenjewellery.com/api/stone_preference_data/feed"

HEADERS = {
    "accept": "application/json",
    "content-type": "application/json",
    "origin": "https://cullenjewellery.com",
    "referer": "https://cullenjewellery.com/",
    "user-agent": UserAgent().random,
}

BASE_FILTER = {
    "filter": {
        "type": "diamond",
        "shapes": ["round","oval","emerald","radiant","pear","cushion",
                   "marquise",
                   "princess","asscher","heart"],
        "minCarat": 0.5,          "maxCarat": 10,
        "minClarity": "i1",     "maxClarity": "if1",
        "minColour": "j",       "maxColour": "d",
        "minCut": "ex",         "maxCut": "id",
        "moissaniteColours": ["colourless","clover_green","smokey_grey",
                              "sunshine_yellow","champagne","midnight_blue",
                              "seafoam_green","sandy_yellow"],
        "sapphireColours": ["kashmir_blue","sky_blue","royal_purple",
                            "scarlet_red","flamingo_pink","blossom_pink"],
        "fancyColours": ["fancy_pink"],
        "minFancyColour": "faint",  "maxFancyColour": "fancy_deep",
        "minDwRatio": 0.5,  "maxDwRatio": 0.9,
        "minLwRatio": 0.5,  "maxLwRatio": 2.5,
        "minTwRatio": 0.5,  "maxTwRatio": 0.9,
    },
    "sort": "recommended",
    "addedRanOut": False,
    "currency": "USD",
}

FIELDS = [
    "id","price","dimensions","carat","length","width","depth","table",
    "dw_ratio","lw_ratio","tw_ratio","is_wide","shape","cut","colour",
    "colour_rank","colour_intensity","clarity","clarity_rank","polish",
    "symm","culet","lab","certificate_number",
]

_qr_detector = cv2.QRCodeDetector()

def tprint(msg):
    print(f"[{datetime.datetime.now().strftime('%H:%M:%S')}] {msg}")

# ── API call log (persistent, never expires) ──────────────────────
def log_api_call(page, start, stones_count, new_inserted):
    entry = {
        "page": page,
        "start": start,
        "timestamp": datetime.datetime.now().isoformat(),
        "stones_count": stones_count,
        "new_inserted": new_inserted,
    }
    with open(API_LOG_FILE, "a", encoding="utf-8") as f:
        f.write(json.dumps(entry) + "\n")
    tprint(f"  [log] page={page} start={start} stones={stones_count} new={new_inserted}")

def _last_logged_position():
    """Return (next_page, next_start) from the last API log entry, or None."""
    if not os.path.exists(API_LOG_FILE):
        return None
    try:
        # Read only the tail of the log file to avoid loading gigabytes into memory
        with open(API_LOG_FILE, "rb") as f:
            f.seek(0, 2)
            size = f.tell()
            if size == 0:
                return None
            f.seek(max(0, size - 4096))
            tail = f.read().decode("utf-8", errors="replace")
        lines = [l.strip() for l in tail.splitlines() if l.strip()]
        if len(lines) < 2:
            tprint(f"API log has only {len(lines)} entry — starting from page 1.")
            return None
        last = json.loads(lines[-1])
        next_page  = last["page"]  + 1
        next_start = last["start"] + 20
        tprint(f"API log fallback: last completed page {last['page']} "
              f"({last['timestamp'][:10]}) — resuming from page {next_page}")
        return next_page, next_start
    except Exception as e:
        tprint(f"API log read error: {e}")
        return None

# ── Checkpoint helpers ────────────────────────────────────────────
def load_checkpoint():
    if not os.path.exists(CHECKPOINT_FILE):
        return _last_logged_position() or (1, 0)
    try:
        with open(CHECKPOINT_FILE) as f:
            cp = json.load(f)
        age = (datetime.datetime.now() - datetime.datetime.fromisoformat(cp["timestamp"])).days
        if age >= CHECKPOINT_MAX_DAYS:
            os.remove(CHECKPOINT_FILE)
            tprint(f"Checkpoint is {age} day(s) old — falling back to API log.")
            return _last_logged_position() or (1, 0)
        tprint(f"Resuming from page {cp['page']} (age: {age} day(s))")
        return cp["page"], cp["start"]
    except Exception as e:
        tprint(f"Checkpoint error: {e} — falling back to API log.")
        return _last_logged_position() or (1, 0)

def save_checkpoint(page, start):
    with open(CHECKPOINT_FILE, "w") as f:
        json.dump({"page": page, "start": start,
                   "timestamp": datetime.datetime.now().isoformat()}, f)

def clear_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        os.remove(CHECKPOINT_FILE)
        tprint("Checkpoint cleared.")

# ── Helpers ───────────────────────────────────────────────────────
def wait_for_cpu():
    while psutil.cpu_percent(interval=0.5) > CPU_LIMIT:
        time.sleep(1)

def get_certificate(diamond_id):
    try:
        r = _session.get(
            f"https://diamonds.cullenjewellery.com/{diamond_id}/cert.jpeg",
            timeout=10,
        )
        if r.status_code != 200:
            return diamond_id, None
        img = cv2.imdecode(np.frombuffer(r.content, np.uint8), cv2.IMREAD_GRAYSCALE)
        text, _, _ = _qr_detector.detectAndDecode(img)
        return diamond_id, text.split("=")[-1] if text else None
    except Exception:
        return diamond_id, None

def build_row(d):
    row = {"certificate_number": None}
    for field in FIELDS:
        if field == "certificate_number":
            continue
        val = d.get(field)
        # API returns dimensions (and occasionally other fields) as dicts/lists.
        # Serialize them to JSON so commas inside never break the CSV column count.
        if isinstance(val, (dict, list)):
            val = json.dumps(val, separators=(",", ":"))
        row[field] = val
    return row

def load_existing_ids():
    if not os.path.exists(CSV_FILE):
        return set()
    with open(CSV_FILE, "r", encoding="utf-8") as f:
        return {row["id"] for row in csv.DictReader(f)}

# ── Certificate processing ────────────────────────────────────────
def process_certificates(all_ids: list):
    if not all_ids or not os.path.exists(CSV_FILE):
        return

    tprint(f"Scanning {len(all_ids)} certificate(s)...")
    df = pd.read_csv(CSV_FILE, dtype=str, on_bad_lines="warn")
    if "certificate_number" not in df.columns:
        df["certificate_number"] = pd.NA

    id_col = df["id"].astype(str)
    id_set = set(id_col)  # O(1) membership checks
    ids = [i for i in all_ids if i in id_set]
    if not ids:
        tprint("No matching IDs in CSV — skipping cert scan.")
        return

    wait_for_cpu()
    with ThreadPoolExecutor(MAX_WORKERS) as pool:
        results = dict(pool.map(get_certificate, ids))

    found     = {did: cert for did, cert in results.items() if cert}
    not_found = set(ids) - set(found)

    if found:
        # Vectorized update: one pass over the column instead of N individual loc calls
        update_mask = id_col.isin(found)
        df.loc[update_mask, "certificate_number"] = id_col[update_mask].map(found)
        for did, cert in found.items():
            tprint(f"  {did} -> {cert}")

    if not_found:
        df = df[~id_col.isin(not_found)]
        tprint(f"Removed {len(not_found)} row(s) with no certificate.")

    df.to_csv(CSV_FILE, index=False)
    tprint(f"Cert scan done | Collected: {len(found)}/{len(ids)}")

# ── VPN helpers ───────────────────────────────────────────────────
def _iter_vpn_configs():
    """Yield .ovpn paths endlessly, reshuffling each full pass.

    Primary:  vpnbook/scraper_map.json  — configs listed under key "cullen"
    Fallback: vpnbook/ root             — all .ovpn files (before map is built)
    """
    vpn_root = Path(__file__).resolve().parent.parent / "vpnbook"
    map_file = vpn_root / "scraper_map.json"
    configs: list[Path] = []

    if map_file.exists():
        try:
            data  = json.loads(map_file.read_text(encoding="utf-8"))
            names = data.get("by_scraper", {}).get("cullen", [])
            configs = [vpn_root / n for n in names if (vpn_root / n).exists()]
            if configs:
                tprint(f"scraper_map: {len(configs)} cullen config(s)")
        except Exception as e:
            tprint(f"scraper_map read error: {e} — falling back to root")

    if not configs:
        configs = sorted(vpn_root.glob("*.ovpn"))
        if not configs:
            raise VPNError(
                f"No .ovpn files in {vpn_root}.\n"
                "Run sort_vpn_configs.py --apply to build the scraper map."
            )
        tprint(f"VPN fallback: {len(configs)} config(s) from {vpn_root}")

    while True:
        random.shuffle(configs)
        yield from configs

# ── Main scrape loop ──────────────────────────────────────────────
def fetch_all_diamonds():
    existing_ids = load_existing_ids()
    file_exists  = os.path.exists(CSV_FILE)
    page, start  = load_checkpoint()

    try:
        for cfg in _iter_vpn_configs():
            tprint(f"Connecting VPN: {cfg.name}...")
            try:
                conn = VPNConnection(cfg, verify_ip=True, log_file=VPN_LOG_FILE).connect()
            except VPNError as e:
                tprint(f"VPN connect failed ({cfg.name}): {e} — trying next...")
                continue

            HEADERS["user-agent"] = UserAgent().random  # fresh UA per VPN session
            tprint(f"VPN up: {cfg.name} | exit IP: {conn.ip_after}")
            scrape_done      = False
            blocked          = False
            consec_errors    = 0
            pages_this_vpn   = 0
            pending_cert_ids: list[str] = []

            try:
                # Keep the CSV writer open for the full VPN session — one open/close
                # instead of one per page, which matters when pages run into the hundreds.
                with open(CSV_FILE, "a" if file_exists else "w", newline="", encoding="utf-8") as csv_f:
                    writer = csv.DictWriter(csv_f, fieldnames=FIELDS, quoting=csv.QUOTE_ALL)
                    if not file_exists:
                        writer.writeheader()
                        file_exists = True

                    while not blocked:
                        payload = {**BASE_FILTER, "start": start}
                        try:
                            # json= serialises once internally; avoids the manual json.dumps
                            r = _session.post(URL, headers=HEADERS, json=payload, timeout=15)
                        except Exception as e:
                            consec_errors += 1
                            tprint(f"Request error ({consec_errors}/{MAX_CONSEC_ERRORS}): {e}")
                            if consec_errors >= MAX_CONSEC_ERRORS:
                                tprint("Too many errors — rotating VPN...")
                                blocked = True
                            else:
                                time.sleep(5)
                            continue

                        if r.status_code in BLOCK_STATUS_CODES:
                            tprint(f"HTTP {r.status_code} — blocked, rotating VPN...")
                            blocked = True
                            continue

                        if r.status_code != 200:
                            consec_errors += 1
                            tprint(f"HTTP {r.status_code} ({consec_errors}/{MAX_CONSEC_ERRORS}) — retrying...")
                            if consec_errors >= MAX_CONSEC_ERRORS:
                                tprint("Too many errors — rotating VPN...")
                                blocked = True
                            else:
                                time.sleep(5)
                            continue

                        try:
                            stones = r.json().get("stones", [])
                        except Exception:
                            consec_errors += 1
                            tprint(f"Invalid JSON ({consec_errors}/{MAX_CONSEC_ERRORS}) — retrying...")
                            if consec_errors >= MAX_CONSEC_ERRORS:
                                blocked = True
                            else:
                                time.sleep(5)
                            continue

                        consec_errors = 0  # reset on any successful response

                        if not stones:
                            tprint("Finished downloading.")
                            clear_checkpoint()
                            scrape_done = True
                            break

                        new_ids = []
                        for s in stones:
                            d = s.get("info")
                            if not d:
                                continue
                            did = str(d.get("id"))
                            if did in existing_ids:
                                continue
                            writer.writerow(build_row(d))
                            existing_ids.add(did)
                            new_ids.append(did)

                        pages_this_vpn += 1
                        tprint(f"Page {page} | Inserted {len(new_ids)} | Pages on VPN: {pages_this_vpn}")
                        log_api_call(page, start, len(stones), len(new_ids))
                        page  += 1
                        start += 20
                        save_checkpoint(page, start)

                        pending_cert_ids.extend(new_ids)
                        if len(pending_cert_ids) >= CERT_BATCH_SIZE:
                            csv_f.flush()  # ensure rows are on disk before cert scan reads the file
                            process_certificates(pending_cert_ids)
                            pending_cert_ids = []

            finally:
                # Flush any remaining certs before disconnecting
                if pending_cert_ids:
                    process_certificates(pending_cert_ids)
                conn.disconnect()
                tprint(f"VPN down: {cfg.name} | served {pages_this_vpn} page(s)")

            if scrape_done:
                break

            if blocked:
                tprint("Rotating VPN due to block...")

    except KeyboardInterrupt:
        tprint("Interrupted — cleaning up VPN...")
        kill_openvpn_processes()
        raise

if __name__ == "__main__":
    fetch_all_diamonds()
