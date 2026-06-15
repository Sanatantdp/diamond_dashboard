import asyncio
import csv, os, time, json, datetime
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import aiohttp
import cv2, numpy as np, pandas as pd, psutil
import requests
from fake_useragent import UserAgent

_ua = UserAgent()
from requests.adapters import HTTPAdapter

_executor = ThreadPoolExecutor(max_workers=os.cpu_count() or 4)

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
CERT_TIMEOUT        = aiohttp.ClientTimeout(connect=3, sock_read=5)
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
    "user-agent": _ua.random,
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
    while psutil.cpu_percent(interval=0.1) > CPU_LIMIT:
        time.sleep(1)

def _backoff(attempt: int):
    time.sleep(min(5 * (2 ** attempt), 60))

def _decode_qr(data: bytes):
    """CPU-bound — runs in thread pool so it never blocks the event loop."""
    img = cv2.imdecode(np.frombuffer(data, np.uint8), cv2.IMREAD_GRAYSCALE)
    if img is None:
        return None
    text, _, _ = _qr_detector.detectAndDecode(img)
    return text.split("=")[-1] if text else None

async def _fetch_cert_async(session, sem, loop, diamond_id):
    try:
        async with sem:
            async with session.get(
                f"https://diamonds.cullenjewellery.com/{diamond_id}/cert.jpeg"
            ) as r:
                if r.status != 200:
                    return diamond_id, None
                data = await r.read()
        cert = await loop.run_in_executor(_executor, _decode_qr, data)
        return diamond_id, cert
    except Exception:
        return diamond_id, None

async def _fetch_certs_async(ids):
    sem  = asyncio.Semaphore(50)
    loop = asyncio.get_event_loop()
    connector = aiohttp.TCPConnector(limit=200, limit_per_host=100)
    async with aiohttp.ClientSession(connector=connector, timeout=CERT_TIMEOUT) as session:
        return await asyncio.gather(*[_fetch_cert_async(session, sem, loop, did) for did in ids])

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
_CERT_SCAN_CHUNK = 300

def process_certificates(all_ids: list):
    if not all_ids or not os.path.exists(CSV_FILE):
        return

    tprint(f"Scanning {len(all_ids)} certificate(s)...")
    df = pd.read_csv(CSV_FILE, dtype=str, on_bad_lines="warn")
    if "certificate_number" not in df.columns:
        df["certificate_number"] = pd.NA

    id_col = df["id"].astype(str)
    ids = [i for i in all_ids if i in set(id_col)]
    if not ids:
        tprint("No matching IDs in CSV — skipping cert scan.")
        return

    total_found = total_removed = 0

    for chunk_start in range(0, len(ids), _CERT_SCAN_CHUNK):
        chunk = ids[chunk_start: chunk_start + _CERT_SCAN_CHUNK]
        wait_for_cpu()
        results = dict(asyncio.run(_fetch_certs_async(chunk)))

        found     = {did: cert for did, cert in results.items() if cert}
        not_found = set(chunk) - set(found)

        if found:
            update_mask = id_col.isin(found)
            df.loc[update_mask, "certificate_number"] = id_col[update_mask].map(found)
            for did, cert in found.items():
                tprint(f"  {did} -> {cert}")

        if not_found:
            df     = df[~id_col.isin(not_found)]
            id_col = df["id"].astype(str)
            tprint(f"  Removed {len(not_found)} row(s) with no certificate.")

        total_found   += len(found)
        total_removed += len(not_found)
        df.to_csv(CSV_FILE, index=False)
        tprint(f"  Progress: {chunk_start + len(chunk)}/{len(ids)} scanned")

    tprint(f"Cert scan done | Collected: {total_found} | Removed: {total_removed} | Total: {len(ids)}")

# ── Main scrape loop ──────────────────────────────────────────────
def _ids_missing_cert() -> list:
    """Return IDs already in the CSV that have no certificate number."""
    if not os.path.exists(CSV_FILE):
        return []
    try:
        df = pd.read_csv(CSV_FILE, dtype=str, usecols=["id", "certificate_number"])
        mask = df["certificate_number"].isna() | (df["certificate_number"].str.strip() == "")
        ids = df.loc[mask, "id"].tolist()
        if ids:
            tprint(f"Found {len(ids)} existing row(s) with no certificate — will scan.")
        return ids
    except Exception as e:
        tprint(f"Could not check missing certs: {e}")
        return []

def fetch_all_diamonds():
    existing_ids     = load_existing_ids()
    file_exists      = os.path.exists(CSV_FILE)
    page, start      = load_checkpoint()
    consec_errors    = 0
    pending_cert_ids: list[str] = _ids_missing_cert()

    with open(CSV_FILE, "a" if file_exists else "w", newline="", encoding="utf-8") as csv_f:
        writer = csv.DictWriter(csv_f, fieldnames=FIELDS, quoting=csv.QUOTE_ALL)
        if not file_exists:
            writer.writeheader()

        while True:
            HEADERS["user-agent"] = _ua.random
            payload = {**BASE_FILTER, "start": start}
            try:
                r = _session.post(URL, headers=HEADERS, json=payload, timeout=15)
            except Exception as e:
                consec_errors += 1
                tprint(f"Request error ({consec_errors}/{MAX_CONSEC_ERRORS}): {e}")
                if consec_errors >= MAX_CONSEC_ERRORS:
                    tprint("Too many errors — stopping.")
                    break
                time.sleep(5)
                continue

            if r.status_code in BLOCK_STATUS_CODES:
                tprint(f"HTTP {r.status_code} — blocked, stopping.")
                break

            if r.status_code != 200:
                consec_errors += 1
                tprint(f"HTTP {r.status_code} ({consec_errors}/{MAX_CONSEC_ERRORS}) — retrying...")
                if consec_errors >= MAX_CONSEC_ERRORS:
                    tprint("Too many errors — stopping.")
                    break
                time.sleep(5)
                continue

            try:
                stones = r.json().get("stones", [])
            except Exception:
                consec_errors += 1
                tprint(f"Invalid JSON ({consec_errors}/{MAX_CONSEC_ERRORS}) — retrying...")
                if consec_errors >= MAX_CONSEC_ERRORS:
                    break
                time.sleep(5)
                continue

            consec_errors = 0

            if not stones:
                tprint("Finished downloading.")
                clear_checkpoint()
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

            tprint(f"Page {page} | Inserted {len(new_ids)}")
            log_api_call(page, start, len(stones), len(new_ids))
            page  += 1
            start += 20
            save_checkpoint(page, start)

            pending_cert_ids.extend(new_ids)
            if len(pending_cert_ids) >= CERT_BATCH_SIZE:
                csv_f.flush()
                process_certificates(pending_cert_ids)
                pending_cert_ids = []

    if pending_cert_ids:
        process_certificates(pending_cert_ids)

if __name__ == "__main__":
    fetch_all_diamonds()
