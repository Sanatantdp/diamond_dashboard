import json
import os
import datetime
from threading import Lock

STATUS_DIR = os.path.join(os.getcwd(), "status")
os.makedirs(STATUS_DIR, exist_ok=True)

_locks: dict[str, Lock] = {}


def _get_lock(name: str) -> Lock:
    if name not in _locks:
        _locks[name] = Lock()
    return _locks[name]


def _status_path(scraper_name: str) -> str:
    return os.path.join(STATUS_DIR, f"{scraper_name}_status.json")


def write_status(
    scraper_name: str,
    *,
    state: str,                  # "idle" | "running" | "done" | "failed"
    current_page: int   = 0,
    total_pages: int    = 0,
    diamonds_saved: int = 0,
    duplicates: int     = 0,
    failed_pages: int   = 0,
    message: str        = "",
    extra: dict         = None,
):
    """
    Write / overwrite the status JSON for a scraper.
    Thread-safe — called from worker threads.
    """
    pct = round(current_page / total_pages * 100, 1) if total_pages > 0 else 0.0

    payload = {
        "scraper":        scraper_name,
        "state":          state,
        "current_page":   current_page,
        "total_pages":    total_pages,
        "pct_complete":   pct,
        "diamonds_saved": diamonds_saved,
        "duplicates":     duplicates,
        "failed_pages":   failed_pages,
        "message":        message,
        "updated_at":     datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    if extra:
        payload.update(extra)

    path = _status_path(scraper_name)
    with _get_lock(scraper_name):
        try:
            tmp = path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(payload, f, indent=2, ensure_ascii=False)
            os.replace(tmp, path)
        except Exception as e:
            pass   # never crash the scraper because of a status write


def read_status(scraper_name: str) -> dict:
    """Read the current status for a scraper (returns empty dict if missing)."""
    path = _status_path(scraper_name)
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def read_all_statuses() -> dict:
    """Read status files for all scrapers found in the status folder."""
    result = {}
    if not os.path.exists(STATUS_DIR):
        return result
    for fname in os.listdir(STATUS_DIR):
        if fname.endswith("_status.json"):
            name = fname.replace("_status.json", "")
            result[name] = read_status(name)
    return result