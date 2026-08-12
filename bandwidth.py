"""
bandwidth.py
============

Measures how much internet each scraper actually downloads, per day.

The scrapers are separate processes launched by scheduler.py, and between them
they use three different HTTP stacks (curl_cffi, requests, aiohttp). Rather
than editing every call site, this module wraps the one choke point in each
stack and counts bytes as responses come back. A scraper opts in with a single
line near the top of its main entry point:

    import bandwidth
    bandwidth.track("novita", "germany")     # or bandwidth.track() to auto-detect

From then on every response that scraper receives is counted, attributed to
that (origin, scraper) pair and to the calendar day it arrived on.

How the numbers are produced
----------------------------
Per response we add:
  * body   — the Content-Length header when the server sends one (that is the
             real number of bytes on the wire, already compressed), otherwise
             the decompressed body length as an upper bound.
  * headers — the status line and header block, roughly.
Uploads are counted the same way from the request side.

So the figure is "HTTP bytes", not "bytes billed by your ISP": TLS handshakes,
TCP/IP overhead and retried connections are not visible from inside the
process. Responses without a Content-Length (chunked transfer) are counted
decompressed, which over-states them. Both effects are noted in the payload as
`estimated_from_body` so the dashboard can be honest about it.

Storage
-------
Each process owns its own file and rewrites it atomically on a timer and at
exit, so concurrent scrapers never interleave writes:

    bandwidth/<YYYY-MM-DD>/<origin>__<scraper>__<pid>.json

`daily_usage()` sums those files back up; `upload_origin_bandwidth()` pushes
them into the shared "bandwidth_usage" Postgres table, which is what the
dashboards read.
"""

from __future__ import annotations

import atexit
import json
import os
import sys
import threading
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from urllib.parse import urlsplit

REPO_ROOT = Path(__file__).resolve().parent
BANDWIDTH_DIR = REPO_ROOT / "bandwidth"

# How often the in-memory counters are written out. A scraper run can last
# hours, so we checkpoint rather than only writing at exit — a killed process
# (VPN rotation, Ctrl-C, crash) still leaves its usage on disk.
FLUSH_SECONDS = 30

# How often a running scraper pushes its usage into the "bandwidth_usage"
# Postgres table, so the deployed dashboard shows a run while it is still
# going instead of only after the pipeline's loader finishes.
#
# The deployed API cannot read the local bandwidth/ files — it runs on another
# host — so this table is the only way those numbers reach it.
#
# Half an hour: frequent enough that the live page tracks a run in progress,
# rare enough to be a negligible write load (~40 rows per upload).
#
# Override with BANDWIDTH_UPLOAD_MINUTES; 0 disables in-scraper uploading
# entirely, leaving the region loader to upload at the end of the pipeline.
UPLOAD_MINUTES = int(os.environ.get("BANDWIDTH_UPLOAD_MINUTES", "30"))

# Local JSON is pruned after this many days; the Postgres table keeps the full
# history, so nothing is actually lost.
KEEP_LOCAL_DAYS = 120

# Folder name -> origin key, for auto-detection.
_DIR_TO_ORIGIN = {
    "usa_compare":       "usa",
    "uk_scrapping":      "uk",
    "aus_scrapping":     "aus",
    "germany_scrapping": "germany",
}

ORIGINS = ("usa", "uk", "aus", "germany")

# Endpoint bookkeeping. Usage is counted per API endpoint, not just per
# scraper, so you can see which call is actually consuming the bandwidth.
UNKNOWN_ENDPOINT = "(unknown)"
OTHER_ENDPOINT = "(other)"
MAX_TRACKED_ENDPOINTS = 300     # per process; overflow folds into "(other)"


# ── Process-wide state ───────────────────────────────────────────────────────
_lock = threading.Lock()
# (day, origin, scraper, endpoint) -> {"requests": n, "bytes_down": n,
#                       "bytes_up": n, "responses_without_length": n}
_counters: dict[tuple[str, str, str, str], dict[str, int]] = {}

_current: tuple[str, str] | None = None   # (origin, scraper) being attributed
_installed = False
_flusher: threading.Timer | None = None
_started_at: str | None = None
_last_upload = 0.0


# ══════════════════════════════════════════════════════════════════════════════
# Counting
# ══════════════════════════════════════════════════════════════════════════════

def _header_bytes(headers) -> int:
    """Rough size of a header block: 'Name: value\\r\\n' per header."""
    if not headers:
        return 0
    try:
        items = headers.items()
    except AttributeError:
        return 0
    total = 0
    for k, v in items:
        try:
            total += len(str(k)) + len(str(v)) + 4
        except Exception:
            continue
    return total + 2   # trailing CRLF


def _content_length(headers) -> int | None:
    if not headers:
        return None
    try:
        raw = headers.get("content-length") or headers.get("Content-Length")
    except Exception:
        return None
    try:
        return int(raw)
    except (TypeError, ValueError):
        return None


def normalize_endpoint(url) -> str:
    """
    Reduce a request URL to a stable 'host/path' key.

    Scrapers walk thousands of pages, so the raw URL would be a distinct key
    per request. The query string is dropped and numeric or id-shaped path
    segments are collapsed, which folds .../diamonds?page=812 and
    .../product/99413 onto one row each — the endpoint, not the call.
    """
    try:
        s = urlsplit(str(url))
    except Exception:
        return UNKNOWN_ENDPOINT
    if not s.netloc:
        return UNKNOWN_ENDPOINT

    parts = []
    for seg in s.path.split("/"):
        if not seg:
            continue
        if seg.isdigit():
            parts.append("{n}")
        elif len(seg) >= 24 and not any(c in seg for c in ".-_"):
            parts.append("{id}")          # long opaque token, e.g. a hash
        else:
            parts.append(seg)
    return f"{s.netloc}/{'/'.join(parts)}"[:200]


def record(down: int, up: int = 0, *, endpoint: str = UNKNOWN_ENDPOINT,
           had_length: bool = True) -> None:
    """Add one response's bytes to the current attribution. Never raises —
    accounting must not be able to break a scrape."""
    if _current is None:
        return
    origin, scraper = _current
    today = date.today().isoformat()
    key = (today, origin, scraper, endpoint or UNKNOWN_ENDPOINT)
    with _lock:
        # Defensive cap: a URL shape the normaliser doesn't fold would
        # otherwise grow this dict without bound over a long run.
        if key not in _counters and len(_counters) >= MAX_TRACKED_ENDPOINTS:
            key = (today, origin, scraper, OTHER_ENDPOINT)
        c = _counters.setdefault(
            key,
            {"requests": 0, "bytes_down": 0, "bytes_up": 0,
             "responses_without_length": 0},
        )
        c["requests"] += 1
        c["bytes_down"] += max(0, int(down or 0))
        c["bytes_up"] += max(0, int(up or 0))
        if not had_length:
            c["responses_without_length"] += 1


def _record_response(resp, req_headers=None, req_body=None) -> None:
    """Measure a requests/curl_cffi-style response object."""
    try:
        headers = getattr(resp, "headers", None)
        clen = _content_length(headers)
        if clen is None:
            body = getattr(resp, "content", b"") or b""
            down = len(body)
            had_length = False
        else:
            down = clen
            had_length = True
        down += _header_bytes(headers)

        up = _header_bytes(req_headers)
        if req_body is not None:
            try:
                up += len(req_body) if isinstance(req_body, (bytes, bytearray, str)) else 0
            except Exception:
                pass
        record(down, up,
               endpoint=normalize_endpoint(getattr(resp, "url", None)),
               had_length=had_length)
    except Exception:
        pass


# ══════════════════════════════════════════════════════════════════════════════
# Instrumentation — one wrapper per HTTP stack
# ══════════════════════════════════════════════════════════════════════════════

def _install_curl_cffi() -> bool:
    """curl_cffi: every module-level get/post routes through Session.request."""
    try:
        from curl_cffi import requests as cr
    except Exception:
        return False
    Session = getattr(cr, "Session", None)
    if Session is None or getattr(Session.request, "_bw_wrapped", False):
        return False

    original = Session.request

    def request(self, *args, **kwargs):
        resp = original(self, *args, **kwargs)
        _record_response(resp, kwargs.get("headers"),
                         kwargs.get("data") or kwargs.get("json"))
        return resp

    request._bw_wrapped = True
    Session.request = request
    return True


def _install_requests() -> bool:
    """requests: Session.send is below every helper, including requests.get()."""
    try:
        import requests as rq
    except Exception:
        return False
    if getattr(rq.Session.send, "_bw_wrapped", False):
        return False

    original = rq.Session.send

    def send(self, request, **kwargs):
        resp = original(self, request, **kwargs)
        _record_response(resp, getattr(request, "headers", None),
                         getattr(request, "body", None))
        return resp

    send._bw_wrapped = True
    rq.Session.send = send
    return True


def _install_aiohttp() -> bool:
    """aiohttp: text()/json() both go through ClientResponse.read()."""
    try:
        import aiohttp
    except Exception:
        return False
    ClientResponse = getattr(aiohttp, "ClientResponse", None)
    if ClientResponse is None or getattr(ClientResponse.read, "_bw_wrapped", False):
        return False

    original = ClientResponse.read

    async def read(self):
        body = await original(self)
        try:
            headers = getattr(self, "headers", None)
            clen = _content_length(headers)
            had_length = clen is not None
            down = (clen if had_length else len(body or b"")) + _header_bytes(headers)
            record(down, 0,
                   endpoint=normalize_endpoint(getattr(self, "url", None)),
                   had_length=had_length)
        except Exception:
            pass
        return body

    read._bw_wrapped = True
    ClientResponse.read = read
    return True


def _install_all() -> list[str]:
    return [name for name, fn in (
        ("curl_cffi", _install_curl_cffi),
        ("requests",  _install_requests),
        ("aiohttp",   _install_aiohttp),
    ) if fn()]


# ══════════════════════════════════════════════════════════════════════════════
# Public entry point
# ══════════════════════════════════════════════════════════════════════════════

def _detect() -> tuple[str, str]:
    """Infer (origin, scraper) from the running script's path."""
    main = sys.modules.get("__main__")
    path = Path(getattr(main, "__file__", "") or sys.argv[0] or "unknown.py").resolve()
    scraper = path.stem
    origin = "unknown"
    for part in path.parts:
        if part in _DIR_TO_ORIGIN:
            origin = _DIR_TO_ORIGIN[part]
            break
    return origin, scraper


def track(scraper: str | None = None, origin: str | None = None) -> None:
    """
    Start counting this process's HTTP traffic against (origin, scraper).

    Safe to call more than once; the wrappers are installed only on the first
    call and simply re-point at the new attribution afterwards.
    """
    global _current, _installed, _started_at

    det_origin, det_scraper = _detect()
    _current = (origin or det_origin, scraper or det_scraper)

    if not _installed:
        global _last_upload
        stacks = _install_all()
        _installed = True
        _started_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        _last_upload = time.time()      # first push one interval from now
        atexit.register(_on_exit)
        _schedule_flush()
        every = (f"every {UPLOAD_MINUTES} min" if UPLOAD_MINUTES > 0
                 else "disabled (loader uploads at end of pipeline)")
        print(f"[bandwidth] tracking {_current[0]}/{_current[1]} "
              f"via {', '.join(stacks) or 'no HTTP stack found'} "
              f"| db upload {every}")


def _schedule_flush() -> None:
    global _flusher
    _flusher = threading.Timer(FLUSH_SECONDS, _tick)
    _flusher.daemon = True   # never hold the process open
    _flusher.start()


def _tick() -> None:
    flush()
    maybe_upload()
    _schedule_flush()


def _on_exit() -> None:
    """Final checkpoint: write the files, then push the run's totals so the
    deployed dashboard ends up with the complete picture even if the interval
    hadn't come round again."""
    flush()
    maybe_upload(force=True)


def maybe_upload(force: bool = False) -> None:
    """
    Push this origin's usage into the "bandwidth_usage" table.

    Uploads the whole origin (every scraper's files), not just this process's,
    so whichever scraper's timer fires writes a complete, current picture for
    the region. The delete+insert runs in one transaction, so concurrent
    scrapers can interleave without leaving a partial state behind.

    Never raises: a database hiccup must not disturb a scrape in progress.
    """
    global _last_upload
    if UPLOAD_MINUTES <= 0 or _current is None:
        return
    now = time.time()
    if not force and now - _last_upload < UPLOAD_MINUTES * 60:
        return
    _last_upload = now

    origin = _current[0]
    try:
        conn = _connect()
        try:
            with conn.cursor() as cur:
                upload_origin_bandwidth(cur, origin)
            conn.commit()
        finally:
            conn.close()
    except Exception as e:
        print(f"[bandwidth] db upload skipped ({type(e).__name__}: {e})")


# ══════════════════════════════════════════════════════════════════════════════
# Persistence
# ══════════════════════════════════════════════════════════════════════════════

def flush() -> None:
    """Write this process's counters out, one file per day touched."""
    with _lock:
        snapshot = {k: dict(v) for k, v in _counters.items()}
    if not snapshot:
        return

    pid = os.getpid()
    # (day, origin, scraper) -> {"totals": {...}, "endpoints": {ep: {...}}}
    by_file: dict[tuple[str, str, str], dict] = {}
    for (day, origin, scraper, endpoint), c in snapshot.items():
        f = by_file.setdefault((day, origin, scraper), {
            "totals": {"requests": 0, "bytes_down": 0, "bytes_up": 0,
                       "responses_without_length": 0},
            "endpoints": {},
        })
        f["endpoints"][endpoint] = dict(c)
        for k, v in c.items():
            f["totals"][k] += v

    for (day, origin, scraper), agg in by_file.items():
        out_dir = BANDWIDTH_DIR / day
        try:
            out_dir.mkdir(parents=True, exist_ok=True)
            # One file per process, rewritten in full — concurrent scrapers can
            # never interleave, and a partial write can't corrupt a previous
            # good one (write temp, then atomic replace).
            path = out_dir / f"{origin}__{scraper}__{pid}.json"
            payload = {
                "day": day, "pid": pid, "started_at": _started_at,
                "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "origin": origin, "scraper": scraper,
                **agg["totals"],
                "endpoints": agg["endpoints"],
            }
            tmp = path.with_suffix(".json.tmp")
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(payload, f, indent=2)
            os.replace(tmp, path)
        except Exception:
            pass   # accounting must never break a scrape


def prune_local(keep_days: int = KEEP_LOCAL_DAYS) -> int:
    """Drop local day folders older than keep_days (Postgres keeps history)."""
    if not BANDWIDTH_DIR.exists():
        return 0
    cutoff = date.today() - timedelta(days=keep_days)
    removed = 0
    for d in BANDWIDTH_DIR.iterdir():
        if not d.is_dir():
            continue
        try:
            if date.fromisoformat(d.name) >= cutoff:
                continue
        except ValueError:
            continue
        for f in d.iterdir():
            f.unlink(missing_ok=True)
        d.rmdir()
        removed += 1
    return removed


def daily_usage(origin: str | None = None) -> list[dict]:
    """
    Aggregate the local files into one row per (day, origin, scraper, endpoint).

    Files written before endpoint tracking existed have no "endpoints" key —
    and a scraper started before this change keeps writing that shape until it
    restarts. Those are folded under a single "(unknown)" endpoint so an
    in-flight run's totals are still counted rather than dropped.
    """
    if not BANDWIDTH_DIR.exists():
        return []

    agg: dict[tuple[str, str, str, str], dict] = {}
    for day_dir in sorted(BANDWIDTH_DIR.iterdir()):
        if not day_dir.is_dir():
            continue
        for f in day_dir.glob("*.json"):
            try:
                with open(f, encoding="utf-8") as fh:
                    d = json.load(fh)
            except Exception:
                continue
            if origin and d.get("origin") != origin:
                continue

            day = d.get("day") or day_dir.name
            org = d.get("origin", "unknown")
            scr = d.get("scraper", "unknown")

            endpoints = d.get("endpoints")
            if not isinstance(endpoints, dict) or not endpoints:
                endpoints = {UNKNOWN_ENDPOINT: {
                    "requests": d.get("requests"),
                    "bytes_down": d.get("bytes_down"),
                    "bytes_up": d.get("bytes_up"),
                    "responses_without_length": d.get("responses_without_length"),
                }}

            for ep, c in endpoints.items():
                key = (day, org, scr, ep)
                row = agg.setdefault(key, {
                    "day": day, "origin": org, "scraper": scr, "endpoint": ep,
                    "requests": 0, "bytes_down": 0, "bytes_up": 0,
                    "responses_without_length": 0, "runs": 0,
                })
                row["requests"] += int(c.get("requests") or 0)
                row["bytes_down"] += int(c.get("bytes_down") or 0)
                row["bytes_up"] += int(c.get("bytes_up") or 0)
                row["responses_without_length"] += int(c.get("responses_without_length") or 0)
                row["runs"] += 1

    return [agg[k] for k in sorted(agg)]


def human_bytes(n: float) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(n) < 1024 or unit == "TB":
            return f"{n:,.1f} {unit}" if unit != "B" else f"{int(n)} B"
        n /= 1024
    return f"{n:,.1f} TB"


# ══════════════════════════════════════════════════════════════════════════════
# Postgres
# ══════════════════════════════════════════════════════════════════════════════

USAGE_TABLE = "bandwidth_usage"

# Grain is one row per day x origin x scraper x endpoint, so "which API call
# is eating the bandwidth" is answerable directly; per-scraper and per-day
# figures are sums over it.
USAGE_COLUMNS: list[tuple[str, str]] = [
    ("day",                      "DATE"),
    ("origin",                   "VARCHAR(16)"),
    ("scraper",                  "VARCHAR(64)"),
    ("endpoint",                 "VARCHAR(200)"),
    ("runs",                     "INT"),
    ("requests",                 "BIGINT"),
    ("bytes_down",               "BIGINT"),
    ("bytes_up",                 "BIGINT"),
    ("responses_without_length", "BIGINT"),
    ("updated_at",               "TIMESTAMP"),
]


def ensure_usage_table(cur) -> None:
    cols_sql = ",\n  ".join(f'"{n}" {t}' for n, t in USAGE_COLUMNS)
    cur.execute(
        f'CREATE TABLE IF NOT EXISTS "{USAGE_TABLE}" (\n'
        f'  "id" BIGSERIAL PRIMARY KEY,\n'
        f'  {cols_sql}\n'
        f")"
    )
    for name, ctype in USAGE_COLUMNS:
        cur.execute(
            f'ALTER TABLE "{USAGE_TABLE}" ADD COLUMN IF NOT EXISTS "{name}" {ctype}'
        )
    for col in ("day", "origin"):
        cur.execute(
            f'CREATE INDEX IF NOT EXISTS "idx_{USAGE_TABLE}_{col}" '
            f'ON "{USAGE_TABLE}" ("{col}")'
        )


def upload_origin_bandwidth(cur, origin: str) -> int:
    """
    Push this origin's local usage into Postgres.

    Only the days we still hold locally are replaced, so pruning old local
    files never deletes history from the table.
    """
    from psycopg2.extras import execute_values

    ensure_usage_table(cur)
    rows = daily_usage(origin)
    if not rows:
        print(f'  no local bandwidth records for "{origin}" — nothing uploaded')
        return 0

    days = sorted({r["day"] for r in rows})
    # The day keys are ISO strings and the column is DATE — Postgres will not
    # compare those without an explicit cast on the array.
    cur.execute(
        f'DELETE FROM "{USAGE_TABLE}" WHERE "origin" = %s AND "day" = ANY(%s::date[])',
        (origin, days),
    )

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    values = [
        (r["day"], r["origin"], r["scraper"], r.get("endpoint", UNKNOWN_ENDPOINT),
         r["runs"], r["requests"], r["bytes_down"], r["bytes_up"],
         r["responses_without_length"], now)
        for r in rows
    ]
    cols_sql = ", ".join(f'"{n}"' for n, _ in USAGE_COLUMNS)
    execute_values(
        cur, f'INSERT INTO "{USAGE_TABLE}" ({cols_sql}) VALUES %s', values, page_size=500
    )
    total = sum(r["bytes_down"] for r in rows)
    print(f'  {len(values):>7,} bandwidth rows -> "{USAGE_TABLE}" '
          f'(origin={origin}, {len(days)} day(s), {human_bytes(total)} down)')
    return len(values)


def refresh_origin(origin: str, cur=None) -> list[dict]:
    """Entry point for the regional loaders: prune, then upload."""
    prune_local()
    rows = daily_usage(origin)
    if cur is not None:
        upload_origin_bandwidth(cur, origin)
    return rows


# ══════════════════════════════════════════════════════════════════════════════
# CLI — `python bandwidth.py` prints the local daily report
# ══════════════════════════════════════════════════════════════════════════════

def _connect():
    import psycopg2
    from dotenv import load_dotenv

    load_dotenv(REPO_ROOT / ".env")
    url = os.environ.get("DATABASE_URL", "")
    if not url:
        raise SystemExit("DATABASE_URL env var is required for --upload")
    if "sslmode=" not in url:
        url += ("&" if "?" in url else "?") + "sslmode=require"
    return psycopg2.connect(url, connect_timeout=30)


def main(argv) -> None:
    args = list(argv)
    upload = "--upload" in args
    if upload:
        args.remove("--upload")
    origins = args or list(ORIGINS)

    rows = [r for r in daily_usage() if r["origin"] in origins]
    if not rows:
        print("No bandwidth records yet — scrapers write them as they run.")
        return

    by_day: dict[str, list[dict]] = {}
    for r in rows:
        by_day.setdefault(r["day"], []).append(r)

    grand = 0
    for day in sorted(by_day):
        day_rows = by_day[day]
        day_total = sum(r["bytes_down"] for r in day_rows)
        grand += day_total
        print(f"\n{day}   {human_bytes(day_total)} down   "
              f"{sum(r['requests'] for r in day_rows):,} requests")

        # group the endpoint rows back under their scraper for the report
        by_scraper: dict[tuple[str, str], list[dict]] = {}
        for r in day_rows:
            by_scraper.setdefault((r["origin"], r["scraper"]), []).append(r)

        for (origin, scraper), eps in sorted(
            by_scraper.items(), key=lambda kv: -sum(r["bytes_down"] for r in kv[1])
        ):
            tot = sum(r["bytes_down"] for r in eps)
            req = sum(r["requests"] for r in eps)
            print(f"    {origin:8} {scraper:24} "
                  f"{human_bytes(tot):>12}  {req:>8,} req")
            for r in sorted(eps, key=lambda x: -x["bytes_down"]):
                avg = r["bytes_down"] / r["requests"] if r["requests"] else 0
                print(f"        {r['endpoint'][:56]:56} "
                      f"{human_bytes(r['bytes_down']):>11}  "
                      f"{r['requests']:>7,} calls  "
                      f"{human_bytes(avg):>9}/call")
    print(f"\nTOTAL {human_bytes(grand)} across {len(by_day)} day(s)")

    if upload:
        conn = _connect()
        try:
            with conn.cursor() as cur:
                for origin in origins:
                    upload_origin_bandwidth(cur, origin)
            conn.commit()
        finally:
            conn.close()


if __name__ == "__main__":
    main(sys.argv[1:])
