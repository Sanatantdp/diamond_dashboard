"""
main.py — FastAPI Diamond Price Dashboard API

Scraper endpoints
─────────────────
POST /api/scrape/brilliance      → run Brilliance scraper
POST /api/scrape/loosegrown      → run Loose Grown Diamond scraper
POST /api/scrape/luvansh         → run Luvansh scraper
POST /api/scrape/precious-carbon → run PreciousCarbon scraper
POST /api/scrape/all             → run ALL scrapers sequentially

GET  /api/scrape/status          → background-job status for all scrapers
GET  /api/logs/{scraper}         → last N lines of a scraper's log file
GET  /api/logs                   → metadata for all log files
"""

import os
import re
import json
import datetime
import threading
from pathlib import Path

import pandas as pd
from fastapi import FastAPI, HTTPException, UploadFile, File, Form, Query
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware

# ── Paths ───────────────────────────────────────────────────────────
BASE_DIR             = Path(os.getcwd())
COMPARE_DIR          = BASE_DIR / "compare"
COMPARE_JSON         = COMPARE_DIR / "compare.json"
COMPARE_PARTIAL_JSON = COMPARE_DIR / "compare_partial.json"
COMPARE_ALL_JSON     = COMPARE_DIR / "compare_all.json"
COMPARE_CSV          = COMPARE_DIR / "compare.csv"
DIAMOND_STATUS_JSON  = COMPARE_DIR / "diamond_status.json"
LOG_DIR              = BASE_DIR / "logs"

COMPARE_DIR.mkdir(exist_ok=True)
LOG_DIR.mkdir(exist_ok=True)

# ── Scraper registry ────────────────────────────────────────────────
#   key        → (log_name,  importable_module,  callable_name)
SCRAPERS = {
    "brilliance":      ("brilliance",     "brilliance_scraper",      "brilliance_diamonds_scraper"),
    "loosegrown":      ("loosegrown",     "loosegrown_scraper",      "loose_grown_diamonds_scrappe"),
    "luvansh":         ("luvansh",        "luvansh_scraper",         "main"),
    "precious-carbon": ("precious_carbon","precious_carbon_scraper", "scrape"),
}

# ── In-memory job tracker ────────────────────────────────────────────
#   { scraper_key: { "status": "idle"|"running"|"done"|"error",
#                    "started_at": ..., "finished_at": ...,
#                    "result": ..., "error": ... } }
_job_state: dict = {k: {"status": "idle"} for k in SCRAPERS}
_job_lock = threading.Lock()

# ── App ──────────────────────────────────────────────────────────────
app = FastAPI(title="Diamond Price Dashboard API", version="1.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_methods=["*"], allow_headers=["*"]
)


# ════════════════════════════════════════════════════════════════════
# INTERNAL HELPERS
# ════════════════════════════════════════════════════════════════════

def csv_to_json(csv_path: Path, output_path: Path, fill_na="") -> dict:
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV not found: {csv_path}")
    df = pd.read_csv(csv_path, low_memory=False, on_bad_lines="skip")
    if fill_na is not None:
        df = df.fillna(fill_na)
    data = df.where(df.notna(), other=None).to_dict(orient="records")
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    return {"rows": len(df), "columns": list(df.columns), "output": str(output_path)}


def _file_info(path: Path) -> dict:
    if not path.exists():
        return {"exists": False}
    stat = path.stat()
    return {
        "exists": True,
        "size_kb": round(stat.st_size / 1024, 1),
        "last_modified": datetime.datetime.fromtimestamp(stat.st_mtime).strftime(
            "%Y-%m-%d %H:%M:%S"
        ),
    }


def _sanitize_json_text(raw: str) -> str:
    raw = re.sub(r'\bNaN\b', 'null', raw)
    raw = re.sub(r'\bInfinity\b', 'null', raw)
    raw = re.sub(r'\b-Infinity\b', 'null', raw)
    return raw


def _run_scraper(key: str):
    """Execute a scraper in a background thread and update _job_state."""
    log_name, module_name, func_name = SCRAPERS[key]

    with _job_lock:
        _job_state[key] = {
            "status":     "running",
            "started_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }

    try:
        import importlib
        module = importlib.import_module(module_name)
        func   = getattr(module, func_name)
        result = func()

        with _job_lock:
            _job_state[key].update({
                "status":      "done",
                "finished_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "result":      result,
            })

    except Exception as exc:
        with _job_lock:
            _job_state[key].update({
                "status":      "error",
                "finished_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "error":       str(exc),
            })


def _launch(key: str):
    """Start a scraper in a daemon thread (fire-and-forget)."""
    t = threading.Thread(target=_run_scraper, args=(key,), daemon=True)
    t.start()


# ════════════════════════════════════════════════════════════════════
# EXISTING DASHBOARD ROUTES
# ════════════════════════════════════════════════════════════════════

@app.get("/", response_class=HTMLResponse)
async def serve_dashboard():
    html_path = BASE_DIR / "dashboard.html"
    if not html_path.exists():
        raise HTTPException(status_code=404, detail="dashboard.html not found")
    return HTMLResponse(content=html_path.read_text(encoding="utf-8"))


@app.get("/api/compare")
async def get_default_compare():
    if not COMPARE_JSON.exists():
        raise HTTPException(
            status_code=404,
            detail="compare/compare.json not found. Run diamond_compare.py first or upload a JSON/CSV via /api/upload.",
        )
    try:
        raw  = COMPARE_JSON.read_text(encoding="utf-8")
        data = json.loads(_sanitize_json_text(raw))
        return JSONResponse(content=data)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to read compare.json: {e}")


@app.post("/api/convert")
async def convert_csv(csv_filename: str = Form(...), output_name: str = Form(None)):
    csv_path = BASE_DIR / "diamond_files" / csv_filename
    if not csv_path.exists():
        csv_path = BASE_DIR / csv_filename
    if not csv_path.exists():
        raise HTTPException(status_code=404, detail=f"File not found: {csv_filename}")
    stem        = Path(csv_filename).stem
    out_name    = output_name or f"{stem}.json"
    output_path = COMPARE_DIR / out_name
    try:
        summary = csv_to_json(csv_path, output_path)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    return {
        "status":  "ok",
        "message": f"Converted {csv_filename} -> compare/{out_name}",
        "rows":    summary["rows"],
        "columns": summary["columns"],
        "file":    out_name,
    }


@app.post("/api/upload")
async def upload_and_convert(file: UploadFile = File(...), output_name: str = Form(None)):
    filename = file.filename or ""
    suffix   = Path(filename).suffix.lower()

    if suffix not in (".csv", ".json"):
        raise HTTPException(status_code=400, detail="Only .csv or .json files are accepted")

    content = await file.read()
    stem    = Path(filename).stem

    if suffix == ".json":
        try:
            raw  = content.decode("utf-8")
            raw  = _sanitize_json_text(raw)
            data = json.loads(raw)
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Invalid JSON: {e}")
        if not isinstance(data, list):
            raise HTTPException(status_code=400, detail="JSON must be an array of records")
        out_name    = output_name or filename
        output_path = COMPARE_DIR / out_name
        output_path.write_text(raw, encoding="utf-8")
        return {
            "status":  "ok",
            "message": f"Uploaded JSON -> compare/{out_name}",
            "rows":    len(data),
            "columns": list(data[0].keys()) if data else [],
            "file":    out_name,
            "type":    "json",
        }

    import tempfile
    out_name    = output_name or f"{stem}.json"
    output_path = COMPARE_DIR / out_name
    with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmp:
        tmp.write(content)
        tmp_path = Path(tmp.name)
    try:
        summary = csv_to_json(tmp_path, output_path)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        try:
            tmp_path.unlink(missing_ok=True)
        except Exception:
            pass
    return {
        "status":  "ok",
        "message": f"Uploaded & converted {filename} -> compare/{out_name}",
        "rows":    summary["rows"],
        "columns": summary["columns"],
        "file":    out_name,
        "type":    "csv->json",
    }


@app.get("/api/data/{filename}")
async def get_json_data(filename: str):
    if ".." in filename or "/" in filename or "\\" in filename:
        raise HTTPException(status_code=400, detail="Invalid filename")
    if not filename.endswith(".json"):
        raise HTTPException(status_code=400, detail=f"Only .json files are supported")
    json_path = COMPARE_DIR / filename
    if not json_path.exists():
        raise HTTPException(status_code=404, detail=f"JSON file not found: {filename}")
    try:
        raw  = json_path.read_text(encoding="utf-8")
        data = json.loads(_sanitize_json_text(raw))
        INTERNAL_COLS = {"_vendor_count", "_all_matched", "_some_matched"}
        if isinstance(data, list) and data:
            keys_to_strip = INTERNAL_COLS & set(data[0].keys())
            if keys_to_strip:
                data = [{k: v for k, v in row.items() if k not in keys_to_strip} for row in data]
        return JSONResponse(content=data)
    except json.JSONDecodeError as e:
        raise HTTPException(status_code=500, detail=f"Invalid JSON in {filename}: {e}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to read {filename}: {e}")


@app.get("/api/files")
async def list_json_files():
    files = []
    for p in sorted(COMPARE_DIR.glob("*.json")):
        stat = p.stat()
        files.append({
            "filename":      p.name,
            "size_kb":       round(stat.st_size / 1024, 1),
            "last_modified": datetime.datetime.fromtimestamp(stat.st_mtime).strftime(
                "%Y-%m-%d %H:%M:%S"
            ),
            "is_default":    p.name == "compare.json",
        })
    return {"files": files, "count": len(files)}


@app.post("/api/run-compare")
async def run_compare():
    import subprocess, sys
    compare_script = BASE_DIR / "diamond_compare.py"
    if not compare_script.exists():
        raise HTTPException(status_code=404, detail="diamond_compare.py not found")
    try:
        result  = subprocess.run(
            [sys.executable, str(compare_script)],
            capture_output=True, text=True, timeout=600
        )
        success = result.returncode == 0

        compare_data      = None
        rows              = 0
        cji               = _file_info(COMPARE_JSON)
        cpji              = _file_info(COMPARE_PARTIAL_JSON)
        caji              = _file_info(COMPARE_ALL_JSON)
        ccvi              = _file_info(COMPARE_CSV)

        if COMPARE_JSON.exists():
            try:
                compare_data = json.loads(COMPARE_JSON.read_text(encoding="utf-8"))
                rows         = len(compare_data)
            except Exception as parse_err:
                cji["parse_error"] = str(parse_err)

        return JSONResponse(content={
            "status":     "ok" if success else "error",
            "returncode": result.returncode,
            "rows":       rows,
            "files": {
                "common":  {**cji,  "filename": "compare.json"},
                "partial": {**cpji, "filename": "compare_partial.json"},
                "all":     {**caji, "filename": "compare_all.json"},
                "csv":     {**ccvi, "filename": "compare.csv"},
            },
            "compare_json": compare_data,
            "stdout": result.stdout[-3000:] if result.stdout else "",
            "stderr": result.stderr[-2000:] if result.stderr else "",
        })
    except subprocess.TimeoutExpired:
        raise HTTPException(status_code=504, detail="Compare pipeline timed out (>10 min)")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/status")
async def get_diamond_status():
    if not DIAMOND_STATUS_JSON.exists():
        raise HTTPException(
            status_code=404,
            detail="diamond_status.json not found — run pipeline first"
        )
    try:
        raw  = DIAMOND_STATUS_JSON.read_text(encoding="utf-8")
        data = json.loads(_sanitize_json_text(raw))
        return JSONResponse(content=data)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to read diamond_status.json: {e}")


@app.get("/api/health")
async def health():
    return {
        "status":          "ok",
        "compare_dir":     str(COMPARE_DIR),
        "json_files":      len(list(COMPARE_DIR.glob("*.json"))),
        "default_dataset": COMPARE_JSON.exists(),
        "datasets": {
            "common":  COMPARE_JSON.exists(),
            "partial": COMPARE_PARTIAL_JSON.exists(),
            "all":     COMPARE_ALL_JSON.exists(),
        },
        "compare_csv": COMPARE_CSV.exists(),
        "timestamp":   datetime.datetime.now().isoformat(),
    }


# ════════════════════════════════════════════════════════════════════
# SCRAPER TRIGGER ENDPOINTS
# ════════════════════════════════════════════════════════════════════

def _scraper_response(key: str, background: bool = True) -> dict:
    """Standard response body for a scraper-launch request."""
    return {
        "scraper":    key,
        "status":     "started" if background else "running",
        "message":    f"{key} scraper launched in background.",
        "started_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "log_file":   str(LOG_DIR / f"{SCRAPERS[key][0]}.log"),
        "tip":        f"Poll GET /api/scrape/status for live progress.",
    }


@app.post("/api/scrape/brilliance", summary="Run Brilliance scraper")
async def scrape_brilliance():
    """
    Launches the Brilliance lab-grown diamond scraper in a background thread.
    Results are written to  diamond_files/brilliance_diamonds.csv
    and logged to  logs/brilliance.log
    """
    with _job_lock:
        if _job_state["brilliance"].get("status") == "running":
            raise HTTPException(status_code=409, detail="Brilliance scraper is already running.")
    _launch("brilliance")
    return _scraper_response("brilliance")


@app.post("/api/scrape/loosegrown", summary="Run Loose Grown Diamond scraper")
async def scrape_loosegrown():
    """
    Launches the LooseGrownDiamond scraper in a background thread.
    Results are written to  diamond_files/loosegrowndiamond.csv
    and logged to  logs/loosegrown.log
    """
    with _job_lock:
        if _job_state["loosegrown"].get("status") == "running":
            raise HTTPException(status_code=409, detail="LooseGrown scraper is already running.")
    _launch("loosegrown")
    return _scraper_response("loosegrown")


@app.post("/api/scrape/luvansh", summary="Run Luvansh scraper")
async def scrape_luvansh():
    """
    Launches the Luvansh two-phase diamond scraper in a background thread.
    Results are written to  diamond_files/luvansh_diamonds.csv
    and logged to  logs/luvansh.log
    """
    with _job_lock:
        if _job_state["luvansh"].get("status") == "running":
            raise HTTPException(status_code=409, detail="Luvansh scraper is already running.")
    _launch("luvansh")
    return _scraper_response("luvansh")


@app.post("/api/scrape/precious-carbon", summary="Run PreciousCarbon scraper")
async def scrape_precious_carbon():
    """
    Launches the PreciousCarbon concurrent scraper in a background thread.
    Results are written to  diamond_files/precious_carbon.csv
    and logged to  logs/precious_carbon.log
    """
    with _job_lock:
        if _job_state["precious-carbon"].get("status") == "running":
            raise HTTPException(status_code=409, detail="PreciousCarbon scraper is already running.")
    _launch("precious-carbon")
    return _scraper_response("precious-carbon")


@app.post("/api/scrape/all", summary="Run ALL scrapers sequentially")
async def scrape_all():
    """
    Launches all four scrapers **one after another** in a single background thread.
    Each scraper must finish before the next one starts.
    Returns immediately; poll /api/scrape/status for progress.
    """
    already_running = [k for k, v in _job_state.items() if v.get("status") == "running"]
    if already_running:
        raise HTTPException(
            status_code=409,
            detail=f"The following scrapers are already running: {already_running}"
        )

    def _run_all():
        for key in SCRAPERS:
            _run_scraper(key)

    t = threading.Thread(target=_run_all, daemon=True)
    t.start()

    return {
        "status":     "started",
        "message":    "All 4 scrapers queued and running sequentially in background.",
        "order":      list(SCRAPERS.keys()),
        "started_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "tip":        "Poll GET /api/scrape/status for individual progress.",
    }


@app.get("/api/scrape/status", summary="Background job status for all scrapers")
async def scrape_status():
    """
    Returns the current status of every scraper job:
      idle | running | done | error
    """
    with _job_lock:
        snapshot = {k: dict(v) for k, v in _job_state.items()}

    # Attach log-file metadata for convenience
    for key, state in snapshot.items():
        log_name = SCRAPERS[key][0]
        log_path = LOG_DIR / f"{log_name}.log"
        state["log"] = _file_info(log_path)

    return snapshot


# ════════════════════════════════════════════════════════════════════
# LOG VIEWER ENDPOINTS
# ════════════════════════════════════════════════════════════════════

@app.get("/api/logs", summary="Metadata for all scraper log files")
async def list_logs():
    """
    Returns size, age, and rotation info for every  logs/*.log  file.
    """
    import time as _time
    MAX_AGE_SECS = 2 * 24 * 3600
    result = []
    for log_path in sorted(LOG_DIR.glob("*.log")):
        stat      = log_path.stat()
        age_hours = (_time.time() - stat.st_mtime) / 3600
        result.append({
            "name":               log_path.stem,
            "path":               str(log_path),
            "size_kb":            round(stat.st_size / 1024, 2),
            "age_hours":          round(age_hours, 2),
            "will_rotate_in_h":   round(max(0, MAX_AGE_SECS / 3600 - age_hours), 2),
            "last_modified":      datetime.datetime.fromtimestamp(stat.st_mtime).strftime(
                "%Y-%m-%d %H:%M:%S"
            ),
        })
    return {"logs": result, "log_dir": str(LOG_DIR)}


@app.get("/api/logs/{scraper}", summary="Tail a scraper's log file")
async def get_log(
    scraper: str,
    lines: int = Query(default=100, ge=1, le=5000, description="Number of tail lines to return"),
):
    """
    Returns the last *N* lines (default 100, max 5000) of a scraper log.

    `scraper` must be one of: brilliance | loosegrown | luvansh | precious-carbon
    (or the raw log filename stem, e.g. precious_carbon).
    """
    # Accept both dash and underscore variants
    name_map = {
        "brilliance":      "brilliance",
        "loosegrown":      "loosegrown",
        "luvansh":         "luvansh",
        "precious-carbon": "precious_carbon",
        "precious_carbon": "precious_carbon",
    }
    log_stem = name_map.get(scraper)
    if not log_stem:
        raise HTTPException(
            status_code=404,
            detail=f"Unknown scraper '{scraper}'. Valid values: {list(name_map.keys())}",
        )

    log_path = LOG_DIR / f"{log_stem}.log"
    if not log_path.exists():
        return {
            "scraper":  scraper,
            "log_file": str(log_path),
            "lines":    [],
            "message":  "Log file does not exist yet — scraper has not been run.",
        }

    try:
        with open(log_path, encoding="utf-8", errors="replace") as f:
            all_lines = f.readlines()
        tail = [l.rstrip("\n") for l in all_lines[-lines:]]
        return {
            "scraper":       scraper,
            "log_file":      str(log_path),
            "total_lines":   len(all_lines),
            "returned_lines": len(tail),
            "lines":         tail,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Could not read log: {e}")


# ════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)