"""
main.py — FastAPI Diamond Price Dashboard API
"""

import os
import json
import datetime
from pathlib import Path

import pandas as pd
from fastapi import FastAPI, HTTPException, UploadFile, File, Form
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware

BASE_DIR             = Path(os.getcwd())
COMPARE_DIR          = BASE_DIR / "compare"
COMPARE_JSON         = COMPARE_DIR / "compare.json"          # common (all vendors)
COMPARE_PARTIAL_JSON = COMPARE_DIR / "compare_partial.json"  # 2+ vendors
COMPARE_ALL_JSON     = COMPARE_DIR / "compare_all.json"      # all PC certs
COMPARE_CSV          = COMPARE_DIR / "compare.csv"
COMPARE_DIR.mkdir(exist_ok=True)

app = FastAPI(title="Diamond Price Dashboard API", version="1.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


def csv_to_json(csv_path: Path, output_path: Path, fill_na="") -> dict:
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV not found: {csv_path}")
    df = pd.read_csv(csv_path, low_memory=False, on_bad_lines="skip")
    if fill_na is not None:
        df = df.fillna(fill_na)
    # Replace NaN/Inf with None so json.dump never sees float('nan') or float('inf')
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
        "last_modified": datetime.datetime.fromtimestamp(stat.st_mtime).strftime("%Y-%m-%d %H:%M:%S"),
    }


@app.get("/", response_class=HTMLResponse)
async def serve_dashboard():
    html_path = BASE_DIR / "dashboard.html"
    if not html_path.exists():
        raise HTTPException(status_code=404, detail="dashboard.html not found")
    return HTMLResponse(content=html_path.read_text(encoding="utf-8"))


@app.get("/api/compare")
async def get_default_compare():
    if not COMPARE_JSON.exists():
        raise HTTPException(status_code=404, detail="compare/compare.json not found. Run diamond_compare.py first or upload a JSON/CSV via /api/upload.")
    try:
        raw = COMPARE_JSON.read_text(encoding="utf-8")
        # Replace bare NaN/Infinity that pandas sometimes writes (not valid JSON)
        import re as _re
        raw = _re.sub(r'\bNaN\b', 'null', raw)
        raw = _re.sub(r'\bInfinity\b', 'null', raw)
        raw = _re.sub(r'\b-Infinity\b', 'null', raw)
        data = json.loads(raw)
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
    stem = Path(csv_filename).stem
    out_name = output_name or f"{stem}.json"
    output_path = COMPARE_DIR / out_name
    try:
        summary = csv_to_json(csv_path, output_path)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    return {"status": "ok", "message": f"Converted {csv_filename} -> compare/{out_name}", "rows": summary["rows"], "columns": summary["columns"], "file": out_name}


@app.post("/api/upload")
async def upload_and_convert(file: UploadFile = File(...), output_name: str = Form(None)):
    """
    Upload a .csv OR .json file directly into compare/.
    - CSV  -> converted to JSON automatically
    - JSON -> validated (must be array) and saved as-is
    """
    filename = file.filename or ""
    suffix   = Path(filename).suffix.lower()

    if suffix not in (".csv", ".json"):
        raise HTTPException(status_code=400, detail="Only .csv or .json files are accepted")

    content = await file.read()
    stem    = Path(filename).stem

    # JSON upload — validate, sanitize NaN/Inf, and save
    if suffix == ".json":
        try:
            import re as _re
            raw = content.decode("utf-8")
            # Fix bare NaN/Infinity that pandas sometimes writes into JSON files
            raw = _re.sub(r'\bNaN\b', 'null', raw)
            raw = _re.sub(r'\bInfinity\b', 'null', raw)
            raw = _re.sub(r'\b-Infinity\b', 'null', raw)
            data = json.loads(raw)
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Invalid JSON — file could not be parsed: {e}")
        if not isinstance(data, list):
            raise HTTPException(status_code=400, detail="JSON must be an array of records (got object/other)")
        out_name    = output_name or filename
        output_path = COMPARE_DIR / out_name
        # Save the sanitized version (NaN replaced with null)
        output_path.write_text(raw, encoding="utf-8")
        return {
            "status":  "ok",
            "message": f"Uploaded JSON -> compare/{out_name}",
            "rows":    len(data),
            "columns": list(data[0].keys()) if data else [],
            "file":    out_name,
            "type":    "json",
        }

    # CSV upload — convert to JSON
    # Use system temp dir to avoid Windows file-lock clash with compare/ files
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
            pass  # ignore cleanup errors on Windows
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
    # Security: only allow .json files, no path traversal
    if ".." in filename or "/" in filename or "\\" in filename:
        raise HTTPException(status_code=400, detail="Invalid filename")
    if not filename.endswith(".json"):
        raise HTTPException(status_code=400, detail=f"Only .json files are supported, got: {filename}")
    json_path = COMPARE_DIR / filename
    if not json_path.exists():
        raise HTTPException(status_code=404, detail=f"JSON file not found: {filename}")
    try:
        import re as _re
        raw = json_path.read_text(encoding="utf-8")
        raw = _re.sub(r'\bNaN\b', 'null', raw)
        raw = _re.sub(r'\bInfinity\b', 'null', raw)
        raw = _re.sub(r'\b-Infinity\b', 'null', raw)
        data = json.loads(raw)
        # Strip internal helper columns that should never reach the frontend
        INTERNAL_COLS = {"_vendor_count", "_all_matched", "_some_matched"}
        if isinstance(data, list) and data:
            keys_to_strip = INTERNAL_COLS & set(data[0].keys())
            if keys_to_strip:
                data = [{k: v for k, v in row.items() if k not in keys_to_strip} for row in data]
        return JSONResponse(content=data)
    except json.JSONDecodeError as e:
        raise HTTPException(status_code=500, detail=f"Invalid JSON in file {filename}: {e}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to read {filename}: {e}")


@app.get("/api/files")
async def list_json_files():
    files = []
    for p in sorted(COMPARE_DIR.glob("*.json")):
        stat = p.stat()
        files.append({
            "filename": p.name,
            "size_kb": round(stat.st_size / 1024, 1),
            "last_modified": datetime.datetime.fromtimestamp(stat.st_mtime).strftime("%Y-%m-%d %H:%M:%S"),
            "is_default": p.name == "compare.json",
        })
    return {"files": files, "count": len(files)}


@app.post("/api/run-compare")
async def run_compare():
    import subprocess, sys
    compare_script = BASE_DIR / "diamond_compare.py"
    if not compare_script.exists():
        raise HTTPException(status_code=404, detail="diamond_compare.py not found in project root")
    try:
        result = subprocess.run([sys.executable, str(compare_script)], capture_output=True, text=True, timeout=600)
        success = result.returncode == 0
        compare_data = None
        rows = 0
        compare_json_info         = _file_info(COMPARE_JSON)
        compare_partial_json_info = _file_info(COMPARE_PARTIAL_JSON)
        compare_all_json_info     = _file_info(COMPARE_ALL_JSON)
        compare_csv_info          = _file_info(COMPARE_CSV)
        if COMPARE_JSON.exists():
            try:
                compare_data = json.loads(COMPARE_JSON.read_text(encoding="utf-8"))
                rows = len(compare_data)
            except Exception as parse_err:
                compare_json_info["parse_error"] = str(parse_err)
        return JSONResponse(content={
            "status":    "ok" if success else "error",
            "returncode": result.returncode,
            "rows":       rows,
            # File metadata for all 3 outputs
            "files": {
                "common":  {**compare_json_info,         "filename": "compare.json"},
                "partial": {**compare_partial_json_info, "filename": "compare_partial.json"},
                "all":     {**compare_all_json_info,     "filename": "compare_all.json"},
                "csv":     {**compare_csv_info,          "filename": "compare.csv"},
            },
            "compare_json": compare_data,   # full common data for immediate dashboard use
            "stdout": result.stdout[-3000:] if result.stdout else "",
            "stderr": result.stderr[-2000:] if result.stderr else "",
        })
    except subprocess.TimeoutExpired:
        raise HTTPException(status_code=504, detail="Compare pipeline timed out (>10 min)")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


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


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)