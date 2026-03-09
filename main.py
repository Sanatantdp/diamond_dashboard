"""
main.py — FastAPI Diamond Price Dashboard API

Endpoints:
  GET  /                        → serve the dashboard HTML
  GET  /api/compare             → serve compare/compare.json directly (default dataset)
  POST /api/convert             → convert a CSV to JSON, store in compare/
  GET  /api/data/{filename}     → return stored JSON data
  GET  /api/files               → list all JSON files in compare/
  POST /api/upload              → upload a CSV and auto-convert
  GET  /api/health              → health check
"""

import os
import json
import datetime
from pathlib import Path

import pandas as pd
from fastapi import FastAPI, HTTPException, UploadFile, File, Form
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware

# ── Directories ───────────────────────────────────────────────
BASE_DIR     = Path(os.getcwd())
COMPARE_DIR  = BASE_DIR / "compare"
COMPARE_JSON = COMPARE_DIR / "compare.json"   # ← default dataset
COMPARE_DIR.mkdir(exist_ok=True)

app = FastAPI(title="Diamond Price Dashboard API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ══════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════

def csv_to_json(csv_path: Path, output_path: Path, fill_na="") -> dict:
    """Convert a CSV to a JSON file in compare/ and return a summary."""
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV not found: {csv_path}")

    df = pd.read_csv(csv_path, low_memory=False, on_bad_lines="skip")

    if fill_na is not None:
        df = df.fillna(fill_na)

    data = df.to_dict(orient="records")

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    return {
        "rows":    len(df),
        "columns": list(df.columns),
        "output":  str(output_path),
    }


# ══════════════════════════════════════════════════════════════
# ROUTES
# ══════════════════════════════════════════════════════════════

@app.get("/", response_class=HTMLResponse)
async def serve_dashboard():
    """Serve the main dashboard HTML page."""
    html_path = BASE_DIR / "dashboard.html"
    if not html_path.exists():
        raise HTTPException(status_code=404, detail="dashboard.html not found")
    return HTMLResponse(content=html_path.read_text(encoding="utf-8"))


# ── DEFAULT DATASET ───────────────────────────────────────────
@app.get("/api/compare")
async def get_default_compare():
    """
    Return compare/compare.json — the default dataset for the dashboard.
    The dashboard calls this on boot to auto-populate without user interaction.
    Returns 404 with a clear message if the file hasn't been generated yet.
    """
    if not COMPARE_JSON.exists():
        raise HTTPException(
            status_code=404,
            detail=(
                "compare/compare.json not found. "
                "Run diamond_compare.py first to generate it, "
                "or upload a CSV via /api/upload."
            ),
        )
    try:
        data = json.loads(COMPARE_JSON.read_text(encoding="utf-8"))
        return JSONResponse(content=data)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to read compare.json: {e}")


# ── CONVERT EXISTING CSV BY NAME ──────────────────────────────
@app.post("/api/convert")
async def convert_csv(
    csv_filename: str = Form(..., description="CSV filename inside diamond_files/ or compare/"),
    output_name:  str = Form(None, description="Output JSON filename (optional)"),
):
    """Convert a named CSV to JSON and store in compare/."""
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
        "message": f"Converted {csv_filename} → compare/{out_name}",
        "rows":    summary["rows"],
        "columns": summary["columns"],
        "file":    out_name,
    }


# ── UPLOAD CSV → JSON ─────────────────────────────────────────
@app.post("/api/upload")
async def upload_and_convert(
    file:        UploadFile = File(..., description="CSV file to upload and convert"),
    output_name: str        = Form(None, description="Output JSON filename (optional)"),
):
    """Upload a CSV, convert to JSON in compare/, return the JSON filename."""
    if not file.filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only .csv files are accepted")

    tmp_csv = COMPARE_DIR / file.filename
    content = await file.read()
    tmp_csv.write_bytes(content)

    stem        = Path(file.filename).stem
    out_name    = output_name or f"{stem}.json"
    output_path = COMPARE_DIR / out_name

    try:
        summary = csv_to_json(tmp_csv, output_path)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        tmp_csv.unlink(missing_ok=True)

    return {
        "status":  "ok",
        "message": f"Uploaded & converted {file.filename} → compare/{out_name}",
        "rows":    summary["rows"],
        "columns": summary["columns"],
        "file":    out_name,
    }


# ── GET ANY JSON FILE ─────────────────────────────────────────
@app.get("/api/data/{filename}")
async def get_json_data(filename: str):
    """Return parsed JSON data from compare/<filename>."""
    json_path = COMPARE_DIR / filename
    if not json_path.exists() or json_path.suffix != ".json":
        raise HTTPException(status_code=404, detail=f"JSON file not found: {filename}")
    try:
        data = json.loads(json_path.read_text(encoding="utf-8"))
        return JSONResponse(content=data)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to read JSON: {e}")


# ── LIST ALL JSON FILES ───────────────────────────────────────
@app.get("/api/files")
async def list_json_files():
    """List all JSON files available in compare/."""
    files = []
    for p in sorted(COMPARE_DIR.glob("*.json")):
        stat = p.stat()
        files.append({
            "filename":      p.name,
            "size_kb":       round(stat.st_size / 1024, 1),
            "last_modified": datetime.datetime.fromtimestamp(stat.st_mtime).strftime("%Y-%m-%d %H:%M:%S"),
            "is_default":    p.name == "compare.json",
        })
    return {"files": files, "count": len(files)}


# ── HEALTH CHECK ──────────────────────────────────────────────
@app.get("/api/health")
async def health():
    return {
        "status":          "ok",
        "compare_dir":     str(COMPARE_DIR),
        "json_files":      len(list(COMPARE_DIR.glob("*.json"))),
        "default_dataset": COMPARE_JSON.exists(),
        "timestamp":       datetime.datetime.now().isoformat(),
    }


# ── Run with:  uvicorn main:app --reload ──────────────────────
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)