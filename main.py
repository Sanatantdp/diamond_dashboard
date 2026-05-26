"""
main.py
=======

Diamond Price Dashboard API — backed by PostgreSQL (Render managed DB).

Data source
-----------
Reads from the PostgreSQL tables created/refreshed by `compare_to_mysql.py`:
  • compare_common      (3-vendor matches    -> served at /api/compare)
  • compare_partial     (>=1 vendor match    -> served at /api/partial)
  • compare_all         (every PC diamond    -> served at /api/data/compare_all.json)
  • compare_br_vs_lg    (BR vs LG inner join -> served at /api/br-vs-lg)
  • diamond_status      (snapshot row        -> served at /api/status)

DB connection
-------------
Reads DATABASE_URL from a .env file (locally) or from process env (on Render).
Example DATABASE_URL:
    postgresql://user:pass@host.oregon-postgres.render.com/dbname

Render deployment
-----------------
1. Push this repo to GitHub.
2. Create a Render **Web Service** -> connect your repo.
3. Build command   :  pip install -r requirements.txt
   Start command   :  uvicorn main:app --host 0.0.0.0 --port $PORT
4. Add environment variables in the Render dashboard:
       DATABASE_URL       = <internal or external Postgres URL from Render>
       DASHBOARD_PASSWORD = diamond-6087
5. Render automatically sets PORT — the start command above binds to it.
"""

from __future__ import annotations

import datetime
import hashlib
import json
import os
from contextlib import contextmanager
from decimal import Decimal
from pathlib import Path
from typing import Any

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel
from starlette.responses import Response

# ── Paths ────────────────────────────────────────────────────────────────────
BASE_DIR    = Path(__file__).parent.resolve()
COMPARE_DIR = BASE_DIR / "compare"
COMPARE_DIR.mkdir(exist_ok=True)

# ── Load .env (no-op on Render where env vars come from the platform) ────────
load_dotenv(BASE_DIR / ".env")

# ── PostgreSQL config ────────────────────────────────────────────────────────
DATABASE_URL = os.environ.get("DATABASE_URL", "")
if not DATABASE_URL:
    raise RuntimeError(
        "DATABASE_URL env var is required. "
        "Put it in .env locally, or set it in Render's Environment tab."
    )
# Render's external Postgres requires SSL — append if caller didn't set it.
if "sslmode=" not in DATABASE_URL:
    DATABASE_URL += ("&" if "?" in DATABASE_URL else "?") + "sslmode=require"

# ── Auth ─────────────────────────────────────────────────────────────────────
DASHBOARD_PASSWORD = os.environ.get("DASHBOARD_PASSWORD", "diamond-6087")

# ── FastAPI app ──────────────────────────────────────────────────────────────
app = FastAPI(title="Diamond Price Dashboard API", version="2.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

PUBLIC_PATHS = {"/", "/dashboard", "/api/login", "/api/login/config", "/api/health"}


@app.middleware("http")
async def bearer_auth_middleware(request: Request, call_next):
    if request.url.path in PUBLIC_PATHS:
        return await call_next(request)
    auth_header = request.headers.get("Authorization", "")
    if not auth_header.startswith("Bearer ") or auth_header[7:] != DASHBOARD_PASSWORD:
        return Response(
            content='{"detail": "Unauthorized - provide a valid Bearer token"}',
            status_code=401,
            media_type="application/json",
        )
    return await call_next(request)


# ═════════════════════════════════════════════════════════════════════════════
# PostgreSQL helpers
# ═════════════════════════════════════════════════════════════════════════════
@contextmanager
def _get_conn():
    """Open a fresh PostgreSQL connection; close it on exit."""
    conn = psycopg2.connect(
        DATABASE_URL,
        cursor_factory=psycopg2.extras.RealDictCursor,
        connect_timeout=10,
    )
    conn.autocommit = True
    try:
        yield conn
    finally:
        conn.close()


def _jsonify_value(v: Any) -> Any:
    """Convert DB types into JSON-serialisable Python types."""
    if isinstance(v, Decimal):
        # Convert without scientific notation; preserve ints as ints
        as_float = float(v)
        return int(as_float) if as_float.is_integer() else as_float
    if isinstance(v, (datetime.datetime, datetime.date)):
        return v.strftime("%Y-%m-%d %H:%M:%S") if isinstance(v, datetime.datetime) else v.isoformat()
    return v


# DB column -> original JSON key the dashboard expects
COMPARE_DB_TO_JSON = {
    "certificate_number":       "certificate_number",
    "vendors_matched":          "vendors_matched",
    "shape":                    "shape",
    "color":                    "color",
    "clarity":                  "clarity",
    "cut":                      "cut",
    "carat_weight":             "caratWeight",
    "lg_price_usd":             "loose-grown Price USD",
    "lg_minus_30_usd":          "loose-grown -30% USD",
    "lg_vs_pc_usd":             "loose-grown vs PC USD",
    "lg_vs_pc_pct":             "loose-grown vs PC %",
    "brilliance_price_usd":     "brilliance Price USD",
    "brilliance_minus_30_usd":  "brilliance -30% USD",
    "brilliance_vs_pc_usd":     "brilliance vs PC USD",
    "brilliance_vs_pc_pct":     "brilliance vs PC %",
    "pc_price_usd":             "PC Price USD",
    "pc_minus_30_usd":          "PC -30% USD",
}

BR_VS_LG_DB_TO_JSON = {
    "diamond_id":               "diamond_id",
    "shape":                    "shape",
    "carat_weight":             "caratWeight",
    "cut":                      "cut",
    "color":                    "color",
    "clarity":                  "clarity",
    "lg_price_usd":             "loose-grown Price USD",
    "brilliance_price_usd":     "brilliance Price USD",
    "lg_minus_30_usd":          "loose-grown -30% USD",
    "brilliance_minus_30_usd":  "brilliance -30% USD",
    "br_minus_30_vs_lg_usd":    "BR -30% vs LG USD",
    "br_minus_30_vs_lg_pct":    "BR -30% vs LG %",
}


def _fetch_compare(table: str, key_map: dict[str, str]) -> list[dict]:
    """Query a compare_* table and remap columns back to the dashboard's JSON key names."""
    db_cols = list(key_map.keys())
    cols_sql = ", ".join(f'"{c}"' for c in db_cols)
    sql = f'SELECT {cols_sql} FROM "{table}"'

    with _get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()

    out: list[dict] = []
    for r in rows:
        out.append({key_map[c]: _jsonify_value(r[c]) for c in db_cols})
    return out


def _fetch_status() -> dict | None:
    """Latest row of diamond_status, rebuilt in the nested JSON shape the UI expects."""
    sql = 'SELECT * FROM "diamond_status" ORDER BY "id" DESC LIMIT 1'
    with _get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql)
        r = cur.fetchone()
    if not r:
        return None
    return {
        "generated_at": _jsonify_value(r["generated_at"]),
        "pc_total":     r["pc_total"],
        "vendor_totals": {
            "loose-grown": r["lg_vendor_total"],
            "brilliance":  r["brilliance_vendor_total"],
        },
        "pc_vs_vendors": {
            "loose-grown": {
                "matched":         r["lg_matched"],
                "match_rate_pct":  _jsonify_value(r["lg_match_rate_pct"]),
            },
            "brilliance": {
                "matched":         r["brilliance_matched"],
                "match_rate_pct":  _jsonify_value(r["brilliance_match_rate_pct"]),
            },
        },
        "dataset_counts": {
            "common":   r["common_count"],
            "partial":  r["partial_count"],
            "all_pc":   r["all_pc_count"],
            "br_vs_lg": r["br_vs_lg_count"],
        },
    }


def _table_row_count(table: str) -> int:
    try:
        with _get_conn() as conn, conn.cursor() as cur:
            cur.execute(f'SELECT COUNT(*) AS c FROM "{table}"')
            row = cur.fetchone()
            return int(row["c"]) if row else 0
    except Exception:
        return 0


# ═════════════════════════════════════════════════════════════════════════════
# Pages
# ═════════════════════════════════════════════════════════════════════════════
@app.get("/", response_class=HTMLResponse)
async def serve_login():
    html_path = BASE_DIR / "login.html"
    if not html_path.exists():
        raise HTTPException(status_code=404, detail="login.html not found")
    return HTMLResponse(content=html_path.read_text(encoding="utf-8"))


@app.get("/dashboard", response_class=HTMLResponse)
async def serve_dashboard():
    """
    Serve dashboard.html with the `compare_partial` dataset injected inline,
    pulled live from MySQL. Avoids dashboard race conditions on first paint.
    """
    html_path = BASE_DIR / "dashboard.html"
    if not html_path.exists():
        raise HTTPException(status_code=404, detail="dashboard.html not found")
    html = html_path.read_text(encoding="utf-8")

    partial_json   = "[]"
    partial_status = "not_found"
    partial_rows   = 0

    try:
        data = _fetch_compare("compare_partial", COMPARE_DB_TO_JSON)
        if data:
            partial_json   = json.dumps(data, ensure_ascii=False, separators=(",", ":"))
            partial_rows   = len(data)
            partial_status = "loaded"
        else:
            partial_status = "empty"
    except Exception as e:
        partial_status = f"error: {e}"

    # Prevent the JSON from terminating the <script> tag
    partial_json = partial_json.replace("</", "<\\/")

    injection = (
        "\n<!-- PARTIAL DATA INJECTION START -->\n"
        f"<!-- status: {partial_status} | rows: {partial_rows} | source: postgres.compare_partial -->\n"
        f'<script id="__PARTIAL_DATA__" type="application/json">{partial_json}</script>\n'
        "<!-- PARTIAL DATA INJECTION END -->\n"
    )
    html = html.replace("</body>", f"{injection}</body>", 1)

    return HTMLResponse(
        content=html,
        headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
    )


# ═════════════════════════════════════════════════════════════════════════════
# Auth
# ═════════════════════════════════════════════════════════════════════════════
class LoginRequest(BaseModel):
    password: str


@app.post("/api/login")
async def login(req: LoginRequest):
    if req.password == DASHBOARD_PASSWORD:
        return {"status": "ok", "message": "Access granted"}
    raise HTTPException(status_code=401, detail="Incorrect password")


@app.get("/api/login/config")
async def login_config():
    return {"pw_hash": hashlib.sha256(DASHBOARD_PASSWORD.encode()).hexdigest()}


# ═════════════════════════════════════════════════════════════════════════════
# Data endpoints — all served from MySQL
# ═════════════════════════════════════════════════════════════════════════════
@app.get("/api/compare")
async def get_default_compare():
    """3-vendor matches (compare_common table)."""
    try:
        return JSONResponse(content=_fetch_compare("compare_common", COMPARE_DB_TO_JSON))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB read failed: {e}")


@app.get("/api/partial")
async def get_partial():
    """compare_partial — diamonds with >=1 vendor match (used by Vendor Price Buckets)."""
    try:
        rows = _fetch_compare("compare_partial", COMPARE_DB_TO_JSON)
        print(f"[OK] /api/partial -> {len(rows):,} rows from MySQL")
        return JSONResponse(content=rows)
    except Exception as e:
        print(f"[ERR] /api/partial: {e}")
        raise HTTPException(status_code=500, detail=f"DB read failed: {e}")


@app.get("/api/br-vs-lg")
async def get_br_vs_lg():
    """compare_br_vs_lg — diamonds present in both Brilliance AND Loose-Grown."""
    try:
        return JSONResponse(content=_fetch_compare("compare_br_vs_lg", BR_VS_LG_DB_TO_JSON))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB read failed: {e}")


@app.get("/api/status")
async def get_diamond_status():
    """Latest snapshot from diamond_status table."""
    data = _fetch_status()
    if data is None:
        raise HTTPException(status_code=404, detail="diamond_status table is empty — run the pipeline first")
    return JSONResponse(content=data)


# Legacy filename-based endpoint kept so the dashboard "files" dropdown still works.
# Maps the old JSON filename -> the corresponding DB table.
FILENAME_TO_TABLE = {
    "compare.json":          ("compare_common",   COMPARE_DB_TO_JSON),
    "compare_partial.json":  ("compare_partial",  COMPARE_DB_TO_JSON),
    "compare_all.json":      ("compare_all",      COMPARE_DB_TO_JSON),
    "compare_br_vs_lg.json": ("compare_br_vs_lg", BR_VS_LG_DB_TO_JSON),
}


@app.get("/api/data/{filename}")
async def get_json_data(filename: str):
    if ".." in filename or "/" in filename or "\\" in filename:
        raise HTTPException(status_code=400, detail="Invalid filename")
    if filename not in FILENAME_TO_TABLE:
        raise HTTPException(status_code=404, detail=f"Unknown dataset: {filename}")
    table, key_map = FILENAME_TO_TABLE[filename]
    try:
        return JSONResponse(content=_fetch_compare(table, key_map))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB read failed: {e}")


@app.get("/api/files")
async def list_datasets():
    """Lists DB-backed datasets so dashboard.html can populate its dataset picker."""
    files = []
    for filename, (table, _) in FILENAME_TO_TABLE.items():
        rows = _table_row_count(table)
        files.append({
            "filename":      filename,
            "table":         table,
            "rows":          rows,
            "is_default":    filename == "compare.json",
        })
    return {"files": files, "count": len(files)}


# ═════════════════════════════════════════════════════════════════════════════
# Pipeline trigger — kept for local use (subprocess runs compare + reloads DB)
# ═════════════════════════════════════════════════════════════════════════════
@app.post("/api/run-compare")
async def run_compare():
    """
    Local convenience: runs diamond_compare.py (generates compare/*.json) and
    then compare_to_mysql.py (reloads the DB tables). Not useful on Render
    because Render's filesystem is ephemeral and the scrapers don't run there.
    """
    import subprocess, sys

    compare_script = BASE_DIR / "diamond_compare.py"
    mysql_script   = BASE_DIR / "compare_to_mysql.py"

    if not compare_script.exists():
        raise HTTPException(status_code=404, detail="diamond_compare.py not found")

    try:
        cmp_result = subprocess.run(
            [sys.executable, str(compare_script)],
            capture_output=True, text=True, timeout=600,
        )
        cmp_ok = cmp_result.returncode == 0

        mysql_ok = False
        if cmp_ok and mysql_script.exists():
            ml_result = subprocess.run(
                [sys.executable, str(mysql_script)],
                capture_output=True, text=True, timeout=600,
            )
            mysql_ok = ml_result.returncode == 0

        common_rows = _table_row_count("compare_common")
        return JSONResponse(content={
            "status":     "ok" if (cmp_ok and mysql_ok) else "error",
            "comparator": "ok" if cmp_ok else "failed",
            "mysql_load": "ok" if mysql_ok else "failed",
            "common_rows": common_rows,
            "stdout":      (cmp_result.stdout or "")[-3000:],
            "stderr":      (cmp_result.stderr or "")[-2000:],
        })
    except subprocess.TimeoutExpired:
        raise HTTPException(status_code=504, detail="Compare pipeline timed out (>10 min)")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ═════════════════════════════════════════════════════════════════════════════
# Health
# ═════════════════════════════════════════════════════════════════════════════
@app.get("/api/health")
async def health():
    db_ok       = False
    db_error    = None
    table_counts: dict[str, int] = {}

    try:
        with _get_conn() as conn, conn.cursor() as cur:
            cur.execute("SELECT 1 AS ok")
            cur.fetchone()
            db_ok = True
            for table in ("compare_common", "compare_partial", "compare_all", "compare_br_vs_lg", "diamond_status"):
                try:
                    cur.execute(f'SELECT COUNT(*) AS c FROM "{table}"')
                    row = cur.fetchone()
                    table_counts[table] = int(row["c"]) if row else 0
                except Exception:
                    table_counts[table] = -1   # table missing
    except Exception as e:
        db_error = str(e)

    # Show host only (never the full URL with credentials).
    db_host_display = "unknown"
    try:
        from urllib.parse import urlparse
        db_host_display = urlparse(DATABASE_URL).hostname or "unknown"
    except Exception:
        pass

    return {
        "status":          "ok" if db_ok else "degraded",
        "db_engine":       "postgresql",
        "db_host":         db_host_display,
        "db_ok":           db_ok,
        "db_error":        db_error,
        "tables":          table_counts,
        # Legacy flag the dashboard boot path expects: true when compare_common
        # has data, so the UI knows to call /api/compare for the initial load.
        "default_dataset": table_counts.get("compare_common", 0) > 0,
        "timestamp":       datetime.datetime.now().isoformat(),
    }


# ═════════════════════════════════════════════════════════════════════════════
# Local dev entrypoint — Render uses its own start command (see top docstring)
# ═════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", "4000"))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=True)
