
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
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel
from starlette.responses import Response

# ---- Paths -----------------------------------------------------------------
BASE_DIR = Path(__file__).parent.resolve()
STATIC_DIR = BASE_DIR / "static"
COMPARE_DIR = BASE_DIR / "compare"
COMPARE_DIR.mkdir(exist_ok=True)
DIAMOND_FILES_DIR = BASE_DIR / "diamond_files"
FILE_STATUS_JSON = DIAMOND_FILES_DIR / "file_status.json"

# ---- Load .env -------------------------------------------------------------
load_dotenv(BASE_DIR / ".env")

# ---- PostgreSQL config -----------------------------------------------------
DATABASE_URL = os.environ.get("DATABASE_URL", "")
if not DATABASE_URL:
    raise RuntimeError(
        "DATABASE_URL env var is required. "
        "Put it in .env locally, or set it in Render's Environment tab."
    )
if "sslmode=" not in DATABASE_URL:
    DATABASE_URL += ("&" if "?" in DATABASE_URL else "?") + "sslmode=require"

# ---- Auth ------------------------------------------------------------------
DASHBOARD_PASSWORD = os.environ.get("DASHBOARD_PASSWORD", "diamond-6087")

# ---- FastAPI app -----------------------------------------------------------
app = FastAPI(title="Diamond Price Dashboard API", version="3.0.0")
app.add_middleware(GZipMiddleware, minimum_size=1000)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

PUBLIC_PATHS = {"/", "/dashboard", "/api/login", "/api/login/config", "/api/health", "/api/file-dates"}


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


# ============================================================================
# PostgreSQL helpers
# ============================================================================
@contextmanager
def _get_conn():
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
    if isinstance(v, Decimal):
        f = float(v)
        return int(f) if f.is_integer() else f
    if isinstance(v, (datetime.datetime, datetime.date)):
        return (
            v.strftime("%Y-%m-%d %H:%M:%S")
            if isinstance(v, datetime.datetime)
            else v.isoformat()
        )
    return v


# ============================================================================
# Column maps (DB -> JSON keys the dashboard expects)
# ============================================================================
COMPARE_DB_TO_JSON = {
    "certificate_number":      "certificate_number",
    "vendors_matched":         "vendors_matched",
    "shape":                   "shape",
    "color":                   "color",
    "clarity":                 "clarity",
    "cut":                     "cut",
    "carat_weight":            "caratWeight",
    "lg_price_usd":            "loose-grown Price USD",
    "lg_minus_30_usd":         "loose-grown -30% USD",
    "lg_vs_pc_usd":            "loose-grown vs PC USD",
    "lg_vs_pc_pct":            "loose-grown vs PC %",
    "brilliance_price_usd":    "brilliance Price USD",
    "brilliance_minus_30_usd": "brilliance -30% USD",
    "brilliance_vs_pc_usd":    "brilliance vs PC USD",
    "brilliance_vs_pc_pct":    "brilliance vs PC %",
    "pc_price_usd":            "PC Price USD",
    "pc_minus_30_usd":         "PC -30% USD",
}

BR_VS_LG_DB_TO_JSON = {
    "diamond_id":              "diamond_id",
    "shape":                   "shape",
    "carat_weight":            "caratWeight",
    "cut":                     "cut",
    "color":                   "color",
    "clarity":                 "clarity",
    "lg_price_usd":            "loose-grown Price USD",
    "brilliance_price_usd":    "brilliance Price USD",
    "lg_minus_30_usd":         "loose-grown -30% USD",
    "brilliance_minus_30_usd": "brilliance -30% USD",
    "br_minus_30_vs_lg_usd":   "BR -30% vs LG USD",
    "br_minus_30_vs_lg_pct":   "BR -30% vs LG %",
}

# vendor key -> { price_col, minus30_col, pct_col }
VENDOR_COLS = {
    "lg": {
        "price_col":   "lg_price_usd",
        "minus30_col": "lg_minus_30_usd",
        "pct_col":     "lg_vs_pc_pct",
    },
    "br": {
        "price_col":   "brilliance_price_usd",
        "minus30_col": "brilliance_minus_30_usd",
        "pct_col":     "brilliance_vs_pc_pct",
    },
    # luvansh isn't in DB yet; included so client doesn't break on missing key
    "lu": {
        "price_col":   None,
        "minus30_col": None,
        "pct_col":     None,
    },
}

# Bucket definitions MUST match dashboard.html (BUCKETS, PRICE_BUCKETS)
PCT_BUCKETS = [
    {"label": "< 0%",      "min": None,  "max": 0.0},
    {"label": "0 - 99%",   "min": 0.0,   "max": 100.0},
    {"label": "100-199%",  "min": 100.0, "max": 200.0},
    {"label": "200-299%",  "min": 200.0, "max": 300.0},
    {"label": "300-399%",  "min": 300.0, "max": 400.0},
    {"label": "400-499%",  "min": 400.0, "max": 500.0},
    {"label": "500%+",     "min": 500.0, "max": None},
]

PRICE_BUCKETS = [
    {"label": "$0 - $150",       "min": 0.0,    "max": 151.0},
    {"label": "$151 - $300",     "min": 151.0,  "max": 301.0},
    {"label": "$301 - $400",     "min": 301.0,  "max": 401.0},
    {"label": "$401 - $550",     "min": 401.0,  "max": 551.0},
    {"label": "$551 - $800",     "min": 551.0,  "max": 801.0},
    {"label": "$801 - $1,200",   "min": 801.0,  "max": 1201.0},
    {"label": "$1,200+",         "min": 1201.0, "max": None},
]

# Tables exposed via /api/data/{filename}
FILENAME_TO_TABLE = {
    "compare.json":          ("compare_common",   COMPARE_DB_TO_JSON),
    "compare_partial.json":  ("compare_partial",  COMPARE_DB_TO_JSON),
    "compare_all.json":      ("compare_all",      COMPARE_DB_TO_JSON),
    "compare_br_vs_lg.json": ("compare_br_vs_lg", BR_VS_LG_DB_TO_JSON),
}

ALLOWED_TABLES = {t for t, _ in FILENAME_TO_TABLE.values()}
MAX_LIMIT = 500
DEFAULT_LIMIT = 20


# ============================================================================
# Filter -> WHERE clause builder
# ============================================================================
def _parse_bucket_param(raw: str | None) -> tuple[str | None, int | None]:
    """Parse 'lg:3' -> ('lg', 3). Returns (None, None) on invalid."""
    if not raw or ":" not in raw:
        return None, None
    vendor_key, idx_s = raw.split(":", 1)
    vendor_key = vendor_key.strip()
    if vendor_key not in VENDOR_COLS:
        return None, None
    try:
        idx = int(idx_s)
    except ValueError:
        return None, None
    return vendor_key, idx


def _build_where(table: str, params: dict) -> tuple[str, list]:
    """
    Build a SQL WHERE clause + parameter list from the request's filter params.
    Only filter columns that exist in the given table are applied.
    """
    clauses: list[str] = []
    args: list[Any] = []

    cert = (params.get("cert") or "").strip()
    if cert:
        # certificate_number / diamond_id depending on table
        col = "diamond_id" if table == "compare_br_vs_lg" else "certificate_number"
        clauses.append(f'"{col}"::text ILIKE %s')
        args.append(f"%{cert}%")

    # PC price range only valid for compare_* tables (not br_vs_lg)
    if table != "compare_br_vs_lg":
        pmin = params.get("price_min")
        pmax = params.get("price_max")
        if pmin not in (None, ""):
            try:
                clauses.append('"pc_price_usd" >= %s'); args.append(float(pmin))
            except ValueError:
                pass
        if pmax not in (None, ""):
            try:
                clauses.append('"pc_price_usd" <= %s'); args.append(float(pmax))
            except ValueError:
                pass

    # vendor_pct: vendor_key:bucket_idx -> filter on <vendor>_vs_pc_pct in bucket range
    vp_key, vp_idx = _parse_bucket_param(params.get("vendor_pct"))
    if vp_key and vp_idx is not None and 0 <= vp_idx < len(PCT_BUCKETS):
        col = VENDOR_COLS[vp_key]["pct_col"]
        if col:
            bucket = PCT_BUCKETS[vp_idx]
            if bucket["min"] is not None:
                clauses.append(f'"{col}" >= %s'); args.append(bucket["min"])
            if bucket["max"] is not None:
                clauses.append(f'"{col}" < %s'); args.append(bucket["max"])
            clauses.append(f'"{col}" IS NOT NULL')

    # vendor_price: vendor_key:bucket_idx -> filter on <vendor>_minus_30_usd in bucket range
    vpr_key, vpr_idx = _parse_bucket_param(params.get("vendor_price"))
    if vpr_key and vpr_idx is not None and 0 <= vpr_idx < len(PRICE_BUCKETS):
        col = VENDOR_COLS[vpr_key]["minus30_col"]
        if col:
            bucket = PRICE_BUCKETS[vpr_idx]
            if bucket["min"] is not None:
                clauses.append(f'"{col}" >= %s'); args.append(bucket["min"])
            if bucket["max"] is not None:
                clauses.append(f'"{col}" < %s'); args.append(bucket["max"])
            clauses.append(f'"{col}" IS NOT NULL AND "{col}" > 0')

    if (params.get("common_only") or "").lower() == "true" and table != "compare_br_vs_lg":
        for c in ("lg_price_usd", "brilliance_price_usd", "pc_price_usd"):
            clauses.append(f'"{c}" IS NOT NULL AND "{c}" > 0')

    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    return where, args


def _sanitize_sort(sort_col: str | None, key_map: dict[str, str]) -> str | None:
    if not sort_col:
        return None
    if sort_col in key_map:
        return sort_col
    return None


def _clamp_int(raw: Any, default: int, lo: int, hi: int) -> int:
    try:
        v = int(raw)
    except (TypeError, ValueError):
        return default
    return max(lo, min(hi, v))


# ============================================================================
# Paginated row fetcher
# ============================================================================
def _fetch_rows_paginated(
    table: str,
    key_map: dict[str, str],
    params: dict,
) -> dict:
    if table not in ALLOWED_TABLES:
        raise HTTPException(status_code=404, detail=f"Unknown table: {table}")

    limit = _clamp_int(params.get("limit"), DEFAULT_LIMIT, 1, MAX_LIMIT)
    offset = _clamp_int(params.get("offset"), 0, 0, 10_000_000)

    where_sql, where_args = _build_where(table, params)

    sort_col = _sanitize_sort(params.get("sort_col"), key_map)
    sort_dir = "ASC" if (params.get("sort_dir") or "asc").lower() == "asc" else "DESC"
    order_sql = f'ORDER BY "{sort_col}" {sort_dir} NULLS LAST' if sort_col else ""

    db_cols = list(key_map.keys())
    cols_sql = ", ".join(f'"{c}"' for c in db_cols)

    rows_sql = f'SELECT {cols_sql} FROM "{table}" {where_sql} {order_sql} LIMIT %s OFFSET %s'
    count_sql = f'SELECT COUNT(*) AS c FROM "{table}" {where_sql}'

    with _get_conn() as conn, conn.cursor() as cur:
        cur.execute(count_sql, where_args)
        total = int(cur.fetchone()["c"])
        cur.execute(rows_sql, [*where_args, limit, offset])
        raw = cur.fetchall()

    out_rows = [
        {key_map[c]: _jsonify_value(r[c]) for c in db_cols} for r in raw
    ]
    return {
        "rows": out_rows,
        "total": total,
        "limit": limit,
        "offset": offset,
    }


# ============================================================================
# Aggregates: per-vendor price & pct bucket counts + averages, in one SQL pass
# ============================================================================
def _aggregate_sql_for_vendor(vendor_key: str) -> tuple[list[str], list[str]] | None:
    """Returns (pct_expressions, price_expressions) for the SELECT clause."""
    cols = VENDOR_COLS.get(vendor_key)
    if not cols or not cols["pct_col"] or not cols["minus30_col"]:
        return None
    pct_col = cols["pct_col"]
    pr_col = cols["minus30_col"]
    price_col = cols["price_col"]

    pct_exprs: list[str] = []
    for i, b in enumerate(PCT_BUCKETS):
        conds = [f'"{pct_col}" IS NOT NULL']
        if b["min"] is not None:
            conds.append(f'"{pct_col}" >= {b["min"]}')
        if b["max"] is not None:
            conds.append(f'"{pct_col}" < {b["max"]}')
        cond_sql = " AND ".join(conds)
        pct_exprs.append(
            f'COUNT(*) FILTER (WHERE {cond_sql}) AS pct_{vendor_key}_{i}_cnt'
        )
        pct_exprs.append(
            f'AVG("pc_price_usd") FILTER (WHERE {cond_sql} AND "pc_price_usd" > 0) '
            f'AS pct_{vendor_key}_{i}_avgpc'
        )

    price_exprs: list[str] = []
    for i, b in enumerate(PRICE_BUCKETS):
        conds = [f'"{pr_col}" IS NOT NULL', f'"{pr_col}" > 0']
        if b["min"] is not None:
            conds.append(f'"{pr_col}" >= {b["min"]}')
        if b["max"] is not None:
            conds.append(f'"{pr_col}" < {b["max"]}')
        cond_sql = " AND ".join(conds)
        price_exprs.append(
            f'COUNT(*) FILTER (WHERE {cond_sql}) AS price_{vendor_key}_{i}_cnt'
        )
        price_exprs.append(
            f'AVG("pc_price_usd") FILTER (WHERE {cond_sql} AND "pc_price_usd" > 0) '
            f'AS price_{vendor_key}_{i}_avgpc'
        )

    # vendor total (rows where this vendor has a price)
    if price_col:
        pct_exprs.append(
            f'COUNT(*) FILTER (WHERE "{price_col}" IS NOT NULL AND "{price_col}" > 0) '
            f'AS vendor_{vendor_key}_total'
        )

    return pct_exprs, price_exprs


def _compute_aggregates(table: str, params: dict) -> dict:
    """One aggregate query returning bucket counts + averages for all vendors."""
    if table not in ALLOWED_TABLES or table == "compare_br_vs_lg":
        return {"vendors": {}, "total": 0}

    where_sql, where_args = _build_where(table, params)

    select_exprs: list[str] = ['COUNT(*) AS total_rows']
    active_vendors: list[str] = []
    for vk in ("lg", "br"):  # luvansh has no DB columns yet
        out = _aggregate_sql_for_vendor(vk)
        if out is None:
            continue
        pct_exprs, price_exprs = out
        select_exprs.extend(pct_exprs)
        select_exprs.extend(price_exprs)
        active_vendors.append(vk)

    sql = f'SELECT {", ".join(select_exprs)} FROM "{table}" {where_sql}'
    with _get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, where_args)
        row = cur.fetchone()

    def _f(key: str) -> float | None:
        v = row.get(key)
        if v is None:
            return None
        return _jsonify_value(v)

    vendors: dict[str, dict] = {}
    for vk in active_vendors:
        pct_bs = []
        for i in range(len(PCT_BUCKETS)):
            pct_bs.append({
                "idx":   i,
                "count": int(row.get(f"pct_{vk}_{i}_cnt") or 0),
                "avg_pc": _f(f"pct_{vk}_{i}_avgpc"),
            })
        price_bs = []
        for i in range(len(PRICE_BUCKETS)):
            price_bs.append({
                "idx":   i,
                "count": int(row.get(f"price_{vk}_{i}_cnt") or 0),
                "avg_pc": _f(f"price_{vk}_{i}_avgpc"),
            })
        vendors[vk] = {
            "vendor_total": int(row.get(f"vendor_{vk}_total") or 0),
            "pct_buckets":   pct_bs,
            "price_buckets": price_bs,
        }

    return {
        "vendors": vendors,
        "total": int(row.get("total_rows") or 0),
    }


# ============================================================================
# Status / files
# ============================================================================
def _fetch_status() -> dict | None:
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
                "matched":        r["lg_matched"],
                "match_rate_pct": _jsonify_value(r["lg_match_rate_pct"]),
            },
            "brilliance": {
                "matched":        r["brilliance_matched"],
                "match_rate_pct": _jsonify_value(r["brilliance_match_rate_pct"]),
            },
        },
        "dataset_counts": {
            "common":   r["common_count"],
            "partial":  r["partial_count"],
            "all_pc":   r["all_pc_count"],
            "br_vs_lg": r["br_vs_lg_count"],
        },
        "file_dates": {
            "pc":          _jsonify_value(r.get("pc_file_date")),
            "loose-grown": _jsonify_value(r.get("lg_file_date")),
            "brilliance":  _jsonify_value(r.get("brilliance_file_date")),
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


# ============================================================================
# Pages
# ============================================================================
@app.get("/", response_class=HTMLResponse)
async def serve_login():
    html_path = STATIC_DIR / "login.html"
    if not html_path.exists():
        raise HTTPException(status_code=404, detail="login.html not found")
    return HTMLResponse(content=html_path.read_text(encoding="utf-8"))


@app.get("/dashboard", response_class=HTMLResponse)
async def serve_dashboard():
    """Serve dashboard.html as-is. No data is inlined - the client paginates from /api/*."""
    html_path = STATIC_DIR / "dashboard.html"
    if not html_path.exists():
        raise HTTPException(status_code=404, detail="dashboard.html not found")
    return HTMLResponse(
        content=html_path.read_text(encoding="utf-8"),
        headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
    )


# ============================================================================
# Auth
# ============================================================================
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


# ============================================================================
# Paginated data endpoints
# ============================================================================
def _params_dict(request: Request) -> dict:
    return {k: v for k, v in request.query_params.items()}


@app.get("/api/compare")
async def get_default_compare(request: Request):
    """compare_common (3-vendor matches), paginated."""
    try:
        return JSONResponse(content=_fetch_rows_paginated(
            "compare_common", COMPARE_DB_TO_JSON, _params_dict(request)
        ))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB read failed: {e}")


@app.get("/api/partial")
async def get_partial(request: Request):
    """compare_partial (>=1 vendor match), paginated."""
    try:
        return JSONResponse(content=_fetch_rows_paginated(
            "compare_partial", COMPARE_DB_TO_JSON, _params_dict(request)
        ))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB read failed: {e}")


@app.get("/api/br-vs-lg")
async def get_br_vs_lg(request: Request):
    """compare_br_vs_lg, paginated."""
    try:
        return JSONResponse(content=_fetch_rows_paginated(
            "compare_br_vs_lg", BR_VS_LG_DB_TO_JSON, _params_dict(request)
        ))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB read failed: {e}")


@app.get("/api/data/{filename}")
async def get_json_data(filename: str, request: Request):
    if ".." in filename or "/" in filename or "\\" in filename:
        raise HTTPException(status_code=400, detail="Invalid filename")
    if filename not in FILENAME_TO_TABLE:
        raise HTTPException(status_code=404, detail=f"Unknown dataset: {filename}")
    table, key_map = FILENAME_TO_TABLE[filename]
    try:
        return JSONResponse(content=_fetch_rows_paginated(
            table, key_map, _params_dict(request)
        ))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB read failed: {e}")


@app.get("/api/aggregates")
async def get_aggregates(request: Request, dataset: str = Query("compare_partial.json")):
    """Per-vendor bucket counts + averages for the given dataset filename, with filters applied."""
    if dataset not in FILENAME_TO_TABLE:
        raise HTTPException(status_code=404, detail=f"Unknown dataset: {dataset}")
    table, _ = FILENAME_TO_TABLE[dataset]
    try:
        return JSONResponse(content=_compute_aggregates(table, _params_dict(request)))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Aggregate query failed: {e}")


@app.get("/api/status")
async def get_diamond_status():
    data = _fetch_status()
    if data is None:
        raise HTTPException(
            status_code=404,
            detail="diamond_status table is empty - run the pipeline first",
        )
    db_fd = data.get("file_dates") or {}
    if not any(db_fd.get(k) for k in ("pc", "loose-grown", "brilliance")):
        data["file_dates"] = _read_file_dates()
    return JSONResponse(content=data)


# Vendor key -> the "vendor" label written into file_status.json by diamond_compare.py
_FILE_DATE_VENDOR_MAP = {
    "PC":          "pc",
    "loose-grown": "loose-grown",
    "brilliance":  "brilliance",
}


def _read_file_dates() -> dict:
    """Return {pc: 'YYYY-MM-DD HH:MM:SS' | None, 'loose-grown': ..., 'brilliance': ...,
    checked_at: ... | None} from diamond_files/file_status.json. Returns all-None on
    missing/invalid file so the dashboard can still render."""
    out: dict = {"pc": None, "loose-grown": None, "brilliance": None, "checked_at": None}
    if not FILE_STATUS_JSON.exists():
        return out
    try:
        payload = json.loads(FILE_STATUS_JSON.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return out
    out["checked_at"] = payload.get("checked_at")
    for entry in payload.get("files", []) or []:
        vendor = entry.get("vendor")
        key = _FILE_DATE_VENDOR_MAP.get(vendor)
        if key:
            out[key] = entry.get("last_modified")
    return out


@app.get("/api/file-dates")
async def get_file_dates():
    """Per-vendor source CSV last-modified timestamps (PC, loose-grown, brilliance).
    Read from diamond_files/file_status.json which is refreshed every pipeline run."""
    return JSONResponse(content=_read_file_dates())


@app.get("/api/files")
async def list_datasets():
    files = []
    for filename, (table, _) in FILENAME_TO_TABLE.items():
        rows = _table_row_count(table)
        files.append({
            "filename":   filename,
            "table":      table,
            "rows":       rows,
            "is_default": filename == "compare.json",
        })
    return {"files": files, "count": len(files)}


# ============================================================================
# Pipeline trigger (local only)
# ============================================================================
@app.post("/api/run-compare")
async def run_compare():
    import subprocess
    import sys

    compare_script = BASE_DIR / "diamond_compare.py"
    mysql_script = BASE_DIR / "compare_to_mysql.py"

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
            "status":      "ok" if (cmp_ok and mysql_ok) else "error",
            "comparator":  "ok" if cmp_ok else "failed",
            "mysql_load":  "ok" if mysql_ok else "failed",
            "rows":        common_rows,
            "common_rows": common_rows,
            "stdout":      (cmp_result.stdout or "")[-3000:],
            "stderr":      (cmp_result.stderr or "")[-2000:],
        })
    except subprocess.TimeoutExpired:
        raise HTTPException(status_code=504, detail="Compare pipeline timed out (>10 min)")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# Health
# ============================================================================
@app.get("/api/health")
async def health():
    db_ok = False
    db_error = None
    table_counts: dict[str, int] = {}

    try:
        with _get_conn() as conn, conn.cursor() as cur:
            cur.execute("SELECT 1 AS ok")
            cur.fetchone()
            db_ok = True
            for table in ("compare_common", "compare_partial", "compare_all",
                          "compare_br_vs_lg", "diamond_status"):
                try:
                    cur.execute(f'SELECT COUNT(*) AS c FROM "{table}"')
                    row = cur.fetchone()
                    table_counts[table] = int(row["c"]) if row else 0
                except Exception:
                    table_counts[table] = -1
    except Exception as e:
        db_error = str(e)

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
        "default_dataset": table_counts.get("compare_common", 0) > 0,
        "timestamp":       datetime.datetime.now().isoformat(),
    }


# ============================================================================
# Local dev entrypoint
# ============================================================================
if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", "4000"))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=True)
