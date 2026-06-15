
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
STATIC_DIR = BASE_DIR / "usa_compare" / "static"
COMPARE_DIR = BASE_DIR / "compare" / "usa"
COMPARE_DIR.mkdir(parents=True, exist_ok=True)
DIAMOND_FILES_DIR = BASE_DIR / "diamond_files" / "usa"
FILE_STATUS_JSON = DIAMOND_FILES_DIR / "file_status.json"

# UK side: dashboard lives in uk_scrapping/, the UK pipeline writes its compare
# JSON to uk_scrapping/compare/uk, and the UK loader (uk_compare_to_db.py)
# pushes those into the uk_* PostgreSQL tables.
UK_DIR = BASE_DIR / "uk_scrapping"
UK_STATIC_DIR = UK_DIR / "static"
UK_COMPARE_DIR = UK_DIR / "compare" / "uk"

ROA_DIR = BASE_DIR / "roa_scrapping"
ROA_STATIC_DIR = ROA_DIR / "static"

ROG_DIR = BASE_DIR / "germany_scrapping"
ROG_STATIC_DIR = ROG_DIR / "static"

SELECT_PAGE = STATIC_DIR / "select.html"

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

PUBLIC_PATHS = {
    "/", "/dashboard", "/dashboard/usa", "/dashboard/uk", "/dashboard/roa", "/dashboard/rog",
    "/api/login", "/api/login/config", "/api/health", "/api/file-dates",
    "/api/uk/health", "/api/roa/health", "/api/rog/health",
}


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
async def serve_select():
    """Region selector shown after login. Links to /dashboard/usa and /dashboard/uk."""
    if not SELECT_PAGE.exists():
        raise HTTPException(status_code=404, detail="select.html not found")
    return HTMLResponse(
        content=SELECT_PAGE.read_text(encoding="utf-8"),
        headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
    )


@app.get("/dashboard/usa", response_class=HTMLResponse)
async def serve_dashboard_usa():
    """USA dashboard. The client paginates from /api/* (PostgreSQL, USD)."""
    html_path = STATIC_DIR / "dashboard.html"
    if not html_path.exists():
        raise HTTPException(status_code=404, detail="USA dashboard.html not found")
    return HTMLResponse(
        content=html_path.read_text(encoding="utf-8"),
        headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
    )


@app.get("/dashboard/uk", response_class=HTMLResponse)
async def serve_dashboard_uk():
    """UK dashboard. The client reads from /api/uk/* (PostgreSQL, GBP)."""
    html_path = UK_STATIC_DIR / "dashboard.html"
    if not html_path.exists():
        raise HTTPException(status_code=404, detail="UK dashboard.html not found")
    return HTMLResponse(
        content=html_path.read_text(encoding="utf-8"),
        headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
    )


@app.get("/dashboard/aus", response_class=HTMLResponse)
async def serve_dashboard_roa():
    """AUS/ROA dashboard. The client reads from /api/roa/* (PostgreSQL, USD)."""
    html_path = ROA_STATIC_DIR / "dashboard.html"
    if not html_path.exists():
        raise HTTPException(status_code=404, detail="ROA dashboard.html not found")
    return HTMLResponse(
        content=html_path.read_text(encoding="utf-8"),
        headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
    )


@app.get("/dashboard/germany", response_class=HTMLResponse)
async def serve_dashboard_rog():
    """Germany/ROG dashboard. The client reads from /api/rog/* (PostgreSQL, EUR)."""
    html_path = ROG_STATIC_DIR / "dashboard.html"
    if not html_path.exists():
        raise HTTPException(status_code=404, detail="ROG dashboard.html not found")
    return HTMLResponse(
        content=html_path.read_text(encoding="utf-8"),
        headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
    )


# ============================================================================
# ============================================================================
# ROG SIDE  —  /api/rog/*   (novita · 77diamonds · renesim vs ROG, EUR)
# ============================================================================
# ============================================================================
ROG_DB_TO_JSON = {
    "certificate_number":  "certificate_number",
    "vendors_matched":     "vendors_matched",
    "shape":               "shape",
    "color":               "color",
    "clarity":             "clarity",
    "cut":                 "cut",
    "carat_weight":        "caratWeight",
    "rog_price_eur":       "ROG Price EUR",
    "rog_minus_30_eur":    "ROG -30% EUR",
    "renesim_price_eur":   "renesim Price EUR",
    "renesim_minus_eur":   "renesim -0% EUR",
    "renesim_vs_rog_eur":  "renesim vs ROG EUR",
    "renesim_vs_rog_pct":  "renesim vs ROG %",
    "novita_price_eur":    "novita Price EUR",
    "novita_minus_eur":    "novita -0% EUR",
    "novita_vs_rog_eur":   "novita vs ROG EUR",
    "novita_vs_rog_pct":   "novita vs ROG %",
    "d77_price_eur":       "77diamonds Price EUR",
    "d77_minus_30_eur":    "77diamonds -30% EUR",
    "d77_vs_rog_eur":      "77diamonds vs ROG EUR",
    "d77_vs_rog_pct":      "77diamonds vs ROG %",
}

ROG_FILENAME_TO_TABLE = {
    "compare.json":         "rog_compare_common",
    "compare_partial.json": "rog_compare_partial",
    "compare_all.json":     "rog_compare_all",
}
ROG_ALLOWED_TABLES = set(ROG_FILENAME_TO_TABLE.values())

ROG_VENDOR_PRICE_COL = {
    "novita":     "novita_price_eur",
    "77diamonds": "d77_price_eur",
    "renesim":    "renesim_price_eur",
}


def _fetch_all_rog(table: str, where_sql: str = "", where_args: list | None = None) -> list[dict]:
    if table not in ROG_ALLOWED_TABLES:
        raise HTTPException(status_code=404, detail=f"Unknown ROG table: {table}")
    db_cols = list(ROG_DB_TO_JSON.keys())
    cols_sql = ", ".join(f'"{c}"' for c in db_cols)
    sql = f'SELECT {cols_sql} FROM "{table}" {where_sql}'
    with _get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, where_args or [])
        raw = cur.fetchall()
    return [{ROG_DB_TO_JSON[c]: _jsonify_value(r[c]) for c in db_cols} for r in raw]


def _rog_table_row_count(table: str) -> int:
    try:
        with _get_conn() as conn, conn.cursor() as cur:
            cur.execute(f'SELECT COUNT(*) AS c FROM "{table}"')
            row = cur.fetchone()
            return int(row["c"]) if row else 0
    except Exception:
        return 0


def _fetch_rog_status() -> dict | None:
    sql = 'SELECT * FROM "rog_diamond_status" ORDER BY "id" DESC LIMIT 1'
    try:
        with _get_conn() as conn, conn.cursor() as cur:
            cur.execute(sql)
            r = cur.fetchone()
    except Exception:
        return None
    if not r:
        return None

    def _vblock(matched, total, rate):
        matched = matched or 0
        total   = total or 0
        return {
            "matched":           matched,
            "vendor_total":      total,
            "vendor_not_in_rog": max(total - matched, 0),
            "match_rate_pct":    _jsonify_value(rate),
        }

    return {
        "generated_at": _jsonify_value(r["generated_at"]),
        "ROG_total":    r["rog_total"] or 0,
        "vendor_totals": {
            "renesim":    r["renesim_vendor_total"],
            "novita":     r["novita_vendor_total"],
            "77diamonds": r["d77_vendor_total"],
        },
        "ROG_vs_vendors": {
            "renesim":    _vblock(r["renesim_matched"], r["renesim_vendor_total"], r["renesim_match_rate_pct"]),
            "novita":     _vblock(r["novita_matched"],  r["novita_vendor_total"],  r["novita_match_rate_pct"]),
            "77diamonds": _vblock(r["d77_matched"],     r["d77_vendor_total"],     r["d77_match_rate_pct"]),
        },
        "dataset_counts": {
            "common":  r["common_count"],
            "partial": r["partial_count"],
            "all_rog": r["all_rog_count"],
        },
    }


@app.get("/api/rog/health")
async def rog_health():
    db_ok = False
    db_error = None
    table_counts: dict[str, int] = {}
    try:
        with _get_conn() as conn, conn.cursor() as cur:
            cur.execute("SELECT 1 AS ok")
            cur.fetchone()
            db_ok = True
            for table in ("rog_compare_common", "rog_compare_partial",
                          "rog_compare_all", "rog_diamond_status"):
                try:
                    cur.execute(f'SELECT COUNT(*) AS c FROM "{table}"')
                    row = cur.fetchone()
                    table_counts[table] = int(row["c"]) if row else 0
                except Exception:
                    table_counts[table] = -1
    except Exception as e:
        db_error = str(e)
    return {
        "status":          "ok" if db_ok else "degraded",
        "db_engine":       "postgresql",
        "db_ok":           db_ok,
        "db_error":        db_error,
        "tables":          table_counts,
        "default_dataset": table_counts.get("rog_compare_common", 0) > 0,
        "timestamp":       datetime.datetime.now().isoformat(),
    }


@app.get("/api/rog/files")
async def rog_list_datasets():
    files = []
    for filename, table in ROG_FILENAME_TO_TABLE.items():
        files.append({
            "filename":   filename,
            "table":      table,
            "rows":       _rog_table_row_count(table),
            "is_default": filename == "compare.json",
        })
    return {"files": files, "count": len(files)}


@app.get("/api/rog/compare")
async def rog_get_default_compare():
    try:
        return JSONResponse(content=_fetch_all_rog("rog_compare_common"))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB read failed: {e}")


@app.get("/api/rog/data/{filename}")
async def rog_get_json_data(filename: str):
    if ".." in filename or "/" in filename or "\\" in filename:
        raise HTTPException(status_code=400, detail="Invalid filename")
    if filename not in ROG_FILENAME_TO_TABLE:
        raise HTTPException(status_code=404, detail=f"Unknown ROG dataset: {filename}")
    try:
        return JSONResponse(content=_fetch_all_rog(ROG_FILENAME_TO_TABLE[filename]))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB read failed: {e}")


@app.get("/api/rog/status")
async def rog_get_status():
    data = _fetch_rog_status()
    if data is None:
        raise HTTPException(
            status_code=404,
            detail="rog_diamond_status table is empty — run the ROG pipeline first",
        )
    return JSONResponse(content=data)


@app.get("/api/rog/vendor-common/summary")
async def rog_vendor_common_summary():
    status = _fetch_rog_status()
    generated_at = status.get("generated_at") if status else None
    vendors: dict[str, dict] = {}
    for name, price_col in ROG_VENDOR_PRICE_COL.items():
        where = (
            f'WHERE "{price_col}" IS NOT NULL AND "{price_col}" > 0 '
            f'AND "rog_price_eur" IS NOT NULL AND "rog_price_eur" > 0'
        )
        try:
            with _get_conn() as conn, conn.cursor() as cur:
                cur.execute(f'SELECT COUNT(*) AS c FROM "rog_compare_all" {where}')
                row = cur.fetchone()
                vendors[name] = {"rows": int(row["c"]) if row else 0}
        except Exception:
            vendors[name] = {"rows": 0}
    return {"generated_at": generated_at, "vendors": vendors}


@app.get("/api/rog/vendor-common/{vendor}")
async def rog_vendor_common_rows(vendor: str):
    price_col = ROG_VENDOR_PRICE_COL.get(vendor)
    if not price_col:
        raise HTTPException(status_code=404, detail=f"Unknown ROG vendor: {vendor}")
    where = (
        f'WHERE "{price_col}" IS NOT NULL AND "{price_col}" > 0 '
        f'AND "rog_price_eur" IS NOT NULL AND "rog_price_eur" > 0'
    )
    try:
        return JSONResponse(content=_fetch_all_rog("rog_compare_all", where))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB read failed: {e}")


@app.get("/api/rog/vendor-overlap")
async def rog_vendor_overlap():
    """Diamonds whose certificate appears across novita, 77diamonds, and renesim."""
    try:
        conditions = " AND ".join(
            f'"{col}" IS NOT NULL AND "{col}" > 0'
            for col in ROG_VENDOR_PRICE_COL.values()
        )
        where = f"WHERE {conditions}"
        return JSONResponse(content=_fetch_all_rog("rog_compare_all", where))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB read failed: {e}")


@app.post("/api/rog/run-compare")
async def rog_run_compare():
    """Run the ROG compare pipeline then load output into rog_* tables."""
    import subprocess
    import sys

    compare_script = ROG_DIR / "diamond_compare.py"
    loader_script  = ROG_DIR / "germany_compare_to_db.py"

    if not compare_script.exists():
        raise HTTPException(status_code=404, detail="diamond_compare.py not found in germany_scrapping/")

    try:
        cmp_result = subprocess.run(
            [sys.executable, str(compare_script)],
            cwd=str(ROG_DIR),
            capture_output=True, text=True, timeout=600,
        )
        cmp_ok = cmp_result.returncode == 0

        load_ok = False
        if loader_script.exists():
            ld_result = subprocess.run(
                [sys.executable, str(loader_script)],
                cwd=str(ROG_DIR),
                capture_output=True, text=True, timeout=600,
            )
            load_ok = ld_result.returncode == 0

        rows = _rog_table_row_count("rog_compare_common")
        return JSONResponse(content={
            "status":     "ok" if (cmp_ok and load_ok) else "error",
            "returncode": cmp_result.returncode,
            "comparator": "ok" if cmp_ok else "failed",
            "db_load":    "ok" if load_ok else "failed",
            "rows":       rows,
            "stdout":     (cmp_result.stdout or "")[-3000:],
            "stderr":     (cmp_result.stderr or "")[-2000:],
        })
    except subprocess.TimeoutExpired:
        raise HTTPException(status_code=504, detail="ROG compare pipeline timed out (>10 min)")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/rog/upload-db")
async def rog_upload_db():
    """Truncate ROG tables and reload from the latest compare JSON files."""
    import subprocess
    import sys

    loader_script = ROG_DIR / "germany_compare_to_db.py"
    if not loader_script.exists():
        raise HTTPException(status_code=404, detail="germany_compare_to_db.py not found")

    try:
        result = subprocess.run(
            [sys.executable, str(loader_script)],
            cwd=str(ROG_DIR),
            capture_output=True, text=True, timeout=600,
        )
        rows = _rog_table_row_count("rog_compare_common")
        return JSONResponse(content={
            "status":     "ok" if result.returncode == 0 else "error",
            "returncode": result.returncode,
            "rows":       rows,
            "stdout":     (result.stdout or "")[-3000:],
            "stderr":     (result.stderr or "")[-2000:],
        })
    except subprocess.TimeoutExpired:
        raise HTTPException(status_code=504, detail="ROG DB upload timed out (>10 min)")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


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

    compare_script = BASE_DIR / "usa_compare" / "diamond_compare.py"
    mysql_script = BASE_DIR / "usa_compare" / "compare_to_mysql.py"

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
# ============================================================================
# UK SIDE  —  /api/uk/*   (ROUK baseline vs diamond-heavon & abelini, GBP)
# ============================================================================
# ============================================================================
# DB column -> the JSON key the UK dashboard expects (matches the keys written
# by uk_scrapping/diamond_compare_uk.py). The UK dashboard does client-side
# filtering/pagination, so these endpoints return full arrays.
UK_DB_TO_JSON = {
    "certificate_number": "certificate_number",
    "vendors_matched":    "vendors_matched",
    "shape":              "shape",
    "color":              "color",
    "clarity":            "clarity",
    "cut":                "cut",
    "carat_weight":       "caratWeight",
    "dh_price_gbp":       "diamond-heavon Price Pound",
    "dh_minus_30_gbp":    "diamond-heavon -30% Pound",
    "dh_vs_rouk_gbp":     "diamond-heavon vs ROUK Pound",
    "dh_vs_rouk_pct":     "diamond-heavon vs ROUK %",
    "ab_price_gbp":       "abelini Price Pound",
    "ab_minus_30_gbp":    "abelini -30% Pound",
    "ab_vs_rouk_gbp":     "abelini vs ROUK Pound",
    "ab_vs_rouk_pct":     "abelini vs ROUK %",
    "rouk_price_gbp":     "ROUK Price Pound",
    "rouk_minus_30_gbp":  "ROUK -30% Pound",
}

UK_FILENAME_TO_TABLE = {
    "compare.json":         "uk_compare_common",
    "compare_partial.json": "uk_compare_partial",
    "compare_all.json":     "uk_compare_all",
}
UK_ALLOWED_TABLES = set(UK_FILENAME_TO_TABLE.values())

# vendor URL key -> price column used for "common with ROUK" filtering
UK_VENDOR_PRICE_COL = {
    "diamond-heavon": "dh_price_gbp",
    "abelini":        "ab_price_gbp",
}


def _fetch_all_uk(table: str, where_sql: str = "", where_args: list | None = None) -> list[dict]:
    """Return every row of a uk_* table as a list of dicts keyed by the UK JSON keys."""
    if table not in UK_ALLOWED_TABLES:
        raise HTTPException(status_code=404, detail=f"Unknown UK table: {table}")
    db_cols = list(UK_DB_TO_JSON.keys())
    cols_sql = ", ".join(f'"{c}"' for c in db_cols)
    sql = f'SELECT {cols_sql} FROM "{table}" {where_sql}'
    with _get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, where_args or [])
        raw = cur.fetchall()
    return [{UK_DB_TO_JSON[c]: _jsonify_value(r[c]) for c in db_cols} for r in raw]


def _uk_table_row_count(table: str) -> int:
    try:
        with _get_conn() as conn, conn.cursor() as cur:
            cur.execute(f'SELECT COUNT(*) AS c FROM "{table}"')
            row = cur.fetchone()
            return int(row["c"]) if row else 0
    except Exception:
        return 0


def _fetch_uk_status() -> dict | None:
    sql = 'SELECT * FROM "uk_diamond_status" ORDER BY "id" DESC LIMIT 1'
    with _get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql)
        r = cur.fetchone()
    if not r:
        return None
    rouk_total = r["rouk_total"] or 0

    def _vendor_block(matched, total, rate):
        matched = matched or 0
        total = total or 0
        return {
            "matched":           matched,
            "vendor_total":      total,
            "vendor_not_in_rouk": max(total - matched, 0),
            "match_rate_pct":    _jsonify_value(rate),
        }

    return {
        "generated_at": _jsonify_value(r["generated_at"]),
        "rouk_total":   rouk_total,
        "vendor_totals": {
            "diamond-heavon": r["dh_vendor_total"],
            "abelini":        r["ab_vendor_total"],
        },
        "rouk_vs_vendors": {
            "diamond-heavon": _vendor_block(r["dh_matched"], r["dh_vendor_total"], r["dh_match_rate_pct"]),
            "abelini":        _vendor_block(r["ab_matched"], r["ab_vendor_total"], r["ab_match_rate_pct"]),
        },
        "dataset_counts": {
            "common":   r["common_count"],
            "partial":  r["partial_count"],
            "all_ROUK": r["all_rouk_count"],
        },
    }


@app.get("/api/uk/health")
async def uk_health():
    db_ok = False
    db_error = None
    table_counts: dict[str, int] = {}
    try:
        with _get_conn() as conn, conn.cursor() as cur:
            cur.execute("SELECT 1 AS ok")
            cur.fetchone()
            db_ok = True
            for table in ("uk_compare_common", "uk_compare_partial",
                          "uk_compare_all", "uk_diamond_status"):
                try:
                    cur.execute(f'SELECT COUNT(*) AS c FROM "{table}"')
                    row = cur.fetchone()
                    table_counts[table] = int(row["c"]) if row else 0
                except Exception:
                    table_counts[table] = -1
    except Exception as e:
        db_error = str(e)

    return {
        "status":          "ok" if db_ok else "degraded",
        "db_engine":       "postgresql",
        "db_ok":           db_ok,
        "db_error":        db_error,
        "tables":          table_counts,
        "default_dataset": table_counts.get("uk_compare_common", 0) > 0,
        "timestamp":       datetime.datetime.now().isoformat(),
    }


@app.get("/api/uk/files")
async def uk_list_datasets():
    files = []
    for filename, table in UK_FILENAME_TO_TABLE.items():
        files.append({
            "filename":   filename,
            "table":      table,
            "rows":       _uk_table_row_count(table),
            "is_default": filename == "compare.json",
        })
    return {"files": files, "count": len(files)}


@app.get("/api/uk/compare")
async def uk_get_default_compare():
    """uk_compare_common (ROUK + all vendors), full array."""
    try:
        return JSONResponse(content=_fetch_all_uk("uk_compare_common"))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB read failed: {e}")


@app.get("/api/uk/data/{filename}")
async def uk_get_json_data(filename: str):
    if ".." in filename or "/" in filename or "\\" in filename:
        raise HTTPException(status_code=400, detail="Invalid filename")
    if filename not in UK_FILENAME_TO_TABLE:
        raise HTTPException(status_code=404, detail=f"Unknown UK dataset: {filename}")
    try:
        return JSONResponse(content=_fetch_all_uk(UK_FILENAME_TO_TABLE[filename]))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB read failed: {e}")


@app.get("/api/uk/status")
async def uk_get_status():
    data = _fetch_uk_status()
    if data is None:
        raise HTTPException(
            status_code=404,
            detail="uk_diamond_status table is empty - run the UK pipeline first",
        )
    return JSONResponse(content=data)


@app.get("/api/uk/vendor-common/summary")
async def uk_vendor_common_summary():
    """Per-vendor count of rows matched against ROUK (both prices present)."""
    status = _fetch_uk_status()
    generated_at = status.get("generated_at") if status else None
    vendors: dict[str, dict] = {}
    for name, price_col in UK_VENDOR_PRICE_COL.items():
        where = (f'WHERE "{price_col}" IS NOT NULL AND "{price_col}" > 0 '
                 f'AND "rouk_price_gbp" IS NOT NULL AND "rouk_price_gbp" > 0')
        try:
            with _get_conn() as conn, conn.cursor() as cur:
                cur.execute(f'SELECT COUNT(*) AS c FROM "uk_compare_all" {where}')
                row = cur.fetchone()
                vendors[name] = {"rows": int(row["c"]) if row else 0}
        except Exception:
            vendors[name] = {"rows": 0}
    return {"generated_at": generated_at, "vendors": vendors}


@app.get("/api/uk/vendor-common/{vendor}")
async def uk_vendor_common_rows(vendor: str):
    """Rows where the given vendor AND ROUK both have a price (matched set)."""
    vendor = vendor.replace("_", "-") if vendor not in UK_VENDOR_PRICE_COL else vendor
    price_col = UK_VENDOR_PRICE_COL.get(vendor)
    if not price_col:
        raise HTTPException(status_code=404, detail=f"Unknown UK vendor: {vendor}")
    where = (f'WHERE "{price_col}" IS NOT NULL AND "{price_col}" > 0 '
             f'AND "rouk_price_gbp" IS NOT NULL AND "rouk_price_gbp" > 0')
    try:
        return JSONResponse(content=_fetch_all_uk("uk_compare_all", where))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB read failed: {e}")


@app.post("/api/uk/run-compare")
async def uk_run_compare():
    """Run the UK comparator, then load its output into the uk_* tables."""
    import subprocess
    import sys

    compare_script = UK_DIR / "diamond_compare_uk.py"
    loader_script = BASE_DIR / "usa_compare" / "uk_compare_to_db.py"

    if not compare_script.exists():
        raise HTTPException(status_code=404, detail="diamond_compare_uk.py not found")

    try:
        cmp_result = subprocess.run(
            [sys.executable, str(compare_script)],
            cwd=str(UK_DIR),
            capture_output=True, text=True, timeout=600,
        )
        cmp_ok = cmp_result.returncode == 0

        load_ok = False
        if cmp_ok and loader_script.exists():
            env = {**os.environ, "UK_COMPARE_DIR": str(UK_COMPARE_DIR)}
            ld_result = subprocess.run(
                [sys.executable, str(loader_script)],
                env=env, capture_output=True, text=True, timeout=600,
            )
            load_ok = ld_result.returncode == 0

        rows = _uk_table_row_count("uk_compare_common")
        return JSONResponse(content={
            "status":     "ok" if (cmp_ok and load_ok) else "error",
            "returncode": cmp_result.returncode,
            "comparator": "ok" if cmp_ok else "failed",
            "db_load":    "ok" if load_ok else "failed",
            "rows":       rows,
            "stdout":     (cmp_result.stdout or "")[-3000:],
            "stderr":     (cmp_result.stderr or "")[-2000:],
        })
    except subprocess.TimeoutExpired:
        raise HTTPException(status_code=504, detail="UK compare pipeline timed out (>10 min)")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# ============================================================================
# ROA SIDE  —  /api/roa/*   (cullen & GS Diamonds vs ROA baseline, USD)
# ============================================================================
# ============================================================================
ROA_DB_TO_JSON = {
    "certificate_number": "certificate_number",
    "vendors_matched":    "vendors_matched",
    "shape":              "shape",
    "color":              "color",
    "clarity":            "clarity",
    "cut":                "cut",
    "carat_weight":       "caratWeight",
    "roa_price_usd":      "ROA Price USD",
    "roa_minus_30_usd":   "ROA -30% USD",
    "cullen_price_usd":   "cullen Price USD",
    "gs_price_usd":       "gs Price USD",
    "cullen_vs_roa_usd":  "cullen vs ROA USD",
    "cullen_vs_roa_pct":  "cullen vs ROA %",
    "gs_vs_roa_usd":      "gs vs ROA USD",
    "gs_vs_roa_pct":      "gs vs ROA %",
}

ROA_OVERLAP_DB_TO_JSON = {
    "certificate_number": "certificate_number",
    "shape":              "shape",
    "color":              "color",
    "clarity":            "clarity",
    "cut":                "cut",
    "carat_weight":       "caratWeight",
    "cullen_price_usd":   "cullen Price USD",
    "gs_price_usd":       "gs Price USD",
    "roa_price_usd":      "ROA Price USD",
    "cullen_vs_roa_pct":  "cullen vs ROA %",
    "gs_vs_roa_pct":      "gs vs ROA %",
}

ROA_FILENAME_TO_TABLE = {
    "compare.json":         "roa_compare_common",
    "compare_partial.json": "roa_compare_partial",
    "compare_all.json":     "roa_compare_all",
}
ROA_ALLOWED_TABLES = set(ROA_FILENAME_TO_TABLE.values())

ROA_VENDOR_PRICE_COL = {
    "cullen": "cullen_price_usd",
    "gs":     "gs_price_usd",
}


def _fetch_all_roa(table: str, where_sql: str = "", where_args: list | None = None) -> list[dict]:
    if table not in ROA_ALLOWED_TABLES:
        raise HTTPException(status_code=404, detail=f"Unknown ROA table: {table}")
    db_cols = list(ROA_DB_TO_JSON.keys())
    cols_sql = ", ".join(f'"{c}"' for c in db_cols)
    sql = f'SELECT {cols_sql} FROM "{table}" {where_sql}'
    with _get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, where_args or [])
        raw = cur.fetchall()
    return [{ROA_DB_TO_JSON[c]: _jsonify_value(r[c]) for c in db_cols} for r in raw]


def _roa_table_row_count(table: str) -> int:
    try:
        with _get_conn() as conn, conn.cursor() as cur:
            cur.execute(f'SELECT COUNT(*) AS c FROM "{table}"')
            row = cur.fetchone()
            return int(row["c"]) if row else 0
    except Exception:
        return 0


def _fetch_roa_status() -> dict | None:
    sql = 'SELECT * FROM "roa_diamond_status" ORDER BY "id" DESC LIMIT 1'
    with _get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql)
        r = cur.fetchone()
    if not r:
        return None

    def _vendor_block(matched, total, rate):
        matched = matched or 0
        total   = total or 0
        return {
            "matched":           matched,
            "vendor_total":      total,
            "vendor_not_in_roa": max(total - matched, 0),
            "match_rate_pct":    _jsonify_value(rate),
        }

    return {
        "generated_at": _jsonify_value(r["generated_at"]),
        "ROA_total":    r["roa_total"] or 0,
        "vendor_totals": {
            "cullen": r["cullen_vendor_total"],
            "gs":     r["gs_vendor_total"],
        },
        "ROA_vs_vendors": {
            "cullen": _vendor_block(r["cullen_matched"], r["cullen_vendor_total"], r["cullen_match_rate_pct"]),
            "gs":     _vendor_block(r["gs_matched"],     r["gs_vendor_total"],     r["gs_match_rate_pct"]),
        },
        "dataset_counts": {
            "common":         r["common_count"],
            "partial":        r["partial_count"],
            "all_roa":        r["all_roa_count"],
            "vendor_overlap": r["vendor_overlap_count"],
        },
    }


@app.get("/api/roa/health")
async def roa_health():
    db_ok = False
    db_error = None
    table_counts: dict[str, int] = {}
    try:
        with _get_conn() as conn, conn.cursor() as cur:
            cur.execute("SELECT 1 AS ok")
            cur.fetchone()
            db_ok = True
            for table in ("roa_compare_common", "roa_compare_partial",
                          "roa_compare_all", "roa_diamond_status"):
                try:
                    cur.execute(f'SELECT COUNT(*) AS c FROM "{table}"')
                    row = cur.fetchone()
                    table_counts[table] = int(row["c"]) if row else 0
                except Exception:
                    table_counts[table] = -1
    except Exception as e:
        db_error = str(e)

    return {
        "status":          "ok" if db_ok else "degraded",
        "db_engine":       "postgresql",
        "db_ok":           db_ok,
        "db_error":        db_error,
        "tables":          table_counts,
        "default_dataset": table_counts.get("roa_compare_common", 0) > 0,
        "timestamp":       datetime.datetime.now().isoformat(),
    }


@app.get("/api/roa/files")
async def roa_list_datasets():
    files = []
    for filename, table in ROA_FILENAME_TO_TABLE.items():
        files.append({
            "filename":   filename,
            "table":      table,
            "rows":       _roa_table_row_count(table),
            "is_default": filename == "compare.json",
        })
    return {"files": files, "count": len(files)}


@app.get("/api/roa/compare")
async def roa_get_default_compare():
    """roa_compare_common — all rows as an array."""
    try:
        return JSONResponse(content=_fetch_all_roa("roa_compare_common"))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB read failed: {e}")


@app.get("/api/roa/data/{filename}")
async def roa_get_json_data(filename: str):
    if ".." in filename or "/" in filename or "\\" in filename:
        raise HTTPException(status_code=400, detail="Invalid filename")
    if filename not in ROA_FILENAME_TO_TABLE:
        raise HTTPException(status_code=404, detail=f"Unknown ROA dataset: {filename}")
    try:
        return JSONResponse(content=_fetch_all_roa(ROA_FILENAME_TO_TABLE[filename]))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB read failed: {e}")


@app.get("/api/roa/status")
async def roa_get_status():
    data = _fetch_roa_status()
    if data is None:
        raise HTTPException(
            status_code=404,
            detail="roa_diamond_status table is empty - run the ROA pipeline first",
        )
    return JSONResponse(content=data)


@app.get("/api/roa/vendor-common/summary")
async def roa_vendor_common_summary():
    status = _fetch_roa_status()
    generated_at = status.get("generated_at") if status else None
    vendors: dict[str, dict] = {}
    for name, price_col in ROA_VENDOR_PRICE_COL.items():
        where = (
            f'WHERE "{price_col}" IS NOT NULL AND "{price_col}" > 0 '
            f'AND "roa_price_usd" IS NOT NULL AND "roa_price_usd" > 0'
        )
        try:
            with _get_conn() as conn, conn.cursor() as cur:
                cur.execute(f'SELECT COUNT(*) AS c FROM "roa_compare_all" {where}')
                row = cur.fetchone()
                vendors[name] = {"rows": int(row["c"]) if row else 0}
        except Exception:
            vendors[name] = {"rows": 0}
    return {"generated_at": generated_at, "vendors": vendors}


@app.get("/api/roa/vendor-common/{vendor}")
async def roa_vendor_common_rows(vendor: str):
    """Rows where this vendor AND ROA both have a price."""
    price_col = ROA_VENDOR_PRICE_COL.get(vendor)
    if not price_col:
        raise HTTPException(status_code=404, detail=f"Unknown ROA vendor: {vendor}")
    where = (
        f'WHERE "{price_col}" IS NOT NULL AND "{price_col}" > 0 '
        f'AND "roa_price_usd" IS NOT NULL AND "roa_price_usd" > 0'
    )
    try:
        return JSONResponse(content=_fetch_all_roa("roa_compare_all", where))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB read failed: {e}")


@app.get("/api/roa/vendor-overlap")
async def roa_vendor_overlap():
    """Diamonds whose certificate appears in both cullen and gs data."""
    try:
        db_cols = list(ROA_OVERLAP_DB_TO_JSON.keys())
        cols_sql = ", ".join(f'"{c}"' for c in db_cols)
        sql = f'SELECT {cols_sql} FROM "roa_compare_vendor_overlap"'
        with _get_conn() as conn, conn.cursor() as cur:
            cur.execute(sql)
            raw = cur.fetchall()
        return JSONResponse(content=[
            {ROA_OVERLAP_DB_TO_JSON[c]: _jsonify_value(r[c]) for c in db_cols}
            for r in raw
        ])
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB read failed: {e}")


@app.post("/api/roa/run-compare")
async def roa_run_compare():
    import subprocess
    import sys

    compare_script = ROA_DIR / "diamond_compare_roa.py"
    loader_script  = ROA_DIR / "roa_compare_to_db.py"

    if not compare_script.exists():
        raise HTTPException(status_code=404, detail="diamond_compare_roa.py not found in roa_scrapping/")

    try:
        cmp_result = subprocess.run(
            [sys.executable, str(compare_script)],
            cwd=str(ROA_DIR),
            capture_output=True, text=True, timeout=600,
        )
        cmp_ok = cmp_result.returncode == 0

        load_ok = False
        if cmp_ok and loader_script.exists():
            ld_result = subprocess.run(
                [sys.executable, str(loader_script)],
                cwd=str(ROA_DIR),
                capture_output=True, text=True, timeout=600,
            )
            load_ok = ld_result.returncode == 0

        rows = _roa_table_row_count("roa_compare_common")
        return JSONResponse(content={
            "status":     "ok" if (cmp_ok and load_ok) else "error",
            "returncode": cmp_result.returncode,
            "comparator": "ok" if cmp_ok else "failed",
            "db_load":    "ok" if load_ok else "failed",
            "rows":       rows,
            "stdout":     (cmp_result.stdout or "")[-3000:],
            "stderr":     (cmp_result.stderr or "")[-2000:],
        })
    except subprocess.TimeoutExpired:
        raise HTTPException(status_code=504, detail="ROA compare pipeline timed out (>10 min)")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# Local dev entrypoint
# ============================================================================
if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", "4000"))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=True)
