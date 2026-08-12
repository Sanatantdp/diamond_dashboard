
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

ROA_DIR = BASE_DIR / "aus_scrapping"
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

# Page shells are public: the browser navigates to them with no Authorization
# header, and their JS then calls the API with the token from sessionStorage.
# Any new dashboard page must be listed here or the navigation 401s.
PUBLIC_PATHS = {
    "/", "/dashboard", "/dashboard/usa", "/dashboard/uk", "/dashboard/aus",
    "/dashboard/germany", "/dashboard/usage", "/dashboard/db-usage",
    "/dashboard/coverage",
    "/favicon.ico",
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
    "lg_vs_pc_usd":            "loose-grown vs PC USD",
    "lg_vs_pc_pct":            "loose-grown vs PC %",
    "lg_link":                 "loose-grown Link",
    "brilliance_price_usd":    "brilliance Price USD",
    "brilliance_minus_30_usd": "brilliance -30% USD",
    "brilliance_vs_pc_usd":    "brilliance vs PC USD",
    "brilliance_vs_pc_pct":    "brilliance vs PC %",
    "brilliance_link":         "brilliance Link",
    "friendly_price_usd":      "friendly Price USD",
    "friendly_vs_pc_usd":      "friendly vs PC USD",
    "friendly_vs_pc_pct":      "friendly vs PC %",
    "friendly_link":           "friendly Link",
    "pc_price_usd":            "PC Price USD",
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

# vendor key -> { price_col, minus30_col, pct_col }. "minus30_col" is the
# column used for $-bucketing; it points at the discounted price where a real
# discount applies, or back at the raw price column for vendors with none
# (lg, friendly — see diamond_compare.py).
VENDOR_COLS = {
    "lg": {
        "price_col":   "lg_price_usd",
        "minus30_col": "lg_price_usd",
        "pct_col":     "lg_vs_pc_pct",
    },
    "br": {
        "price_col":   "brilliance_price_usd",
        "minus30_col": "brilliance_minus_30_usd",
        "pct_col":     "brilliance_vs_pc_pct",
    },
    "fr": {
        "price_col":   "friendly_price_usd",
        "minus30_col": "friendly_price_usd",
        "pct_col":     "friendly_vs_pc_pct",
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

# ============================================================================
# Shared diamond attribute filters (shape / color / clarity / cut / carat).
# Every compare table across all 4 stores exposes these same 5 columns, so
# one filter builder covers USA/ROG/UK/ROA instead of duplicating it 4x.
# ============================================================================
COLOR_ORDER = ["K", "J", "I", "H", "G", "F", "E", "D"]              # worst -> best
CLARITY_ORDER = ["FL", "IF", "VVS1", "VVS2", "VS1", "VS2", "SI1"]   # best -> worst
CUT_ORDER = ["IDEAL", "EXCELLENT", "VERY GOOD", "GOOD"]             # best -> worst


def _ordered_range(order: list[str], lo: str | None, hi: str | None) -> list[str] | None:
    """Slice `order` between the positions of lo/hi (inclusive); None means no filter requested."""
    if not lo and not hi:
        return None
    lo_idx = order.index(lo.upper()) if lo and lo.upper() in order else 0
    hi_idx = order.index(hi.upper()) if hi and hi.upper() in order else len(order) - 1
    if lo_idx > hi_idx:
        lo_idx, hi_idx = hi_idx, lo_idx
    return order[lo_idx : hi_idx + 1]


def _add_diamond_attr_clauses(params: dict, clauses: list[str], args: list) -> None:
    """Append shape/color/clarity/cut/carat WHERE clauses shared by every store."""
    shapes = [s.strip().upper() for s in (params.get("shape") or "").split(",") if s.strip()]
    if shapes:
        clauses.append('UPPER("shape") = ANY(%s)'); args.append(shapes)

    colors = _ordered_range(COLOR_ORDER, params.get("color_min"), params.get("color_max"))
    if colors is not None:
        clauses.append('UPPER("color") = ANY(%s)'); args.append(colors)

    clarities = _ordered_range(CLARITY_ORDER, params.get("clarity_min"), params.get("clarity_max"))
    if clarities is not None:
        clauses.append('UPPER("clarity") = ANY(%s)'); args.append(clarities)

    cuts = _ordered_range(CUT_ORDER, params.get("cut_min"), params.get("cut_max"))
    if cuts is not None:
        clauses.append('UPPER("cut") = ANY(%s)'); args.append(cuts)

    cmin = params.get("carat_min")
    cmax = params.get("carat_max")
    if cmin not in (None, ""):
        try:
            clauses.append('"carat_weight" >= %s'); args.append(float(cmin))
        except ValueError:
            pass
    if cmax not in (None, ""):
        try:
            clauses.append('"carat_weight" <= %s'); args.append(float(cmax))
        except ValueError:
            pass


# Tables exposed via /api/data/{filename}
FILENAME_TO_TABLE = {
    # compare_all is intentionally absent: nothing in the USA dashboard read it,
    # and the loaders no longer create the table.
    "compare.json":          ("compare_common",   COMPARE_DB_TO_JSON),
    "compare_partial.json":  ("compare_partial",  COMPARE_DB_TO_JSON),
    "compare_br_vs_lg.json": ("compare_br_vs_lg", BR_VS_LG_DB_TO_JSON),
}

ALLOWED_TABLES = {t for t, _ in FILENAME_TO_TABLE.values()}
MAX_LIMIT = 500
DEFAULT_LIMIT = 20


# ============================================================================
# Filter -> WHERE clause builder
# ============================================================================
def _parse_bucket_param(
    raw: str | None, valid_keys: dict | set | None = None
) -> tuple[str | None, int | None]:
    """Parse 'lg:3' -> ('lg', 3). Returns (None, None) on invalid."""
    if not raw or ":" not in raw:
        return None, None
    vendor_key, idx_s = raw.split(":", 1)
    vendor_key = vendor_key.strip()
    valid = valid_keys if valid_keys is not None else VENDOR_COLS
    if vendor_key not in valid:
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

    _add_diamond_attr_clauses(params, clauses, args)

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
    for vk in ("lg", "br", "fr"):  # luvansh has no DB columns yet
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
    try:
        with _get_conn() as conn, conn.cursor() as cur:
            cur.execute(sql)
            r = cur.fetchone()
    except Exception:
        # Table absent until the USA loader has run at least once — same
        # treatment as the other regions: 404 "no data", not a 500.
        return None
    if not r:
        return None
    return {
        "generated_at": _jsonify_value(r["generated_at"]),
        "pc_total":     r["pc_total"],
        "vendor_totals": {
            "loose-grown": r["lg_vendor_total"],
            "brilliance":  r["brilliance_vendor_total"],
            "friendly":    r.get("friendly_vendor_total"),
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
            "friendly": {
                "matched":        r.get("friendly_matched"),
                "match_rate_pct": _jsonify_value(r.get("friendly_match_rate_pct")),
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
            "friendly":    _jsonify_value(r.get("friendly_file_date")),
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
# Status-history (trend) — shared reader for all 4 regions' *_status_history
# tables written by snapshot_history.py before each region's loader truncates.
# Each table has the same shape as its live status table plus snapshot_date,
# and is append-only (never truncated), so this is the source for trend charts
# (match-rate %, vendor totals, dataset counts over time).
# ============================================================================
def _fetch_status_history(history_table: str, limit: int) -> list[dict]:
    """Most recent `limit` snapshots, returned oldest -> newest (chart order)."""
    sql = (
        f'SELECT * FROM "{history_table}" '
        f'ORDER BY "snapshot_date" DESC, "id" DESC LIMIT %s'
    )
    with _get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, [limit])
        raw = cur.fetchall()
    rows = [
        {k: _jsonify_value(v) for k, v in dict(r).items() if k != "id"}
        for r in raw
    ]
    rows.reverse()
    return rows


# ============================================================================
# Vendor attribute coverage — shared across all 4 origins.
# One "vendor_coverage" table holds every origin (each loader replaces only its
# own rows), so a single reader serves usa/uk/aus/germany. Rows are tidy/long:
# one per origin x vendor x attribute x bucket. See vendor_coverage.py.
# ============================================================================
COVERAGE_ORIGINS = {"usa", "uk", "aus", "germany"}
COVERAGE_ATTRIBUTES = ["carat", "color", "cut", "clarity"]


def _fetch_vendor_coverage(origin: str) -> dict:
    """Re-nest the flat coverage rows into {vendor: {attribute: [buckets]}}."""
    sql = (
        'SELECT * FROM "vendor_coverage" WHERE "origin" = %s '
        'ORDER BY "vendor", "attribute", "bucket_order"'
    )
    with _get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, [origin])
        raw = [dict(r) for r in cur.fetchall()]

    vendors: dict[str, dict] = {}
    generated_at = None
    for r in raw:
        generated_at = generated_at or _jsonify_value(r.get("generated_at"))
        v = vendors.setdefault(r["vendor"], {
            "vendor":      r["vendor"],
            "source_file": r.get("source_file"),
            "total_rows":  int(r.get("vendor_total") or 0),
            "attributes":  {},
        })
        attr = v["attributes"].setdefault(r["attribute"], {
            "has_data":     bool(r.get("has_data")),
            "graded":       int(r.get("attribute_graded") or 0),
            "coverage_pct": _jsonify_value(r.get("attribute_coverage_pct")),
            "buckets":      [],
        })
        attr["buckets"].append({
            "bucket": r["bucket"],
            # bucket_order is the grading rank (D before E, IF before VS1, …).
            # The UI needs it to merge every vendor's buckets into one ordered
            # cross-tab, since vendors don't all carry the same buckets.
            "order":  int(r.get("bucket_order") or 0),
            "count":  int(r.get("diamond_count") or 0),
            "pct":    _jsonify_value(r.get("pct_of_vendor")),
        })

    return {
        "origin":       origin,
        "generated_at": generated_at,
        "attributes":   COVERAGE_ATTRIBUTES,
        "coverage_matrix": {
            name: {a: v["attributes"].get(a, {}).get("has_data", False)
                   for a in COVERAGE_ATTRIBUTES}
            for name, v in vendors.items()
        },
        "vendor_count": len(vendors),
        "vendors":      list(vendors.values()),
    }


@app.get("/api/vendor-coverage")
async def get_vendor_coverage(origin: str = Query("usa")):
    """Which vendor actually carries which grading data, per origin.

    For each vendor: the carat/color/cut/clarity distribution over normalised
    buckets, plus a `coverage_matrix` giving the headline yes/no per attribute
    (e.g. the AUS vendor `gs` publishes no cut grade at all).
    """
    origin = origin.strip().lower()
    if origin not in COVERAGE_ORIGINS:
        raise HTTPException(
            status_code=400,
            detail=f"unknown origin '{origin}' — valid: {', '.join(sorted(COVERAGE_ORIGINS))}",
        )
    try:
        data = _fetch_vendor_coverage(origin)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB read failed: {e}")

    if not data["vendors"]:
        raise HTTPException(
            status_code=404,
            detail=f"no vendor coverage for '{origin}' — run vendor_coverage.py first",
        )
    return JSONResponse(content=data)


# Region-prefixed aliases. Each dashboard's api() helper prepends its own
# API_BASE ("/api/roa", "/api/uk", "/api/rog"), so give every region the route
# it can reach without special-casing the base — same handler, fixed origin.
@app.get("/api/roa/vendor-coverage")
async def roa_vendor_coverage():
    return await get_vendor_coverage(origin="aus")


@app.get("/api/uk/vendor-coverage")
async def uk_vendor_coverage():
    return await get_vendor_coverage(origin="uk")


@app.get("/api/rog/vendor-coverage")
async def rog_vendor_coverage():
    return await get_vendor_coverage(origin="germany")


# ============================================================================
# Daily internet usage — how much each scraper downloaded, per day.
# Recorded by bandwidth.py inside the scraper processes and loaded into the
# shared "bandwidth_usage" table by each region's loader.
# ============================================================================
def _human_bytes_short(n: float) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(n) < 1024 or unit == "TB":
            return f"{int(n)} B" if unit == "B" else f"{n:,.1f} {unit}"
        n /= 1024
    return f"{n:,.1f} TB"


def _live_bandwidth_rows(origin: str, days: int) -> list[dict]:
    """
    Usage from the on-disk records the scrapers are writing right now.

    The DB table is only refreshed when a region's loader runs, which is the
    last step of the pipeline — so mid-run there is nothing in it yet. When the
    API shares a filesystem with the scrapers we can read their live files and
    show the current run immediately. Absent or unreadable files are not an
    error; that is simply the deployed case where the API runs elsewhere.
    """
    try:
        import bandwidth
    except Exception:
        return []
    try:
        rows = bandwidth.daily_usage(None if origin == "all" else origin)
    except Exception:
        return []

    cutoff = (datetime.date.today() - datetime.timedelta(days=days)).isoformat()
    return [r for r in rows if r.get("day", "") >= cutoff]


def _fetch_bandwidth(origin: str, days: int) -> dict:
    """Daily totals plus a per-scraper split for the trailing `days` days."""
    where = '"day" >= CURRENT_DATE - %s::int'
    args: list[Any] = [days]
    if origin != "all":
        where += ' AND "origin" = %s'
        args.append(origin)

    sql = (
        'SELECT "day", "origin", "scraper", "endpoint", "runs", "requests", '
        '"bytes_down", "bytes_up", "responses_without_length" '
        f'FROM "bandwidth_usage" WHERE {where} '
        'ORDER BY "day", "origin", "scraper"'
    )
    try:
        with _get_conn() as conn, conn.cursor() as cur:
            cur.execute(sql, args)
            db_rows = [dict(r) for r in cur.fetchall()]
    except Exception:
        # Table absent (or missing the endpoint column) until a loader has run
        # once — live files may still have something, so don't fail the request.
        db_rows = []

    # Merge live over stored. A day still held on disk is the same day the
    # loader would replace wholesale on its next upload, so the local numbers
    # are the fresher truth for it — keying on (day, origin, scraper, endpoint)
    # means an in-progress run supersedes its own earlier snapshot, never doubles.
    def _key(r):
        return (str(_jsonify_value(r["day"])), r["origin"], r["scraper"],
                r.get("endpoint") or "(unknown)")

    merged: dict[tuple, dict] = {_key(r): r for r in db_rows}
    live_keys = set()
    for r in _live_bandwidth_rows(origin, days):
        k = _key(r)
        merged[k] = r
        live_keys.add(k)

    raw = [merged[k] for k in sorted(merged)]

    daily: dict[str, dict] = {}
    per_scraper: dict[str, dict] = {}
    totals = {"requests": 0, "bytes_down": 0, "bytes_up": 0,
              "responses_without_length": 0}

    per_endpoint: dict[tuple[str, str], dict] = {}
    scraper_days: dict[str, set] = {}

    for r in raw:
        day = str(_jsonify_value(r["day"]))
        down = int(r.get("bytes_down") or 0)
        up = int(r.get("bytes_up") or 0)
        reqs = int(r.get("requests") or 0)
        nolen = int(r.get("responses_without_length") or 0)
        scraper = r["scraper"]
        endpoint = r.get("endpoint") or "(unknown)"

        d = daily.setdefault(day, {
            "day": day, "requests": 0, "bytes_down": 0, "bytes_up": 0,
            "scrapers": {},
        })
        d["requests"] += reqs
        d["bytes_down"] += down
        d["bytes_up"] += up
        # Several endpoint rows share a scraper — accumulate, don't overwrite.
        ds = d["scrapers"].setdefault(scraper, {
            "origin": r["origin"], "requests": 0, "bytes_down": 0, "bytes_up": 0,
        })
        ds["requests"] += reqs
        ds["bytes_down"] += down
        ds["bytes_up"] += up

        s = per_scraper.setdefault(scraper, {
            "scraper": scraper, "origin": r["origin"],
            "requests": 0, "bytes_down": 0, "bytes_up": 0, "days": 0,
        })
        s["requests"] += reqs
        s["bytes_down"] += down
        s["bytes_up"] += up
        scraper_days.setdefault(scraper, set()).add(day)

        e = per_endpoint.setdefault((scraper, endpoint), {
            "endpoint": endpoint, "scraper": scraper, "origin": r["origin"],
            "requests": 0, "bytes_down": 0, "bytes_up": 0,
        })
        e["requests"] += reqs
        e["bytes_down"] += down
        e["bytes_up"] += up

        totals["requests"] += reqs
        totals["bytes_down"] += down
        totals["bytes_up"] += up
        totals["responses_without_length"] += nolen

    daily_list = [daily[k] for k in sorted(daily)]
    for d in daily_list:
        d["bytes_down_pretty"] = _human_bytes_short(d["bytes_down"])

    scrapers = sorted(per_scraper.values(), key=lambda s: -s["bytes_down"])
    for s in scrapers:
        s["days"] = len(scraper_days.get(s["scraper"], ()))
        s["bytes_down_pretty"] = _human_bytes_short(s["bytes_down"])

    endpoints = sorted(per_endpoint.values(), key=lambda e: -e["bytes_down"])
    for e in endpoints:
        # The headline number for "what does one call cost".
        e["avg_bytes_per_call"] = (
            e["bytes_down"] // e["requests"] if e["requests"] else 0
        )
        e["bytes_down_pretty"] = _human_bytes_short(e["bytes_down"])
        e["avg_per_call_pretty"] = _human_bytes_short(e["avg_bytes_per_call"])

    n_days = len(daily_list)
    return {
        "origin": origin,
        "days_requested": days,
        "days_with_data": n_days,
        "totals": {
            **totals,
            "bytes_down_pretty": _human_bytes_short(totals["bytes_down"]),
            "bytes_up_pretty": _human_bytes_short(totals["bytes_up"]),
            "avg_per_day": totals["bytes_down"] // n_days if n_days else 0,
            "avg_per_day_pretty": _human_bytes_short(
                totals["bytes_down"] / n_days if n_days else 0
            ),
        },
        # Chunked/compressed responses carry no Content-Length, so their bytes
        # are counted decompressed. Surfaced so the UI can say so rather than
        # implying the figure is exact.
        "estimated_from_body": totals["responses_without_length"],
        # How many (day, scraper) rows came from files a scraper is still
        # writing, rather than from the database.
        "live_rows": len(live_keys),
        "daily": daily_list,
        "scrapers": scrapers,
        "scraper_names": [s["scraper"] for s in scrapers],
        "endpoints": endpoints,
    }


@app.get("/api/bandwidth")
async def get_bandwidth(
    origin: str = Query("usa"),
    days: int = Query(30, ge=1, le=365),
):
    """Daily download volume per scraper. `origin=all` spans every region."""
    origin = origin.strip().lower()
    if origin != "all" and origin not in COVERAGE_ORIGINS:
        raise HTTPException(
            status_code=400,
            detail=f"unknown origin '{origin}' — valid: all, "
                   f"{', '.join(sorted(COVERAGE_ORIGINS))}",
        )
    try:
        return JSONResponse(content=_fetch_bandwidth(origin, days))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB read failed: {e}")


@app.get("/api/roa/bandwidth")
async def roa_bandwidth(days: int = Query(30, ge=1, le=365)):
    return await get_bandwidth(origin="aus", days=days)


@app.get("/api/uk/bandwidth")
async def uk_bandwidth(days: int = Query(30, ge=1, le=365)):
    return await get_bandwidth(origin="uk", days=days)


@app.get("/api/rog/bandwidth")
async def rog_bandwidth(days: int = Query(30, ge=1, le=365)):
    return await get_bandwidth(origin="germany", days=days)


# ============================================================================
# Pages
# ============================================================================

@app.head("/ping")
@app.get("/ping")
def ping():
    return Response(content="OK", status_code=200)

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


@app.get("/dashboard/coverage", response_class=HTMLResponse)
async def serve_dashboard_coverage():
    """Standalone vendor data-coverage dashboard, with its own region picker."""
    html_path = STATIC_DIR / "coverage.html"
    if not html_path.exists():
        raise HTTPException(status_code=404, detail="coverage.html not found")
    return HTMLResponse(
        content=html_path.read_text(encoding="utf-8"),
        headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
    )


@app.get("/dashboard/db-usage", response_class=HTMLResponse)
async def serve_dashboard_db_usage():
    """Standalone database-storage dashboard, reading /api/db-usage."""
    html_path = STATIC_DIR / "db_usage.html"
    if not html_path.exists():
        raise HTTPException(status_code=404, detail="db_usage.html not found")
    return HTMLResponse(
        content=html_path.read_text(encoding="utf-8"),
        headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
    )


@app.get("/dashboard/usage", response_class=HTMLResponse)
async def serve_dashboard_usage():
    """Standalone internet-usage dashboard. Region-agnostic — it reads
    /api/bandwidth directly and can show all four regions at once, so it lives
    beside select.html rather than in any one region's static folder."""
    html_path = STATIC_DIR / "usage.html"
    if not html_path.exists():
        raise HTTPException(status_code=404, detail="usage.html not found")
    return HTMLResponse(
        content=html_path.read_text(encoding="utf-8"),
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
    """AUS/ROA dashboard. The client reads from /api/roa/* (PostgreSQL, AUD)."""
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
    "renesim_link":        "renesim Link",
    "novita_link":         "novita Link",
}

ROG_FILENAME_TO_TABLE = {
    # rog_compare_all is intentionally absent: nothing in the Germany
    # dashboard read it, and the loader no longer creates the table.
    "compare.json":         "rog_compare_common",
    "compare_partial.json": "rog_compare_partial",
}
ROG_ALLOWED_TABLES = set(ROG_FILENAME_TO_TABLE.values())

ROG_VENDOR_PRICE_COL = {
    "novita":     "novita_price_eur",
    "77diamonds": "d77_price_eur",
    "renesim":    "renesim_price_eur",
}
ROG_VENDOR_PCT_COL = {
    "novita":     "novita_vs_rog_pct",
    "77diamonds": "d77_vs_rog_pct",
    "renesim":    "renesim_vs_rog_pct",
}

ROG_DEFAULT_LIMIT = 25
ROG_MAX_LIMIT = 500


def _rog_build_where(
    table: str,
    params: dict,
    extra_clauses: list[str] | None = None,
    extra_args: list | None = None,
) -> tuple[str, list]:
    """Build a SQL WHERE clause + parameter list from the request's filter params."""
    clauses: list[str] = list(extra_clauses or [])
    args: list[Any] = list(extra_args or [])

    cert = (params.get("cert") or "").strip()
    if cert:
        clauses.append('"certificate_number"::text ILIKE %s')
        args.append(f"%{cert}%")

    pmin = params.get("price_min")
    pmax = params.get("price_max")
    if pmin not in (None, ""):
        try:
            clauses.append('"rog_price_eur" >= %s'); args.append(float(pmin))
        except ValueError:
            pass
    if pmax not in (None, ""):
        try:
            clauses.append('"rog_price_eur" <= %s'); args.append(float(pmax))
        except ValueError:
            pass

    vp_key, vp_idx = _parse_bucket_param(params.get("vendor_pct"), ROG_VENDOR_PCT_COL)
    if vp_key and vp_idx is not None and 0 <= vp_idx < len(PCT_BUCKETS):
        col = ROG_VENDOR_PCT_COL[vp_key]
        bucket = PCT_BUCKETS[vp_idx]
        if bucket["min"] is not None:
            clauses.append(f'"{col}" >= %s'); args.append(bucket["min"])
        if bucket["max"] is not None:
            clauses.append(f'"{col}" < %s'); args.append(bucket["max"])
        clauses.append(f'"{col}" IS NOT NULL')

    vpr_key, vpr_idx = _parse_bucket_param(params.get("vendor_price"), ROG_VENDOR_PRICE_COL)
    if vpr_key and vpr_idx is not None and 0 <= vpr_idx < len(PRICE_BUCKETS):
        col = ROG_VENDOR_PRICE_COL[vpr_key]
        bucket = PRICE_BUCKETS[vpr_idx]
        if bucket["min"] is not None:
            clauses.append(f'"{col}" >= %s'); args.append(bucket["min"])
        if bucket["max"] is not None:
            clauses.append(f'"{col}" < %s'); args.append(bucket["max"])
        clauses.append(f'"{col}" IS NOT NULL AND "{col}" > 0')

    if (params.get("common_only") or "").lower() == "true":
        for c in ROG_VENDOR_PRICE_COL.values():
            clauses.append(f'"{c}" IS NOT NULL AND "{c}" > 0')

    _add_diamond_attr_clauses(params, clauses, args)

    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    return where, args


def _fetch_rog_rows_paginated(
    table: str,
    params: dict,
    extra_clauses: list[str] | None = None,
    extra_args: list | None = None,
) -> dict:
    """Paginated + filtered row fetch — never loads a whole ROG table into memory."""
    if table not in ROG_ALLOWED_TABLES:
        raise HTTPException(status_code=404, detail=f"Unknown ROG table: {table}")

    limit = _clamp_int(params.get("limit"), ROG_DEFAULT_LIMIT, 1, ROG_MAX_LIMIT)
    offset = _clamp_int(params.get("offset"), 0, 0, 10_000_000)

    where_sql, where_args = _rog_build_where(table, params, extra_clauses, extra_args)

    sort_col = _sanitize_sort(params.get("sort_col"), ROG_DB_TO_JSON)
    sort_dir = "ASC" if (params.get("sort_dir") or "asc").lower() == "asc" else "DESC"
    order_sql = f'ORDER BY "{sort_col}" {sort_dir} NULLS LAST' if sort_col else ""

    db_cols = list(ROG_DB_TO_JSON.keys())
    cols_sql = ", ".join(f'"{c}"' for c in db_cols)

    rows_sql = f'SELECT {cols_sql} FROM "{table}" {where_sql} {order_sql} LIMIT %s OFFSET %s'
    count_sql = f'SELECT COUNT(*) AS c FROM "{table}" {where_sql}'

    with _get_conn() as conn, conn.cursor() as cur:
        cur.execute(count_sql, where_args)
        total = int(cur.fetchone()["c"])
        cur.execute(rows_sql, [*where_args, limit, offset])
        raw = cur.fetchall()

    out_rows = [{ROG_DB_TO_JSON[c]: _jsonify_value(r[c]) for c in db_cols} for r in raw]
    return {"rows": out_rows, "total": total, "limit": limit, "offset": offset}


def _rog_aggregate_sql_for_vendor(vendor_key: str) -> tuple[list[str], list[str]] | None:
    """Returns (pct_expressions, price_expressions) for the SELECT clause."""
    pct_col = ROG_VENDOR_PCT_COL.get(vendor_key)
    price_col = ROG_VENDOR_PRICE_COL.get(vendor_key)
    if not pct_col or not price_col:
        return None

    pct_exprs: list[str] = []
    for i, b in enumerate(PCT_BUCKETS):
        conds = [f'"{pct_col}" IS NOT NULL']
        if b["min"] is not None:
            conds.append(f'"{pct_col}" >= {b["min"]}')
        if b["max"] is not None:
            conds.append(f'"{pct_col}" < {b["max"]}')
        cond_sql = " AND ".join(conds)
        pct_exprs.append(f'COUNT(*) FILTER (WHERE {cond_sql}) AS pct_{vendor_key}_{i}_cnt')
        pct_exprs.append(
            f'AVG("rog_price_eur") FILTER (WHERE {cond_sql} AND "rog_price_eur" > 0) '
            f'AS pct_{vendor_key}_{i}_avgrog'
        )

    price_exprs: list[str] = []
    for i, b in enumerate(PRICE_BUCKETS):
        conds = [f'"{price_col}" IS NOT NULL', f'"{price_col}" > 0']
        if b["min"] is not None:
            conds.append(f'"{price_col}" >= {b["min"]}')
        if b["max"] is not None:
            conds.append(f'"{price_col}" < {b["max"]}')
        cond_sql = " AND ".join(conds)
        price_exprs.append(f'COUNT(*) FILTER (WHERE {cond_sql}) AS price_{vendor_key}_{i}_cnt')
        price_exprs.append(
            f'AVG("rog_price_eur") FILTER (WHERE {cond_sql} AND "rog_price_eur" > 0) '
            f'AS price_{vendor_key}_{i}_avgrog'
        )

    pct_exprs.append(
        f'COUNT(*) FILTER (WHERE "{price_col}" IS NOT NULL AND "{price_col}" > 0) '
        f'AS vendor_{vendor_key}_total'
    )
    return pct_exprs, price_exprs


def _compute_rog_aggregates(table: str, params: dict) -> dict:
    """One aggregate query returning bucket counts + averages for all ROG vendors."""
    if table not in ROG_ALLOWED_TABLES:
        return {"vendors": {}, "total": 0}

    where_sql, where_args = _rog_build_where(table, params)

    select_exprs: list[str] = ['COUNT(*) AS total_rows']
    active_vendors: list[str] = []
    for vk in ("novita", "77diamonds", "renesim"):
        out = _rog_aggregate_sql_for_vendor(vk)
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
        return None if v is None else _jsonify_value(v)

    vendors: dict[str, dict] = {}
    for vk in active_vendors:
        pct_bs = [
            {
                "idx":     i,
                "count":   int(row.get(f"pct_{vk}_{i}_cnt") or 0),
                "avg_rog": _f(f"pct_{vk}_{i}_avgrog"),
            }
            for i in range(len(PCT_BUCKETS))
        ]
        price_bs = [
            {
                "idx":     i,
                "count":   int(row.get(f"price_{vk}_{i}_cnt") or 0),
                "avg_rog": _f(f"price_{vk}_{i}_avgrog"),
            }
            for i in range(len(PRICE_BUCKETS))
        ]
        vendors[vk] = {
            "vendor_total":  int(row.get(f"vendor_{vk}_total") or 0),
            "pct_buckets":   pct_bs,
            "price_buckets": price_bs,
        }

    return {"vendors": vendors, "total": int(row.get("total_rows") or 0)}


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
        "file_dates": json.loads(r["file_dates"]) if r.get("file_dates") else {},
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
                          "rog_diamond_status"):
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
async def rog_get_default_compare(request: Request):
    """rog_compare_common, paginated (default 25/page) + server-side filters."""
    try:
        return JSONResponse(content=_fetch_rog_rows_paginated(
            "rog_compare_common", _params_dict(request)
        ))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB read failed: {e}")


@app.get("/api/rog/data/{filename}")
async def rog_get_json_data(filename: str, request: Request):
    if ".." in filename or "/" in filename or "\\" in filename:
        raise HTTPException(status_code=400, detail="Invalid filename")
    if filename not in ROG_FILENAME_TO_TABLE:
        raise HTTPException(status_code=404, detail=f"Unknown ROG dataset: {filename}")
    try:
        return JSONResponse(content=_fetch_rog_rows_paginated(
            ROG_FILENAME_TO_TABLE[filename], _params_dict(request)
        ))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB read failed: {e}")


@app.get("/api/rog/aggregates")
async def rog_get_aggregates(request: Request, dataset: str = Query("compare_partial.json")):
    """Per-vendor bucket counts + averages for the given ROG dataset, with filters applied."""
    if dataset not in ROG_FILENAME_TO_TABLE:
        raise HTTPException(status_code=404, detail=f"Unknown ROG dataset: {dataset}")
    table = ROG_FILENAME_TO_TABLE[dataset]
    try:
        return JSONResponse(content=_compute_rog_aggregates(table, _params_dict(request)))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Aggregate query failed: {e}")


@app.get("/api/rog/status")
async def rog_get_status():
    data = _fetch_rog_status()
    if data is None:
        raise HTTPException(
            status_code=404,
            detail="rog_diamond_status table is empty — run the ROG pipeline first",
        )
    return JSONResponse(content=data)


# NOTE: /api/rog/status/history used to live here. History is a Germany/ROG-
# only removal (UK and AUS still expose their own /status/history routes via
# _fetch_status_history()); see germany_compare_to_db.py for the loader side.


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
                # Reads compare_partial, not compare_all: the WHERE clause
                # requires the vendor to have a price, so every matching row is
                # by definition matched by >=1 vendor and therefore present in
                # compare_partial. Same reasoning as the UK endpoint above.
                cur.execute(f'SELECT COUNT(*) AS c FROM "rog_compare_partial" {where}')
                row = cur.fetchone()
                vendors[name] = {"rows": int(row["c"]) if row else 0}
        except Exception:
            vendors[name] = {"rows": 0}
    return {"generated_at": generated_at, "vendors": vendors}


@app.get("/api/rog/vendor-common/{vendor}")
async def rog_vendor_common_rows(vendor: str, request: Request):
    """Rows where this vendor AND ROG both have a price (matched set), paginated."""
    price_col = ROG_VENDOR_PRICE_COL.get(vendor)
    if not price_col:
        raise HTTPException(status_code=404, detail=f"Unknown ROG vendor: {vendor}")
    extra = [
        f'"{price_col}" IS NOT NULL', f'"{price_col}" > 0',
        '"rog_price_eur" IS NOT NULL', '"rog_price_eur" > 0',
    ]
    try:
        return JSONResponse(content=_fetch_rog_rows_paginated(
            "rog_compare_partial", _params_dict(request), extra_clauses=extra
        ))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB read failed: {e}")


@app.get("/api/rog/vendor-overlap")
async def rog_vendor_overlap(request: Request):
    """Diamonds whose certificate appears across novita, 77diamonds, and renesim (paginated).
    Reads compare_common, not compare_all: the WHERE clause requires ALL three
    vendor prices present, which is exactly rog_compare_common's "matched by all
    3 vendors" criterion (see diamond_compare.py's _all_matched)."""
    extra = [
        clause
        for col in ROG_VENDOR_PRICE_COL.values()
        for clause in (f'"{col}" IS NOT NULL', f'"{col}" > 0')
    ]
    try:
        return JSONResponse(content=_fetch_rog_rows_paginated(
            "rog_compare_common", _params_dict(request), extra_clauses=extra
        ))
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


# NOTE: /api/status/history and /api/anomalies used to live here. Both were
# USA-only features and have been removed along with price_anomaly.py and the
# USA loader's snapshot calls. The other three regions still expose their own
# /status/history routes via _fetch_status_history().


# Vendor key -> the "vendor" label written into file_status.json by diamond_compare.py
_FILE_DATE_VENDOR_MAP = {
    "PC":          "pc",
    "loose-grown": "loose-grown",
    "brilliance":  "brilliance",
    "friendly":    "friendly",
}


def _read_file_dates() -> dict:
    """Return {pc: 'YYYY-MM-DD HH:MM:SS' | None, 'loose-grown': ..., 'brilliance': ...,
    'friendly': ..., checked_at: ... | None} from diamond_files/file_status.json.
    Returns all-None on missing/invalid file so the dashboard can still render."""
    out: dict = {"pc": None, "loose-grown": None, "brilliance": None, "friendly": None, "checked_at": None}
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
            for table in ("compare_common", "compare_partial",
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
# Database usage / storage quota  —  powers the Settings panel
# ============================================================================
# Managed Postgres plans bill on storage, and there is no way to read the plan's
# limit from inside Postgres, so the ceiling is configurable. Default is 512 MB
# (the common free-tier size); override with DB_STORAGE_LIMIT_MB in .env.
DB_STORAGE_LIMIT_MB = float(os.environ.get("DB_STORAGE_LIMIT_MB", "512") or 512)
_DB_LIMIT_FROM_ENV = "DB_STORAGE_LIMIT_MB" in os.environ

# Table-name prefix -> region label, for grouping the breakdown.
_TABLE_REGION_PREFIX = (("uk_", "UK"), ("roa_", "AUS"), ("rog_", "Germany"))


def _region_for_table(table: str) -> str:
    for prefix, label in _TABLE_REGION_PREFIX:
        if table.startswith(prefix):
            return label
    return "USA"


def _human_bytes(n: float) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(n) < 1024 or unit == "TB":
            return f"{n:.0f} {unit}" if unit in ("B", "KB") else f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} TB"


def _archive_usage() -> dict:
    """Size of the Parquet snapshot archive on disk. This is where compare_all
    now lives for USA/UK/AUS, so the Settings panel shows it next to the DB
    figure — otherwise it looks like that data simply vanished."""
    root = BASE_DIR / "history"
    if not root.exists():
        return {"exists": False, "files": 0, "size_bytes": 0, "size_pretty": "0 B", "regions": {}}
    total = 0
    files = 0
    regions: dict[str, dict] = {}
    for path in root.glob("*/*/*.parquet"):
        try:
            size = path.stat().st_size
        except OSError:
            continue
        total += size
        files += 1
        region = path.parts[-3].upper()
        slot = regions.setdefault(region, {"files": 0, "size_bytes": 0})
        slot["files"] += 1
        slot["size_bytes"] += size
    for slot in regions.values():
        slot["size_pretty"] = _human_bytes(slot["size_bytes"])
    return {
        "exists": True,
        "files": files,
        "size_bytes": total,
        "size_pretty": _human_bytes(total),
        "regions": regions,
    }


@app.get("/api/db-usage")
async def get_db_usage():
    """Storage usage for the connected database: total size against the
    configured quota, plus a per-table breakdown grouped by region."""
    from urllib.parse import urlparse

    parsed = urlparse(DATABASE_URL)
    host = parsed.hostname or "unknown"
    provider = (
        "Neon" if "neon.tech" in host
        else "Render" if "render.com" in host
        else "Supabase" if "supabase" in host
        else "PostgreSQL"
    )

    info: dict[str, Any] = {
        "provider": provider,
        "host": host,
        "database": (parsed.path or "/").lstrip("/") or "unknown",
        "limit_mb": DB_STORAGE_LIMIT_MB,
        "limit_source": (
            "DB_STORAGE_LIMIT_MB in .env" if _DB_LIMIT_FROM_ENV
            else "default assumption — set DB_STORAGE_LIMIT_MB in .env to match your plan"
        ),
        "archive": _archive_usage(),
    }

    try:
        with _get_conn() as conn, conn.cursor() as cur:
            cur.execute("SELECT pg_database_size(current_database()) AS b")
            size_bytes = int(cur.fetchone()["b"])

            cur.execute("SHOW server_version")
            info["postgres_version"] = cur.fetchone()["server_version"]

            cur.execute(
                "SELECT count(*) AS active, "
                "current_setting('max_connections')::int AS max "
                "FROM pg_stat_activity"
            )
            row = cur.fetchone()
            info["connections"] = {"active": int(row["active"]), "max": int(row["max"])}

            cur.execute(
                "SELECT tablename, "
                "  pg_total_relation_size(quote_ident(tablename)::regclass) AS bytes "
                "FROM pg_tables WHERE schemaname = 'public' "
                "ORDER BY bytes DESC"
            )
            raw_tables = cur.fetchall()

            # Exact counts: these tables top out around 250K rows, so COUNT(*)
            # is cheap enough and honest, unlike pg_class.reltuples which is
            # stale (or -1) until something ANALYZEs after the bulk load.
            tables = []
            for t in raw_tables:
                name = t["tablename"]
                try:
                    cur.execute(f'SELECT COUNT(*) AS c FROM "{name}"')
                    rows = int(cur.fetchone()["c"])
                except Exception:
                    rows = -1
                tables.append({
                    "table": name,
                    "region": _region_for_table(name),
                    "rows": rows,
                    "size_bytes": int(t["bytes"]),
                    "size_pretty": _human_bytes(int(t["bytes"])),
                })
    except Exception as e:
        return JSONResponse(content={**info, "ok": False, "error": str(e)})

    limit_bytes = DB_STORAGE_LIMIT_MB * 1024 * 1024
    by_region: dict[str, dict] = {}
    for t in tables:
        slot = by_region.setdefault(t["region"], {"tables": 0, "rows": 0, "size_bytes": 0})
        slot["tables"] += 1
        slot["rows"] += max(t["rows"], 0)
        slot["size_bytes"] += t["size_bytes"]
    for slot in by_region.values():
        slot["size_pretty"] = _human_bytes(slot["size_bytes"])

    return JSONResponse(content={
        **info,
        "ok": True,
        "size_bytes": size_bytes,
        "size_pretty": _human_bytes(size_bytes),
        "limit_bytes": limit_bytes,
        "used_pct": round(size_bytes / limit_bytes * 100, 1) if limit_bytes else None,
        "free_bytes": max(limit_bytes - size_bytes, 0),
        "free_pretty": _human_bytes(max(limit_bytes - size_bytes, 0)),
        "tables": tables,
        "by_region": by_region,
        "checked_at": datetime.datetime.now().isoformat(timespec="seconds"),
    })


# ============================================================================
# ============================================================================
# UK SIDE  —  /api/uk/*   (ROUK baseline vs diamond-heavon & abelini, GBP)
# ============================================================================
# ============================================================================
# DB column -> the JSON key the UK dashboard expects (matches the keys written
# by uk_scrapping/diamond_compare_uk.py).
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
    "dh_link":            "diamond-heavon Link",
}

UK_FILENAME_TO_TABLE = {
    "compare.json":         "uk_compare_common",
    "compare_partial.json": "uk_compare_partial",
}
UK_ALLOWED_TABLES = set(UK_FILENAME_TO_TABLE.values())

# vendor URL key -> price column used for "common with ROUK" filtering
UK_VENDOR_PRICE_COL = {
    "diamond-heavon": "dh_price_gbp",
    "abelini":        "ab_price_gbp",
}
UK_VENDOR_PCT_COL = {
    "diamond-heavon": "dh_vs_rouk_pct",
    "abelini":        "ab_vs_rouk_pct",
}

UK_DEFAULT_LIMIT = 25
UK_MAX_LIMIT = 500


def _uk_build_where(
    table: str,
    params: dict,
    extra_clauses: list[str] | None = None,
    extra_args: list | None = None,
) -> tuple[str, list]:
    """Build a SQL WHERE clause + parameter list from the request's filter params."""
    clauses: list[str] = list(extra_clauses or [])
    args: list[Any] = list(extra_args or [])

    cert = (params.get("cert") or "").strip()
    if cert:
        clauses.append('"certificate_number"::text ILIKE %s')
        args.append(f"%{cert}%")

    pmin = params.get("price_min")
    pmax = params.get("price_max")
    if pmin not in (None, ""):
        try:
            clauses.append('"rouk_price_gbp" >= %s'); args.append(float(pmin))
        except ValueError:
            pass
    if pmax not in (None, ""):
        try:
            clauses.append('"rouk_price_gbp" <= %s'); args.append(float(pmax))
        except ValueError:
            pass

    vp_key, vp_idx = _parse_bucket_param(params.get("vendor_pct"), UK_VENDOR_PCT_COL)
    if vp_key and vp_idx is not None and 0 <= vp_idx < len(PCT_BUCKETS):
        col = UK_VENDOR_PCT_COL[vp_key]
        bucket = PCT_BUCKETS[vp_idx]
        if bucket["min"] is not None:
            clauses.append(f'"{col}" >= %s'); args.append(bucket["min"])
        if bucket["max"] is not None:
            clauses.append(f'"{col}" < %s'); args.append(bucket["max"])
        clauses.append(f'"{col}" IS NOT NULL')

    vpr_key, vpr_idx = _parse_bucket_param(params.get("vendor_price"), UK_VENDOR_PRICE_COL)
    if vpr_key and vpr_idx is not None and 0 <= vpr_idx < len(PRICE_BUCKETS):
        col = UK_VENDOR_PRICE_COL[vpr_key]
        bucket = PRICE_BUCKETS[vpr_idx]
        if bucket["min"] is not None:
            clauses.append(f'"{col}" >= %s'); args.append(bucket["min"])
        if bucket["max"] is not None:
            clauses.append(f'"{col}" < %s'); args.append(bucket["max"])
        clauses.append(f'"{col}" IS NOT NULL AND "{col}" > 0')

    if (params.get("common_only") or "").lower() == "true":
        for c in UK_VENDOR_PRICE_COL.values():
            clauses.append(f'"{c}" IS NOT NULL AND "{c}" > 0')

    _add_diamond_attr_clauses(params, clauses, args)

    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    return where, args


def _fetch_uk_rows_paginated(
    table: str,
    key_map: dict[str, str],
    params: dict,
    extra_clauses: list[str] | None = None,
    extra_args: list | None = None,
) -> dict:
    """Paginated + filtered row fetch — never loads a whole UK table into memory."""
    if table not in UK_ALLOWED_TABLES:
        raise HTTPException(status_code=404, detail=f"Unknown UK table: {table}")

    limit = _clamp_int(params.get("limit"), UK_DEFAULT_LIMIT, 1, UK_MAX_LIMIT)
    offset = _clamp_int(params.get("offset"), 0, 0, 10_000_000)

    where_sql, where_args = _uk_build_where(table, params, extra_clauses, extra_args)

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

    out_rows = [{key_map[c]: _jsonify_value(r[c]) for c in db_cols} for r in raw]
    return {"rows": out_rows, "total": total, "limit": limit, "offset": offset}


def _uk_aggregate_sql_for_vendor(vendor_key: str) -> tuple[list[str], list[str]] | None:
    """Returns (pct_expressions, price_expressions) for the SELECT clause."""
    pct_col = UK_VENDOR_PCT_COL.get(vendor_key)
    price_col = UK_VENDOR_PRICE_COL.get(vendor_key)
    if not pct_col or not price_col:
        return None

    # SELECT aliases must be valid unquoted SQL identifiers — UK vendor keys
    # like "diamond-heavon" contain a hyphen, which Postgres would otherwise
    # parse as subtraction (e.g. "pct_diamond-heavon_0_cnt" -> syntax error).
    safe_vk = vendor_key.replace("-", "_")

    pct_exprs: list[str] = []
    for i, b in enumerate(PCT_BUCKETS):
        conds = [f'"{pct_col}" IS NOT NULL']
        if b["min"] is not None:
            conds.append(f'"{pct_col}" >= {b["min"]}')
        if b["max"] is not None:
            conds.append(f'"{pct_col}" < {b["max"]}')
        cond_sql = " AND ".join(conds)
        pct_exprs.append(f'COUNT(*) FILTER (WHERE {cond_sql}) AS pct_{safe_vk}_{i}_cnt')
        pct_exprs.append(
            f'AVG("rouk_price_gbp") FILTER (WHERE {cond_sql} AND "rouk_price_gbp" > 0) '
            f'AS pct_{safe_vk}_{i}_avgrouk'
        )

    price_exprs: list[str] = []
    for i, b in enumerate(PRICE_BUCKETS):
        conds = [f'"{price_col}" IS NOT NULL', f'"{price_col}" > 0']
        if b["min"] is not None:
            conds.append(f'"{price_col}" >= {b["min"]}')
        if b["max"] is not None:
            conds.append(f'"{price_col}" < {b["max"]}')
        cond_sql = " AND ".join(conds)
        price_exprs.append(f'COUNT(*) FILTER (WHERE {cond_sql}) AS price_{safe_vk}_{i}_cnt')
        price_exprs.append(
            f'AVG("rouk_price_gbp") FILTER (WHERE {cond_sql} AND "rouk_price_gbp" > 0) '
            f'AS price_{safe_vk}_{i}_avgrouk'
        )

    pct_exprs.append(
        f'COUNT(*) FILTER (WHERE "{price_col}" IS NOT NULL AND "{price_col}" > 0) '
        f'AS vendor_{safe_vk}_total'
    )
    return pct_exprs, price_exprs


def _compute_uk_aggregates(table: str, params: dict) -> dict:
    """One aggregate query returning bucket counts + averages for all UK vendors."""
    if table not in UK_ALLOWED_TABLES:
        return {"vendors": {}, "total": 0}

    where_sql, where_args = _uk_build_where(table, params)

    select_exprs: list[str] = ['COUNT(*) AS total_rows']
    active_vendors: list[str] = []
    for vk in ("diamond-heavon", "abelini"):
        out = _uk_aggregate_sql_for_vendor(vk)
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
        return None if v is None else _jsonify_value(v)

    vendors: dict[str, dict] = {}
    for vk in active_vendors:
        safe_vk = vk.replace("-", "_")
        pct_bs = [
            {
                "idx":      i,
                "count":    int(row.get(f"pct_{safe_vk}_{i}_cnt") or 0),
                "avg_rouk": _f(f"pct_{safe_vk}_{i}_avgrouk"),
            }
            for i in range(len(PCT_BUCKETS))
        ]
        price_bs = [
            {
                "idx":      i,
                "count":    int(row.get(f"price_{safe_vk}_{i}_cnt") or 0),
                "avg_rouk": _f(f"price_{safe_vk}_{i}_avgrouk"),
            }
            for i in range(len(PRICE_BUCKETS))
        ]
        vendors[vk] = {
            "vendor_total":  int(row.get(f"vendor_{safe_vk}_total") or 0),
            "pct_buckets":   pct_bs,
            "price_buckets": price_bs,
        }

    return {"vendors": vendors, "total": int(row.get("total_rows") or 0)}


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
        "file_dates": json.loads(r["file_dates"]) if r.get("file_dates") else {},
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
                          "uk_diamond_status"):
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
async def uk_get_default_compare(request: Request):
    """uk_compare_common (ROUK + all vendors), paginated (default 25/page) + server-side filters."""
    try:
        return JSONResponse(content=_fetch_uk_rows_paginated(
            "uk_compare_common", UK_DB_TO_JSON, _params_dict(request)
        ))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB read failed: {e}")


@app.get("/api/uk/data/{filename}")
async def uk_get_json_data(filename: str, request: Request):
    if ".." in filename or "/" in filename or "\\" in filename:
        raise HTTPException(status_code=400, detail="Invalid filename")
    if filename not in UK_FILENAME_TO_TABLE:
        raise HTTPException(status_code=404, detail=f"Unknown UK dataset: {filename}")
    try:
        return JSONResponse(content=_fetch_uk_rows_paginated(
            UK_FILENAME_TO_TABLE[filename], UK_DB_TO_JSON, _params_dict(request)
        ))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB read failed: {e}")


@app.get("/api/uk/aggregates")
async def uk_get_aggregates(request: Request, dataset: str = Query("compare_partial.json")):
    """Per-vendor bucket counts + averages for the given UK dataset, with filters applied."""
    if dataset not in UK_FILENAME_TO_TABLE:
        raise HTTPException(status_code=404, detail=f"Unknown UK dataset: {dataset}")
    table = UK_FILENAME_TO_TABLE[dataset]
    try:
        return JSONResponse(content=_compute_uk_aggregates(table, _params_dict(request)))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Aggregate query failed: {e}")


@app.get("/api/uk/status")
async def uk_get_status():
    data = _fetch_uk_status()
    if data is None:
        raise HTTPException(
            status_code=404,
            detail="uk_diamond_status table is empty - run the UK pipeline first",
        )
    return JSONResponse(content=data)


@app.get("/api/uk/status/history")
async def uk_get_status_history(limit: int = Query(52, ge=1, le=520)):
    """Trend data from uk_diamond_status_history, oldest -> newest."""
    try:
        return JSONResponse(content={
            "history": _fetch_status_history("uk_diamond_status_history", limit)
        })
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB read failed: {e}")


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
                # Reads compare_partial, not compare_all: the WHERE clause
                # requires the vendor to have a price, so every matching row is
                # by definition matched by >=1 vendor and therefore present in
                # compare_partial. Verified identical cert sets per vendor.
                cur.execute(f'SELECT COUNT(*) AS c FROM "uk_compare_partial" {where}')
                row = cur.fetchone()
                vendors[name] = {"rows": int(row["c"]) if row else 0}
        except Exception:
            vendors[name] = {"rows": 0}
    return {"generated_at": generated_at, "vendors": vendors}


@app.get("/api/uk/vendor-common/{vendor}")
async def uk_vendor_common_rows(vendor: str, request: Request):
    """Rows where the given vendor AND ROUK both have a price (matched set), paginated."""
    vendor = vendor.replace("_", "-") if vendor not in UK_VENDOR_PRICE_COL else vendor
    price_col = UK_VENDOR_PRICE_COL.get(vendor)
    if not price_col:
        raise HTTPException(status_code=404, detail=f"Unknown UK vendor: {vendor}")
    extra = [
        f'"{price_col}" IS NOT NULL', f'"{price_col}" > 0',
        '"rouk_price_gbp" IS NOT NULL', '"rouk_price_gbp" > 0',
    ]
    try:
        return JSONResponse(content=_fetch_uk_rows_paginated(
            "uk_compare_partial", UK_DB_TO_JSON, _params_dict(request), extra_clauses=extra
        ))
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
# ROA SIDE  —  /api/roa/*   (cullen & GS Diamonds vs ROA baseline, AUD)
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
    "roa_price_usd":      "ROA Price AUD",
    "roa_minus_30_usd":   "ROA -30% AUD",
    "cullen_price_usd":   "cullen Price AUD",
    "gs_price_usd":       "gs Price AUD",
    "cullen_vs_roa_usd":  "cullen vs ROA AUD",
    "cullen_vs_roa_pct":  "cullen vs ROA %",
    "gs_vs_roa_usd":      "gs vs ROA AUD",
    "gs_vs_roa_pct":      "gs vs ROA %",
    "cullen_link":        "cullen Link",
    "gs_link":            "gs Link",
}

ROA_OVERLAP_DB_TO_JSON = {
    "certificate_number": "certificate_number",
    "shape":              "shape",
    "color":              "color",
    "clarity":            "clarity",
    "cut":                "cut",
    "carat_weight":       "caratWeight",
    "cullen_price_usd":   "cullen Price AUD",
    "gs_price_usd":       "gs Price AUD",
    "roa_price_usd":      "ROA Price AUD",
    "cullen_vs_roa_pct":  "cullen vs ROA %",
    "gs_vs_roa_pct":      "gs vs ROA %",
}

ROA_FILENAME_TO_TABLE = {
    "compare.json":         "roa_compare_common",
    "compare_partial.json": "roa_compare_partial",
}
# roa_compare_vendor_overlap isn't one of the JSON-filename datasets, but it's a
# real paginated-fetch target (see roa_vendor_overlap below), so it must be
# allowed through the same table-name guard as the filename-mapped tables.
ROA_ALLOWED_TABLES = set(ROA_FILENAME_TO_TABLE.values()) | {"roa_compare_vendor_overlap"}

ROA_VENDOR_PRICE_COL = {
    "cullen": "cullen_price_usd",
    "gs":     "gs_price_usd",
}
ROA_VENDOR_PCT_COL = {
    "cullen": "cullen_vs_roa_pct",
    "gs":     "gs_vs_roa_pct",
}

ROA_DEFAULT_LIMIT = 25
ROA_MAX_LIMIT = 500


def _roa_build_where(
    table: str,
    params: dict,
    extra_clauses: list[str] | None = None,
    extra_args: list | None = None,
) -> tuple[str, list]:
    """Build a SQL WHERE clause + parameter list from the request's filter params."""
    clauses: list[str] = list(extra_clauses or [])
    args: list[Any] = list(extra_args or [])

    cert = (params.get("cert") or "").strip()
    if cert:
        clauses.append('"certificate_number"::text ILIKE %s')
        args.append(f"%{cert}%")

    pmin = params.get("price_min")
    pmax = params.get("price_max")
    if pmin not in (None, ""):
        try:
            clauses.append('"roa_price_usd" >= %s'); args.append(float(pmin))
        except ValueError:
            pass
    if pmax not in (None, ""):
        try:
            clauses.append('"roa_price_usd" <= %s'); args.append(float(pmax))
        except ValueError:
            pass

    vp_key, vp_idx = _parse_bucket_param(params.get("vendor_pct"), ROA_VENDOR_PCT_COL)
    if vp_key and vp_idx is not None and 0 <= vp_idx < len(PCT_BUCKETS):
        col = ROA_VENDOR_PCT_COL[vp_key]
        bucket = PCT_BUCKETS[vp_idx]
        if bucket["min"] is not None:
            clauses.append(f'"{col}" >= %s'); args.append(bucket["min"])
        if bucket["max"] is not None:
            clauses.append(f'"{col}" < %s'); args.append(bucket["max"])
        clauses.append(f'"{col}" IS NOT NULL')

    vpr_key, vpr_idx = _parse_bucket_param(params.get("vendor_price"), ROA_VENDOR_PRICE_COL)
    if vpr_key and vpr_idx is not None and 0 <= vpr_idx < len(PRICE_BUCKETS):
        col = ROA_VENDOR_PRICE_COL[vpr_key]
        bucket = PRICE_BUCKETS[vpr_idx]
        if bucket["min"] is not None:
            clauses.append(f'"{col}" >= %s'); args.append(bucket["min"])
        if bucket["max"] is not None:
            clauses.append(f'"{col}" < %s'); args.append(bucket["max"])
        clauses.append(f'"{col}" IS NOT NULL AND "{col}" > 0')

    if (params.get("common_only") or "").lower() == "true":
        for c in ROA_VENDOR_PRICE_COL.values():
            clauses.append(f'"{c}" IS NOT NULL AND "{c}" > 0')

    _add_diamond_attr_clauses(params, clauses, args)

    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    return where, args


def _fetch_roa_rows_paginated(
    table: str,
    key_map: dict[str, str],
    params: dict,
    extra_clauses: list[str] | None = None,
    extra_args: list | None = None,
) -> dict:
    """Paginated + filtered row fetch — never loads a whole ROA table into memory.
    key_map is explicit (not hardcoded to ROA_DB_TO_JSON) because ROA has a second,
    smaller column map (ROA_OVERLAP_DB_TO_JSON) for roa_compare_vendor_overlap."""
    if table not in ROA_ALLOWED_TABLES:
        raise HTTPException(status_code=404, detail=f"Unknown ROA table: {table}")

    limit = _clamp_int(params.get("limit"), ROA_DEFAULT_LIMIT, 1, ROA_MAX_LIMIT)
    offset = _clamp_int(params.get("offset"), 0, 0, 10_000_000)

    where_sql, where_args = _roa_build_where(table, params, extra_clauses, extra_args)

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

    out_rows = [{key_map[c]: _jsonify_value(r[c]) for c in db_cols} for r in raw]
    return {"rows": out_rows, "total": total, "limit": limit, "offset": offset}


def _roa_aggregate_sql_for_vendor(vendor_key: str) -> tuple[list[str], list[str]] | None:
    """Returns (pct_expressions, price_expressions) for the SELECT clause."""
    pct_col = ROA_VENDOR_PCT_COL.get(vendor_key)
    price_col = ROA_VENDOR_PRICE_COL.get(vendor_key)
    if not pct_col or not price_col:
        return None

    # SELECT aliases must be valid unquoted SQL identifiers — sanitize hyphens
    # (cheap insurance; current ROA vendor keys have none, but this keeps the
    # pattern consistent with the UK port where it mattered).
    safe_vk = vendor_key.replace("-", "_")

    pct_exprs: list[str] = []
    for i, b in enumerate(PCT_BUCKETS):
        conds = [f'"{pct_col}" IS NOT NULL']
        if b["min"] is not None:
            conds.append(f'"{pct_col}" >= {b["min"]}')
        if b["max"] is not None:
            conds.append(f'"{pct_col}" < {b["max"]}')
        cond_sql = " AND ".join(conds)
        pct_exprs.append(f'COUNT(*) FILTER (WHERE {cond_sql}) AS pct_{safe_vk}_{i}_cnt')
        pct_exprs.append(
            f'AVG("roa_price_usd") FILTER (WHERE {cond_sql} AND "roa_price_usd" > 0) '
            f'AS pct_{safe_vk}_{i}_avgroa'
        )

    price_exprs: list[str] = []
    for i, b in enumerate(PRICE_BUCKETS):
        conds = [f'"{price_col}" IS NOT NULL', f'"{price_col}" > 0']
        if b["min"] is not None:
            conds.append(f'"{price_col}" >= {b["min"]}')
        if b["max"] is not None:
            conds.append(f'"{price_col}" < {b["max"]}')
        cond_sql = " AND ".join(conds)
        price_exprs.append(f'COUNT(*) FILTER (WHERE {cond_sql}) AS price_{safe_vk}_{i}_cnt')
        price_exprs.append(
            f'AVG("roa_price_usd") FILTER (WHERE {cond_sql} AND "roa_price_usd" > 0) '
            f'AS price_{safe_vk}_{i}_avgroa'
        )

    pct_exprs.append(
        f'COUNT(*) FILTER (WHERE "{price_col}" IS NOT NULL AND "{price_col}" > 0) '
        f'AS vendor_{safe_vk}_total'
    )
    return pct_exprs, price_exprs


def _compute_roa_aggregates(table: str, params: dict) -> dict:
    """One aggregate query returning bucket counts + averages for all ROA vendors."""
    if table not in ROA_ALLOWED_TABLES:
        return {"vendors": {}, "total": 0}

    where_sql, where_args = _roa_build_where(table, params)

    select_exprs: list[str] = ['COUNT(*) AS total_rows']
    active_vendors: list[str] = []
    for vk in ("cullen", "gs"):
        out = _roa_aggregate_sql_for_vendor(vk)
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
        return None if v is None else _jsonify_value(v)

    vendors: dict[str, dict] = {}
    for vk in active_vendors:
        safe_vk = vk.replace("-", "_")
        pct_bs = [
            {
                "idx":     i,
                "count":   int(row.get(f"pct_{safe_vk}_{i}_cnt") or 0),
                "avg_roa": _f(f"pct_{safe_vk}_{i}_avgroa"),
            }
            for i in range(len(PCT_BUCKETS))
        ]
        price_bs = [
            {
                "idx":     i,
                "count":   int(row.get(f"price_{safe_vk}_{i}_cnt") or 0),
                "avg_roa": _f(f"price_{safe_vk}_{i}_avgroa"),
            }
            for i in range(len(PRICE_BUCKETS))
        ]
        vendors[vk] = {
            "vendor_total":  int(row.get(f"vendor_{safe_vk}_total") or 0),
            "pct_buckets":   pct_bs,
            "price_buckets": price_bs,
        }

    return {"vendors": vendors, "total": int(row.get("total_rows") or 0)}


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
    try:
        with _get_conn() as conn, conn.cursor() as cur:
            cur.execute(sql)
            r = cur.fetchone()
    except Exception:
        # Table absent until the ROA loader has run at least once. Report that
        # as "no data" (404, like the other regions) instead of a 500 traceback.
        return None
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
        "file_dates": json.loads(r["file_dates"]) if r.get("file_dates") else {},
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
                          "roa_diamond_status"):
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
async def roa_get_default_compare(request: Request):
    """roa_compare_common, paginated (default 25/page) + server-side filters."""
    try:
        return JSONResponse(content=_fetch_roa_rows_paginated(
            "roa_compare_common", ROA_DB_TO_JSON, _params_dict(request)
        ))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB read failed: {e}")


@app.get("/api/roa/data/{filename}")
async def roa_get_json_data(filename: str, request: Request):
    if ".." in filename or "/" in filename or "\\" in filename:
        raise HTTPException(status_code=400, detail="Invalid filename")
    if filename not in ROA_FILENAME_TO_TABLE:
        raise HTTPException(status_code=404, detail=f"Unknown ROA dataset: {filename}")
    try:
        return JSONResponse(content=_fetch_roa_rows_paginated(
            ROA_FILENAME_TO_TABLE[filename], ROA_DB_TO_JSON, _params_dict(request)
        ))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB read failed: {e}")


@app.get("/api/roa/aggregates")
async def roa_get_aggregates(request: Request, dataset: str = Query("compare_partial.json")):
    """Per-vendor bucket counts + averages for the given ROA dataset, with filters applied."""
    if dataset not in ROA_FILENAME_TO_TABLE:
        raise HTTPException(status_code=404, detail=f"Unknown ROA dataset: {dataset}")
    table = ROA_FILENAME_TO_TABLE[dataset]
    try:
        return JSONResponse(content=_compute_roa_aggregates(table, _params_dict(request)))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Aggregate query failed: {e}")


@app.get("/api/roa/status")
async def roa_get_status():
    data = _fetch_roa_status()
    if data is None:
        raise HTTPException(
            status_code=404,
            detail="roa_diamond_status table is empty - run the ROA pipeline first",
        )
    return JSONResponse(content=data)


@app.get("/api/roa/status/history")
async def roa_get_status_history(limit: int = Query(52, ge=1, le=520)):
    """Trend data from roa_diamond_status_history, oldest -> newest."""
    try:
        return JSONResponse(content={
            "history": _fetch_status_history("roa_diamond_status_history", limit)
        })
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB read failed: {e}")


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
                # compare_partial rather than compare_all — see the UK note above.
                cur.execute(f'SELECT COUNT(*) AS c FROM "roa_compare_partial" {where}')
                row = cur.fetchone()
                vendors[name] = {"rows": int(row["c"]) if row else 0}
        except Exception:
            vendors[name] = {"rows": 0}
    return {"generated_at": generated_at, "vendors": vendors}


@app.get("/api/roa/vendor-common/{vendor}")
async def roa_vendor_common_rows(vendor: str, request: Request):
    """Rows where this vendor AND ROA both have a price (matched set), paginated."""
    price_col = ROA_VENDOR_PRICE_COL.get(vendor)
    if not price_col:
        raise HTTPException(status_code=404, detail=f"Unknown ROA vendor: {vendor}")
    extra = [
        f'"{price_col}" IS NOT NULL', f'"{price_col}" > 0',
        '"roa_price_usd" IS NOT NULL', '"roa_price_usd" > 0',
    ]
    try:
        return JSONResponse(content=_fetch_roa_rows_paginated(
            "roa_compare_partial", ROA_DB_TO_JSON, _params_dict(request), extra_clauses=extra
        ))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB read failed: {e}")


@app.get("/api/roa/vendor-overlap")
async def roa_vendor_overlap(request: Request):
    """Diamonds whose certificate appears in both cullen and gs data, paginated.
    roa_compare_vendor_overlap is a pre-joined table (already filtered to overlap
    rows), so no extra_clauses are needed — just the standard filters + paging."""
    try:
        return JSONResponse(content=_fetch_roa_rows_paginated(
            "roa_compare_vendor_overlap", ROA_OVERLAP_DB_TO_JSON, _params_dict(request)
        ))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB read failed: {e}")


@app.post("/api/roa/run-compare")
async def roa_run_compare():
    import subprocess
    import sys

    compare_script = ROA_DIR / "diamond_compare_roa.py"
    loader_script  = ROA_DIR / "roa_compare_to_db.py"

    if not compare_script.exists():
        raise HTTPException(status_code=404, detail="diamond_compare_roa.py not found in aus_scrapping/")

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
