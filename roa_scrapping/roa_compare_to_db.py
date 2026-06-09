"""
roa_compare_to_db.py
====================

Load the AUS/ROA compare JSON files into the Render PostgreSQL database
(target connection read from the DATABASE_URL env var / .env file).

This is the ROA counterpart of compare_to_mysql.py (USA) and
uk_compare_to_db.py (UK). The ROA pipeline uses ROA as the baseline vendor
and compares two vendors against it: Cullen Jewellery and GS Diamonds.
All prices are in USD ($).

Tables managed
--------------
- roa_compare_common          from compare.json               (rows matched by all 3)
- roa_compare_all             from compare_all.json           (every ROA diamond)
- roa_compare_partial         from compare_partial.json       (rows matched by 2+)
- roa_compare_vendor_overlap  from compare_vendor_overlap.json (cullen ∩ gs certs)
- roa_diamond_status          from diamond_status.json        (1 snapshot per import)

On first run : creates all 5 tables (CREATE TABLE IF NOT EXISTS).
On every run : TRUNCATES all tables (clears data, keeps schema + indexes),
               then bulk-inserts fresh rows from the JSON files.

Source folder: <repo-root>/compare/roa  (override with ROA_COMPARE_DIR env var).
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from urllib.parse import urlparse

import psycopg2
from dotenv import load_dotenv
from psycopg2.extensions import cursor as PgCursor
from psycopg2.extras import execute_values

# ── Config ───────────────────────────────────────────────────────────────────
REPO_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(REPO_ROOT / ".env")

DATABASE_URL = os.environ.get("DATABASE_URL", "")
if not DATABASE_URL:
    raise SystemExit(
        "DATABASE_URL env var is required. "
        "Put it in .env locally, or set it in Render's Environment tab."
    )
if "sslmode=" not in DATABASE_URL:
    DATABASE_URL += ("&" if "?" in DATABASE_URL else "?") + "sslmode=require"

COMPARE_DIR = Path(os.environ.get("ROA_COMPARE_DIR", REPO_ROOT / "compare" / "roa"))

BATCH_SIZE = 1000


# ── Schema ───────────────────────────────────────────────────────────────────
# cullen = Cullen Jewellery, gs = GS Diamonds, roa = ROA baseline. USD ($).
ROA_COMPARE_COLUMNS = [
    ("certificate_number",  "VARCHAR(64)"),
    ("vendors_matched",     "VARCHAR(64)"),
    ("shape",               "VARCHAR(32)"),
    ("color",               "VARCHAR(16)"),
    ("clarity",             "VARCHAR(16)"),
    ("cut",                 "VARCHAR(32)"),
    ("carat_weight",        "DECIMAL(6,3)"),
    ("roa_price_usd",       "DECIMAL(12,2)"),
    ("roa_minus_30_usd",    "DECIMAL(12,2)"),
    ("cullen_price_usd",    "DECIMAL(12,2)"),
    ("cullen_vs_roa_usd",   "DECIMAL(12,2)"),
    ("cullen_vs_roa_pct",   "DECIMAL(8,2)"),
    ("gs_price_usd",        "DECIMAL(12,2)"),
    ("gs_vs_roa_usd",       "DECIMAL(12,2)"),
    ("gs_vs_roa_pct",       "DECIMAL(8,2)"),
]

# JSON key (written by diamond_compare_roa.py) -> DB column name
ROA_COMPARE_KEY_MAP = {
    "certificate_number":  "certificate_number",
    "vendors_matched":     "vendors_matched",
    "shape":               "shape",
    "color":               "color",
    "clarity":             "clarity",
    "cut":                 "cut",
    "caratWeight":         "carat_weight",
    "ROA Price USD":       "roa_price_usd",
    "ROA -30% USD":        "roa_minus_30_usd",
    "cullen Price USD":    "cullen_price_usd",
    "cullen vs ROA USD":   "cullen_vs_roa_usd",
    "cullen vs ROA %":     "cullen_vs_roa_pct",
    "gs Price USD":        "gs_price_usd",
    "gs vs ROA USD":       "gs_vs_roa_usd",
    "gs vs ROA %":         "gs_vs_roa_pct",
}

ROA_OVERLAP_COLUMNS = [
    ("certificate_number",  "VARCHAR(64)"),
    ("shape",               "VARCHAR(32)"),
    ("color",               "VARCHAR(16)"),
    ("clarity",             "VARCHAR(16)"),
    ("cut",                 "VARCHAR(32)"),
    ("carat_weight",        "DECIMAL(6,3)"),
    ("cullen_price_usd",    "DECIMAL(12,2)"),
    ("gs_price_usd",        "DECIMAL(12,2)"),
    ("roa_price_usd",       "DECIMAL(12,2)"),
    ("cullen_vs_roa_pct",   "DECIMAL(8,2)"),
    ("gs_vs_roa_pct",       "DECIMAL(8,2)"),
]

ROA_OVERLAP_KEY_MAP = {
    "certificate_number":  "certificate_number",
    "shape":               "shape",
    "color":               "color",
    "clarity":             "clarity",
    "cut":                 "cut",
    "caratWeight":         "carat_weight",
    "cullen Price USD":    "cullen_price_usd",
    "gs Price USD":        "gs_price_usd",
    "ROA Price USD":       "roa_price_usd",
    "cullen vs ROA %":     "cullen_vs_roa_pct",
    "gs vs ROA %":         "gs_vs_roa_pct",
}

ROA_STATUS_COLUMNS = [
    ("generated_at",          "TIMESTAMP"),
    ("roa_total",             "INT"),
    ("cullen_vendor_total",   "INT"),
    ("gs_vendor_total",       "INT"),
    ("cullen_matched",        "INT"),
    ("cullen_match_rate_pct", "DECIMAL(6,2)"),
    ("gs_matched",            "INT"),
    ("gs_match_rate_pct",     "DECIMAL(6,2)"),
    ("common_count",          "INT"),
    ("partial_count",         "INT"),
    ("all_roa_count",         "INT"),
    ("vendor_overlap_count",  "INT"),
]


# ── Helpers ──────────────────────────────────────────────────────────────────
def connect():
    return psycopg2.connect(DATABASE_URL, connect_timeout=30)


def ensure_table_exists(
    cur: PgCursor,
    table: str,
    columns: list[tuple[str, str]],
    indexes: list[str],
) -> None:
    cols_sql = ",\n  ".join(f'"{name}" {ctype}' for name, ctype in columns)
    cur.execute(
        f'CREATE TABLE IF NOT EXISTS "{table}" (\n'
        f'  "id" BIGSERIAL PRIMARY KEY,\n'
        f'  {cols_sql}\n'
        f")"
    )
    for col in indexes:
        cur.execute(
            f'CREATE INDEX IF NOT EXISTS "idx_{table}_{col}" ON "{table}" ("{col}")'
        )


def truncate_all_roa_tables(cur: PgCursor) -> None:
    tables = [
        "roa_compare_common",
        "roa_compare_all",
        "roa_compare_partial",
        "roa_compare_vendor_overlap",
        "roa_diamond_status",
    ]
    print("Truncating ROA tables…")
    for table in tables:
        cur.execute(f'TRUNCATE TABLE "{table}" RESTART IDENTITY CASCADE')
        print(f'  truncated "{table}"')


def load_compare_rows(
    json_path: Path, key_map: dict[str, str]
) -> tuple[list[str], list[tuple]]:
    with open(json_path, encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, list):
        raise ValueError(f"{json_path.name} is not a JSON list")
    db_cols = list(key_map.values())
    rows = [tuple(item.get(jk) for jk in key_map.keys()) for item in data]
    return db_cols, rows


def insert_rows(
    cur: PgCursor, table: str, db_cols: list[str], rows: list[tuple]
) -> int:
    if not rows:
        return 0
    cols_sql = ", ".join(f'"{c}"' for c in db_cols)
    execute_values(cur, f'INSERT INTO "{table}" ({cols_sql}) VALUES %s', rows, page_size=BATCH_SIZE)
    print(f'    {len(rows):>7,} rows -> "{table}"')
    return len(rows)


# ── Upload ───────────────────────────────────────────────────────────────────
def upload_all_roa_json_files(cur: PgCursor) -> None:
    # Step 1 — create tables on first run (no-op thereafter)
    ensure_table_exists(cur, "roa_compare_common",         ROA_COMPARE_COLUMNS, ["certificate_number", "shape"])
    ensure_table_exists(cur, "roa_compare_all",            ROA_COMPARE_COLUMNS, ["certificate_number", "shape"])
    ensure_table_exists(cur, "roa_compare_partial",        ROA_COMPARE_COLUMNS, ["certificate_number", "shape"])
    ensure_table_exists(cur, "roa_compare_vendor_overlap", ROA_OVERLAP_COLUMNS, ["certificate_number"])
    ensure_table_exists(cur, "roa_diamond_status",         ROA_STATUS_COLUMNS,  ["generated_at"])

    # Step 2 — truncate all ROA tables
    truncate_all_roa_tables(cur)

    # Step 3 — upload compare JSON files
    print("Uploading ROA JSON files…")
    for table, json_file in [
        ("roa_compare_common",  "compare.json"),
        ("roa_compare_all",     "compare_all.json"),
        ("roa_compare_partial", "compare_partial.json"),
    ]:
        path = COMPARE_DIR / json_file
        if not path.exists():
            print(f'  SKIP "{table}" — file missing: {json_file}')
            continue
        db_cols, rows = load_compare_rows(path, ROA_COMPARE_KEY_MAP)
        insert_rows(cur, table, db_cols, rows)

    # Step 4 — vendor overlap
    overlap_path = COMPARE_DIR / "compare_vendor_overlap.json"
    if overlap_path.exists():
        db_cols, rows = load_compare_rows(overlap_path, ROA_OVERLAP_KEY_MAP)
        insert_rows(cur, "roa_compare_vendor_overlap", db_cols, rows)
    else:
        print('  SKIP "roa_compare_vendor_overlap" — file missing')

    # Step 5 — status snapshot
    status_path = COMPARE_DIR / "diamond_status.json"
    if not status_path.exists():
        print('  SKIP "roa_diamond_status" — file missing')
        return

    with open(status_path, encoding="utf-8") as f:
        s = json.load(f)

    vendor_totals   = s.get("vendor_totals", {}) or {}
    roa_vs_vendors  = s.get("ROA_vs_vendors", {}) or {}
    counts          = s.get("dataset_counts", {}) or {}
    cullen_stats    = roa_vs_vendors.get("cullen", {}) or {}
    gs_stats        = roa_vs_vendors.get("gs", {}) or {}

    status_row = (
        s.get("generated_at"),
        s.get("ROA_total"),
        vendor_totals.get("cullen"),
        vendor_totals.get("gs"),
        cullen_stats.get("matched"),
        cullen_stats.get("match_rate_pct"),
        gs_stats.get("matched"),
        gs_stats.get("match_rate_pct"),
        counts.get("common"),
        counts.get("partial"),
        counts.get("all_roa"),
        counts.get("vendor_overlap"),
    )
    insert_rows(cur, "roa_diamond_status", [c[0] for c in ROA_STATUS_COLUMNS], [status_row])


# ── Main ─────────────────────────────────────────────────────────────────────
def main() -> None:
    parsed = urlparse(DATABASE_URL)
    target = f"postgresql://{parsed.username}@{parsed.hostname}{parsed.path}"
    print(f"ROA compare folder : {COMPARE_DIR}")
    print(f"Target             : {target}")
    print("-" * 72)

    if not COMPARE_DIR.exists():
        raise SystemExit(f"compare/roa folder not found: {COMPARE_DIR}")

    conn = connect()
    try:
        with conn.cursor() as cur:
            upload_all_roa_json_files(cur)
        conn.commit()
    finally:
        conn.close()

    print("-" * 72)
    print("DONE.")


if __name__ == "__main__":
    main()
