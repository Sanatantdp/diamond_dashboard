"""
uk_compare_to_db.py
===================

Load the UK compare JSON files into the Render PostgreSQL database
(target connection read from the DATABASE_URL env var / .env file).

This is the UK counterpart of compare_to_mysql.py. The UK pipeline
(uk_scrapping/diamond_compare_uk.py) uses ROUK as the baseline vendor
(the equivalent of "PC" on the USA side) and compares two vendors against
it: diamond-heavon and abelini. All prices are in GBP (£).

Tables managed
--------------
- uk_compare_common    from compare.json          (rows matched by all 3 vendors)
  (uk_compare_all is deliberately NOT loaded — see truncate_all_uk_tables)
- uk_compare_partial   from compare_partial.json   (rows matched by 2+ vendors)
- uk_diamond_status    from diamond_status.json    (1 snapshot row per import)

On first run: creates all 4 tables (CREATE TABLE IF NOT EXISTS).
On every run: TRUNCATES all tables (clears data, keeps schema + indexes),
then bulk-inserts fresh rows from the JSON files.

Source folder: <repo-root>/compare/uk  (override with UK_COMPARE_DIR env var).
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from urllib.parse import urlparse

import psycopg2
from dotenv import load_dotenv
from psycopg2.extensions import cursor as PgCursor
from psycopg2.extras import execute_values

# ── Config (DATABASE_URL from .env or Render env) ────────────────────────────
REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))
import snapshot_history  # noqa: E402  (needs REPO_ROOT on sys.path first)

load_dotenv(REPO_ROOT / ".env")
DATABASE_URL = os.environ.get("DATABASE_URL", "")
if not DATABASE_URL:
    raise SystemExit(
        "DATABASE_URL env var is required. "
        "Put it in .env locally, or set it in Render's Environment tab."
    )
if "sslmode=" not in DATABASE_URL:
    DATABASE_URL += ("&" if "?" in DATABASE_URL else "?") + "sslmode=require"

COMPARE_DIR = Path(os.environ.get("UK_COMPARE_DIR", REPO_ROOT / "compare" / "uk"))

BATCH_SIZE = 1000


# ── Schema ───────────────────────────────────────────────────────────────────
# dh = diamond-heavon, ab = abelini, rouk = ROUK (baseline). Prices in GBP.
UK_COMPARE_COLUMNS = [
    ("certificate_number",  "VARCHAR(64)"),
    ("vendors_matched",     "VARCHAR(64)"),
    ("shape",               "VARCHAR(32)"),
    ("color",               "VARCHAR(16)"),
    ("clarity",             "VARCHAR(16)"),
    ("cut",                 "VARCHAR(32)"),
    ("carat_weight",        "DECIMAL(6,3)"),
    ("dh_price_gbp",        "DECIMAL(12,2)"),
    ("dh_minus_30_gbp",     "DECIMAL(12,2)"),
    ("dh_vs_rouk_gbp",      "DECIMAL(12,2)"),
    ("dh_vs_rouk_pct",      "DECIMAL(8,2)"),
    ("ab_price_gbp",        "DECIMAL(12,2)"),
    ("ab_minus_30_gbp",     "DECIMAL(12,2)"),
    ("ab_vs_rouk_gbp",      "DECIMAL(12,2)"),
    ("ab_vs_rouk_pct",      "DECIMAL(8,2)"),
    ("rouk_price_gbp",      "DECIMAL(12,2)"),
    ("rouk_minus_30_gbp",   "DECIMAL(12,2)"),
    ("dh_link",             "VARCHAR(512)"),
]

# Map JSON key (as written by diamond_compare_uk.py) -> DB column name.
UK_COMPARE_KEY_MAP = {
    "certificate_number":            "certificate_number",
    "vendors_matched":               "vendors_matched",
    "shape":                         "shape",
    "color":                         "color",
    "clarity":                       "clarity",
    "cut":                           "cut",
    "caratWeight":                   "carat_weight",
    "diamond-heavon Price Pound":    "dh_price_gbp",
    "diamond-heavon -30% Pound":     "dh_minus_30_gbp",
    "diamond-heavon vs ROUK Pound":  "dh_vs_rouk_gbp",
    "diamond-heavon vs ROUK %":      "dh_vs_rouk_pct",
    "abelini Price Pound":           "ab_price_gbp",
    "abelini -30% Pound":            "ab_minus_30_gbp",
    "abelini vs ROUK Pound":         "ab_vs_rouk_gbp",
    "abelini vs ROUK %":             "ab_vs_rouk_pct",
    "ROUK Price Pound":              "rouk_price_gbp",
    "ROUK -30% Pound":               "rouk_minus_30_gbp",
    "diamond-heavon Link":           "dh_link",
}

UK_STATUS_COLUMNS = [
    ("generated_at",        "DATETIME"),
    ("rouk_total",          "INT"),
    ("dh_vendor_total",     "INT"),
    ("ab_vendor_total",     "INT"),
    ("dh_matched",          "INT"),
    ("dh_match_rate_pct",   "DECIMAL(6,2)"),
    ("ab_matched",          "INT"),
    ("ab_match_rate_pct",   "DECIMAL(6,2)"),
    ("common_count",        "INT"),
    ("partial_count",       "INT"),
    ("all_rouk_count",      "INT"),
    ("file_dates",          "TEXT"),
]


# ── Helpers ──────────────────────────────────────────────────────────────────
def connect():
    """Open a connection to the Render PostgreSQL database."""
    return psycopg2.connect(DATABASE_URL, connect_timeout=30)


def _to_pg_type(mysql_type: str) -> str:
    """Translate the small number of MySQL-style types we use into Postgres ones."""
    t = mysql_type.strip().upper()
    if t == "DATETIME":
        return "TIMESTAMP"
    return mysql_type


def build_create_sql(table: str, columns: list[tuple[str, str]]) -> str:
    cols_sql = ",\n  ".join(f'"{name}" {_to_pg_type(ctype)}' for name, ctype in columns)
    return (
        f'CREATE TABLE "{table}" (\n'
        f'  "id" BIGSERIAL PRIMARY KEY,\n'
        f'  {cols_sql}\n'
        f")"
    )


def ensure_table_exists(cur: PgCursor, table: str, columns: list[tuple[str, str]], indexes: list[str]) -> None:
    """Create table + indexes if missing, and add any newly-introduced columns
    to a table that already exists from a prior run of this script."""
    cols_sql = ",\n  ".join(f'"{name}" {_to_pg_type(ctype)}' for name, ctype in columns)
    cur.execute(
        f'CREATE TABLE IF NOT EXISTS "{table}" (\n'
        f'  "id" BIGSERIAL PRIMARY KEY,\n'
        f'  {cols_sql}\n'
        f")"
    )
    for name, ctype in columns:
        cur.execute(
            f'ALTER TABLE "{table}" ADD COLUMN IF NOT EXISTS "{name}" {_to_pg_type(ctype)}'
        )
    for col in indexes:
        cur.execute(
            f'CREATE INDEX IF NOT EXISTS "idx_{table}_{col}" ON "{table}" ("{col}")'
        )


def truncate_all_uk_tables(cur: PgCursor) -> None:
    """Truncate the UK diamond tables and reset their serial IDs."""
    tables = [
        "uk_compare_common",
        "uk_compare_partial",
        "uk_diamond_status",
    ]
    print("Truncating UK tables…")
    for table in tables:
        cur.execute(f'TRUNCATE TABLE "{table}" RESTART IDENTITY CASCADE')
        print(f'  truncated "{table}"')

    # uk_compare_all is no longer loaded — the UK dashboard never reads it, and
    # at ~222K rows / 64 MB it was the largest table in the database. The data
    # is not lost: snapshot_history.py archives compare_all.json to Parquet on
    # every run. Dropped rather than truncated so the storage is released.
    cur.execute('DROP TABLE IF EXISTS "uk_compare_all"')
    print('  dropped "uk_compare_all" (unused — served from the Parquet archive)')


def load_compare_rows(json_path: Path, key_map: dict[str, str]) -> tuple[list[str], list[tuple]]:
    """Read a JSON list-of-dicts and return (db_columns, rows-as-tuples)."""
    with open(json_path, encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, list):
        raise ValueError(f"{json_path.name} is not a JSON list")

    db_cols = list(key_map.values())
    rows = [tuple(item.get(json_key) for json_key in key_map.keys()) for item in data]
    return db_cols, rows


def insert_rows(cur: PgCursor, table: str, db_cols: list[str], rows: list[tuple]) -> int:
    if not rows:
        return 0
    cols_sql = ", ".join(f'"{c}"' for c in db_cols)
    sql = f'INSERT INTO "{table}" ({cols_sql}) VALUES %s'
    execute_values(cur, sql, rows, page_size=BATCH_SIZE)
    print(f'    {len(rows):>7,} rows -> "{table}"')
    return len(rows)


def _parse_uk_status_row(path: Path) -> tuple | None:
    """Read diamond_status.json and build the UK_STATUS_COLUMNS row tuple, or
    None if the file is missing. Shared by the history snapshot (pre-truncate)
    and the live uk_diamond_status insert (post-truncate)."""
    if not path.exists():
        return None

    with open(path, encoding="utf-8") as f:
        s = json.load(f)

    vendor_totals = s.get("vendor_totals", {}) or {}
    rouk_vs       = s.get("rouk_vs_vendors", {}) or {}
    counts        = s.get("dataset_counts", {}) or {}
    dh_stats      = rouk_vs.get("diamond-heavon", {}) or {}
    ab_stats      = rouk_vs.get("abelini", {}) or {}

    return (
        s.get("generated_at"),
        s.get("rouk_total"),
        vendor_totals.get("diamond-heavon"),
        vendor_totals.get("abelini"),
        dh_stats.get("matched"),
        dh_stats.get("match_rate_pct"),
        ab_stats.get("matched"),
        ab_stats.get("match_rate_pct"),
        counts.get("common"),
        counts.get("partial"),
        counts.get("all_ROUK"),
        json.dumps(s.get("file_dates") or {}),
    )


# ── Upload all JSON files ─────────────────────────────────────────────────────
def upload_all_uk_json_files(cur: PgCursor) -> None:
    """Ensure all UK tables exist, snapshot history, truncate, then reload."""
    # Step 1 — ensure tables exist (creates them on first run, no-op thereafter)
    ensure_table_exists(cur, "uk_compare_common",  UK_COMPARE_COLUMNS, ["certificate_number", "shape"])
    ensure_table_exists(cur, "uk_compare_partial", UK_COMPARE_COLUMNS, ["certificate_number", "shape"])
    ensure_table_exists(cur, "uk_diamond_status",  UK_STATUS_COLUMNS,  ["generated_at"])
    snapshot_history.ensure_status_history_table(cur, "uk_diamond_status_history", UK_STATUS_COLUMNS)

    # Step 2 — historical snapshot, BEFORE truncate
    status_row = _parse_uk_status_row(COMPARE_DIR / "diamond_status.json")
    snapshot_history.archive_compare_json("uk", COMPARE_DIR)
    if status_row is not None:
        snapshot_history.insert_status_history_row(
            cur, "uk_diamond_status_history", [c[0] for c in UK_STATUS_COLUMNS], status_row
        )

    # Step 3 — truncate all tables
    truncate_all_uk_tables(cur)

    # Step 4 — upload JSON files
    print("Uploading UK JSON files…")
    for table, json_file in [
        ("uk_compare_common",  "compare.json"),
        ("uk_compare_partial", "compare_partial.json"),
    ]:
        path = COMPARE_DIR / json_file
        if not path.exists():
            print(f'  SKIP "{table}" — source file missing: {json_file}')
            continue
        db_cols, rows = load_compare_rows(path, UK_COMPARE_KEY_MAP)
        insert_rows(cur, table, db_cols, rows)

    if status_row is None:
        print('  SKIP "uk_diamond_status" — source file missing')
        return
    insert_rows(cur, "uk_diamond_status", [c[0] for c in UK_STATUS_COLUMNS], [status_row])


# ── Main ─────────────────────────────────────────────────────────────────────
def main() -> None:
    parsed = urlparse(DATABASE_URL)
    target = f"postgresql://{parsed.username}@{parsed.hostname}{parsed.path}"
    print(f"UK compare folder: {COMPARE_DIR}")
    print(f"Target: {target}")
    print("-" * 72)

    if not COMPARE_DIR.exists():
        raise SystemExit(f"compare/uk folder not found: {COMPARE_DIR}")

    # Guard: if ALL main compare files are missing, abort before touching the DB.
    # Without this, the script would truncate all tables to empty and exit 0.
    main_files = ["compare.json", "compare_partial.json"]
    present = [f for f in main_files if (COMPARE_DIR / f).exists()]
    if not present:
        raise SystemExit(
            f"ABORT — no compare JSON files found in {COMPARE_DIR}\n"
            "Run diamond_compare_uk.py first. Database was NOT modified."
        )

    conn = connect()
    try:
        with conn.cursor() as cur:
            upload_all_uk_json_files(cur)
        conn.commit()
    finally:
        conn.close()

    print("-" * 72)
    print("DONE.")


if __name__ == "__main__":
    main()
