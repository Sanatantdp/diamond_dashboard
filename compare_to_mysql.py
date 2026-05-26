"""
compare_to_mysql.py
===================

Load every JSON file in ./compare/ into the Render PostgreSQL database
(target connection read from the DATABASE_URL env var / .env file).

Behaviour
---------
- Connects to the database specified by DATABASE_URL (Render Postgres).
- Creates 5 tables (drops & rebuilds them on every run so schema stays in sync
  with the JSON).
- Bulk-inserts rows with execute_values() in chunks of 1000 for speed.
- Skips compare.csv (it is identical to compare.json).

Tables created
--------------
- compare_common      from compare.json          (~318 rows, 3-vendor matches)
- compare_all         from compare_all.json      (~107K rows, every PC diamond)
- compare_partial     from compare_partial.json  (~92K rows, partial matches)
- compare_br_vs_lg    from compare_br_vs_lg.json (~2.5K rows, brilliance vs LG)
- diamond_status      from diamond_status.json   (1 snapshot row per import)

Note: filename kept as compare_to_mysql.py for backwards compatibility with
scheduler.py and any external automation that calls it by path.
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

# ── Config (DATABASE_URL from .env or Render env) ────────────────────────────
load_dotenv(Path(__file__).parent / ".env")
DATABASE_URL = os.environ.get("DATABASE_URL", "")
if not DATABASE_URL:
    raise SystemExit(
        "DATABASE_URL env var is required. "
        "Put it in .env locally, or set it in Render's Environment tab."
    )
if "sslmode=" not in DATABASE_URL:
    DATABASE_URL += ("&" if "?" in DATABASE_URL else "?") + "sslmode=require"

COMPARE_DIR = Path(__file__).parent / "compare"

BATCH_SIZE = 1000


# ── Schema ───────────────────────────────────────────────────────────────────
COMPARE_COLUMNS = [
    ("certificate_number",    "VARCHAR(64)"),
    ("vendors_matched",       "VARCHAR(64)"),
    ("shape",                 "VARCHAR(32)"),
    ("color",                 "VARCHAR(16)"),
    ("clarity",               "VARCHAR(16)"),
    ("cut",                   "VARCHAR(32)"),
    ("carat_weight",          "DECIMAL(6,3)"),
    ("lg_price_usd",          "DECIMAL(12,2)"),
    ("lg_minus_30_usd",       "DECIMAL(12,2)"),
    ("lg_vs_pc_usd",          "DECIMAL(12,2)"),
    ("lg_vs_pc_pct",          "DECIMAL(8,2)"),
    ("brilliance_price_usd",  "DECIMAL(12,2)"),
    ("brilliance_minus_30_usd", "DECIMAL(12,2)"),
    ("brilliance_vs_pc_usd",  "DECIMAL(12,2)"),
    ("brilliance_vs_pc_pct",  "DECIMAL(8,2)"),
    ("pc_price_usd",          "DECIMAL(12,2)"),
    ("pc_minus_30_usd",       "DECIMAL(12,2)"),
]

# Map JSON key -> column name for the 3 compare_* tables (same schema)
COMPARE_KEY_MAP = {
    "certificate_number":      "certificate_number",
    "vendors_matched":         "vendors_matched",
    "shape":                   "shape",
    "color":                   "color",
    "clarity":                 "clarity",
    "cut":                     "cut",
    "caratWeight":             "carat_weight",
    "loose-grown Price USD":   "lg_price_usd",
    "loose-grown -30% USD":    "lg_minus_30_usd",
    "loose-grown vs PC USD":   "lg_vs_pc_usd",
    "loose-grown vs PC %":     "lg_vs_pc_pct",
    "brilliance Price USD":    "brilliance_price_usd",
    "brilliance -30% USD":     "brilliance_minus_30_usd",
    "brilliance vs PC USD":    "brilliance_vs_pc_usd",
    "brilliance vs PC %":      "brilliance_vs_pc_pct",
    "PC Price USD":            "pc_price_usd",
    "PC -30% USD":             "pc_minus_30_usd",
}

BR_VS_LG_COLUMNS = [
    ("diamond_id",            "VARCHAR(64)"),
    ("shape",                 "VARCHAR(32)"),
    ("carat_weight",          "DECIMAL(6,3)"),
    ("cut",                   "VARCHAR(32)"),
    ("color",                 "VARCHAR(16)"),
    ("clarity",               "VARCHAR(16)"),
    ("lg_price_usd",          "DECIMAL(12,2)"),
    ("brilliance_price_usd",  "DECIMAL(12,2)"),
    ("lg_minus_30_usd",       "DECIMAL(12,2)"),
    ("brilliance_minus_30_usd", "DECIMAL(12,2)"),
    ("br_minus_30_vs_lg_usd", "DECIMAL(12,2)"),
    ("br_minus_30_vs_lg_pct", "DECIMAL(8,2)"),
]

BR_VS_LG_KEY_MAP = {
    "diamond_id":              "diamond_id",
    "shape":                   "shape",
    "caratWeight":             "carat_weight",
    "cut":                     "cut",
    "color":                   "color",
    "clarity":                 "clarity",
    "loose-grown Price USD":   "lg_price_usd",
    "brilliance Price USD":    "brilliance_price_usd",
    "loose-grown -30% USD":    "lg_minus_30_usd",
    "brilliance -30% USD":     "brilliance_minus_30_usd",
    "BR -30% vs LG USD":       "br_minus_30_vs_lg_usd",
    "BR -30% vs LG %":         "br_minus_30_vs_lg_pct",
}

STATUS_COLUMNS = [
    ("generated_at",           "DATETIME"),
    ("pc_total",               "INT"),
    ("lg_vendor_total",        "INT"),
    ("brilliance_vendor_total", "INT"),
    ("lg_matched",             "INT"),
    ("lg_match_rate_pct",      "DECIMAL(6,2)"),
    ("brilliance_matched",     "INT"),
    ("brilliance_match_rate_pct", "DECIMAL(6,2)"),
    ("common_count",           "INT"),
    ("partial_count",          "INT"),
    ("all_pc_count",           "INT"),
    ("br_vs_lg_count",         "INT"),
    ("pc_file_date",           "DATETIME"),
    ("lg_file_date",           "DATETIME"),
    ("brilliance_file_date",   "DATETIME"),
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


def recreate_table(cur: PgCursor, table: str, columns: list[tuple[str, str]], indexes: list[str]) -> None:
    cur.execute(f'DROP TABLE IF EXISTS "{table}" CASCADE')
    cur.execute(build_create_sql(table, columns))
    for col in indexes:
        cur.execute(f'CREATE INDEX "idx_{table}_{col}" ON "{table}" ("{col}")')
    print(f'  table "{table}" (re)created')


def load_compare_rows(json_path: Path, key_map: dict[str, str]) -> tuple[list[str], list[tuple]]:
    """Read a JSON list-of-dicts and return (db_columns, rows-as-tuples)."""
    with open(json_path, encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, list):
        raise ValueError(f"{json_path.name} is not a JSON list")

    db_cols = list(key_map.values())
    rows = []
    for item in data:
        row = tuple(item.get(json_key) for json_key in key_map.keys())
        rows.append(row)
    return db_cols, rows


def insert_rows(cur: PgCursor, table: str, db_cols: list[str], rows: list[tuple]) -> int:
    if not rows:
        return 0
    cols_sql = ", ".join(f'"{c}"' for c in db_cols)
    sql = f'INSERT INTO "{table}" ({cols_sql}) VALUES %s'
    execute_values(cur, sql, rows, page_size=BATCH_SIZE)
    print(f'    {len(rows):>7,} rows -> "{table}"')
    return len(rows)


# ── Per-file loaders ─────────────────────────────────────────────────────────
def load_compare_table(cur: PgCursor, table: str, json_file: str) -> None:
    path = COMPARE_DIR / json_file
    if not path.exists():
        print(f'  SKIP "{table}" — source file missing: {json_file}')
        return
    recreate_table(cur, table, COMPARE_COLUMNS, indexes=["certificate_number", "shape"])
    db_cols, rows = load_compare_rows(path, COMPARE_KEY_MAP)
    insert_rows(cur, table, db_cols, rows)


def load_br_vs_lg(cur: PgCursor) -> None:
    path = COMPARE_DIR / "compare_br_vs_lg.json"
    if not path.exists():
        print('  SKIP "compare_br_vs_lg" — source file missing')
        return
    recreate_table(cur, "compare_br_vs_lg", BR_VS_LG_COLUMNS, indexes=["diamond_id"])
    db_cols, rows = load_compare_rows(path, BR_VS_LG_KEY_MAP)
    insert_rows(cur, "compare_br_vs_lg", db_cols, rows)


def load_diamond_status(cur: PgCursor) -> None:
    path = COMPARE_DIR / "diamond_status.json"
    if not path.exists():
        print('  SKIP "diamond_status" — source file missing')
        return
    recreate_table(cur, "diamond_status", STATUS_COLUMNS, indexes=["generated_at"])

    with open(path, encoding="utf-8") as f:
        s = json.load(f)

    vendor_totals = s.get("vendor_totals", {}) or {}
    pc_vs        = s.get("pc_vs_vendors", {}) or {}
    counts       = s.get("dataset_counts", {}) or {}
    file_dates   = s.get("file_dates", {}) or {}

    lg_stats = pc_vs.get("loose-grown", {}) or {}
    br_stats = pc_vs.get("brilliance", {}) or {}

    row = (
        s.get("generated_at"),
        s.get("pc_total"),
        vendor_totals.get("loose-grown"),
        vendor_totals.get("brilliance"),
        lg_stats.get("matched"),
        lg_stats.get("match_rate_pct"),
        br_stats.get("matched"),
        br_stats.get("match_rate_pct"),
        counts.get("common"),
        counts.get("partial"),
        counts.get("all_pc"),
        counts.get("br_vs_lg"),
        file_dates.get("pc"),
        file_dates.get("loose-grown"),
        file_dates.get("brilliance"),
    )
    db_cols = [c[0] for c in STATUS_COLUMNS]
    insert_rows(cur, "diamond_status", db_cols, [row])


# ── Main ─────────────────────────────────────────────────────────────────────
def main() -> None:
    parsed = urlparse(DATABASE_URL)
    target = f"postgresql://{parsed.username}@{parsed.hostname}{parsed.path}"
    print(f"Compare folder: {COMPARE_DIR}")
    print(f"Target: {target}")
    print("-" * 72)

    if not COMPARE_DIR.exists():
        raise SystemExit(f"compare/ folder not found: {COMPARE_DIR}")

    conn = connect()
    try:
        with conn.cursor() as cur:
            load_compare_table(cur, "compare_common",  "compare.json")
            load_compare_table(cur, "compare_all",     "compare_all.json")
            load_compare_table(cur, "compare_partial", "compare_partial.json")
            load_br_vs_lg(cur)
            load_diamond_status(cur)
        conn.commit()
    finally:
        conn.close()

    print("-" * 72)
    print("DONE.")


if __name__ == "__main__":
    main()
