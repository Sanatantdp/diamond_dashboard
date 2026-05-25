"""
compare_to_mysql.py
===================

Load every JSON file in ./compare/ into the local MySQL `scrapper` database.

Behaviour
---------
- Creates database `scrapper` if missing.
- Creates 5 tables (drops & rebuilds them on every run so schema stays in sync
  with the JSON).
- Bulk-inserts rows with executemany() in chunks of 1000 for speed.
- Skips compare.csv (it is identical to compare.json).

Tables created
--------------
- compare_common      from compare.json          (~318 rows, 3-vendor matches)
- compare_all         from compare_all.json      (~107K rows, every PC diamond)
- compare_partial     from compare_partial.json  (~92K rows, partial matches)
- compare_br_vs_lg    from compare_br_vs_lg.json (~2.5K rows, brilliance vs LG)
- diamond_status      from diamond_status.json   (1 snapshot row per import)
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import pymysql
from pymysql.cursors import Cursor

# ── Config (env vars override the localhost defaults) ───────────────────────
DB_HOST     = os.environ.get("DB_HOST", "127.0.0.1")
DB_PORT     = int(os.environ.get("DB_PORT", "3306"))
DB_USER     = os.environ.get("DB_USER", "root")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "")
DB_NAME     = os.environ.get("DB_NAME", "scrapper")

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
]


# ── Helpers ──────────────────────────────────────────────────────────────────
def connect(database: str | None = None) -> pymysql.connections.Connection:
    return pymysql.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        database=database,
        charset="utf8mb4",
        autocommit=False,
        local_infile=False,
    )


def ensure_database() -> None:
    with connect() as conn, conn.cursor() as cur:
        cur.execute(
            f"CREATE DATABASE IF NOT EXISTS `{DB_NAME}` "
            f"CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
        )
        conn.commit()
    print(f"  database `{DB_NAME}` ready")


def build_create_sql(table: str, columns: list[tuple[str, str]], indexes: list[str]) -> str:
    cols_sql = ",\n  ".join(f"`{name}` {ctype}" for name, ctype in columns)
    idx_sql = ""
    if indexes:
        idx_sql = ",\n  " + ",\n  ".join(f"INDEX `idx_{i}` (`{i}`)" for i in indexes)
    return (
        f"CREATE TABLE `{table}` (\n"
        f"  `id` BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,\n"
        f"  {cols_sql}"
        f"{idx_sql}\n"
        f") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci"
    )


def recreate_table(cur: Cursor, table: str, columns: list[tuple[str, str]], indexes: list[str]) -> None:
    cur.execute(f"DROP TABLE IF EXISTS `{table}`")
    cur.execute(build_create_sql(table, columns, indexes))
    print(f"  table `{table}` (re)created")


def chunked(seq, n):
    for i in range(0, len(seq), n):
        yield seq[i:i + n]


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


def insert_rows(cur: Cursor, table: str, db_cols: list[str], rows: list[tuple]) -> int:
    if not rows:
        return 0
    cols_sql = ", ".join(f"`{c}`" for c in db_cols)
    placeholders = ", ".join(["%s"] * len(db_cols))
    sql = f"INSERT INTO `{table}` ({cols_sql}) VALUES ({placeholders})"
    inserted = 0
    for batch in chunked(rows, BATCH_SIZE):
        cur.executemany(sql, batch)
        inserted += len(batch)
        print(f"    {inserted:>7,} / {len(rows):,} rows -> `{table}`", end="\r")
    print()
    return inserted


# ── Per-file loaders ─────────────────────────────────────────────────────────
def load_compare_table(cur: Cursor, table: str, json_file: str) -> None:
    path = COMPARE_DIR / json_file
    if not path.exists():
        print(f"  SKIP `{table}` — source file missing: {json_file}")
        return
    recreate_table(cur, table, COMPARE_COLUMNS, indexes=["certificate_number", "shape"])
    db_cols, rows = load_compare_rows(path, COMPARE_KEY_MAP)
    insert_rows(cur, table, db_cols, rows)


def load_br_vs_lg(cur: Cursor) -> None:
    path = COMPARE_DIR / "compare_br_vs_lg.json"
    if not path.exists():
        print("  SKIP `compare_br_vs_lg` — source file missing")
        return
    recreate_table(cur, "compare_br_vs_lg", BR_VS_LG_COLUMNS, indexes=["diamond_id"])
    db_cols, rows = load_compare_rows(path, BR_VS_LG_KEY_MAP)
    insert_rows(cur, "compare_br_vs_lg", db_cols, rows)


def load_diamond_status(cur: Cursor) -> None:
    path = COMPARE_DIR / "diamond_status.json"
    if not path.exists():
        print("  SKIP `diamond_status` — source file missing")
        return
    recreate_table(cur, "diamond_status", STATUS_COLUMNS, indexes=["generated_at"])

    with open(path, encoding="utf-8") as f:
        s = json.load(f)

    vendor_totals = s.get("vendor_totals", {}) or {}
    pc_vs        = s.get("pc_vs_vendors", {}) or {}
    counts       = s.get("dataset_counts", {}) or {}

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
    )
    db_cols = [c[0] for c in STATUS_COLUMNS]
    insert_rows(cur, "diamond_status", db_cols, [row])


# ── Main ─────────────────────────────────────────────────────────────────────
def main() -> None:
    print(f"Compare folder: {COMPARE_DIR}")
    print(f"Target: mysql://{DB_USER}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
    print("-" * 72)

    if not COMPARE_DIR.exists():
        raise SystemExit(f"compare/ folder not found: {COMPARE_DIR}")

    ensure_database()

    with connect(DB_NAME) as conn, conn.cursor() as cur:
        load_compare_table(cur, "compare_common",  "compare.json")
        load_compare_table(cur, "compare_all",     "compare_all.json")
        load_compare_table(cur, "compare_partial", "compare_partial.json")
        load_br_vs_lg(cur)
        load_diamond_status(cur)
        conn.commit()

    print("-" * 72)
    print("DONE.")


if __name__ == "__main__":
    main()
