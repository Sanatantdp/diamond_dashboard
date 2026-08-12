"""
roa_compare_to_db.py
====================

Load the AUS/ROA compare JSON files into the Render PostgreSQL database
(target connection read from the DATABASE_URL env var / .env file).

This is the ROA counterpart of compare_to_mysql.py (USA) and
uk_compare_to_db.py (UK). The ROA pipeline uses ROA as the baseline vendor
and compares two vendors against it: Cullen Jewellery and GS Diamonds.
ROA, GS, and Cullen prices are all AUD. DB columns keep their historical
"_usd" names regardless (no schema change) — see ROA_COMPARE_KEY_MAP below.

Tables managed
--------------
- roa_compare_common          from compare.json               (rows matched by all 3)
  (roa_compare_all is deliberately NOT loaded — see truncate_all_roa_tables)
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
import sys
from pathlib import Path
from urllib.parse import urlparse

import psycopg2
from dotenv import load_dotenv
from psycopg2.extensions import cursor as PgCursor
from psycopg2.extras import execute_values

# ── Config ───────────────────────────────────────────────────────────────────
REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))
import snapshot_history  # noqa: E402  (needs REPO_ROOT on sys.path first)
import vendor_coverage   # noqa: E402
import bandwidth         # noqa: E402

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
# cullen = Cullen Jewellery, gs = GS Diamonds, roa = ROA baseline. All AUD.
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
    ("cullen_link",         "TEXT"),
    ("gs_link",             "TEXT"),
]

# JSON key (written by diamond_compare_roa.py) -> DB column name
# NOTE: JSON keys now carry each vendor's real currency — ROA/GS/Cullen are
# all AUD (see diamond_compare.py's _price_col). The DB column names on the
# right are left as "_usd" for compatibility — no DB schema change — even
# though every value underneath is AUD, not USD.
ROA_COMPARE_KEY_MAP = {
    "certificate_number":  "certificate_number",
    "vendors_matched":     "vendors_matched",
    "shape":               "shape",
    "color":               "color",
    "clarity":             "clarity",
    "cut":                 "cut",
    "caratWeight":         "carat_weight",
    "ROA Price AUD":       "roa_price_usd",
    "ROA -30% AUD":        "roa_minus_30_usd",
    "cullen Price AUD":    "cullen_price_usd",
    "cullen vs ROA AUD":   "cullen_vs_roa_usd",
    "cullen vs ROA %":     "cullen_vs_roa_pct",
    "gs Price AUD":        "gs_price_usd",
    "gs vs ROA AUD":       "gs_vs_roa_usd",
    "gs vs ROA %":         "gs_vs_roa_pct",
    "cullen Link":         "cullen_link",
    "gs Link":             "gs_link",
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
    "cullen Price AUD":    "cullen_price_usd",
    "gs Price AUD":        "gs_price_usd",
    "ROA Price AUD":       "roa_price_usd",
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
    ("file_dates",            "TEXT"),
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
    # Table may already exist from a previous run with fewer columns —
    # add any new ones so older deployments pick them up automatically.
    for name, ctype in columns:
        cur.execute(
            f'ALTER TABLE "{table}" ADD COLUMN IF NOT EXISTS "{name}" {ctype}'
        )
    for col in indexes:
        cur.execute(
            f'CREATE INDEX IF NOT EXISTS "idx_{table}_{col}" ON "{table}" ("{col}")'
        )


def truncate_all_roa_tables(cur: PgCursor) -> None:
    tables = [
        "roa_compare_common",
        "roa_compare_partial",
        "roa_compare_vendor_overlap",
        "roa_diamond_status",
    ]
    print("Truncating ROA tables…")
    for table in tables:
        cur.execute(f'TRUNCATE TABLE "{table}" RESTART IDENTITY CASCADE')
        print(f'  truncated "{table}"')

    # roa_compare_all is no longer loaded — the AUS dashboard never reads it,
    # and it was the largest table in the database. The data is not lost:
    # snapshot_history.py archives compare_all.json to Parquet on every run.
    # Dropped rather than truncated so the storage is actually released.
    cur.execute('DROP TABLE IF EXISTS "roa_compare_all"')
    print('  dropped "roa_compare_all" (unused — served from the Parquet archive)')


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


def _parse_roa_status_row(status_path: Path) -> tuple | None:
    """Read diamond_status.json and build the ROA_STATUS_COLUMNS row tuple, or
    None if the file is missing. Shared by the history snapshot (pre-truncate)
    and the live roa_diamond_status insert (post-truncate)."""
    if not status_path.exists():
        return None

    with open(status_path, encoding="utf-8") as f:
        s = json.load(f)

    vendor_totals   = s.get("vendor_totals", {}) or {}
    roa_vs_vendors  = s.get("ROA_vs_vendors", {}) or {}
    counts          = s.get("dataset_counts", {}) or {}
    cullen_stats    = roa_vs_vendors.get("cullen", {}) or {}
    gs_stats        = roa_vs_vendors.get("gs", {}) or {}

    return (
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
        json.dumps(s.get("file_dates") or {}),
    )


# ── Upload ───────────────────────────────────────────────────────────────────
def upload_all_roa_json_files(cur: PgCursor) -> None:
    # Step 1 — create tables on first run (no-op thereafter)
    ensure_table_exists(cur, "roa_compare_common",         ROA_COMPARE_COLUMNS, ["certificate_number", "shape"])
    ensure_table_exists(cur, "roa_compare_partial",        ROA_COMPARE_COLUMNS, ["certificate_number", "shape"])
    ensure_table_exists(cur, "roa_compare_vendor_overlap", ROA_OVERLAP_COLUMNS, ["certificate_number"])
    ensure_table_exists(cur, "roa_diamond_status",         ROA_STATUS_COLUMNS,  ["generated_at"])
    snapshot_history.ensure_status_history_table(cur, "roa_diamond_status_history", ROA_STATUS_COLUMNS)

    # Step 2 — historical snapshot, BEFORE truncate
    status_row = _parse_roa_status_row(COMPARE_DIR / "diamond_status.json")
    snapshot_history.archive_compare_json("aus", COMPARE_DIR)
    if status_row is not None:
        snapshot_history.insert_status_history_row(
            cur, "roa_diamond_status_history", [c[0] for c in ROA_STATUS_COLUMNS], status_row
        )

    # Step 3 — truncate all ROA tables
    truncate_all_roa_tables(cur)

    # Step 4 — upload compare JSON files
    print("Uploading ROA JSON files…")
    for table, json_file in [
        ("roa_compare_common",  "compare.json"),
        ("roa_compare_partial", "compare_partial.json"),
    ]:
        path = COMPARE_DIR / json_file
        if not path.exists():
            print(f'  SKIP "{table}" — file missing: {json_file}')
            continue
        db_cols, rows = load_compare_rows(path, ROA_COMPARE_KEY_MAP)
        insert_rows(cur, table, db_cols, rows)

    # Step 5 — vendor overlap
    overlap_path = COMPARE_DIR / "compare_vendor_overlap.json"
    if overlap_path.exists():
        db_cols, rows = load_compare_rows(overlap_path, ROA_OVERLAP_KEY_MAP)
        insert_rows(cur, "roa_compare_vendor_overlap", db_cols, rows)
    else:
        print('  SKIP "roa_compare_vendor_overlap" — file missing')

    # Step 6 — status snapshot (live table)
    if status_row is None:
        print('  SKIP "roa_diamond_status" — file missing')
    else:
        insert_rows(cur, "roa_diamond_status", [c[0] for c in ROA_STATUS_COLUMNS], [status_row])

    # Step 7 — vendor attribute coverage (carat/color/cut/clarity per vendor).
    # Built from the raw vendor CSVs, into the shared cross-origin table;
    # only the origin='aus' rows are replaced.
    vendor_coverage.refresh_origin("aus", cur)

    # Daily download volume recorded by the scrapers (see bandwidth.py).
    # Only the days still held locally are replaced, so the table keeps
    # history after the local files are pruned.
    bandwidth.refresh_origin('aus', cur)


# ── Main ─────────────────────────────────────────────────────────────────────
def main() -> None:
    parsed = urlparse(DATABASE_URL)
    target = f"postgresql://{parsed.username}@{parsed.hostname}{parsed.path}"
    print(f"ROA compare folder : {COMPARE_DIR}")
    print(f"Target             : {target}")
    print("-" * 72)

    if not COMPARE_DIR.exists():
        raise SystemExit(f"compare/roa folder not found: {COMPARE_DIR}")

    # Guard: if ALL main compare files are missing, abort before touching the DB.
    # Without this, the script would truncate all tables to empty and exit 0.
    main_files = ["compare.json", "compare_partial.json"]
    present = [f for f in main_files if (COMPARE_DIR / f).exists()]
    if not present:
        raise SystemExit(
            f"ABORT — no compare JSON files found in {COMPARE_DIR}\n"
            "Run roa_compare_to_db first. Database was NOT modified."
        )

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
