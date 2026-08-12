"""
rog_compare_to_db.py
====================

Load the Germany/ROG compare JSON files into the Render PostgreSQL database
(target connection read from the DATABASE_URL env var / .env file).

ROG is the German baseline vendor. The source JSON uses "Pound" labels for
price columns (legacy naming); they are stored in DB as *_eur columns and
served back to the dashboard under "EUR" key names.

Tables managed
--------------
- rog_compare_common    from compare.json           (rows matched by all 3 vendors)
  (rog_compare_all is deliberately NOT loaded — see truncate_tables)
- rog_compare_partial   from compare_partial.json    (rows matched by 2+ vendors)
- rog_diamond_status    from diamond_status.json     (1 snapshot row per import)

On first run : creates all 4 tables (CREATE TABLE IF NOT EXISTS).
On every run : TRUNCATES all tables (clears data, keeps schema + indexes),
               then bulk-inserts fresh rows from the JSON files.

Source folder: <repo-root>/rog_scrapping/compare/
               (override with ROG_COMPARE_DIR env var)
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

COMPARE_DIR = Path(
    os.environ.get("ROG_COMPARE_DIR", Path(__file__).resolve().parent / "compare")
)
BATCH_SIZE = 1000


# ── Schema ───────────────────────────────────────────────────────────────────
# JSON source uses "Pound" labels; DB stores them as *_eur (Germany = EUR).
ROG_COMPARE_COLUMNS = [
    ("certificate_number",  "VARCHAR(64)"),
    ("vendors_matched",     "VARCHAR(64)"),
    ("shape",               "VARCHAR(32)"),
    ("color",               "VARCHAR(16)"),
    ("clarity",             "VARCHAR(16)"),
    ("cut",                 "VARCHAR(32)"),
    ("carat_weight",        "DECIMAL(6,3)"),
    ("rog_price_eur",       "DECIMAL(12,2)"),
    ("rog_minus_30_eur",    "DECIMAL(12,2)"),
    ("renesim_price_eur",   "DECIMAL(12,2)"),
    ("renesim_minus_eur",   "DECIMAL(12,2)"),
    ("renesim_vs_rog_eur",  "DECIMAL(12,2)"),
    ("renesim_vs_rog_pct",  "DECIMAL(8,2)"),
    ("novita_price_eur",    "DECIMAL(12,2)"),
    ("novita_minus_eur",    "DECIMAL(12,2)"),
    ("novita_vs_rog_eur",   "DECIMAL(12,2)"),
    ("novita_vs_rog_pct",   "DECIMAL(8,2)"),
    ("d77_price_eur",       "DECIMAL(12,2)"),
    ("d77_minus_30_eur",    "DECIMAL(12,2)"),
    ("d77_vs_rog_eur",      "DECIMAL(12,2)"),
    ("d77_vs_rog_pct",      "DECIMAL(8,2)"),
    ("renesim_link",        "TEXT"),
    ("novita_link",         "TEXT"),
]

# JSON key (as written by diamond_compare.py) → DB column name.
# The JSON uses "Pound" as a price-column label; we map it to *_eur DB names.
ROG_COMPARE_KEY_MAP = {
    "certificate_number":       "certificate_number",
    "vendors_matched":          "vendors_matched",
    "shape":                    "shape",
    "color":                    "color",
    "clarity":                  "clarity",
    "cut":                      "cut",
    "caratWeight":              "carat_weight",
    "ROG Price Pound":          "rog_price_eur",
    "ROG -30% Pound":           "rog_minus_30_eur",
    "renesim Price Pound":      "renesim_price_eur",
    "renesim -0% Pound":        "renesim_minus_eur",
    "renesim vs ROG Pound":     "renesim_vs_rog_eur",
    "renesim vs ROG %":         "renesim_vs_rog_pct",
    "novita Price Pound":       "novita_price_eur",
    "novita -0% Pound":         "novita_minus_eur",
    "novita vs ROG Pound":      "novita_vs_rog_eur",
    "novita vs ROG %":          "novita_vs_rog_pct",
    "77diamonds Price Pound":   "d77_price_eur",
    "77diamonds -30% Pound":    "d77_minus_30_eur",
    "77diamonds vs ROG Pound":  "d77_vs_rog_eur",
    "77diamonds vs ROG %":      "d77_vs_rog_pct",
    "renesim Link":             "renesim_link",
    "novita Link":              "novita_link",
}

ROG_STATUS_COLUMNS = [
    ("generated_at",            "TIMESTAMP"),
    ("rog_total",               "INT"),
    ("renesim_vendor_total",    "INT"),
    ("novita_vendor_total",     "INT"),
    ("d77_vendor_total",        "INT"),
    ("renesim_matched",         "INT"),
    ("renesim_match_rate_pct",  "DECIMAL(6,2)"),
    ("novita_matched",          "INT"),
    ("novita_match_rate_pct",   "DECIMAL(6,2)"),
    ("d77_matched",             "INT"),
    ("d77_match_rate_pct",      "DECIMAL(6,2)"),
    ("common_count",            "INT"),
    ("partial_count",           "INT"),
    ("all_rog_count",           "INT"),
    ("file_dates",              "TEXT"),
]


# ── Helpers ──────────────────────────────────────────────────────────────────
def connect():
    return psycopg2.connect(DATABASE_URL, connect_timeout=30)


def _to_pg_type(t: str) -> str:
    """Translate DATETIME → TIMESTAMP for PostgreSQL compatibility."""
    return "TIMESTAMP" if t.strip().upper() == "DATETIME" else t


def ensure_table(cur: PgCursor, table: str, columns: list[tuple[str, str]],
                 indexes: list[str]) -> None:
    cols_sql = ",\n  ".join(
        f'"{name}" {_to_pg_type(ctype)}' for name, ctype in columns
    )
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
            f'ALTER TABLE "{table}" ADD COLUMN IF NOT EXISTS '
            f'"{name}" {_to_pg_type(ctype)}'
        )
    for col in indexes:
        cur.execute(
            f'CREATE INDEX IF NOT EXISTS "idx_{table}_{col}" '
            f'ON "{table}" ("{col}")'
        )


def truncate_tables(cur: PgCursor) -> None:
    tables = [
        "rog_compare_common",
        "rog_compare_partial",
        "rog_diamond_status",
    ]
    print("Truncating ROG tables…")
    for table in tables:
        cur.execute(f'TRUNCATE TABLE "{table}" RESTART IDENTITY CASCADE')
        print(f'  truncated "{table}"')

    # rog_compare_all is no longer loaded — the Germany dashboard never reads
    # it, and at ~220K rows it was the largest table in the database. The data
    # is not lost: snapshot_history.py archives compare_all.json to Parquet on
    # every run. Dropped rather than truncated so the storage is released.
    cur.execute('DROP TABLE IF EXISTS "rog_compare_all"')
    print('  dropped "rog_compare_all" (unused — served from the Parquet archive)')


def load_rows(json_path: Path, key_map: dict[str, str]) -> tuple[list[str], list[tuple]]:
    with open(json_path, encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, list):
        raise ValueError(f"{json_path.name} is not a JSON array")
    db_cols = list(key_map.values())
    rows = [tuple(item.get(k) for k in key_map) for item in data]
    return db_cols, rows


def insert_rows(cur: PgCursor, table: str, db_cols: list[str],
                rows: list[tuple]) -> int:
    if not rows:
        return 0
    cols_sql = ", ".join(f'"{c}"' for c in db_cols)
    execute_values(
        cur,
        f'INSERT INTO "{table}" ({cols_sql}) VALUES %s',
        rows,
        page_size=BATCH_SIZE,
    )
    print(f'    {len(rows):>8,} rows -> "{table}"')
    return len(rows)


def _parse_rog_status_row(status_path: Path) -> tuple | None:
    """Read diamond_status.json and build the ROG_STATUS_COLUMNS row tuple, or
    None if the file is missing. Shared by the history snapshot (pre-truncate)
    and the live rog_diamond_status insert (post-truncate)."""
    if not status_path.exists():
        return None

    with open(status_path, encoding="utf-8") as f:
        s = json.load(f)

    vendor_totals = s.get("vendor_totals", {}) or {}
    rog_vs        = s.get("rog_vs_vendors", {}) or {}
    dataset       = s.get("dataset_counts", {}) or {}

    ren  = rog_vs.get("renesim", {}) or {}
    nov  = rog_vs.get("novita",  {}) or {}
    d77  = rog_vs.get("77diamonds", {}) or {}

    return (
        s.get("generated_at"),
        s.get("rog_total"),
        vendor_totals.get("renesim"),
        vendor_totals.get("novita"),
        vendor_totals.get("77diamonds"),
        ren.get("matched"),
        ren.get("match_rate_pct"),
        nov.get("matched"),
        nov.get("match_rate_pct"),
        d77.get("matched"),
        d77.get("match_rate_pct"),
        dataset.get("common"),
        dataset.get("partial"),
        dataset.get("all_ROG") or dataset.get("all_rog"),
        json.dumps(s.get("file_dates") or {}),
    )


# ── Main upload logic ─────────────────────────────────────────────────────────
def upload_all(cur: PgCursor) -> dict[str, int]:
    # Step 1 – ensure tables exist
    ensure_table(cur, "rog_compare_common",  ROG_COMPARE_COLUMNS, ["certificate_number", "shape"])
    ensure_table(cur, "rog_compare_partial", ROG_COMPARE_COLUMNS, ["certificate_number", "shape"])
    ensure_table(cur, "rog_diamond_status",  ROG_STATUS_COLUMNS,  ["generated_at"])
    snapshot_history.ensure_status_history_table(cur, "rog_diamond_status_history", ROG_STATUS_COLUMNS)

    # Step 2 – historical snapshot, BEFORE truncate
    status_row = _parse_rog_status_row(COMPARE_DIR / "diamond_status.json")
    snapshot_history.archive_compare_json("germany", COMPARE_DIR)
    if status_row is not None:
        snapshot_history.insert_status_history_row(
            cur, "rog_diamond_status_history", [c[0] for c in ROG_STATUS_COLUMNS], status_row
        )

    # Step 3 – truncate
    truncate_tables(cur)

    # Step 4 – upload compare files
    print("Uploading ROG JSON files…")
    counts: dict[str, int] = {}
    for table, filename in [
        ("rog_compare_common",  "compare.json"),
        ("rog_compare_partial", "compare_partial.json"),
    ]:
        path = COMPARE_DIR / filename
        if not path.exists():
            print(f'  SKIP "{table}" — {filename} not found')
            counts[table] = 0
            continue
        db_cols, rows = load_rows(path, ROG_COMPARE_KEY_MAP)
        counts[table] = insert_rows(cur, table, db_cols, rows)

    # Step 5 – upload diamond_status (live table)
    if status_row is None:
        print('  SKIP "rog_diamond_status" — diamond_status.json not found')
        counts["rog_diamond_status"] = 0
    else:
        status_cols = [c[0] for c in ROG_STATUS_COLUMNS]
        counts["rog_diamond_status"] = insert_rows(
            cur, "rog_diamond_status", status_cols, [status_row]
        )

    # Step 6 – vendor attribute coverage (carat/color/cut/clarity per vendor).
    # Built from the raw vendor CSVs, into the shared cross-origin table;
    # only the origin='germany' rows are replaced.
    counts["vendor_coverage"] = len(
        vendor_coverage.coverage_rows(vendor_coverage.refresh_origin("germany", cur))
    )

    # Daily download volume recorded by the scrapers (see bandwidth.py).
    counts["bandwidth_usage"] = len(bandwidth.refresh_origin("germany", cur))
    return counts


# ── Entry point ───────────────────────────────────────────────────────────────
def main() -> None:
    parsed = urlparse(DATABASE_URL)
    target = f"postgresql://{parsed.username}@{parsed.hostname}{parsed.path}"
    print(f"ROG compare folder : {COMPARE_DIR}")
    print(f"Target             : {target}")
    print("-" * 72)

    if not COMPARE_DIR.exists():
        raise SystemExit(f"compare/ folder not found: {COMPARE_DIR}")

    # Guard: if ALL main compare files are missing, abort before touching the DB.
    # Without this, the script would truncate all tables to empty and exit 0.
    main_files = ["compare.json", "compare_partial.json"]
    present = [f for f in main_files if (COMPARE_DIR / f).exists()]
    if not present:
        raise SystemExit(
            f"ABORT — no compare JSON files found in {COMPARE_DIR}\n"
            "Run diamond_compare.py first. Database was NOT modified."
        )

    conn = connect()
    try:
        with conn.cursor() as cur:
            counts = upload_all(cur)
        conn.commit()
    finally:
        conn.close()

    print("-" * 72)
    total = sum(counts.values())
    print(f"DONE — {total:,} rows uploaded across {len(counts)} tables.")
    for table, n in counts.items():
        print(f"  {table:<30} {n:>8,}")


if __name__ == "__main__":
    main()
