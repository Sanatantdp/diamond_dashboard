"""
snapshot_history.py
====================

Shared historical-snapshot utility used by all 4 regional loaders
(usa_compare/compare_to_mysql.py, uk_scrapping/uk_compare_to_db.py,
aus_scrapping/roa_compare_to_db.py, germany_scrapping/germany_compare_to_db.py).

Every loader TRUNCATEs its Postgres tables and reloads fresh rows on each
weekly run, which means the prior week's data is gone with no way to look
back. This module is called by each loader *before* it truncates anything,
so nothing is lost:

1. Row-level archive — dumps compare.json / compare_all.json /
   compare_partial.json (the files about to be truncated+reloaded) to dated
   Parquet files under history/<region>/<table>/<table>_<YYYY-MM-DD>.parquet.
   compare.json (compare_common) is small and kept forever; compare_all.json
   and compare_partial.json are large and thinned by _prune_old_snapshots.

2. Status trend — appends (never truncates) this run's status row into a
   "<region>_diamond_status_history" Postgres table, tagged with
   snapshot_date, so match-rate / vendor-total / dataset-count trends can be
   queried directly without touching the Parquet archive.

Re-running the pipeline twice on the same day is a no-op for the Parquet
archive (one file per calendar day) but appends another row to the status
history table (each run is a distinct data point).
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pandas as pd
from psycopg2.extensions import cursor as PgCursor

REPO_ROOT = Path(__file__).resolve().parent
HISTORY_DIR = REPO_ROOT / "history"

# Full weekly snapshots are kept as-is for this many days; beyond that, only
# the first snapshot of each calendar month survives for "large" tables.
KEEP_WEEKLY_DAYS = 84  # ~12 weeks

# filename -> is_large (large tables get thinned by retention; small ones
# (compare_common) are cheap enough to keep in full forever).
ARCHIVE_FILES: dict[str, bool] = {
    "compare.json":         False,
    "compare_all.json":     True,
    "compare_partial.json": True,
}


def _snapshot_date() -> str:
    return date.today().isoformat()


def _to_pg_type(t: str) -> str:
    return "TIMESTAMP" if t.strip().upper() == "DATETIME" else t


# ── Tier 2: row-level Parquet archive ────────────────────────────────────────
def archive_compare_json(region: str, compare_dir: Path) -> None:
    """Dump each configured compare_*.json file to a dated Parquet file under
    history/<region>/<table>/ before the loader truncates+reloads its tables."""
    snap = _snapshot_date()
    for filename in ARCHIVE_FILES:
        src = compare_dir / filename
        if not src.exists():
            continue
        table_name = filename[: -len(".json")]
        out_dir = HISTORY_DIR / region / table_name
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"{table_name}_{snap}.parquet"
        if out_path.exists():
            continue  # already archived today

        with open(src, encoding="utf-8") as f:
            data = json.load(f)
        if not data:
            continue

        df = pd.DataFrame(data)
        df.to_parquet(out_path, index=False)
        print(
            f'  [history] {region}/{table_name}: archived {len(df):,} rows '
            f'-> {out_path.relative_to(REPO_ROOT)}'
        )

    _prune_old_snapshots(region)


def _prune_old_snapshots(region: str) -> None:
    """Thin "large" tables' Parquet history: files newer than KEEP_WEEKLY_DAYS
    are always kept; older files are reduced to one (the earliest) per
    calendar month."""
    cutoff_ordinal = date.today().toordinal() - KEEP_WEEKLY_DAYS
    for filename, is_large in ARCHIVE_FILES.items():
        if not is_large:
            continue
        table_name = filename[: -len(".json")]
        table_dir = HISTORY_DIR / region / table_name
        if not table_dir.exists():
            continue

        by_month: dict[str, list[tuple[date, Path]]] = {}
        for f in sorted(table_dir.glob(f"{table_name}_*.parquet")):
            date_str = f.stem[len(table_name) + 1 :]
            try:
                d = date.fromisoformat(date_str)
            except ValueError:
                continue
            if d.toordinal() >= cutoff_ordinal:
                continue  # inside the rolling weekly window — always keep
            by_month.setdefault(f"{d.year}-{d.month:02d}", []).append((d, f))

        for entries in by_month.values():
            entries.sort(key=lambda pair: pair[0])
            for _d, f in entries[1:]:  # keep only the earliest snapshot/month
                f.unlink(missing_ok=True)


# ── Tier 1: append-only status-history table ─────────────────────────────────
def ensure_status_history_table(
    cur: PgCursor, history_table: str, status_columns: list[tuple[str, str]]
) -> None:
    """CREATE TABLE IF NOT EXISTS for an append-only history table with the
    same columns as the region's live status table, plus snapshot_date."""
    cols_sql = ",\n  ".join(
        f'"{name}" {_to_pg_type(ctype)}' for name, ctype in status_columns
    )
    cur.execute(
        f'CREATE TABLE IF NOT EXISTS "{history_table}" (\n'
        f'  "id" BIGSERIAL PRIMARY KEY,\n'
        f'  "snapshot_date" DATE NOT NULL,\n'
        f'  {cols_sql}\n'
        f")"
    )
    for name, ctype in status_columns:
        cur.execute(
            f'ALTER TABLE "{history_table}" ADD COLUMN IF NOT EXISTS '
            f'"{name}" {_to_pg_type(ctype)}'
        )
    cur.execute(
        f'CREATE INDEX IF NOT EXISTS "idx_{history_table}_snapshot_date" '
        f'ON "{history_table}" ("snapshot_date")'
    )


def insert_status_history_row(
    cur: PgCursor,
    history_table: str,
    status_column_names: list[str],
    status_row: tuple,
) -> None:
    """Append (never truncate/upsert) one row into the status-history table —
    each pipeline run is a distinct data point."""
    cols_sql = ", ".join(f'"{c}"' for c in (["snapshot_date"] + status_column_names))
    placeholders = ", ".join(["%s"] * (len(status_column_names) + 1))
    cur.execute(
        f'INSERT INTO "{history_table}" ({cols_sql}) VALUES ({placeholders})',
        (_snapshot_date(), *status_row),
    )
    print(f'  [history] appended snapshot -> "{history_table}"')
