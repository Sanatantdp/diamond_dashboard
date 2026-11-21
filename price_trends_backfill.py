"""
price_trends_backfill.py
========================

Build / refresh the `price_trends` table in PostgreSQL (Neon) from the local
Parquet snapshot archive (history/<region>/compare/*.parquet).

Each `compare_common` snapshot holds one row per diamond matched across all of
a region's vendors, with a `<vendor> Price <currency>` column per vendor. For
every snapshot date we compute, per vendor: average price, median price, the
number of priced diamonds, average carat, and price-per-carat. Those rows feed
the "Price Trends" charts on the dashboards (which read them back from Neon, so
the charts work on serverless/Vercel where the Parquet files are not present).

Usage:
    python price_trends_backfill.py            # backfill all regions

The same aggregation (`aggregate_snapshot`) is reused by snapshot_history.py so
future weekly runs keep the table current without re-reading Parquet.
"""

from __future__ import annotations

import os
import re
import glob
from pathlib import Path

import pandas as pd
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")

# Region -> (parquet glob of the kept-forever compare_common snapshots, currency)
REGIONS = {
    "usa":     (BASE_DIR / "history" / "usa" / "compare",     "USD"),
    "uk":      (BASE_DIR / "history" / "uk" / "compare",      "GBP"),
    "germany": (BASE_DIR / "history" / "germany" / "compare", "EUR"),
}

PRICE_COL_RE = re.compile(r"^(.+) Price (\S+)$")
DATE_RE = re.compile(r"(\d{4}-\d{2}-\d{2})")

DDL = """
CREATE TABLE IF NOT EXISTS price_trends (
    region          TEXT    NOT NULL,
    snapshot_date   DATE    NOT NULL,
    vendor          TEXT    NOT NULL,
    currency        TEXT    NOT NULL,
    avg_price       NUMERIC,
    median_price    NUMERIC,
    diamond_count   INTEGER,
    avg_carat       NUMERIC,
    price_per_carat NUMERIC,
    PRIMARY KEY (region, snapshot_date, vendor)
);
"""

UPSERT = """
INSERT INTO price_trends
    (region, snapshot_date, vendor, currency,
     avg_price, median_price, diamond_count, avg_carat, price_per_carat)
VALUES %s
ON CONFLICT (region, snapshot_date, vendor) DO UPDATE SET
    currency        = EXCLUDED.currency,
    avg_price       = EXCLUDED.avg_price,
    median_price    = EXCLUDED.median_price,
    diamond_count   = EXCLUDED.diamond_count,
    avg_carat       = EXCLUDED.avg_carat,
    price_per_carat = EXCLUDED.price_per_carat;
"""


def _num(x):
    return None if x is None or pd.isna(x) else round(float(x), 2)


def aggregate_snapshot(df: pd.DataFrame, region: str, snapshot_date: str,
                       currency: str) -> list[tuple]:
    """Per-vendor price aggregates for one snapshot dataframe. Returns rows
    ready for the UPSERT above."""
    carat = pd.to_numeric(df["caratWeight"], errors="coerce") if "caratWeight" in df.columns else None
    rows: list[tuple] = []
    for col in df.columns:
        m = PRICE_COL_RE.match(col)
        if not m:
            continue
        vendor = m.group(1).strip()
        price = pd.to_numeric(df[col], errors="coerce")
        price = price[price > 0]
        if price.empty:
            continue
        avg_price = price.mean()
        median_price = price.median()
        count = int(price.notna().sum())
        avg_carat = None
        ppc = None
        if carat is not None:
            c = carat[price.index]
            c = c[c > 0]
            if not c.empty:
                avg_carat = c.mean()
                # price-per-carat computed per-row then averaged (size-normalised)
                ppc = (price.loc[c.index] / c).mean()
        rows.append((
            region, snapshot_date, vendor, currency,
            _num(avg_price), _num(median_price), count, _num(avg_carat), _num(ppc),
        ))
    return rows


def main() -> None:
    dsn = os.environ.get("DATABASE_URL", "")
    if not dsn:
        raise SystemExit("DATABASE_URL not set (put it in .env)")
    if "sslmode=" not in dsn:
        dsn += ("&" if "?" in dsn else "?") + "sslmode=require"

    conn = psycopg2.connect(dsn)
    conn.autocommit = True
    total = 0
    with conn.cursor() as cur:
        cur.execute(DDL)
        for region, (folder, currency) in REGIONS.items():
            files = sorted(glob.glob(str(folder / "*.parquet")))
            if not files:
                print(f"[{region}] no parquet snapshots found in {folder}")
                continue
            region_rows = 0
            for f in files:
                md = DATE_RE.search(os.path.basename(f))
                if not md:
                    print(f"  skip (no date): {f}")
                    continue
                snapshot_date = md.group(1)
                df = pd.read_parquet(f)
                rows = aggregate_snapshot(df, region, snapshot_date, currency)
                if rows:
                    psycopg2.extras.execute_values(cur, UPSERT, rows)
                    region_rows += len(rows)
                vendors = ", ".join(sorted({r[2] for r in rows}))
                print(f"  [{region}] {snapshot_date}: {len(rows)} vendors ({vendors})")
            print(f"[{region}] upserted {region_rows} vendor-rows across {len(files)} snapshots")
            total += region_rows
    conn.close()
    print(f"\nDone. {total} price_trends rows written.")


if __name__ == "__main__":
    main()
