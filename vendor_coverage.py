"""
vendor_coverage.py
==================

Per-origin "who actually has what data" summary.

Every origin (usa / uk / aus / germany) scrapes several vendors, and each
vendor publishes a different subset of the four grading attributes — and
publishes them in its own dialect. Some examples this module has to cope with:

  * loosegrowndiamond ships colour/clarity/cut as **numeric codes** (10 = D,
    11 = FL, 4 = Ideal) with no legend anywhere in its API.
  * friendlydiamonds has both `cut` ("EX") and `cut_grade` ("excellent").
  * renesim grades in German ("Sehr gut", "Exzellent").
  * gs_diamonds has a `cut` column that is empty for every single row.
  * novita mixes fancy colours ("Fancy Vivid Blue") into the colour column.

So "which vendor has which type of data" is a real question with a
non-obvious answer. This module answers it: for each vendor it reports, per
attribute, how many rows actually carry a usable value and how those values
are distributed across normalised buckets.

Output
------
1. One JSON file per origin — <compare_dir>/vendor_coverage.json — holding
   every vendor for that origin.
2. One shared Postgres table, "vendor_coverage", in tidy/long form (one row
   per origin × vendor × attribute × bucket). Each origin's loader replaces
   only its own rows, so the four origins coexist in one table.

Usage
-----
    python vendor_coverage.py                 # all origins -> JSON only
    python vendor_coverage.py usa uk          # named origins -> JSON only
    python vendor_coverage.py --upload usa    # JSON + Postgres upload
"""

from __future__ import annotations

import json
import os
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Iterable

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent

# Chunk size for streaming the big vendor CSVs (loosegrowndiamond.csv is
# ~415 MB / 1.2M rows — never read one of these whole).
CHUNK_ROWS = 200_000


# ══════════════════════════════════════════════════════════════════════════════
# Attribute vocabularies
# ══════════════════════════════════════════════════════════════════════════════

# Ordered best -> worst, matching the canonical orders in main.py so the
# dashboard filters and this summary speak the same language.
# The full GIA D-to-Z scale, not just the D-K range the compare tables filter
# on: renesim lists stones all the way down to Y/Z (the faint-yellow end).
COLOR_ORDER   = [chr(c) for c in range(ord("D"), ord("Z") + 1)] + ["FANCY"]
CLARITY_ORDER = ["FL", "IF", "VVS1", "VVS2", "VS1", "VS2", "SI1", "SI2", "I1", "I2", "I3"]
CUT_ORDER     = ["SUPER IDEAL", "IDEAL", "EXCELLENT", "VERY GOOD", "GOOD", "FAIR", "POOR"]

MISSING = "(missing)"   # column absent, empty, or an explicit "no grade" marker
UNKNOWN = "(unknown)"   # a value we could not map into the vocabulary above

# Values vendors use to mean "we did not grade this".
_NULLISH = {"", "-", "--", "n/a", "na", "n.a.", "none", "null", "nan", "0", "unknown"}

# loosegrowndiamond's numeric codes. Not documented anywhere by the vendor —
# derived by joining ~200K loosegrown rows to precious_carbon and ~611K to
# brilliance on certificate number; both vendors agreed on colour and clarity
# essentially 1:1 (>99.99%). Cut was taken from the precious_carbon join only:
# brilliance grades on its own scale with an extra "Super Ideal" tier, so it
# disagrees by construction, while precious_carbon maps cleanly 1->Good,
# 2->Very Good, 3->Excellent, 4->Ideal.
#
# Both scales are contiguous ranks counting down from the best grade, so the
# thin tails (colour 1-2, clarity 1-2 — together ~0.02% of rows, too rare to
# have surfaced in the joins) are extrapolated from that pattern rather than
# verified. They are marked below.
LG_COLOR_CODES = {
    "1": "M", "2": "L",                       # extrapolated
    "3": "K", "4": "J", "5": "I", "6": "H",   # verified
    "7": "G", "8": "F", "9": "E", "10": "D",  # verified
}
LG_CLARITY_CODES = {
    "1": "I3", "2": "I2", "3": "I1",          # extrapolated
    "4": "SI2", "5": "SI1", "6": "VS2", "7": "VS1",       # verified
    "8": "VVS2", "9": "VVS1", "10": "IF", "11": "FL",     # verified
}
LG_CUT_CODES = {
    "1": "GOOD", "2": "VERY GOOD", "3": "EXCELLENT", "4": "IDEAL",
}

# Abbreviations and non-English grades seen across vendors (friendly: EX/VG/GD,
# abelini: EX/F/GD/VG, cullen: ex, renesim: German).
CUT_ALIASES = {
    "ID": "IDEAL", "IDEAL": "IDEAL", "SUPER IDEAL": "SUPER IDEAL", "SUPERIDEAL": "SUPER IDEAL",
    "EX": "EXCELLENT", "EXCELLENT": "EXCELLENT", "EXZELLENT": "EXCELLENT",
    "VG": "VERY GOOD", "VERY GOOD": "VERY GOOD", "SEHR GUT": "VERY GOOD",
    "GD": "GOOD", "G": "GOOD", "GOOD": "GOOD", "GUT": "GOOD",
    "F": "FAIR", "FR": "FAIR", "FAIR": "FAIR",
    "P": "POOR", "PR": "POOR", "POOR": "POOR",
}

CARAT_BUCKETS: list[tuple[str, float, float | None]] = [
    ("< 0.50",      0.0,  0.50),
    ("0.50 - 0.69", 0.50, 0.70),
    ("0.70 - 0.89", 0.70, 0.90),
    ("0.90 - 0.99", 0.90, 1.00),
    ("1.00 - 1.49", 1.00, 1.50),
    ("1.50 - 1.99", 1.50, 2.00),
    ("2.00 - 2.99", 2.00, 3.00),
    ("3.00 - 3.99", 3.00, 4.00),
    ("4.00 - 4.99", 4.00, 5.00),
    ("5.00+",       5.00, None),
]


# ══════════════════════════════════════════════════════════════════════════════
# Vendor registry
# ══════════════════════════════════════════════════════════════════════════════
# Column names are resolved from candidate lists (first match in the CSV header
# wins) rather than hardcoded per vendor, so a scraper renaming a column does
# not silently zero out a vendor's coverage. Order matters: friendlydiamond has
# both "cut" ("EX") and "cut_grade" ("excellent") and we want the readable one.
COLUMN_CANDIDATES: dict[str, list[str]] = {
    "carat":   ["caratWeight", "carat_weight", "carat", "weight"],
    "color":   ["color", "colour"],
    "clarity": ["clarity"],
    "cut":     ["cut_grade", "cut"],
}

# codes="loosegrown" tells the normaliser this vendor uses numeric codes.
ORIGINS: dict[str, dict] = {
    "usa": {
        "diamond_dir": REPO_ROOT / "diamond_files" / "usa",
        "compare_dir": REPO_ROOT / "compare" / "usa",
        "vendors": [
            {"name": "loose-grown", "csv": "loosegrowndiamond.csv", "codes": "loosegrown"},
            {"name": "brilliance",  "csv": "brilliance_diamonds.csv"},
            {"name": "friendly",    "csv": "friendlydiamond.csv"},
            {"name": "PC",          "csv": "precious_carbon.csv"},
        ],
    },
    "uk": {
        "diamond_dir": REPO_ROOT / "diamond_files" / "uk",
        "compare_dir": REPO_ROOT / "compare" / "uk",
        "vendors": [
            {"name": "abelini",         "csv": "abelini.csv"},
            {"name": "diamond-heaven",  "csv": "diamond_heaven.csv"},
            {"name": "ROUK",            "csv": "rouk_diamonds.csv"},
        ],
    },
    "aus": {
        "diamond_dir": REPO_ROOT / "diamond_files" / "aus",
        "compare_dir": REPO_ROOT / "compare" / "roa",
        "vendors": [
            {"name": "cullen", "csv": "cullen_diamonds.csv"},
            {"name": "gs",     "csv": "gs_diamonds.csv"},
            {"name": "ROA",    "csv": "roa_diamonds.csv"},
        ],
    },
    "germany": {
        "diamond_dir": REPO_ROOT / "diamond_files" / "germany",
        "compare_dir": REPO_ROOT / "germany_scrapping" / "compare",
        "vendors": [
            {"name": "novita",     "csv": "novita_diamonds.csv"},
            {"name": "renesim",    "csv": "renesim_diamonds.csv"},
            {"name": "77diamonds", "csv": "77diamonds.csv"},
            {"name": "ROG",        "csv": "rog_diamonds.csv"},
        ],
    },
}

ATTRIBUTES = ["carat", "color", "cut", "clarity"]


# ══════════════════════════════════════════════════════════════════════════════
# Normalisation
# ══════════════════════════════════════════════════════════════════════════════

def _clean(raw) -> str:
    if raw is None:
        return ""
    s = str(raw).strip()
    return "" if s.lower() in _NULLISH else s


def norm_color(raw, codes: str | None = None) -> str:
    s = _clean(raw)
    if not s:
        return MISSING
    if codes == "loosegrown":
        return LG_COLOR_CODES.get(s, UNKNOWN)
    u = s.upper()
    # novita mixes "Fancy Vivid Blue" etc. into the same column — one bucket is
    # enough here; the point is that the vendor has *a* colour grade.
    if u.startswith("FANCY"):
        return "FANCY"
    return u if u in COLOR_ORDER else UNKNOWN


def norm_clarity(raw, codes: str | None = None) -> str:
    s = _clean(raw)
    if not s:
        return MISSING
    if codes == "loosegrown":
        return LG_CLARITY_CODES.get(s, UNKNOWN)
    u = s.upper().replace(" ", "")
    return u if u in CLARITY_ORDER else UNKNOWN


def norm_cut(raw, codes: str | None = None) -> str:
    s = _clean(raw)
    if not s:
        return MISSING
    if codes == "loosegrown":
        return LG_CUT_CODES.get(s, UNKNOWN)
    u = " ".join(s.upper().split())
    if u in CUT_ORDER:
        return u
    return CUT_ALIASES.get(u, UNKNOWN)


def norm_carat(raw) -> str:
    s = _clean(raw)
    if not s:
        return MISSING
    try:
        ct = float(s)
    except ValueError:
        return UNKNOWN
    if ct <= 0:
        return MISSING
    for label, lo, hi in CARAT_BUCKETS:
        if ct >= lo and (hi is None or ct < hi):
            return label
    return UNKNOWN


NORMALISERS = {
    "carat":   lambda v, codes: norm_carat(v),
    "color":   norm_color,
    "cut":     norm_cut,
    "clarity": norm_clarity,
}


def _bucket_order(attribute: str, bucket: str) -> int:
    """Sort key so buckets come back in grading order, not alphabetically.
    MISSING/UNKNOWN always sort last."""
    order = {
        "carat":   [b[0] for b in CARAT_BUCKETS],
        "color":   COLOR_ORDER,
        "cut":     CUT_ORDER,
        "clarity": CLARITY_ORDER,
    }[attribute]
    if bucket in order:
        return order.index(bucket)
    return 900 if bucket == UNKNOWN else 901   # UNKNOWN, then MISSING


# ══════════════════════════════════════════════════════════════════════════════
# Counting
# ══════════════════════════════════════════════════════════════════════════════

def _resolve_columns(csv_path: Path) -> dict[str, str | None]:
    """Map attribute -> the real column name in this CSV (None if absent)."""
    header = pd.read_csv(csv_path, nrows=0)
    by_lower = {c.lower().strip(): c for c in header.columns}
    resolved: dict[str, str | None] = {}
    for attr, candidates in COLUMN_CANDIDATES.items():
        resolved[attr] = next(
            (by_lower[c.lower()] for c in candidates if c.lower() in by_lower), None
        )
    return resolved


def summarise_vendor(csv_path: Path, vendor: str, codes: str | None = None) -> dict:
    """Stream one vendor CSV and count normalised buckets per attribute."""
    resolved = _resolve_columns(csv_path)
    used_cols = [c for c in resolved.values() if c]

    counters: dict[str, Counter] = {a: Counter() for a in ATTRIBUTES}
    total_rows = 0

    if used_cols:
        reader = pd.read_csv(
            csv_path, usecols=used_cols, dtype=str,
            chunksize=CHUNK_ROWS, low_memory=False,
        )
        for chunk in reader:
            total_rows += len(chunk)
            for attr in ATTRIBUTES:
                col = resolved[attr]
                if col is None:
                    continue
                normalise = NORMALISERS[attr]
                # value_counts first: vendors repeat the same handful of grades
                # millions of times, so we normalise ~10 distinct strings per
                # chunk instead of 200,000 cells.
                for raw, n in chunk[col].value_counts(dropna=False).items():
                    key = normalise(None if pd.isna(raw) else raw, codes)
                    counters[attr][key] += int(n)
    else:
        # No recognisable columns at all — still report the row count.
        total_rows = sum(
            len(c) for c in pd.read_csv(
                csv_path, usecols=[0], dtype=str, chunksize=CHUNK_ROWS, low_memory=False
            )
        )

    attributes = {}
    for attr in ATTRIBUTES:
        col = resolved[attr]
        counts = counters[attr]
        if col is None:
            counts = Counter({MISSING: total_rows})
        graded = sum(n for b, n in counts.items() if b not in (MISSING, UNKNOWN))
        buckets = [
            {
                "bucket": b,
                "order":  _bucket_order(attr, b),
                "count":  n,
                "pct":    round(n / total_rows * 100, 2) if total_rows else 0.0,
            }
            for b, n in counts.items() if n
        ]
        buckets.sort(key=lambda x: x["order"])
        attributes[attr] = {
            "column":       col,
            "has_data":     graded > 0,
            "graded":       graded,
            "missing":      total_rows - graded,
            "coverage_pct": round(graded / total_rows * 100, 2) if total_rows else 0.0,
            "distinct":     len([b for b in buckets if b["bucket"] not in (MISSING, UNKNOWN)]),
            "buckets":      buckets,
        }

    return {
        "vendor":        vendor,
        "source_file":   csv_path.name,
        "file_modified": datetime.fromtimestamp(csv_path.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S"),
        "total_rows":    total_rows,
        "attributes":    attributes,
    }


def build_origin_coverage(origin: str) -> dict:
    """Summarise every vendor of one origin. Missing CSVs are reported, not fatal."""
    cfg = ORIGINS[origin]
    diamond_dir = Path(cfg["diamond_dir"])

    vendors, skipped = [], []
    for spec in cfg["vendors"]:
        csv_path = diamond_dir / spec["csv"]
        if not csv_path.exists():
            print(f"  SKIP {origin}/{spec['name']} — file not found: {spec['csv']}")
            skipped.append({"vendor": spec["name"], "source_file": spec["csv"],
                            "reason": "file not found"})
            continue
        print(f"  reading {origin}/{spec['name']} ({spec['csv']})…")
        summary = summarise_vendor(csv_path, spec["name"], spec.get("codes"))
        have = [a for a in ATTRIBUTES if summary["attributes"][a]["has_data"]]
        lack = [a for a in ATTRIBUTES if not summary["attributes"][a]["has_data"]]
        print(f"    {summary['total_rows']:>9,} rows | has: {', '.join(have) or '—'}"
              + (f" | MISSING: {', '.join(lack)}" if lack else ""))
        vendors.append(summary)

    return {
        "origin":       origin,
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "attributes":   ATTRIBUTES,
        # Quick "who has what" matrix — the headline answer, without having to
        # walk every bucket.
        "coverage_matrix": {
            v["vendor"]: {a: v["attributes"][a]["has_data"] for a in ATTRIBUTES}
            for v in vendors
        },
        "vendor_count": len(vendors),
        "skipped":      skipped,
        "vendors":      vendors,
    }


def write_origin_coverage(origin: str, payload: dict | None = None) -> Path:
    """Write <compare_dir>/vendor_coverage.json for one origin."""
    payload = payload if payload is not None else build_origin_coverage(origin)
    out_dir = Path(ORIGINS[origin]["compare_dir"])
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "vendor_coverage.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
    print(f'  wrote {out_path} ({payload["vendor_count"]} vendor(s))')
    return out_path


# ══════════════════════════════════════════════════════════════════════════════
# Postgres
# ══════════════════════════════════════════════════════════════════════════════

COVERAGE_TABLE = "vendor_coverage"

COVERAGE_COLUMNS: list[tuple[str, str]] = [
    ("origin",                 "VARCHAR(16)"),
    ("vendor",                 "VARCHAR(64)"),
    ("source_file",            "TEXT"),
    ("attribute",              "VARCHAR(16)"),
    ("bucket",                 "VARCHAR(64)"),
    ("bucket_order",           "INT"),
    ("diamond_count",          "BIGINT"),
    ("pct_of_vendor",          "DECIMAL(6,2)"),
    ("vendor_total",           "BIGINT"),
    ("attribute_graded",       "BIGINT"),
    ("attribute_coverage_pct", "DECIMAL(6,2)"),
    ("has_data",               "BOOLEAN"),
    ("generated_at",           "TIMESTAMP"),
]


def ensure_coverage_table(cur) -> None:
    """CREATE TABLE IF NOT EXISTS + add any newly-introduced columns."""
    cols_sql = ",\n  ".join(f'"{n}" {t}' for n, t in COVERAGE_COLUMNS)
    cur.execute(
        f'CREATE TABLE IF NOT EXISTS "{COVERAGE_TABLE}" (\n'
        f'  "id" BIGSERIAL PRIMARY KEY,\n'
        f'  {cols_sql}\n'
        f")"
    )
    for name, ctype in COVERAGE_COLUMNS:
        cur.execute(
            f'ALTER TABLE "{COVERAGE_TABLE}" ADD COLUMN IF NOT EXISTS "{name}" {ctype}'
        )
    for col in ("origin", "vendor", "attribute"):
        cur.execute(
            f'CREATE INDEX IF NOT EXISTS "idx_{COVERAGE_TABLE}_{col}" '
            f'ON "{COVERAGE_TABLE}" ("{col}")'
        )


def coverage_rows(payload: dict) -> list[tuple]:
    """Flatten the nested JSON into tidy rows, one per bucket."""
    rows = []
    origin = payload["origin"]
    generated_at = payload["generated_at"]
    for v in payload["vendors"]:
        for attr in payload.get("attributes", ATTRIBUTES):
            info = v["attributes"][attr]
            for b in info["buckets"]:
                rows.append((
                    origin, v["vendor"], v["source_file"], attr,
                    b["bucket"], b["order"], b["count"], b["pct"],
                    v["total_rows"], info["graded"], info["coverage_pct"],
                    info["has_data"], generated_at,
                ))
    return rows


def upload_origin_coverage(cur, payload: dict) -> int:
    """
    Replace one origin's rows in the shared table.

    Scoped DELETE rather than TRUNCATE on purpose: all four origins share this
    table and each loader runs on its own schedule, so a truncate here would
    wipe the other three regions' summaries.
    """
    from psycopg2.extras import execute_values

    ensure_coverage_table(cur)
    origin = payload["origin"]
    cur.execute(f'DELETE FROM "{COVERAGE_TABLE}" WHERE "origin" = %s', (origin,))

    rows = coverage_rows(payload)
    if not rows:
        print(f'  no coverage rows for "{origin}" — nothing uploaded')
        return 0

    cols_sql = ", ".join(f'"{n}"' for n, _ in COVERAGE_COLUMNS)
    execute_values(
        cur, f'INSERT INTO "{COVERAGE_TABLE}" ({cols_sql}) VALUES %s', rows, page_size=1000
    )
    print(f'  {len(rows):>7,} coverage rows -> "{COVERAGE_TABLE}" (origin={origin})')
    return len(rows)


def refresh_origin(origin: str, cur=None) -> dict:
    """Build the summary, write the JSON, and (if given a cursor) upload it.
    This is the single entry point the regional loaders call."""
    print(f"[vendor-coverage] {origin}")
    payload = build_origin_coverage(origin)
    write_origin_coverage(origin, payload)
    if cur is not None:
        upload_origin_coverage(cur, payload)
    return payload


# ══════════════════════════════════════════════════════════════════════════════
# CLI
# ══════════════════════════════════════════════════════════════════════════════

def _connect():
    """Only imported/needed for --upload, so the JSON-only path has no DB deps."""
    import psycopg2
    from dotenv import load_dotenv

    load_dotenv(REPO_ROOT / ".env")
    url = os.environ.get("DATABASE_URL", "")
    if not url:
        raise SystemExit("DATABASE_URL env var is required for --upload")
    if "sslmode=" not in url:
        url += ("&" if "?" in url else "?") + "sslmode=require"
    return psycopg2.connect(url, connect_timeout=30)


def main(argv: Iterable[str]) -> None:
    args = list(argv)
    upload = "--upload" in args
    if upload:
        args.remove("--upload")

    origins = args or list(ORIGINS)
    unknown = [o for o in origins if o not in ORIGINS]
    if unknown:
        raise SystemExit(f"Unknown origin(s): {', '.join(unknown)}. "
                         f"Valid: {', '.join(ORIGINS)}")

    if not upload:
        for origin in origins:
            refresh_origin(origin)
        return

    conn = _connect()
    try:
        with conn.cursor() as cur:
            for origin in origins:
                refresh_origin(origin, cur)
        conn.commit()
    finally:
        conn.close()


if __name__ == "__main__":
    main(sys.argv[1:])
