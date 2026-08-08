"""
price_anomaly.py
================

Unsupervised price-move anomaly detection over the weekly Parquet snapshots
written by snapshot_history.py.

Why this exists
---------------
Every pipeline run TRUNCATEs and reloads the compare tables, so the dashboard
only ever shows "right now". snapshot_history.py keeps dated Parquet copies —
this module reads that archive and answers the question the live tables can't:
*what moved, and is that move unusual?*

Detector
--------
A robust z-score (median / MAD) over the population of price moves — the
standard unsupervised outlier test for heavy-tailed, unlabelled data. No
training, no model file, no extra dependency beyond pandas/numpy/pyarrow.

    z = (move_pct - median(moves)) / (1.4826 * MAD(moves))

The baseline population is the **non-zero** moves seen anywhere in the window
for that vendor. That matters: most stones don't reprice week to week, so
including the zeros would collapse the MAD to 0 and make every 1c change look
like a 50-sigma event. Restricting to stones that actually moved makes the
question "among stones that repriced, is this reprice unusually large?".

Two independent gates must both fire for a row to be reported:

    |z| >= z_thresh        statistically unusual vs the vendor's own noise
    |move_pct| >= min_pct  materially large in business terms

Three levels of output
----------------------
1. **Stone level**  — per certificate, per vendor: latest snapshot vs the one
   before it. Catches individual mispriced/repriced stones.
2. **Segment level** — median price per (vendor, shape, carat band). Catches a
   competitor repricing a whole band at once, which stone-level z-scores can
   miss because every stone in the band moves by a similar (individually
   unremarkable) amount.
3. **Vendor level**  — move counts, median move, and listing churn
   (new / delisted certificates). A scraper that silently half-fails shows up
   here as a churn spike, not as a price move.

Memory
------
The USA `compare_all` archive is ~210k rows per snapshot. Snapshots are walked
as consecutive pairs and only two are ever held in memory at once; older pairs
contribute nothing but a float array of move percentages.
"""

from __future__ import annotations

import math
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent
HISTORY_DIR = REPO_ROOT / "history"

# Consistency constant that makes MAD a a drop-in estimator for sigma on
# normally distributed data.
MAD_TO_SIGMA = 1.4826

# ── Region wiring ────────────────────────────────────────────────────────────
# Only USA is wired today; the other regions archive the same shape of file, so
# adding them is a matter of listing their price columns here.
REGION_CONFIG: dict[str, dict[str, Any]] = {
    "usa": {
        "label": "USA",
        "baseline": "PC",
        # display name -> (price column, link column or None)
        "vendors": {
            "PC":          ("PC Price USD",          None),
            "loose-grown": ("loose-grown Price USD", "loose-grown Link"),
            "brilliance":  ("brilliance Price USD",  "brilliance Link"),
            "friendly":    ("friendly Price USD",    "friendly Link"),
        },
        "attr_cols": ["shape", "color", "clarity", "cut", "caratWeight"],
    },
}

DATASETS = ("compare_all", "compare_partial", "compare")

CARAT_BAND_EDGES = [0.0, 0.3, 0.5, 0.7, 1.0, 1.5, 2.0, 3.0, 5.0, math.inf]
CARAT_BAND_LABELS = [
    "<0.30", "0.30-0.49", "0.50-0.69", "0.70-0.99", "1.00-1.49",
    "1.50-1.99", "2.00-2.99", "3.00-4.99", "5.00+",
]

# Signature-keyed result cache. The key includes every file's (name, mtime,
# size), so a new pipeline run invalidates it automatically — no TTL needed.
_CACHE: dict[tuple, dict] = {}
_CACHE_MAX = 8


# ── Snapshot discovery ───────────────────────────────────────────────────────
def _snapshot_files(region: str, dataset: str, days: int) -> list[tuple[str, Path]]:
    """Dated Parquet snapshots for `region`/`dataset` inside the trailing
    `days`-day window, oldest first."""
    table_dir = HISTORY_DIR / region / dataset
    if not table_dir.exists():
        return []

    cutoff = date.today() - timedelta(days=days)
    found: list[tuple[str, Path]] = []
    prefix = f"{dataset}_"
    for path in table_dir.glob(f"{dataset}_*.parquet"):
        stamp = path.stem[len(prefix):]
        try:
            snap = date.fromisoformat(stamp)
        except ValueError:
            continue
        if snap >= cutoff:
            found.append((stamp, path))
    found.sort(key=lambda pair: pair[0])
    return found


def _signature(files: list[tuple[str, Path]]) -> tuple:
    sig = []
    for stamp, path in files:
        try:
            st = path.stat()
            sig.append((stamp, int(st.st_mtime), st.st_size))
        except OSError:
            sig.append((stamp, 0, 0))
    return tuple(sig)


def _read_snapshot(path: Path, cfg: dict) -> pd.DataFrame:
    """Read only the columns the detector needs — never the whole file."""
    import pyarrow.parquet as pq

    price_cols = [p for p, _ in cfg["vendors"].values()]
    link_cols = [lnk for _, lnk in cfg["vendors"].values() if lnk]
    wanted = ["certificate_number", *cfg["attr_cols"], *price_cols, *link_cols]

    # pyarrow raises on a missing column, so intersect with the real schema —
    # older snapshots may predate a vendor being added to the pipeline.
    present = set(pq.ParquetFile(path).schema.names)
    if "certificate_number" not in present:
        raise ValueError(
            f"{path.name} has no certificate_number column — snapshots are "
            f"joined on it, so this archive can't be compared."
        )
    cols = [c for c in wanted if c in present]
    df = pd.read_parquet(path, columns=cols, engine="pyarrow")
    df["certificate_number"] = df["certificate_number"].astype(str).str.strip()
    return df


def _carat_band(series: pd.Series) -> pd.Series:
    return pd.cut(
        pd.to_numeric(series, errors="coerce"),
        bins=CARAT_BAND_EDGES,
        labels=CARAT_BAND_LABELS,
        right=False,
    )


# ── Robust statistics ────────────────────────────────────────────────────────
def _robust_scale(values: np.ndarray) -> tuple[float, float]:
    """(center, scale) for a robust z-score, with graded fallbacks.

    MAD is the primary estimator. On snapshot data it can legitimately come out
    as 0 (e.g. an entire vendor shifts every price by exactly the same %), so
    fall back to the IQR, then the standard deviation, then 1.0 — the |move_pct|
    gate is what keeps the output meaningful in those degenerate cases.
    """
    if values.size == 0:
        return 0.0, 1.0
    center = float(np.median(values))
    mad = float(np.median(np.abs(values - center)))
    if mad > 1e-9:
        return center, mad * MAD_TO_SIGMA
    q1, q3 = np.percentile(values, [25, 75])
    iqr = float(q3 - q1)
    if iqr > 1e-9:
        return center, iqr / 1.349
    std = float(np.std(values))
    return center, std if std > 1e-9 else 1.0


def _pct_move(prev: pd.Series, curr: pd.Series) -> pd.Series:
    prev_f = pd.to_numeric(prev, errors="coerce")
    curr_f = pd.to_numeric(curr, errors="coerce")
    with np.errstate(divide="ignore", invalid="ignore"):
        pct = (curr_f - prev_f) / prev_f.replace(0, np.nan) * 100.0
    return pct.replace([np.inf, -np.inf], np.nan)


def _f(v: Any) -> float | None:
    """NaN/inf-safe float for JSON."""
    if v is None:
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return None if (math.isnan(f) or math.isinf(f)) else round(f, 2)


# ── Main entry point ─────────────────────────────────────────────────────────
def detect_price_anomalies(
    region: str = "usa",
    dataset: str = "compare_all",
    days: int = 14,
    compare_mode: str = "window",
    z_thresh: float = 3.5,
    min_pct: float = 5.0,
    min_segment_stones: int = 20,
    max_anomalies: int = 2000,
    use_cache: bool = True,
) -> dict:
    """Detect unusual price moves in the trailing `days`-day snapshot window.

    compare_mode:
      "window" — newest snapshot vs the oldest one in the window (default;
                 "what moved over the last 2 weeks").
      "prev"   — newest snapshot vs the one immediately before it
                 ("what moved since the last run").

    Returns a JSON-serialisable dict; `available` is False (with a `reason`)
    when there aren't at least two snapshots to compare.
    """
    cfg = REGION_CONFIG.get(region)
    if cfg is None:
        raise ValueError(f"unknown region {region!r}")
    if dataset not in DATASETS:
        raise ValueError(f"unknown dataset {dataset!r}")
    if compare_mode not in ("window", "prev"):
        raise ValueError(f"unknown compare_mode {compare_mode!r}")

    files = _snapshot_files(region, dataset, days)
    cache_key = (
        region, dataset, days, compare_mode, z_thresh, min_pct,
        min_segment_stones, max_anomalies, _signature(files),
    )
    if use_cache and cache_key in _CACHE:
        return _CACHE[cache_key]

    base = {
        "region": region,
        "region_label": cfg["label"],
        "dataset": dataset,
        "window_days": days,
        "compare_mode": compare_mode,
        "z_threshold": z_thresh,
        "min_move_pct": min_pct,
        "snapshots": [stamp for stamp, _ in files],
    }

    if len(files) < 2:
        result = {
            **base,
            "available": False,
            "reason": (
                f"Need at least 2 snapshots in the last {days} days to compare "
                f"price moves — found {len(files)}."
            ),
            "vendors": [], "segments": [], "anomalies": [], "trend": [],
            "summary": {},
        }
        _remember(cache_key, result)
        return result

    vendors: dict[str, tuple[str, str | None]] = cfg["vendors"]

    # The two snapshots actually being diffed.
    from_date = files[0][0] if compare_mode == "window" else files[-2][0]
    to_date = files[-1][0]

    # vendor -> non-zero move % arrays from every consecutive pair. Not used for
    # z-scoring (see below) — only to report the vendor's typical run-to-run
    # reprice size next to this window's move.
    step_moves: dict[str, list[np.ndarray]] = {v: [] for v in vendors}
    # vendor -> {(shape, band): {snapshot_date: median_price}} for from/to only
    segment_medians: dict[str, dict[tuple[str, str], dict[str, float]]] = {
        v: {} for v in vendors
    }
    segment_counts: dict[str, dict[tuple[str, str], int]] = {v: {} for v in vendors}
    trend: list[dict] = []

    anchor_df: pd.DataFrame | None = None   # snapshot at from_date
    target_df: pd.DataFrame | None = None   # snapshot at to_date
    prev_df: pd.DataFrame | None = None

    for stamp, path in files:
        curr_df = _read_snapshot(path, cfg)
        trend.append(_trend_row(stamp, curr_df, vendors))

        if prev_df is not None:
            for vendor, (price_col, _link) in vendors.items():
                if price_col not in prev_df.columns or price_col not in curr_df.columns:
                    continue
                pair = curr_df.loc[curr_df[price_col].notna(), ["certificate_number", price_col]].merge(
                    prev_df.loc[prev_df[price_col].notna(), ["certificate_number", price_col]],
                    on="certificate_number", how="inner", suffixes=("", "_prev"),
                )
                moved = _pct_move(pair[f"{price_col}_prev"], pair[price_col])
                moved = moved.loc[moved.abs() > 1e-9].dropna().to_numpy(dtype="float64")
                if moved.size:
                    step_moves[vendor].append(moved)

        if stamp in (from_date, to_date):
            for vendor, (price_col, _link) in vendors.items():
                if price_col in curr_df.columns:
                    _collect_segment_medians(
                        segment_medians[vendor], segment_counts[vendor],
                        curr_df, price_col, stamp,
                    )
        if stamp == from_date:
            anchor_df = curr_df
        if stamp == to_date:
            target_df = curr_df

        prev_df = curr_df

    anomalies: list[dict] = []
    vendor_rows: list[dict] = []
    total_moved = 0

    for vendor, (price_col, link_col) in vendors.items():
        if (
            anchor_df is None or target_df is None
            or price_col not in anchor_df.columns
            or price_col not in target_df.columns
        ):
            continue

        prev_v = anchor_df.loc[
            anchor_df[price_col].notna(), ["certificate_number", price_col]
        ]
        curr_cols = ["certificate_number", price_col, *cfg["attr_cols"]]
        if link_col and link_col in target_df.columns:
            curr_cols.append(link_col)
        curr_v = target_df.loc[target_df[price_col].notna(), curr_cols]

        merged = curr_v.merge(
            prev_v, on="certificate_number", how="inner", suffixes=("", "_prev")
        )
        if merged.empty:
            continue
        prev_price_col = f"{price_col}_prev"
        merged["move_pct"] = _pct_move(merged[prev_price_col], merged[price_col])

        move = merged["move_pct"]
        valid = move.notna()
        moved_mask = valid & (move.abs() > 1e-9)

        # Cross-sectional baseline: the distribution of *this* window's non-zero
        # moves for this vendor. A stone is an anomaly when it repriced far more
        # than its peers did over the same span — which correctly ignores an
        # across-the-board shift (everything +3% is not 200k anomalies).
        pool = move.loc[moved_mask].to_numpy(dtype="float64")
        center, scale = _robust_scale(pool)
        z = (move - center) / scale

        flagged = merged.loc[
            valid & (z.abs() >= z_thresh) & (move.abs() >= min_pct)
        ].copy()
        flagged["z_score"] = z.loc[flagged.index]

        prev_certs = set(prev_v["certificate_number"])
        curr_certs = set(curr_v["certificate_number"])
        step_pool = (
            np.concatenate(step_moves[vendor])
            if step_moves[vendor] else np.array([], dtype="float64")
        )
        total_moved += int(moved_mask.sum())

        vendor_rows.append({
            "vendor": vendor,
            "is_baseline": vendor == cfg["baseline"],
            "tracked": int(valid.sum()),
            "moved": int(moved_mask.sum()),
            "moved_up": int((move > 1e-9).sum()),
            "moved_down": int((move < -1e-9).sum()),
            "median_move_pct": _f(move.loc[moved_mask].median()) if moved_mask.any() else 0.0,
            "typical_step_move_pct": (
                _f(np.median(np.abs(step_pool))) if step_pool.size else None
            ),
            "baseline_center_pct": _f(center),
            "baseline_scale_pct": _f(scale),
            "baseline_sample": int(pool.size),
            "anomalies": int(len(flagged)),
            "anomalies_up": int((flagged["move_pct"] > 0).sum()),
            "anomalies_down": int((flagged["move_pct"] < 0).sum()),
            "new_listings": len(curr_certs - prev_certs),
            "delisted": len(prev_certs - curr_certs),
            "carried_over": len(curr_certs & prev_certs),
            "prev_total": len(prev_certs),
            "curr_total": len(curr_certs),
        })

        flagged = flagged.reindex(
            flagged["move_pct"].abs().sort_values(ascending=False).index
        ).head(max_anomalies)

        # to_dict("records") rather than itertuples — the price columns
        # ("PC Price USD") aren't valid identifiers, so itertuples renames them
        # to _1/_2 and every lookup by real name silently returns None.
        for rec in flagged.to_dict("records"):
            anomalies.append({
                "vendor": vendor,
                "certificate_number": str(rec.get("certificate_number", "")),
                "shape": rec.get("shape"),
                "color": rec.get("color"),
                "clarity": rec.get("clarity"),
                "cut": rec.get("cut"),
                "carat": _f(rec.get("caratWeight")),
                "prev_price": _f(rec.get(prev_price_col)),
                "curr_price": _f(rec.get(price_col)),
                "move_usd": _f(
                    (rec.get(price_col) or 0) - (rec.get(prev_price_col) or 0)
                ),
                "move_pct": _f(rec.get("move_pct")),
                "z_score": _f(rec.get("z_score")),
                "direction": "up" if (rec.get("move_pct") or 0) > 0 else "down",
                "link": rec.get(link_col) if link_col else None,
            })

    anomalies.sort(key=lambda a: abs(a["z_score"] or 0), reverse=True)

    segments = _segment_anomalies(
        segment_medians, segment_counts, from_date, to_date,
        z_thresh, min_pct, min_segment_stones,
    )

    result = {
        **base,
        "available": True,
        "compared_from": from_date,
        "compared_to": to_date,
        "snapshot_count": len(files),
        "vendors": vendor_rows,
        "segments": segments,
        "anomalies": anomalies,
        "trend": trend,
        "summary": {
            # total_flagged is the true detector count; total_anomalies is how
            # many rows actually made it into `anomalies` after the per-vendor
            # cap. They differ when a vendor flags more than max_anomalies —
            # reported rather than silently truncated.
            "total_flagged": sum(v["anomalies"] for v in vendor_rows),
            "total_anomalies": len(anomalies),
            "truncated": sum(v["anomalies"] for v in vendor_rows) > len(anomalies),
            "per_vendor_cap": max_anomalies,
            "up": sum(1 for a in anomalies if a["direction"] == "up"),
            "down": sum(1 for a in anomalies if a["direction"] == "down"),
            "segment_anomalies": len(segments),
            "tracked": sum(v["tracked"] for v in vendor_rows),
            "moved": total_moved,
            "biggest_move_pct": (
                max((abs(a["move_pct"] or 0) for a in anomalies), default=0.0)
            ),
        },
        # Two snapshots with zero moves anywhere means the archive was written
        # twice from the same compare files (a re-run that didn't re-scrape),
        # not that prices were flat. Surfaced so the dashboard can say so
        # instead of showing a misleading, confident "0 anomalies".
        "identical_snapshots": bool(vendor_rows) and total_moved == 0,
    }
    _remember(cache_key, result)
    return result


# ── Helpers ──────────────────────────────────────────────────────────────────
def _remember(key: tuple, result: dict) -> None:
    if len(_CACHE) >= _CACHE_MAX:
        _CACHE.pop(next(iter(_CACHE)))
    _CACHE[key] = result


def _trend_row(stamp: str, df: pd.DataFrame, vendors: dict) -> dict:
    """Per-snapshot median price and listing count per vendor — the sparkline
    behind the anomaly numbers."""
    row: dict[str, Any] = {"snapshot_date": stamp}
    for vendor, (price_col, _link) in vendors.items():
        if price_col not in df.columns:
            continue
        series = pd.to_numeric(df[price_col], errors="coerce").dropna()
        row[vendor] = {
            "count": int(series.size),
            "median_price": _f(series.median()) if series.size else None,
        }
    return row


def _collect_segment_medians(
    store: dict[tuple[str, str], dict[str, float]],
    counts: dict[tuple[str, str], int],
    df: pd.DataFrame,
    price_col: str,
    stamp: str,
) -> None:
    """Median price per (shape, carat band) for one snapshot."""
    if not {price_col, "shape", "caratWeight"} <= set(df.columns):
        return
    sub = df.loc[df[price_col].notna(), ["shape", "caratWeight", price_col]].copy()
    if sub.empty:
        return
    sub["band"] = _carat_band(sub["caratWeight"])
    sub = sub.dropna(subset=["band"])
    if sub.empty:
        return
    grouped = sub.groupby(["shape", "band"], observed=True)[price_col].agg(
        ["median", "size"]
    )
    for (shape, band), agg in grouped.iterrows():
        key = (str(shape), str(band))
        store.setdefault(key, {})[stamp] = float(agg["median"])
        counts[key] = max(counts.get(key, 0), int(agg["size"]))


def _segment_anomalies(
    segment_medians: dict[str, dict[tuple[str, str], dict[str, float]]],
    segment_counts: dict[str, dict[tuple[str, str], int]],
    from_date: str,
    to_date: str,
    z_thresh: float,
    min_pct: float,
    min_stones: int,
) -> list[dict]:
    """Segment-level moves, z-scored across each vendor's own segments.

    A whole-band reprice shows up here even when no individual stone in the band
    clears the stone-level threshold.
    """
    out: list[dict] = []
    for vendor, segments in segment_medians.items():
        rows: list[dict] = []
        for (shape, band), by_date in segments.items():
            prev_p, curr_p = by_date.get(from_date), by_date.get(to_date)
            if not prev_p or not curr_p or prev_p <= 0:
                continue
            if segment_counts[vendor].get((shape, band), 0) < min_stones:
                continue
            rows.append({
                "vendor": vendor, "shape": shape, "carat_band": band,
                "stones": segment_counts[vendor][(shape, band)],
                "prev_median": prev_p, "curr_median": curr_p,
                "move_pct": (curr_p - prev_p) / prev_p * 100.0,
            })
        if not rows:
            continue

        moves = np.array([r["move_pct"] for r in rows], dtype="float64")
        nonzero = moves[np.abs(moves) > 1e-9]
        center, scale = _robust_scale(nonzero if nonzero.size else moves)

        for r in rows:
            z = (r["move_pct"] - center) / scale
            if abs(z) >= z_thresh and abs(r["move_pct"]) >= min_pct:
                out.append({
                    **r,
                    "prev_median": _f(r["prev_median"]),
                    "curr_median": _f(r["curr_median"]),
                    "move_pct": _f(r["move_pct"]),
                    "z_score": _f(z),
                    "direction": "up" if r["move_pct"] > 0 else "down",
                })

    out.sort(key=lambda s: abs(s["z_score"] or 0), reverse=True)
    return out


if __name__ == "__main__":  # manual smoke run
    import json as _json
    import sys

    res = detect_price_anomalies(
        region=sys.argv[1] if len(sys.argv) > 1 else "usa",
        days=int(sys.argv[2]) if len(sys.argv) > 2 else 14,
    )
    res_preview = {**res, "anomalies": res["anomalies"][:5]}
    print(_json.dumps(res_preview, indent=2, default=str))
