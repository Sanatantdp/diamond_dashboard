# -*- coding: utf-8 -*-
"""
be_vs_pc_compare.py
====================
Compare Brilliant Earth prices vs Precious Carbon (baseline) by certificate number.

Outputs:
  compare/usa/be_vs_pc_compare.csv  — matched diamonds with prices + % diff
  compare/usa/be_vs_pc_compare.json — same data as JSON (for dashboard use)

Run:
  python usa_compare/be_vs_pc_compare.py
"""

import os
import json
import datetime
import pandas as pd
from pathlib import Path
from logger import get_logger

log = get_logger("be_vs_pc")

# ── Paths ─────────────────────────────────────────────────────────────────────
BASE_DIR       = Path(__file__).resolve().parent.parent
DIAMOND_DIR    = BASE_DIR / "diamond_files" / "usa"
COMPARE_DIR    = BASE_DIR / "compare" / "usa"
COMPARE_DIR.mkdir(parents=True, exist_ok=True)

PC_CSV         = DIAMOND_DIR / "precious_carbon.csv"
BE_CSV         = DIAMOND_DIR / "brilliant_earth.csv"
OUT_CSV        = COMPARE_DIR / "be_vs_pc_compare.csv"
OUT_JSON       = COMPARE_DIR / "be_vs_pc_compare.json"


# ── Loaders ───────────────────────────────────────────────────────────────────
def load_pc(path: Path) -> pd.DataFrame:
    log.info(f"Loading PC from {path}")
    df = pd.read_csv(path, low_memory=False, on_bad_lines="skip")
    df["certificate_number"]      = df["certificateNumber"].astype(str).str.strip()
    df["PC Price USD"]            = pd.to_numeric(df["finalPriceUsd"], errors="coerce")
    df["PC Discounted Price (60%)"] = (df["PC Price USD"] * 0.60).round(2)
    df["carat"]                   = pd.to_numeric(df["caratWeight"], errors="coerce")
    df["color"]                   = df["color"].astype(str).str.strip().str.upper()
    df["shape"]                   = df["shape"].astype(str).str.strip().str.title()
    df["clarity"]                 = df["clarity"].astype(str).str.strip().str.upper()
    log.info(f"  PC rows: {len(df):,}")
    return df[["certificate_number", "PC Price USD", "PC Discounted Price (60%)",
               "carat", "color", "shape", "clarity"]]


def load_be(path: Path) -> pd.DataFrame:
    log.info(f"Loading Brilliant Earth from {path}")
    df = pd.read_csv(path, low_memory=False, on_bad_lines="skip")
    df["certificate_number"] = (
        df["certificate_number"].astype(str).str.strip()
        .str.replace(r"^LG", "", regex=True)
    )
    df["BE Price USD"] = pd.to_numeric(df["price"], errors="coerce")
    df["br_id"]        = df["id"].astype(str).str.strip()
    log.info(f"  BE rows: {len(df):,}")
    return df[["certificate_number", "BE Price USD", "br_id"]]


# ── Compare ───────────────────────────────────────────────────────────────────
def build_compare(pc_df: pd.DataFrame, be_df: pd.DataFrame) -> pd.DataFrame:
    log.info("Merging on certificate_number …")
    merged = pd.merge(pc_df, be_df, on="certificate_number", how="inner")

    before = len(merged)
    merged = merged.dropna(subset=["PC Discounted Price (60%)", "BE Price USD"])
    merged = merged[(merged["PC Discounted Price (60%)"] > 0) & (merged["BE Price USD"] > 0)]
    log.info(f"  Matched: {before:,} → {len(merged):,} after price filter")

    if merged.empty:
        log.warning("No overlapping certificates found between PC and Brilliant Earth.")
        return merged

    merged["Price Diff USD"] = (merged["BE Price USD"] - merged["PC Discounted Price (60%)"]).round(2)
    merged["BE vs PC %"]     = (
        (merged["BE Price USD"] - merged["PC Discounted Price (60%)"]) / merged["PC Discounted Price (60%)"] * 100
    ).round(2)

    # Sort cheapest BE first
    merged.sort_values("BE vs PC %", inplace=True)
    merged.reset_index(drop=True, inplace=True)

    col_order = [
        "certificate_number", "br_id", "shape", "carat", "color", "clarity",
        "PC Price USD", "PC Discounted Price (60%)", "BE Price USD",
        "Price Diff USD", "BE vs PC %",
    ]
    merged = merged[[c for c in col_order if c in merged.columns]]

    log.info(f"  Avg BE vs PC: {merged['BE vs PC %'].mean():.2f}%")
    log.info(f"  BE cheaper count: {(merged['BE vs PC %'] < 0).sum():,}")
    log.info(f"  BE pricier count: {(merged['BE vs PC %'] > 0).sum():,}")
    return merged


# ── Writers ───────────────────────────────────────────────────────────────────
def write_csv(df: pd.DataFrame, path: Path):
    df.to_csv(path, index=False, encoding="utf-8")
    log.info(f"CSV → {path}  ({len(df):,} rows)")


def write_json(df: pd.DataFrame, path: Path):
    records = df.to_dict(orient="records")
    out = {
        "generated_at": datetime.datetime.now().isoformat(timespec="seconds"),
        "total":        len(records),
        "items":        records,
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(out, f, allow_nan=False, default=str)
    log.info(f"JSON → {path}  ({len(records):,} records)")


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    log.info("=" * 60)
    log.info("be_vs_pc_compare — START")

    for p, label in [(PC_CSV, "PC"), (BE_CSV, "Brilliant Earth")]:
        if not p.exists():
            log.error(f"{label} CSV not found: {p}")
            log.error("Run the scraper first, then re-run this script.")
            return

    pc_df = load_pc(PC_CSV)
    be_df = load_be(BE_CSV)

    result = build_compare(pc_df, be_df)

    if result.empty:
        log.warning("Nothing to write — no matching certificates.")
        return

    write_csv(result, OUT_CSV)
    write_json(result, OUT_JSON)

    log.info("=" * 60)
    log.info(f"Done.  {len(result):,} matched diamonds.")
    log.info(f"  CSV  → {OUT_CSV}")
    log.info(f"  JSON → {OUT_JSON}")


if __name__ == "__main__":
    main()
