import os
import sys
import pandas as pd
import json
import datetime
from itertools import combinations
from pathlib import Path
from openpyxl.styles import PatternFill, Font
from openpyxl.utils import get_column_letter
from dotenv import load_dotenv
from logger import get_logger

load_dotenv()
log = get_logger("comparator")

# ── Directories ────────────────────────────────────────────────
DIAMOND_FILES_DIR       = str(Path(__file__).resolve().parent.parent / "diamond_files" / "uk")
COMPARE_DIR             = str(Path(__file__).resolve().parent.parent / "compare" / "uk")
FILE_STATUS_JSON        = os.path.join(DIAMOND_FILES_DIR, "file_status.json")
COMPARE_CSV              = os.path.join(COMPARE_DIR, "compare.csv")
COMPARE_JSON             = os.path.join(COMPARE_DIR, "compare.json")
COMPARE_PARTIAL_JSON     = os.path.join(COMPARE_DIR, "compare_partial.json")
COMPARE_ALL_JSON         = os.path.join(COMPARE_DIR, "compare_all.json")
DIAMOND_STATUS_JSON      = os.path.join(COMPARE_DIR, "diamond_status.json")
MAX_AGE_DAYS             = 7

os.makedirs(COMPARE_DIR, exist_ok=True)


# ══════════════════════════════════════════════════════════════
# DATA LOADERS
# ══════════════════════════════════════════════════════════════

# ── Vendor product-page link builders ───────────────────────────
# Each takes the raw value of a vendor's "link_col" (see vendor_defaults in
# load_vendors_from_env) and returns the diamond's product-page URL, or None
# when the source value is missing/unusable. Diamond Heaven keys its product
# page off its own internal stock ID, NOT the GIA/IGI certificate number, so
# this ID has to be carried through separately from certificate_number (the
# join key).

def _diamond_heavon_link(stock_id) -> str | None:
    stock_id = str(stock_id).strip() if pd.notna(stock_id) else ""
    if not stock_id or stock_id.lower() == "nan":
        return None
    return (
        f"https://www.diamond-heaven.co.uk/build-your-ring/by-stone"
        f"?diamond_type=labgrown&stock_id={stock_id}"
    )


def load_vendor(csv_path, cert_col, price_col, name, link_col=None, link_builder=None):
    log.debug(f"Loading vendor '{name}' from {csv_path}")
    df = pd.read_csv(csv_path, low_memory=False, on_bad_lines="skip")
    df["certificate_number"] = df[cert_col].astype(str).str.strip()
    keep_cols = ["certificate_number", price_col]
    has_link  = link_col and link_builder and link_col in df.columns
    if has_link:
        keep_cols.append(link_col)
    df = df[keep_cols].copy()
    df = df.rename(columns={price_col: f"{name} Price Pound"})
    df[f"{name} Price Pound"] = pd.to_numeric(df[f"{name} Price Pound"], errors="coerce")
    if has_link:
        df[f"{name} Link"] = df[link_col].apply(link_builder)
        df = df.drop(columns=[link_col])
        log.info(f"  '{name}': {df[f'{name} Link'].notna().sum():,} rows have a product link")
    log.info(f"Loaded '{name}': {len(df):,} rows")
    return df


rouk_EXTRA_COLS = {
    "SHAPE":   "shape",
    "COLOR":   "color",
    "CLARITY": "clarity",
    "CUT":     "cut",
    "CARAT":   "caratWeight",
}

def load_ROUK(csv_path):
    log.debug(f"Loading ROUK from {csv_path}")
    df = pd.read_csv(csv_path, low_memory=False, on_bad_lines="skip")
    df = df.rename(columns={"certificateNumber": "certificate_number", "finalPriceUsd": "ROUK Price Pound"})
    df["certificate_number"] = df["certificate_number"].astype(str).str.strip()
    df["ROUK Price Pound"] = pd.to_numeric(df["ROUK Price Pound"], errors="coerce")

    for src_col, dest_col in rouk_EXTRA_COLS.items():
        for variant in [src_col, src_col.lower(), src_col.title()]:
            if variant in df.columns:
                df[dest_col] = df[variant].astype(str).str.strip().str.title()
                log.debug(f"  ROUK extra col: {variant} → {dest_col}")
                break

    keep = ["certificate_number", "ROUK Price Pound"] + [
        dest for dest in rouk_EXTRA_COLS.values() if dest in df.columns
    ]
    log.info(f"Loaded ROUK: {len(df):,} rows | extra cols: {[c for c in keep if c not in ('certificate_number','ROUK Price Pound')]}")
    return df[keep]

def generate_vendor_vs_rouk_csv(combined: pd.DataFrame, discounts: dict):
    log.info("=" * 50)
    log.info("Generating vendor vs ROUK CSV files")

    if "ROUK Price Pound" not in combined.columns:
        log.error("ROUK price not found — skipping")
        return

    rouk_disc     = discounts.get("ROUK", 0.70)
    rouk_label    = f"-{round((1 - rouk_disc) * 100)}%"
    rouk_disc_col = f"ROUK {rouk_label} Pound"

    attr_cols = ["shape", "color", "clarity", "cut", "caratWeight"]
    attr_cols = [c for c in attr_cols if c in combined.columns]

    for name, disc in discounts.items():
        if name == "ROUK":
            continue

        price_col = f"{name} Price Pound"
        if price_col not in combined.columns:
            continue

        label    = f"-{round((1 - disc) * 100)}%"
        disc_col = f"{name} {label} Pound"

        df = combined.copy()

        # Only matched rows
        df = df.dropna(subset=[price_col, rouk_disc_col])

        # Difference
        diff_col = f"{name} vs ROUK Pound"
        df[diff_col] = (
            pd.to_numeric(df[disc_col], errors="coerce") -
            pd.to_numeric(df[rouk_disc_col], errors="coerce")
        ).round(0)

        # Percentage
        pct_col = f"{name} vs ROUK %"
        df[pct_col] = (
            (pd.to_numeric(df[disc_col], errors="coerce") -
             pd.to_numeric(df[rouk_disc_col], errors="coerce")) /
            pd.to_numeric(df[rouk_disc_col], errors="coerce") * 100
        ).round(2)

        final_cols = (
            ["certificate_number"] +
            attr_cols +
            [
                "ROUK Price Pound",
                rouk_disc_col,
                price_col,
                disc_col,
                diff_col,
                pct_col,
            ]
        )

        final_cols = [c for c in final_cols if c in df.columns]

        output_file = os.path.join(COMPARE_DIR, f"{name}_vs_rouk.csv")
        df[final_cols].to_csv(output_file, index=False)

        log.info(f"✅ {name}: {len(df):,} rows → {output_file}")

# ══════════════════════════════════════════════════════════════
# STEP 1 — BUILD compare files
# ══════════════════════════════════════════════════════════════

def _save_diamond_status(combined: pd.DataFrame, loaded: dict, discounts: dict,
                         common_records: list, partial_records: list, all_records: list,
                        ):
    import math

    vendor_names = [n for n in loaded.keys() if n != "ROUK"]
    rouk_total     = len(loaded["ROUK"]) if "ROUK" in loaded else 0

    vendor_totals = {}
    for name in vendor_names:
        vendor_totals[name] = len(loaded[name]) if name in loaded else 0

    rouk_vs = {}
    for name in vendor_names:
        price_col = f"{name} Price Pound"
        if price_col not in combined.columns:
            rouk_vs[name] = {"matched": 0, "vendor_total": vendor_totals.get(name, 0)}
            continue
        matched   = int(combined[price_col].notna().sum())
        rouk_vs[name] = {
            "matched":          matched,
            "vendor_total":     vendor_totals.get(name, 0),
            "vendor_not_in_ROUK": vendor_totals.get(name, 0) - matched,
            "match_rate_pct":   round(matched / rouk_total * 100, 2) if rouk_total else 0,
        }

    status = {
        "generated_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "rouk_total":     rouk_total,
        "vendor_totals": vendor_totals,
        "rouk_vs_vendors": rouk_vs,
        "dataset_counts": {
            "common":     len(common_records),
            "partial":    len(partial_records),
            "all_ROUK":     len(all_records),
        },
    }

    with open(DIAMOND_STATUS_JSON, "w", encoding="utf-8") as f:
        json.dump(status, f, ensure_ascii=False, indent=2)
    log.info(f"Saved diamond_status.json → {DIAMOND_STATUS_JSON}")

def load_vendors_from_env():
    vendor_defaults = [
        {"name": "diamond-heavon", "env_csv": "DIAMOND_HEAVON_CSV",  "env_cert": "DIAMOND_HEAVON_CERT_COL",  "env_price": "DIAMOND_HEAVON_PRICE_COL",  "env_disc": "DIAMOND_HEAVON_DISCOUNT",  "default_csv": "diamond_heaven.csv",    "default_cert": "cert_no_clean",          "default_price": "price", "default_disc": "0.70", "link_col": "internal_stock_id", "link_builder": _diamond_heavon_link},
        {"name": "abelini",        "env_csv": "ABELINI_CSV",          "env_cert": "ABELINI_CERT_COL",         "env_price": "ABELINI_PRICE_COL",         "env_disc": "ABELINI_DISCOUNT",         "default_csv": "abelini.csv",             "default_cert": "cert_no_clean", "default_price": "carat_price", "default_disc": "0.70", "link_col": None, "link_builder": None},
    ]

    vendors = []
    for v in vendor_defaults:
        csv_name  = get_env(v["env_csv"],   v["default_csv"])
        cert_col  = get_env(v["env_cert"],  v["default_cert"])
        price_col = get_env(v["env_price"], v["default_price"])
        discount  = float(get_env(v["env_disc"], v["default_disc"]))
        vendors.append({
            "name": v["name"], "csv": os.path.join(DIAMOND_FILES_DIR, csv_name),
            "cert_col": cert_col, "price_col": price_col, "discount": discount,
            "link_col": v["link_col"], "link_builder": v["link_builder"],
        })

    rouk_csv_name = get_env("rouk_CSV", "rouk_diamonds.csv")
    rouk_csv      = os.path.join(DIAMOND_FILES_DIR, rouk_csv_name)
    rouk_discount = float(get_env("rouk_DISCOUNT", "0.70"))
    return vendors, rouk_csv, rouk_discount


def build_compare_files(loaded: dict, discounts: dict) -> pd.DataFrame:
    log.info("=" * 50)
    log.info("STEP 1 — Building compare files")
    log.info(f"Vendors loaded: {list(loaded.keys())}")

    if "ROUK" not in loaded:
        log.warning("ROUK not loaded — falling back to outer join")
        combined = None
        for name, df in loaded.items():
            combined = df if combined is None else pd.merge(combined, df, on="certificate_number", how="outer")
    else:
        combined = loaded["ROUK"].copy()
        log.info(f"ROUK master list: {len(combined):,} cert IDs")
        for name, df in loaded.items():
            if name == "ROUK":
                continue
            combined = pd.merge(combined, df, on="certificate_number", how="left")
            matched = combined[f"{name} Price Pound"].notna().sum()
            log.info(f"  '{name}': {matched:,} matched ROUK")

    if combined is None or combined.empty:
        log.error("No data loaded — aborting.")
        return pd.DataFrame()

    combined = combined.reset_index(drop=True)
    price_cols = [f"{name} Price Pound" for name in loaded.keys() if f"{name} Price Pound" in combined.columns]
    combined["_vendor_count"] = combined[price_cols].notna().sum(axis=1)

    # A vendor only counts toward "common" if it actually has at least one
    # match against the ROUK master list this run. Without this, a vendor
    # stuck at 0 matches (e.g. a cert-number format mismatch) would
    # permanently zero out "common" for every other vendor too. This makes
    # the requirement self-healing: if that vendor starts matching again on
    # a later run, it's automatically required again.
    CORE_VENDORS = ["diamond-heavon", "abelini", "ROUK"]
    core_present = []
    for n in CORE_VENDORS:
        col = f"{n} Price Pound"
        if col not in combined.columns:
            continue
        if n == "ROUK" or combined[col].notna().sum() > 0:
            core_present.append(n)
        else:
            log.warning(f"  '{n}': 0 matches vs ROUK this run — excluded from 'common' requirement")
    combined["_all_matched"]  = combined[[f"{n} Price Pound" for n in core_present]].notna().all(axis=1) if core_present else combined["_vendor_count"] >= 2
    combined["_some_matched"] = combined["_vendor_count"] >= 2

    common_count  = combined["_all_matched"].sum()
    partial_count = combined["_some_matched"].sum()
    log.info(f"  Common ({'+'.join(core_present)}): {common_count:,}  |  Partial (2+): {partial_count:,}  |  All ROUK: {len(combined):,}")

    # Discounted price columns
    for name, disc in discounts.items():
        price_col = f"{name} Price Pound"
        label     = f"-{round((1 - disc) * 100)}%"
        disc_col  = f"{name} {label} Pound"
        if price_col in combined.columns:
            combined[price_col] = pd.to_numeric(combined[price_col], errors="coerce").round(0)
            combined[disc_col]  = (combined[price_col] * disc).round(0)

    # % diff vs ROUK
    if "ROUK" in discounts and "ROUK Price Pound" in combined.columns:
        rouk_disc     = discounts["ROUK"]
        rouk_label    = f"-{round((1 - rouk_disc) * 100)}%"
        rouk_disc_col = f"ROUK {rouk_label} Pound"
        if rouk_disc_col in combined.columns:
            for name in discounts:
                if name == "ROUK":
                    continue
                disc     = discounts[name]
                label    = f"-{round((1 - disc) * 100)}%"
                disc_col = f"{name} {label} Pound"
                pct_col  = f"{name} vs ROUK %"
                if disc_col not in combined.columns:
                    continue
                vendor_price = pd.to_numeric(combined[disc_col], errors="coerce").replace(0, float("nan"))
                rouk_price   = pd.to_numeric(combined[rouk_disc_col], errors="coerce").replace(0, float("nan"))
                combined[pct_col] = ((vendor_price - rouk_price) / rouk_price * 100).round(2)
                log.info(f"Added % diff: {pct_col}  ({combined[pct_col].notna().sum():,} valid rows)")

            # diamond-heavon vs ROUK $
            dh_price_col = "diamond-heavon -30% Pound"
            if dh_price_col in combined.columns:
                rouk_vals = pd.to_numeric(combined[rouk_disc_col], errors="coerce").replace(0, float("nan"))
                dh_vals   = pd.to_numeric(combined[dh_price_col], errors="coerce").replace(0, float("nan"))
                combined["diamond-heavon vs ROUK Pound"] = (dh_vals - rouk_vals).round(0)
                log.info(f"Added diamond-heavon vs ROUK Pound  ({combined['diamond-heavon vs ROUK Pound'].notna().sum():,} valid rows)")

            # abelini vs ROUK $
            ab_disc     = discounts.get("abelini", 0.70)
            ab_label    = f"-{round((1 - ab_disc) * 100)}%"
            ab_disc_col = f"abelini {ab_label} Pound"
            if ab_disc_col in combined.columns:
                rouk_vals = pd.to_numeric(combined[rouk_disc_col], errors="coerce").replace(0, float("nan"))
                ab_vals   = pd.to_numeric(combined[ab_disc_col], errors="coerce").replace(0, float("nan"))
                combined["abelini vs ROUK Pound"] = (ab_vals - rouk_vals).round(0)
                combined.to_csv(f"abelini vs ROUK.csv", index=False)
                log.info(f"Added abelini vs ROUK Pound  ({combined['abelini vs ROUK Pound'].notna().sum():,} valid rows)")

    def vendor_list(row):
        return ", ".join(name for name in loaded.keys() if pd.notna(row.get(f"{name} Price Pound")))
    combined["vendors_matched"] = combined.apply(vendor_list, axis=1)

    # Column ordering
    attr_cols = [c for c in list(rouk_EXTRA_COLS.values()) if c in combined.columns]
    base_cols = ["certificate_number", "vendors_matched"] + attr_cols
    VENDOR_ORDER = ["diamond-heavon", "abelini", "ROUK"]
    extra_vendors = [n for n in loaded.keys() if n not in VENDOR_ORDER and n != "ROUK"]
    ordered_vendors = (
        [n for n in VENDOR_ORDER if n in loaded and n != "ROUK"]
        + extra_vendors
        + (["ROUK"] if "ROUK" in loaded else [])
    )
    vendor_cols = []
    for name in ordered_vendors:
        disc  = discounts.get(name, 0.70)
        label = f"-{round((1 - disc) * 100)}%"
        for col in [
            f"{name} Price Pound",
            f"{name} {label} Pound",
            f"{name} vs ROUK Pound",
            f"{name} vs ROUK %",
        ]:
            if col in combined.columns:
                vendor_cols.append(col)
    other_cols = [c for c in combined.columns if c not in base_cols + vendor_cols]
    combined   = combined[base_cols + vendor_cols + other_cols]

    def to_json_records(df: pd.DataFrame) -> list:
        import math
        out = df.drop(columns=["_all_matched", "_some_matched", "_vendor_count"], errors="ignore").copy()
        p_cols = [c for c in out.columns if "Price Pound" in c or c.endswith("Pound")]
        for col in p_cols:
            out[col] = pd.to_numeric(out[col], errors="coerce")
            out[col] = out[col].where(out[col].notna() & (out[col] != 0), other=None)
        pct_cols = [c for c in out.columns if c.endswith("%")]
        for col in pct_cols:
            out[col] = pd.to_numeric(out[col], errors="coerce")
            out[col] = out[col].where(out[col].notna(), other=None)
        records = out.where(out.notna(), other=None).to_dict(orient="records")
        clean = []
        for row in records:
            clean_row = {}
            for k, v in row.items():
                if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
                    clean_row[k] = None
                else:
                    clean_row[k] = v
            clean.append(clean_row)
        return clean

    df_common  = combined[combined["_all_matched"]].copy()
    df_partial = combined[combined["_some_matched"]].copy()
    df_all     = combined.copy()

    common_records  = to_json_records(df_common)
    partial_records = to_json_records(df_partial)
    all_records     = to_json_records(df_all)

    with open(COMPARE_JSON, "w", encoding="utf-8") as f:
        json.dump(common_records, f, ensure_ascii=False, indent=2)
    with open(COMPARE_PARTIAL_JSON, "w", encoding="utf-8") as f:
        json.dump(partial_records, f, ensure_ascii=False, indent=2)
    with open(COMPARE_ALL_JSON, "w", encoding="utf-8") as f:
        json.dump(all_records, f, ensure_ascii=False, indent=2)

    df_common.drop(columns=["_all_matched", "_some_matched", "_vendor_count"], errors="ignore").to_csv(COMPARE_CSV, index=False)

    log.info(f"compare.json ({len(common_records):,}) | compare_partial.json ({len(partial_records):,}) | compare_all.json ({len(all_records):,})")
    return combined, common_records, partial_records, all_records


# ══════════════════════════════════════════════════════════════
# STEP 2 — EXCEL COMPARISON SHEETS
# ══════════════════════════════════════════════════════════════

def categorize_diff(val):
    if pd.isna(val): return "N/A"
    val = abs(val)
    if val <= 100:    return "0-100"
    elif val <= 200:  return "100-200"
    elif val <= 300:  return "200-300"
    elif val <= 400:  return "300-400"
    elif val <= 500:  return "400-500"
    elif val <= 1000: return "500-1000"
    else:             return "1000+"


CATEGORY_COLORS = {
    "0-100": "C6EFCE", "100-200": "FFEB9C", "200-300": "FFE066",
    "300-400": "FFCC99", "400-500": "FFA07A",
    "500-1000": "FF9999", "1000+": "CC0000", "N/A": "D9D9D9",
}
DARK_CATEGORIES = {"1000+"}


def _write_sheet_with_formatting(df, writer, sheet_name, highlight_col=None, category_col=None):
    try:
        def highlight_loss(v):
            return ["background-color: #FF9999" if x < 0 else "" for x in v]
        if highlight_col and highlight_col in df.columns:
            styled = df.style.apply(highlight_loss, subset=[highlight_col])
            styled.to_excel(writer, sheet_name=sheet_name, index=False)
        else:
            df.to_excel(writer, sheet_name=sheet_name, index=False)
    except AttributeError:
        df.to_excel(writer, sheet_name=sheet_name, index=False)

    ws      = writer.sheets[sheet_name]
    headers = [ws.cell(row=1, column=c).value for c in range(1, ws.max_column + 1)]
    for col_idx in range(1, ws.max_column + 1):
        ws.cell(row=1, column=col_idx).font = Font(bold=True)
    if category_col and category_col in headers:
        cat_col_idx = headers.index(category_col) + 1
        for row in range(2, len(df) + 2):
            cell  = ws.cell(row=row, column=cat_col_idx)
            cat   = cell.value
            color = CATEGORY_COLORS.get(str(cat), "FFFFFF")
            cell.fill = PatternFill("solid", start_color=color, fgColor=color)
            cell.font = Font(color="FFFFFF" if cat in DARK_CATEGORIES else "000000", bold=True)
    for col_idx, col_cells in enumerate(ws.columns, 1):
        max_len = max((len(str(c.value)) if c.value else 0) for c in col_cells)
        ws.column_dimensions[get_column_letter(col_idx)].width = min(max_len + 4, 40)


def compare_two_from_combined(combined: pd.DataFrame, name_a, discount_a, name_b, discount_b) -> pd.DataFrame:
    label_a    = f"-{round((1 - discount_a) * 100)}%"
    label_b    = f"-{round((1 - discount_b) * 100)}%"
    price_a    = f"{name_a} Price Pound"
    price_b    = f"{name_b} Price Pound"
    disc_col_a = f"{name_a} {label_a} Pound"
    disc_col_b = f"{name_b} {label_b} Pound"
    needed = ["certificate_number", price_a, disc_col_a, price_b, disc_col_b]
    avail  = [c for c in needed if c in combined.columns]
    df     = combined[avail].dropna(subset=[price_a, price_b]).copy()
    if df.empty:
        return pd.DataFrame()
    compare_col = f"Compare ({name_b}{label_b} - {name_a}{label_a}) Pound"
    if disc_col_a in df.columns and disc_col_b in df.columns:
        df[compare_col] = (df[disc_col_b] - df[disc_col_a]).round(2)
    return df.reset_index(drop=True)


def compare_all_three_from_combined(combined: pd.DataFrame, discounts: dict, names: list, writer):
    price_cols = [f"{n} Price Pound" for n in names]
    df = combined.dropna(subset=price_cols).copy()
    if df.empty:
        return False
    disc_cols = {}
    for name in names:
        disc     = discounts[name]
        label    = f"-{round((1 - disc) * 100)}%"
        disc_col = f"{name} {label} Pound"
        disc_cols[name] = disc_col
        if disc_col not in df.columns:
            df[disc_col] = (pd.to_numeric(df[f"{name} Price Pound"], errors="coerce") * disc).round(0)
    n = names
    diff_ab = f"{n[0]} vs {n[1]} Diff Pound"
    diff_bc = f"{n[1]} vs {n[2]} Diff Pound"
    diff_ca = f"{n[2]} vs {n[0]} Diff Pound"
    df[diff_ab]          = (df[disc_cols[n[0]]] - df[disc_cols[n[1]]]).abs().round(2)
    df[diff_bc]          = (df[disc_cols[n[1]]] - df[disc_cols[n[2]]]).abs().round(2)
    df[diff_ca]          = (df[disc_cols[n[2]]] - df[disc_cols[n[0]]]).abs().round(2)
    df["Min Diff Pound"] = df[[diff_ab, diff_bc, diff_ca]].min(axis=1).round(2)
    df["Category"]       = df["Min Diff Pound"].apply(categorize_diff)
    sheet_name = " vs ".join(names)[:31]
    _write_sheet_with_formatting(df, writer, sheet_name, category_col="Category")
    return True


def compare_all_vendors(vendors, PC_csv=None, PC_discount=0.70, default_discount=0.70):
    log.info("=" * 50)
    log.info("Starting vendor comparison pipeline")

    loaded    = {}
    discounts = {}
    for v in vendors:
        loaded[v["name"]]    = load_vendor(
            v["csv"], v["cert_col"], v["price_col"], v["name"],
            link_col=v.get("link_col"), link_builder=v.get("link_builder"),
        )
        discounts[v["name"]] = v.get("discount", default_discount)
    if PC_csv:
        loaded["ROUK"]    = load_ROUK(PC_csv)
        discounts["ROUK"] = PC_discount

    # STEP 1: build compare files
    result = build_compare_files(loaded, discounts)
    if isinstance(result, tuple):
        combined, common_records, partial_records, all_records = result
    else:
        combined = result
        common_records = partial_records = all_records = []
    generate_vendor_vs_rouk_csv(combined, discounts)
    if combined is None or (hasattr(combined, "empty") and combined.empty):
        log.error("compare.csv is empty — aborting.")
        sys.exit(1)
    # Save status
    _save_diamond_status(combined, loaded, discounts, common_records, partial_records, all_records)


# ══════════════════════════════════════════════════════════════
# FILE AGE CHECK
# ══════════════════════════════════════════════════════════════

def get_env(key, default):
    return os.getenv(key, default)


def check_file_age(filepath: str, label: str) -> dict:
    if not os.path.exists(filepath):
        log.error(f"[{label}] MISSING: {filepath}")
        return {"file": os.path.basename(filepath), "vendor": label,
                "status": "MISSING", "last_modified": None, "age_days": None, "ok": False}
    mtime    = os.path.getmtime(filepath)
    mod_dt   = datetime.datetime.fromtimestamp(mtime)
    age_days = (datetime.datetime.now() - mod_dt).days
    is_fresh = age_days < MAX_AGE_DAYS
    status   = "OK" if is_fresh else "STALE"
    if is_fresh:
        log.info(f"[{label}] {status} — {os.path.basename(filepath)} ({age_days}d old)")
    else:
        log.warning(f"[{label}] {status} — {os.path.basename(filepath)} ({age_days}d old, last: {mod_dt.strftime('%Y-%m-%d %H:%M:%S')})")
    return {
        "file": os.path.basename(filepath), "vendor": label,
        "status": status, "last_modified": mod_dt.strftime("%Y-%m-%d %H:%M:%S"),
        "age_days": age_days, "ok": is_fresh,
    }


def check_all_files_and_run():
    log.info("=" * 50)
    log.info("FILE AGE CHECK")
    vendors, PC_csv, PC_discount = load_vendors_from_env()
    all_files = [(v["csv"], v["name"]) for v in vendors] + [(PC_csv, "ROUK")]
    statuses  = []
    for fpath, label in all_files:
        status = check_file_age(fpath, label)
        statuses.append(status)
    status_report = {
        "checked_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "all_fresh":  all(s["ok"] for s in statuses),
        "files":      statuses,
    }
    os.makedirs(DIAMOND_FILES_DIR, exist_ok=True)
    with open(FILE_STATUS_JSON, "w") as f:
        json.dump(status_report, f, indent=2)
    missing = [s for s in statuses if s["status"] == "MISSING"]
    if missing:
        log.error(f"ABORT: missing files: {[m['file'] for m in missing]}")
        sys.exit(1)
    return compare_all_vendors(vendors=vendors, PC_csv=PC_csv, PC_discount=PC_discount)


if __name__ == "__main__":
    check_all_files_and_run()
    sys.exit(0)