import os
import pandas as pd
import json
import datetime
from itertools import combinations
from openpyxl.styles import PatternFill, Font
from openpyxl.utils import get_column_letter
from dotenv import load_dotenv
from logger import get_logger

load_dotenv()
log = get_logger("comparator")

# ── Directories ────────────────────────────────────────────────
DIAMOND_FILES_DIR = os.path.join(os.getcwd(), "diamond_files")
COMPARE_DIR       = os.path.join(os.getcwd(), "compare")
FILE_STATUS_JSON  = os.path.join(DIAMOND_FILES_DIR, "file_status.json")
COMPARE_CSV       = os.path.join(COMPARE_DIR, "compare.csv")
COMPARE_JSON      = os.path.join(COMPARE_DIR, "compare.json")
MAX_AGE_DAYS      = 7

os.makedirs(COMPARE_DIR, exist_ok=True)


# ══════════════════════════════════════════════════════════════
# DATA LOADERS
# ══════════════════════════════════════════════════════════════

def load_vendor(csv_path, cert_col, price_col, name):
    log.debug(f"Loading vendor '{name}' from {csv_path}")
    df = pd.read_csv(csv_path, low_memory=False, on_bad_lines="skip")
    df["certificate_number"] = df[cert_col].astype(str).str.strip()
    df = df[["certificate_number", price_col]].copy()
    df = df.rename(columns={price_col: f"{name} Price USD"})
    df[f"{name} Price USD"] = pd.to_numeric(df[f"{name} Price USD"], errors="coerce")
    log.info(f"Loaded '{name}': {len(df):,} rows")
    return df


def load_tdp(csv_path):
    log.debug(f"Loading TDP from {csv_path}")
    df = pd.read_csv(csv_path, low_memory=False, on_bad_lines="skip")
    df = df.rename(columns={"CERTIFICATE_NO": "certificate_number", "TOTAL_PRICE": "TDP Price USD"})
    df["certificate_number"] = df["certificate_number"].astype(str).str.strip()
    df["TDP Price USD"] = pd.to_numeric(df["TDP Price USD"], errors="coerce")
    log.info(f"Loaded TDP: {len(df):,} rows")
    return df[["certificate_number", "TDP Price USD"]]


# ══════════════════════════════════════════════════════════════
# STEP 1 — BUILD compare.csv + compare.json
#           Only certificates present in AT LEAST 2 vendors
# ══════════════════════════════════════════════════════════════

def build_compare_files(loaded: dict, discounts: dict) -> pd.DataFrame:
    """
    1. Outer-join all vendor DataFrames on certificate_number.
    2. Keep only rows where at least 2 vendors have a price (vendor_count >= 2).
    3. Add discounted price columns and % diff vs TDP.
    4. Add vendors_matched column showing which vendors share each cert.
    5. Save to compare/compare.csv and compare/compare.json.
    Returns the filtered combined DataFrame.
    """
    log.info("=" * 50)
    log.info("STEP 1 — Building compare.csv / compare.json")
    log.info(f"Vendors loaded: {list(loaded.keys())}")

    # ── Step A: outer join ALL vendors ───────────────────────
    combined = None
    for name, df in loaded.items():
        combined = df if combined is None else pd.merge(
            combined, df, on="certificate_number", how="outer"
        )

    if combined is None or combined.empty:
        log.error("No data loaded at all — aborting.")
        return pd.DataFrame()

    combined = combined.reset_index(drop=True)
    log.info(f"After outer join: {len(combined):,} total unique certificate numbers")

    # ── Step B: count how many vendors have a price per cert ─
    price_cols = [
        f"{name} Price USD"
        for name in loaded.keys()
        if f"{name} Price USD" in combined.columns
    ]
    log.info(f"Price columns: {price_cols}")

    combined["_vendor_count"] = combined[price_cols].notna().sum(axis=1)

    # Log distribution
    dist = combined["_vendor_count"].value_counts().sort_index()
    for cnt, num_certs in dist.items():
        log.info(f"  Certs in exactly {cnt} vendor(s): {num_certs:,}")

    # ── Step C: keep only certs in >= 2 vendors ──────────────
    before   = len(combined)
    combined = combined[combined["_vendor_count"] >= 2].copy()
    combined = combined.drop(columns=["_vendor_count"])
    combined = combined.reset_index(drop=True)
    dropped  = before - len(combined)

    log.info(f"Kept {len(combined):,} certs in ≥2 vendors  (dropped {dropped:,} single-vendor-only certs)")

    if combined.empty:
        log.error("No certificates shared by at least 2 vendors — compare.csv will be empty.")
        log.error("Check that certificate_number columns are formatted consistently across CSVs.")
        return pd.DataFrame()

    # ── Step D: add discounted price columns ─────────────────
    for name, disc in discounts.items():
        price_col = f"{name} Price USD"
        label     = f"-{round((1 - disc) * 100)}%"
        disc_col  = f"{name} {label} USD"
        if price_col in combined.columns:
            combined[price_col] = pd.to_numeric(combined[price_col], errors="coerce").round(0)
            combined[disc_col]  = (combined[price_col] * disc).round(0)

    # ── Step E: add % diff columns vs TDP ────────────────────
    if "TDP" in discounts and "TDP Price USD" in combined.columns:
        tdp_disc     = discounts["TDP"]
        tdp_label    = f"-{round((1 - tdp_disc) * 100)}%"
        tdp_disc_col = f"TDP {tdp_label} USD"

        for name in discounts:
            if name == "TDP":
                continue
            disc     = discounts[name]
            label    = f"-{round((1 - disc) * 100)}%"
            disc_col = f"{name} {label} USD"
            pct_col  = f"{name} vs TDP %"

            if disc_col in combined.columns and tdp_disc_col in combined.columns:
                combined[pct_col] = (
                    (combined[disc_col] - combined[tdp_disc_col])
                    / combined[tdp_disc_col].replace(0, float("nan"))
                    * 100
                ).round(2)
                log.info(f"Added % diff column: {pct_col}")

    # ── Step F: add vendors_matched helper column ─────────────
    def vendor_list(row):
        return ", ".join(
            name for name in loaded.keys()
            if pd.notna(row.get(f"{name} Price USD"))
        )
    combined["vendors_matched"] = combined.apply(vendor_list, axis=1)

    # ── Step G: reorder columns neatly ───────────────────────
    base_cols   = ["certificate_number", "vendors_matched"]
    vendor_cols = []
    for name in loaded.keys():
        disc  = discounts.get(name, 0.70)
        label = f"-{round((1 - disc) * 100)}%"
        for col in [f"{name} Price USD", f"{name} {label} USD", f"{name} vs TDP %"]:
            if col in combined.columns:
                vendor_cols.append(col)
    other_cols = [c for c in combined.columns if c not in base_cols + vendor_cols]
    combined   = combined[base_cols + vendor_cols + other_cols]

    # ── Step H: save CSV ──────────────────────────────────────
    combined.to_csv(COMPARE_CSV, index=False)
    log.info(f"Saved compare.csv → {COMPARE_CSV}  ({len(combined):,} rows × {len(combined.columns)} cols)")

    # ── Step I: save JSON ─────────────────────────────────────
    records = combined.fillna("").to_dict(orient="records")
    with open(COMPARE_JSON, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)
    log.info(f"Saved compare.json → {COMPARE_JSON}  ({len(records):,} records)")

    log.info("-" * 50)
    log.info(f"STEP 1 COMPLETE — {len(combined):,} certs | {len(combined.columns)} cols")
    log.info(f"  compare.csv  → {COMPARE_CSV}")
    log.info(f"  compare.json → {COMPARE_JSON}")

    return combined


# ══════════════════════════════════════════════════════════════
# STEP 2 — EXCEL COMPARISON SHEETS
# ══════════════════════════════════════════════════════════════

def categorize_diff(val):
    if pd.isna(val):
        return "N/A"
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
    """Write a DataFrame to an Excel sheet with optional highlighting and category colors."""
    try:
        def highlight_loss(v):
            return ["background-color: #FF9999" if x < 0 else "" for x in v]

        if highlight_col and highlight_col in df.columns:
            styled = df.style.apply(highlight_loss, subset=[highlight_col])
            styled.to_excel(writer, sheet_name=sheet_name, index=False)
        else:
            df.to_excel(writer, sheet_name=sheet_name, index=False)
    except AttributeError:
        log.warning("jinja2 not installed — writing without cell highlighting. Run: pip install jinja2")
        df.to_excel(writer, sheet_name=sheet_name, index=False)

    ws      = writer.sheets[sheet_name]
    headers = [ws.cell(row=1, column=c).value for c in range(1, ws.max_column + 1)]

    # Bold headers
    for col_idx in range(1, ws.max_column + 1):
        ws.cell(row=1, column=col_idx).font = Font(bold=True)

    # Color category column
    if category_col and category_col in headers:
        cat_col_idx = headers.index(category_col) + 1
        for row in range(2, len(df) + 2):
            cell  = ws.cell(row=row, column=cat_col_idx)
            cat   = cell.value
            color = CATEGORY_COLORS.get(str(cat), "FFFFFF")
            cell.fill = PatternFill("solid", start_color=color, fgColor=color)
            cell.font = Font(color="FFFFFF" if cat in DARK_CATEGORIES else "000000", bold=True)

    # Auto-width
    for col_idx, col_cells in enumerate(ws.columns, 1):
        max_len = max((len(str(c.value)) if c.value else 0) for c in col_cells)
        ws.column_dimensions[get_column_letter(col_idx)].width = min(max_len + 4, 40)


def compare_two_from_combined(combined: pd.DataFrame, name_a, discount_a, name_b, discount_b) -> pd.DataFrame:
    """Extract a pairwise sheet — only rows where BOTH vendors have a price."""
    label_a    = f"-{round((1 - discount_a) * 100)}%"
    label_b    = f"-{round((1 - discount_b) * 100)}%"
    price_a    = f"{name_a} Price USD"
    price_b    = f"{name_b} Price USD"
    disc_col_a = f"{name_a} {label_a} USD"
    disc_col_b = f"{name_b} {label_b} USD"

    needed = ["certificate_number", "vendors_matched", price_a, disc_col_a, price_b, disc_col_b]
    avail  = [c for c in needed if c in combined.columns]
    df     = combined[avail].dropna(subset=[price_a, price_b]).copy()

    if df.empty:
        log.warning(f"No overlapping rows for '{name_a}' vs '{name_b}' — skipping")
        return pd.DataFrame()

    compare_col = f"Compare ({name_b}{label_b} - {name_a}{label_a}) USD"
    if disc_col_a in df.columns and disc_col_b in df.columns:
        df[compare_col] = (df[disc_col_b] - df[disc_col_a]).round(2)

    df = df.reset_index(drop=True)
    log.info(f"Pairwise '{name_a}' vs '{name_b}': {len(df):,} rows")
    return df


def compare_all_three_from_combined(combined: pd.DataFrame, discounts: dict, names: list, writer):
    """Build the A vs B vs C sheet — only rows where ALL THREE vendors have a price."""
    log.info(f"Building '{' vs '.join(names)}' sheet")

    price_cols = [f"{n} Price USD" for n in names]
    df = combined.dropna(subset=price_cols).copy()

    if df.empty:
        log.warning(f"No rows with all prices for {names} — skipping sheet")
        return False

    disc_cols = {}
    for name in names:
        disc     = discounts[name]
        label    = f"-{round((1 - disc) * 100)}%"
        disc_col = f"{name} {label} USD"
        disc_cols[name] = disc_col
        if disc_col not in df.columns:
            df[disc_col] = (pd.to_numeric(df[f"{name} Price USD"], errors="coerce") * disc).round(0)

    n       = names
    diff_ab = f"{n[0]} vs {n[1]} Diff USD"
    diff_bc = f"{n[1]} vs {n[2]} Diff USD"
    diff_ca = f"{n[2]} vs {n[0]} Diff USD"

    df[diff_ab]        = (df[disc_cols[n[0]]] - df[disc_cols[n[1]]]).abs().round(2)
    df[diff_bc]        = (df[disc_cols[n[1]]] - df[disc_cols[n[2]]]).abs().round(2)
    df[diff_ca]        = (df[disc_cols[n[2]]] - df[disc_cols[n[0]]]).abs().round(2)
    df["Min Diff USD"] = df[[diff_ab, diff_bc, diff_ca]].min(axis=1).round(2)
    df["Category"]     = df["Min Diff USD"].apply(categorize_diff)

    sheet_name = " vs ".join(names)[:31]
    _write_sheet_with_formatting(df, writer, sheet_name, category_col="Category")
    log.info(f"Sheet '{sheet_name}': {len(df):,} rows")
    return True


def compare_all_vendors(vendors, tdp_csv=None, tdp_discount=0.70, default_discount=0.70):
    log.info("=" * 50)
    log.info("Starting vendor comparison pipeline")

    # ── Load all vendor DataFrames ────────────────────────────
    loaded    = {}
    discounts = {}

    for v in vendors:
        loaded[v["name"]]    = load_vendor(v["csv"], v["cert_col"], v["price_col"], v["name"])
        discounts[v["name"]] = v.get("discount", default_discount)

    if tdp_csv:
        loaded["TDP"]    = load_tdp(tdp_csv)
        discounts["TDP"] = tdp_discount

    # ── STEP 1: build compare.csv + compare.json ─────────────
    combined = build_compare_files(loaded, discounts)

    if combined.empty:
        log.error("compare.csv is empty — no Excel will be generated.")
        return {}

    # ── STEP 2: build Excel from combined data ────────────────
    log.info("=" * 50)
    log.info("STEP 2 — Generating Excel comparison sheets")

    names          = list(loaded.keys())
    pairs          = list(combinations(names, 2))
    timestamp      = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file    = os.path.join(COMPARE_DIR, f"vendor_comparison_{timestamp}.xlsx")
    sheets_written = 0
    results        = {}

    with pd.ExcelWriter(output_file, engine="openpyxl") as writer:

        # ── Sheet 1: full combined (≥2 vendor matches) ───────
        try:
            _write_sheet_with_formatting(combined, writer, "All Vendors Combined")
            sheets_written += 1
            log.info(f"Sheet 'All Vendors Combined': {len(combined):,} rows")
        except Exception as e:
            log.error(f"Failed writing combined sheet: {e}")

        # ── Pairwise sheets ───────────────────────────────────
        for name_a, name_b in pairs:
            try:
                disc_a = discounts.get(name_a, default_discount)
                disc_b = discounts.get(name_b, default_discount)
                merged = compare_two_from_combined(combined, name_a, disc_a, name_b, disc_b)

                if merged.empty:
                    continue

                sheet_name  = f"{name_a} vs {name_b}"[:31]
                label_a     = f"-{round((1 - disc_a) * 100)}%"
                label_b     = f"-{round((1 - disc_b) * 100)}%"
                compare_col = f"Compare ({name_b}{label_b} - {name_a}{label_a}) USD"

                _write_sheet_with_formatting(merged, writer, sheet_name, highlight_col=compare_col)
                sheets_written += 1
                results[(name_a, name_b)] = merged

            except Exception as e:
                log.error(f"Failed sheet '{name_a}' vs '{name_b}': {e}")
                continue

        # ── A vs B vs C sheet ─────────────────────────────────
        try:
            vendor_names = [n for n in names if n != "TDP"]
            trio         = vendor_names[:3] if len(vendor_names) >= 3 else names[:3]
            if len(trio) >= 3:
                ok = compare_all_three_from_combined(combined, discounts, trio, writer)
                if ok:
                    sheets_written += 1
        except Exception as e:
            log.error(f"Failed A vs B vs C sheet: {e}")

        # ── Safety placeholder ────────────────────────────────
        if sheets_written == 0:
            log.error("No sheets written — inserting placeholder")
            ph       = writer.book.create_sheet("No Data")
            ph["A1"] = "No overlapping certificate numbers found across vendors."

    log.info(f"Excel saved → {output_file}  ({sheets_written} sheets)")
    log.info("=" * 50)
    return results


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


def load_vendors_from_env():
    vendor_defaults = [
        {"name": "loose-grown", "env_csv": "LOOSE_GROWN_CSV", "env_cert": "LOOSE_GROWN_CERT_COL", "env_price": "LOOSE_GROWN_PRICE_COL", "env_disc": "LOOSE_GROWN_DISCOUNT", "default_csv": "loosegrowndiamond.csv",    "default_cert": "sku",                "default_price": "price",            "default_disc": "0.70"},
        {"name": "brilliance",  "env_csv": "BRILLIANCE_CSV",  "env_cert": "BRILLIANCE_CERT_COL",  "env_price": "BRILLIANCE_PRICE_COL",  "env_disc": "BRILLIANCE_DISCOUNT",  "default_csv": "brilliance_diamonds.csv", "default_cert": "reportNumber",       "default_price": "price",            "default_disc": "0.70"},
        {"name": "luvansh",     "env_csv": "LUVANSH_CSV",     "env_cert": "LUVANSH_CERT_COL",     "env_price": "LUVANSH_PRICE_COL",     "env_disc": "LUVANSH_DISCOUNT",     "default_csv": "luvansh_diamonds.csv",    "default_cert": "certificate_number", "default_price": "discounted_price", "default_disc": "1.00"},
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
        })
        log.debug(f"Vendor '{v['name']}': csv={csv_name}, cert={cert_col}, price={price_col}, discount={discount}")

    tdp_csv_name = get_env("TDP_CSV", "PC_diamonds.csv")
    tdp_csv      = os.path.join(DIAMOND_FILES_DIR, tdp_csv_name)
    tdp_discount = float(get_env("TDP_DISCOUNT", "0.70"))

    return vendors, tdp_csv, tdp_discount


def check_all_files_and_run():
    log.info("=" * 50)
    log.info("FILE AGE CHECK")

    vendors, tdp_csv, tdp_discount = load_vendors_from_env()
    all_files  = [(v["csv"], v["name"]) for v in vendors] + [(tdp_csv, "TDP")]
    statuses   = []
    stale_warn = []

    for fpath, label in all_files:
        status = check_file_age(fpath, label)
        statuses.append(status)
        if not status["ok"] and status["status"] == "STALE":
            stale_warn.append(status)

    status_report = {
        "checked_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "all_fresh":  all(s["ok"] for s in statuses),
        "files":      statuses,
    }
    os.makedirs(DIAMOND_FILES_DIR, exist_ok=True)
    with open(FILE_STATUS_JSON, "w") as f:
        json.dump(status_report, f, indent=2)
    log.info(f"Status report saved → {FILE_STATUS_JSON}")

    if stale_warn:
        log.warning(f"{len(stale_warn)} file(s) older than {MAX_AGE_DAYS} days:")
        for s in stale_warn:
            log.warning(f"  {s['file']} — {s['age_days']}d old, last: {s['last_modified']}")

    missing = [s for s in statuses if s["status"] == "MISSING"]
    if missing:
        log.error(f"ABORT: missing files: {[m['file'] for m in missing]}")
        return None

    return compare_all_vendors(vendors=vendors, tdp_csv=tdp_csv, tdp_discount=tdp_discount)


if __name__ == "__main__":
    check_all_files_and_run()