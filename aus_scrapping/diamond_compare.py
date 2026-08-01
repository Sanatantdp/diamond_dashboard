import os
import sys
from pathlib import Path
import pandas as pd
import json
import datetime
from dotenv import load_dotenv
from logger import get_logger

load_dotenv()
log = get_logger("comparator")

# ── Directories ────────────────────────────────────────────────
REPO_ROOT                = Path(__file__).resolve().parent.parent
DIAMOND_FILES_DIR        = str(REPO_ROOT / "diamond_files" / "aus")
COMPARE_DIR              = str(REPO_ROOT / "compare" / "roa")
FILE_STATUS_JSON         = os.path.join(DIAMOND_FILES_DIR, "file_status.json")
COMPARE_JSON             = os.path.join(COMPARE_DIR, "compare.json")
COMPARE_PARTIAL_JSON     = os.path.join(COMPARE_DIR, "compare_partial.json")
COMPARE_ALL_JSON         = os.path.join(COMPARE_DIR, "compare_all.json")
COMPARE_OVERLAP_JSON     = os.path.join(COMPARE_DIR, "compare_vendor_overlap.json")
DIAMOND_STATUS_JSON      = os.path.join(COMPARE_DIR, "diamond_status.json")
MAX_AGE_DAYS             = 7

os.makedirs(COMPARE_DIR, exist_ok=True)


# ══════════════════════════════════════════════════════════════
# DATA LOADERS
# ══════════════════════════════════════════════════════════════

# ── Vendor product-page link builders ───────────────────────────
# Each takes the raw value of a vendor's "link_col" (see vendor_defaults in
# load_vendors_from_env) and returns the diamond's product-page URL, or None
# when the source value is missing/unusable. GS already stores a full product
# URL per row, so its "builder" is just a pass-through/clean-up. Cullen keys
# its cert-image URL off an internal "id" — the product-page URL below is
# built from that same subdomain pattern (unverified against a live page;
# confirm/correct if it 404s).

def _cullen_link(diamond_id) -> str | None:
    diamond_id = str(diamond_id).strip() if pd.notna(diamond_id) else ""
    if not diamond_id or diamond_id.lower() == "nan":
        return None
    return f"https://cullenjewellery.com/diamonds/{diamond_id}"


def _gs_link(url) -> str | None:
    url = str(url).strip() if pd.notna(url) else ""
    if not url or url.lower() == "nan":
        return None
    return url


def load_vendor(csv_path, cert_col, price_col, name, currency="USD", link_col=None, link_builder=None):
    log.debug(f"Loading vendor '{name}' from {csv_path}")
    df = pd.read_csv(csv_path, low_memory=False, on_bad_lines="skip")
    df["certificate_number"] = df[cert_col].astype(str).str.strip()
    keep_cols = ["certificate_number", price_col]
    has_link  = link_col and link_builder and link_col in df.columns
    if has_link:
        keep_cols.append(link_col)
    df = df[keep_cols].copy()
    price_label = f"{name} Price {currency}"
    df = df.rename(columns={price_col: price_label})
    df[price_label] = pd.to_numeric(df[price_label], errors="coerce")
    if has_link:
        df[f"{name} Link"] = df[link_col].apply(link_builder)
        df = df.drop(columns=[link_col])
        log.info(f"  '{name}': {df[f'{name} Link'].notna().sum():,} rows have a product link")
    log.info(f"Loaded '{name}': {len(df):,} rows")
    return df


roa_EXTRA_COLS = {
    "shape":       "shape",
    "color":       "color",
    "clarity":     "clarity",
    "cut":         "cut",
    "caratWeight": "caratWeight",
}

def load_ROA(csv_path, cert_col="certificateNumber", price_col="finalPriceAud"):
    log.debug(f"Loading ROA from {csv_path}")
    df = pd.read_csv(csv_path, low_memory=False, on_bad_lines="skip")
    df = df.rename(columns={cert_col: "certificate_number", price_col: "ROA Price AUD"})
    df["certificate_number"] = df["certificate_number"].astype(str).str.strip()
    df["ROA Price AUD"] = pd.to_numeric(df["ROA Price AUD"], errors="coerce")

    for src_col, dest_col in roa_EXTRA_COLS.items():
        for variant in [src_col, src_col.lower(), src_col.title()]:
            if variant in df.columns:
                df[dest_col] = df[variant].astype(str).str.strip().str.title()
                log.debug(f"  ROA extra col: {variant} → {dest_col}")
                break

    keep = ["certificate_number", "ROA Price AUD"] + [
        dest for dest in roa_EXTRA_COLS.values() if dest in df.columns
    ]
    log.info(f"Loaded ROA: {len(df):,} rows | extra cols: {[c for c in keep if c not in ('certificate_number','ROA Price AUD')]}")
    return df[keep]


# ══════════════════════════════════════════════════════════════
# STEP 1 — BUILD compare files
# ══════════════════════════════════════════════════════════════

def get_env(key, default):
    return os.getenv(key, default)


def _price_col(df: pd.DataFrame, name: str) -> str | None:
    """Find the '{name} Price {currency}' column for a vendor, whatever its
    currency label is (USD, AUD, ...) — avoids hardcoding one currency."""
    prefix = f"{name} Price "
    return next((c for c in df.columns if c.startswith(prefix)), None)


def load_vendors_from_env():
    vendor_defaults = [
        {
            "name": "cullen", "env_csv": "CULLEN_CSV", "env_cert": "CULLEN_CERT_COL",
            "env_price": "CULLEN_PRICE_COL", "env_disc": "CULLEN_DISCOUNT",
            "default_csv": "cullen_diamonds.csv", "default_cert": "certificate_number",
            "default_price": "price", "default_disc": "1", "currency": "AUD",
            "link_col": "id", "link_builder": _cullen_link,
        },
        {
            "name": "gs", "env_csv": "GS_CSV", "env_cert": "GS_CERT_COL",
            "env_price": "GS_PRICE_COL", "env_disc": "GS_DISCOUNT",
            "default_csv": "gs_diamonds.csv", "default_cert": "cert_no",
            "default_price": "price_aud", "default_disc": "1", "currency": "AUD",
            "link_col": "url", "link_builder": _gs_link,
        },
    ]

    vendors = []
    for v in vendor_defaults:
        csv_name  = get_env(v["env_csv"],   v["default_csv"])
        cert_col  = get_env(v["env_cert"],  v["default_cert"])
        price_col = get_env(v["env_price"], v["default_price"])
        discount  = float(get_env(v["env_disc"], v["default_disc"]))
        vendors.append({
            "name": v["name"], "csv": os.path.join(DIAMOND_FILES_DIR, csv_name),
            "cert_col": cert_col, "price_col": price_col, "discount": discount, "currency": v["currency"],
            "link_col": v["link_col"], "link_builder": v["link_builder"],
        })

    roa_csv_name  = get_env("ROA_CSV", "roa_diamonds.csv")
    roa_cert_col  = get_env("ROA_CERT_COL", "certificateNumber")
    roa_price_col = get_env("ROA_PRICE_COL", "finalPriceUsd")
    roa_csv       = os.path.join(DIAMOND_FILES_DIR, roa_csv_name)
    roa_discount  = float(get_env("ROA_DISCOUNT", "0.70"))
    return vendors, roa_csv, roa_cert_col, roa_price_col, roa_discount


def _collect_vendor_file_dates() -> dict:
    """Last-modified timestamps for each vendor source CSV. Returns
    {roa, cullen, gs} -> 'YYYY-MM-DD HH:MM:SS' (or None if missing)."""
    vendors, roa_csv, _roa_cert, _roa_price, _roa_disc = load_vendors_from_env()
    paths = {"roa": roa_csv}
    for v in vendors:
        paths[v["name"]] = v["csv"]
    out: dict = {}
    for key, path in paths.items():
        if path and os.path.exists(path):
            mtime = os.path.getmtime(path)
            out[key] = datetime.datetime.fromtimestamp(mtime).strftime("%Y-%m-%d %H:%M:%S")
        else:
            out[key] = None
    return out


def _save_diamond_status(combined: pd.DataFrame, loaded: dict,
                          common_records: list, partial_records: list,
                          all_records: list, overlap_records: list):
    vendor_names = [n for n in loaded.keys() if n != "ROA"]
    roa_total    = len(loaded["ROA"]) if "ROA" in loaded else 0

    vendor_totals = {name: len(loaded[name]) for name in vendor_names if name in loaded}

    roa_vs_vendors = {}
    for name in vendor_names:
        price_col = _price_col(combined, name)
        if price_col is None:
            roa_vs_vendors[name] = {"matched": 0, "vendor_total": vendor_totals.get(name, 0)}
            continue
        matched = int(combined[price_col].notna().sum())
        roa_vs_vendors[name] = {
            "matched":           matched,
            "vendor_total":      vendor_totals.get(name, 0),
            "vendor_not_in_ROA": vendor_totals.get(name, 0) - matched,
            "match_rate_pct":    round(matched / roa_total * 100, 2) if roa_total else 0,
        }

    status = {
        "generated_at":    datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "ROA_total":       roa_total,
        "vendor_totals":   vendor_totals,
        "ROA_vs_vendors":  roa_vs_vendors,
        "dataset_counts": {
            "common":         len(common_records),
            "partial":        len(partial_records),
            "all_roa":        len(all_records),
            "vendor_overlap": len(overlap_records),
        },
        "file_dates": _collect_vendor_file_dates(),
    }

    with open(DIAMOND_STATUS_JSON, "w", encoding="utf-8") as f:
        json.dump(status, f, ensure_ascii=False, indent=2)
    log.info(f"Saved diamond_status.json → {DIAMOND_STATUS_JSON}")


def build_compare_files(loaded: dict, discounts: dict) -> tuple:
    log.info("=" * 50)
    log.info("STEP 1 — Building compare files")
    log.info(f"Vendors loaded: {list(loaded.keys())}")

    if "ROA" not in loaded:
        log.warning("ROA not loaded — falling back to outer join")
        combined = None
        for name, df in loaded.items():
            combined = df if combined is None else pd.merge(combined, df, on="certificate_number", how="outer")
    else:
        combined = loaded["ROA"].copy()
        log.info(f"ROA master list: {len(combined):,} cert IDs")
        for name, df in loaded.items():
            if name == "ROA":
                continue
            combined = pd.merge(combined, df, on="certificate_number", how="left")
            matched = combined[_price_col(combined, name)].notna().sum()
            log.info(f"  '{name}': {matched:,} matched ROA")

    if combined is None or combined.empty:
        log.error("No data loaded — aborting.")
        return pd.DataFrame(), [], [], [], []

    combined = combined.reset_index(drop=True)

    # Resolve each vendor's actual price column once — vendors/ROA are not
    # all in the same currency (e.g. GS/ROA are AUD, Cullen is USD), so the
    # column name's currency suffix varies and must not be hardcoded.
    price_col_map = {}
    for name in loaded.keys():
        col = _price_col(combined, name)
        if col:
            price_col_map[name] = col
        else:
            log.warning(f"  '{name}': no price column found after merge")

    price_cols = list(price_col_map.values())
    combined["_vendor_count"] = combined[price_cols].notna().sum(axis=1)

    # A vendor only counts toward "common" if it actually has at least one
    # match against the ROA master list this run. Without this, a vendor
    # stuck at 0 matches (e.g. a cert-number format mismatch) would
    # permanently zero out "common" for every other vendor too. This makes
    # the requirement self-healing: if that vendor starts matching again on
    # a later run, it's automatically required again.
    CORE_VENDORS = ["cullen", "gs", "ROA"]
    core_present = []
    for n in CORE_VENDORS:
        col = price_col_map.get(n)
        if col is None:
            continue
        if n == "ROA" or combined[col].notna().sum() > 0:
            core_present.append(n)
        else:
            log.warning(f"  '{n}': 0 matches vs ROA this run — excluded from 'common' requirement")
    combined["_all_matched"]  = combined[[price_col_map[n] for n in core_present]].notna().all(axis=1) if core_present else combined["_vendor_count"] >= 2
    combined["_some_matched"] = combined["_vendor_count"] >= 2

    common_count  = combined["_all_matched"].sum()
    partial_count = combined["_some_matched"].sum()
    log.info(f"  Common ({'+'.join(core_present)}): {common_count:,}  |  Partial (2+): {partial_count:,}  |  All ROA: {len(combined):,}")

    # Discounted price columns
    for name, disc in discounts.items():
        price_col = price_col_map.get(name)
        if price_col is None:
            continue
        currency  = price_col.rsplit(" ", 1)[-1]
        label     = f"-{round((1 - disc) * 100)}%"
        disc_col  = f"{name} {label} {currency}"
        combined[price_col] = pd.to_numeric(combined[price_col], errors="coerce").round(0)
        combined[disc_col]  = (combined[price_col] * disc).round(0)

    # vendor vs ROA (compared against ROA's discounted baseline, e.g. "ROA -30% AUD")
    roa_price_col = price_col_map.get("ROA")
    roa_disc_col  = None
    if roa_price_col:
        roa_disc      = discounts.get("ROA", 0.70)
        roa_currency  = roa_price_col.rsplit(" ", 1)[-1]
        roa_label     = f"-{round((1 - roa_disc) * 100)}%"
        roa_disc_col  = f"ROA {roa_label} {roa_currency}"
    if roa_disc_col and roa_disc_col in combined.columns:
        for name in discounts:
            if name == "ROA":
                continue
            price_col = price_col_map.get(name)
            if price_col is None:
                continue
            currency = price_col.rsplit(" ", 1)[-1]
            diff_col = f"{name} vs ROA {currency}"
            pct_col  = f"{name} vs ROA %"
            vendor_price = pd.to_numeric(combined[price_col], errors="coerce").replace(0, float("nan"))
            roa_price    = pd.to_numeric(combined[roa_disc_col], errors="coerce").replace(0, float("nan"))
            combined[diff_col] = (vendor_price - roa_price).round(2)
            combined[pct_col]  = ((vendor_price - roa_price) / roa_price * 100).round(2)
            log.info(f"Added {diff_col} / {pct_col}  ({combined[pct_col].notna().sum():,} valid rows)")

    def vendor_list(row):
        return ", ".join(name for name in loaded.keys() if pd.notna(row.get(price_col_map.get(name))))
    combined["vendors_matched"] = combined.apply(vendor_list, axis=1)

    # Column ordering
    attr_cols = [c for c in list(roa_EXTRA_COLS.values()) if c in combined.columns]
    base_cols = ["certificate_number", "vendors_matched"] + attr_cols
    VENDOR_ORDER = ["cullen", "gs"]
    ordered_vendors = [n for n in VENDOR_ORDER if n in loaded] + (["ROA"] if "ROA" in loaded else [])
    vendor_cols = []
    for name in ordered_vendors:
        price_col = price_col_map.get(name)
        if price_col is None:
            continue
        currency = price_col.rsplit(" ", 1)[-1]
        disc     = discounts.get(name, 1)
        label    = f"-{round((1 - disc) * 100)}%"
        for col in [
            price_col,
            f"{name} {label} {currency}",
            f"{name} vs ROA {currency}",
            f"{name} vs ROA %",
            f"{name} Link",
        ]:
            if col in combined.columns:
                vendor_cols.append(col)
    other_cols = [c for c in combined.columns if c not in base_cols + vendor_cols]
    combined   = combined[base_cols + vendor_cols + other_cols]

    def to_json_records(df: pd.DataFrame) -> list:
        import math
        out = df.drop(columns=["_all_matched", "_some_matched", "_vendor_count"], errors="ignore").copy()
        p_cols = [c for c in out.columns if c.endswith("USD") or c.endswith("AUD")]
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
    df_overlap = combined[
        combined.get(price_col_map.get("cullen"), pd.Series(dtype=float)).notna()
        & combined.get(price_col_map.get("gs"), pd.Series(dtype=float)).notna()
    ].copy()

    common_records  = to_json_records(df_common)
    partial_records = to_json_records(df_partial)
    all_records      = to_json_records(df_all)

    overlap_cols = ["certificate_number"] + attr_cols + [
        c for c in [price_col_map.get("cullen"), price_col_map.get("gs"), price_col_map.get("ROA"),
                    "cullen vs ROA %", "gs vs ROA %"]
        if c and c in df_overlap.columns
    ]
    overlap_records = to_json_records(df_overlap[overlap_cols]) if not df_overlap.empty else []

    with open(COMPARE_JSON, "w", encoding="utf-8") as f:
        json.dump(common_records, f, ensure_ascii=False, indent=2)
    with open(COMPARE_PARTIAL_JSON, "w", encoding="utf-8") as f:
        json.dump(partial_records, f, ensure_ascii=False, indent=2)
    with open(COMPARE_ALL_JSON, "w", encoding="utf-8") as f:
        json.dump(all_records, f, ensure_ascii=False, indent=2)
    with open(COMPARE_OVERLAP_JSON, "w", encoding="utf-8") as f:
        json.dump(overlap_records, f, ensure_ascii=False, indent=2)

    log.info(
        f"compare.json ({len(common_records):,}) | compare_partial.json ({len(partial_records):,}) | "
        f"compare_all.json ({len(all_records):,}) | compare_vendor_overlap.json ({len(overlap_records):,})"
    )
    return combined, common_records, partial_records, all_records, overlap_records


# ══════════════════════════════════════════════════════════════
# STEP 2 — RUN PIPELINE
# ══════════════════════════════════════════════════════════════

def compare_all_vendors(vendors, roa_csv, roa_cert_col, roa_price_col, roa_discount):
    log.info("=" * 50)
    log.info("Starting vendor comparison pipeline")

    loaded    = {}
    discounts = {}
    for v in vendors:
        loaded[v["name"]]    = load_vendor(
            v["csv"], v["cert_col"], v["price_col"], v["name"],
            currency=v.get("currency", "USD"),
            link_col=v.get("link_col"), link_builder=v.get("link_builder"),
        )
        discounts[v["name"]] = v.get("discount", 1.0)
    if roa_csv:
        loaded["ROA"]    = load_ROA(roa_csv, cert_col=roa_cert_col, price_col=roa_price_col)
        discounts["ROA"] = roa_discount

    # STEP 1: build compare files
    combined, common_records, partial_records, all_records, overlap_records = build_compare_files(loaded, discounts)
    if combined is None or (hasattr(combined, "empty") and combined.empty):
        log.error("compare data is empty — aborting.")
        sys.exit(1)

    # Save status
    _save_diamond_status(combined, loaded, common_records, partial_records, all_records, overlap_records)

    # Upload all JSON files to PostgreSQL (truncate + insert)
    try:
        import importlib.util as _ilu
        _db_path = Path(__file__).resolve().parent / "roa_compare_to_db.py"
        _spec = _ilu.spec_from_file_location("roa_compare_to_db", _db_path)
        roa_compare_to_db = _ilu.module_from_spec(_spec)
        _spec.loader.exec_module(roa_compare_to_db)
        log.info("=" * 50)
        log.info("UPLOADING ROA JSON FILES TO POSTGRESQL")
        roa_compare_to_db.main()
        log.info("PostgreSQL upload complete.")
    except Exception as exc:
        log.error(f"PostgreSQL upload failed (compare files are still saved locally): {exc}")
        sys.exit(1)


# ══════════════════════════════════════════════════════════════
# FILE AGE CHECK
# ══════════════════════════════════════════════════════════════

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
    vendors, roa_csv, roa_cert_col, roa_price_col, roa_discount = load_vendors_from_env()
    all_files = [(v["csv"], v["name"]) for v in vendors] + [(roa_csv, "ROA")]
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
    return compare_all_vendors(vendors, roa_csv, roa_cert_col, roa_price_col, roa_discount)


if __name__ == "__main__":
    check_all_files_and_run()
    sys.exit(0)
