# ══════════════════════════════════════════════════════════════
# ADD THIS BLOCK to your existing comparator.py
# Place it after the build_compare_files() function
# ══════════════════════════════════════════════════════════════

# New per-vendor CSV output directory
import datetime
import json
from logging import log
import os

import pandas as pd

from diamond_compare import ROG_EXTRA_COLS


VENDOR_CSV_DIR = os.path.join(os.getcwd(), "compare", "vendor_common")

def build_vendor_common_csvs(combined: pd.DataFrame, discounts: dict, loaded: dict):
    os.makedirs(VENDOR_CSV_DIR, exist_ok=True)
    vendor_names = [n for n in loaded.keys() if n != "ROG"]
    summary = {
        "generated_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "vendors": {}
    }

    for name in vendor_names:
        price_col = f"{name} Price EUR"
        rog_col   = "ROG Price EUR"

        if price_col not in combined.columns or rog_col not in combined.columns:
            log.warning(f"Skipping vendor CSV for '{name}' — missing columns")
            continue

        # Keep rows where BOTH this vendor AND ROG have a price
        mask = combined[price_col].notna() & combined[rog_col].notna()
        mask &= combined[price_col] != 0
        mask &= combined[rog_col] != 0
        df_vendor = combined[mask].copy()
        df_vendor = df_vendor.drop(
            columns=["_all_matched", "_some_matched", "_vendor_count"],
            errors="ignore"
        )

        # Keep only columns relevant to this vendor + shared attrs
        attr_cols   = [c for c in list(ROG_EXTRA_COLS.values()) if c in df_vendor.columns]
        shared_cols = ["certificate_number", "vendors_matched"] + attr_cols

        disc  = discounts.get(name, 0.70)
        label = f"-{round((1 - disc) * 100)}%"
        rog_disc = discounts.get("ROG", 0.70)
        rog_label = f"-{round((1 - rog_disc) * 100)}%"

        vendor_specific = [
            c for c in df_vendor.columns
            if (name in c) or ("ROG" in c)
        ]
        keep = shared_cols + [c for c in vendor_specific if c in df_vendor.columns]
        keep = list(dict.fromkeys(keep))  # deduplicate preserving order
        df_out = df_vendor[[c for c in keep if c in df_vendor.columns]]

        safe_name = name.replace("/", "_").replace(" ", "_")
        csv_path  = os.path.join(VENDOR_CSV_DIR, f"{safe_name}_common.csv")
        json_path = os.path.join(VENDOR_CSV_DIR, f"{safe_name}_common.json")
        df_out.to_csv(csv_path, index=False)

        # Also write JSON for the dashboard
        import math
        records = []
        for row in df_out.where(df_out.notna(), other=None).to_dict(orient="records"):
            clean = {}
            for k, v in row.items():
                if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
                    clean[k] = None
                else:
                    clean[k] = v
            records.append(clean)

        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(records, f, ensure_ascii=False, indent=2)

        row_count = len(df_out)
        summary["vendors"][name] = {
            "csv":   csv_path,
            "json":  json_path,
            "rows":  row_count,
            "color": {"novita": "#3ecf8e", "77diamonds": "#60a5fa", "renesim": "#a78bfa"}.get(name, "#c9a84c")
        }
        log.info(f"  [{name}] vendor_common CSV → {row_count:,} rows → {csv_path}")

    summary_path = os.path.join(VENDOR_CSV_DIR, "_summary.json")
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)
    log.info(f"Vendor common CSVs done — summary → {summary_path}")
    return summary
