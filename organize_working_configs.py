from __future__ import annotations

import argparse
import json
import shutil
import sys
from pathlib import Path

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

VPN_DIR = Path(__file__).resolve().parent / "vpnbook"
RESULTS_FILE = VPN_DIR / "vpn_scraper_results.json"
SCRAPERS = ("brilliance", "loosegrown")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--dry-run", action="store_true", help="Show what would be copied, but don't change anything.")
    args = parser.parse_args()

    if not RESULTS_FILE.exists():
        sys.exit(
            f"Missing {RESULTS_FILE}.\n"
            "Run `test_vpn_scrapers.py` first to generate the report."
        )

    payload = json.loads(RESULTS_FILE.read_text(encoding="utf-8"))
    results = payload.get("results", [])
    if not results:
        sys.exit("Report has no results — nothing to organize.")

    # Build per-scraper lists from the JSON.
    per_scraper: dict[str, list[Path]] = {s: [] for s in SCRAPERS}
    for entry in results:
        if not entry.get("connected"):
            continue
        src = Path(entry["path"])
        if not src.exists():
            print(f"  [skip] missing source file: {src}")
            continue
        for s in SCRAPERS:
            r = entry.get(s)
            if r and r.get("ok"):
                per_scraper[s].append(src)

    # Wipe + repopulate each subfolder.
    for s in SCRAPERS:
        target_dir = VPN_DIR / s
        sources = per_scraper[s]
        print(f"\n{s}: {len(sources)} working configs")
        if args.dry_run:
            for src in sources:
                print(f"  would copy: {src.name}  ->  {target_dir / src.name}")
            continue

        target_dir.mkdir(exist_ok=True)
        # Clear stale .ovpn files only — leave anything else (notes, etc.) alone.
        for old in target_dir.glob("*.ovpn"):
            old.unlink()
        for src in sources:
            dst = target_dir / src.name
            shutil.copy2(src, dst)
            print(f"  + {dst.relative_to(VPN_DIR)}")

    if args.dry_run:
        print("\n(dry run — no files written)")
    else:
        print("\nDone. Now run scrapers via:")
        print("  env\\Scripts\\python.exe run_with_vpn.py --scraper brilliance")
        print("  env\\Scripts\\python.exe run_with_vpn.py --scraper loosegrown")
    return 0


if __name__ == "__main__":
    sys.exit(main())
