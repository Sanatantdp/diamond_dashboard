"""
sort_vpn_configs.py
===================
Test every .ovpn in vpnbook/ root against all 9 scraper APIs, then copy
each passing config into its scraper's dedicated sub-folder.

Folder layout created inside vpnbook/:

    vpnbook/
      usa/
        brilliance/       <- configs that pass Brilliance API
        loosegrown/       <- configs that pass Loosegrown API
        friendly/         <- configs that pass Friendly Diamonds API
      uk/
        abelini/          <- configs that pass Abelini API
        diamond_heaven/   <- configs that pass Diamond Heaven API
      aus/
        gs/               <- configs that pass GS Diamonds API
        cullen/           <- configs that pass Cullen Jewellery API
      germany/
        novita/           <- configs that pass Novita Diamonds API
        renesim/          <- configs that pass Renesim API
        77diamonds/       <- configs that pass 77 Diamonds API

Dead configs (VPN won't connect) are deleted from vpnbook/ root.
Configs that connect but all APIs block them stay in the root untouched.

Usage
-----
  python sort_vpn_configs.py                       # dry-run – prints plan, touches nothing
  python sort_vpn_configs.py --apply               # run tests + copy files + delete dead
  python sort_vpn_configs.py --apply --connect-timeout 60
"""

from __future__ import annotations

import argparse
import shutil
import sys
import time
from pathlib import Path
from typing import Any

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

_ROOT   = Path(__file__).resolve().parent
VPN_DIR = _ROOT / "vpnbook"

if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

# Import full-fidelity Chrome-impersonating test functions from test_all_apis.
from test_all_apis import (  # noqa: E402
    test_brilliance,
    test_loosegrown,
    test_friendly,
    test_abelini,
    test_diamond_heaven,
    test_cullen,
    test_gs_diamonds as test_gs,
    test_novita,
    test_renesim,
    test_77diamonds,
)

# ── Scraper → (region_folder, scraper_folder, test_fn) ───────────────────────
#
#   vpnbook/<region>/<scraper>/  is where passing .ovpn files are copied.
#
ALL_SCRAPERS: list[tuple[str, str, str, Any]] = [
    # key             region     scraper_dir       test_fn
    ("brilliance",    "usa",     "brilliance",     test_brilliance),
    ("loosegrown",    "usa",     "loosegrown",     test_loosegrown),
    ("friendly",      "usa",     "friendly",       test_friendly),
    ("abelini",       "uk",      "abelini",        test_abelini),
    ("diamond-heaven", "uk",     "diamond_heaven", test_diamond_heaven),
    ("gs-diamonds",   "aus",     "gs",             test_gs),
    ("cullen",        "aus",     "cullen",         test_cullen),
    ("novita",        "germany", "novita",         test_novita),
    ("renesim",       "germany", "renesim",        test_renesim),
    ("77diamonds",    "germany", "77diamonds",     test_77diamonds),
]


def scraper_dir(key: str) -> Path:
    """Return the target directory for a given scraper key."""
    for k, region, folder, _ in ALL_SCRAPERS:
        if k == key:
            return VPN_DIR / region / folder
    raise KeyError(key)


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Test vpnbook/*.ovpn against 9 diamond scraper APIs and copy "
            "passing configs into region/scraper sub-folders."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--apply", action="store_true",
                        help="Actually copy/delete files (default: dry-run).")
    parser.add_argument("--connect-timeout", type=int, default=45,
                        help="Seconds to wait for VPN tunnel (default: 45).")
    args = parser.parse_args()

    from vpn_rotator import VPNConnection, VPNError, is_admin, kill_openvpn_processes

    if not is_admin():
        sys.exit("Administrator required — re-run from an elevated PowerShell.")

    configs = sorted(VPN_DIR.glob("*.ovpn"))
    if not configs:
        sys.exit(f"No .ovpn files found in {VPN_DIR}/")

    # No forced auth_file: VPNConnection resolves the right credentials per
    # config (proton / veepn / etc.) automatically based on its filename.
    mode = "APPLY" if args.apply else "DRY-RUN"

    # Show target folder layout
    print(f"\n{'=' * 72}")
    print(f"  sort_vpn_configs  —  mode: {mode}")
    print(f"  {len(configs)} config(s) in {VPN_DIR}/")
    print(f"  connect-timeout : {args.connect_timeout}s")
    print(f"\n  Target folders:")
    prev_region = None
    for key, region, folder, _ in ALL_SCRAPERS:
        if region != prev_region:
            print(f"    vpnbook/{region}/")
            prev_region = region
        print(f"      {folder}/")
    print(f"{'=' * 72}\n")

    # Create target directories upfront (dry-run skips)
    if args.apply:
        for _, region, folder, _ in ALL_SCRAPERS:
            (VPN_DIR / region / folder).mkdir(parents=True, exist_ok=True)

    # Accumulators
    by_scraper: dict[str, list[str]] = {k: [] for k, *_ in ALL_SCRAPERS}
    dead_configs: list[Path] = []
    blocked_all_count = 0

    start_all = time.perf_counter()
    try:
        for idx, cfg in enumerate(configs, 1):
            print(f"[{idx:>2}/{len(configs)}] {cfg.name}")
            connect_ok = False
            passing: set[str] = set()

            try:
                with VPNConnection(
                    cfg,
                    verify_ip=True,
                    connect_timeout=args.connect_timeout,
                ) as vpn:
                    connect_ok = True
                    print(f"           tunnel up  exit IP={vpn.ip_after}")

                    for key, region, folder, test_fn in ALL_SCRAPERS:
                        res = test_fn()
                        ok  = bool(res.get("ok"))
                        if ok:
                            passing.add(key)
                            by_scraper[key].append(cfg.name)
                        sc  = res.get("status_code") or res.get("error", "ERR")
                        ms  = res.get("elapsed_ms", "?")
                        tag = "OK  " if ok else "FAIL"
                        dest = f"vpnbook/{region}/{folder}/"
                        print(f"           {tag}  [{region.upper():>3}] {key:<16} {sc}  ({ms} ms)  -> {dest if ok else ''}")

            except VPNError as exc:
                print(f"           CONNECT FAILED  {exc}")
                dead_configs.append(cfg)
            except Exception as exc:
                print(f"           ERROR  {exc.__class__.__name__}: {exc}")
                dead_configs.append(cfg)

            if connect_ok:
                if passing:
                    # Copy .ovpn into each scraper folder it passed
                    for key in passing:
                        _, region, folder, _ = next(
                            (k, r, f, t) for k, r, f, t in ALL_SCRAPERS if k == key
                        )
                        dst_dir = VPN_DIR / region / folder
                        dst     = dst_dir / cfg.name
                        if args.apply:
                            dst_dir.mkdir(parents=True, exist_ok=True)
                            if not dst.exists():
                                shutil.copy2(cfg, dst)
                        action = "copied" if args.apply else "would copy"
                        print(f"           {action} -> vpnbook/{region}/{folder}/{cfg.name}")
                else:
                    blocked_all_count += 1
                    print(f"           (connected but all APIs blocked — stays in root)")
            else:
                action = "deleted" if args.apply else "would delete"
                if args.apply:
                    try:
                        cfg.unlink()
                    except FileNotFoundError:
                        pass
                print(f"           {action} dead config: {cfg.name}")

            print()

    except KeyboardInterrupt:
        print("\nInterrupted — tearing down any lingering tunnel.")
        try:
            kill_openvpn_processes()
        except Exception:
            pass

    total = time.perf_counter() - start_all
    h, rem = divmod(int(total), 3600)
    m, s   = divmod(rem, 60)

    # ── Final summary ──────────────────────────────────────────────────────────
    print(f"\n{'=' * 72}")
    print(f"  RESULTS  —  {len(configs)} tested  ({h}h {m}m {s}s)  mode: {mode}")
    print(f"{'=' * 72}")

    prev_region = None
    for key, region, folder, _ in ALL_SCRAPERS:
        if region != prev_region:
            print(f"\n  [{region.upper()}]  vpnbook/{region}/")
            prev_region = region
        n = len(by_scraper[key])
        print(f"    {folder:<18}  {n:>3} config(s)  -> vpnbook/{region}/{folder}/")

    print(f"\n  connected / all-blocked : {blocked_all_count:>3}  (left in root)")
    print(f"  failed to connect       : {len(dead_configs):>3}  ({'deleted' if args.apply else 'would delete'})")

    if not args.apply:
        print(f"\n  DRY-RUN — no files copied or deleted.")
        print(f"  Re-run with --apply to copy configs and delete dead ones.")
    else:
        print(f"\n  Done. Check vpnbook/usa/ vpnbook/uk/ vpnbook/roa/ vpnbook/rog/")
    print()
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\nInterrupted.")
        sys.exit(130)
