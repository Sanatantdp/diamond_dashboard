"""
test_vpn_scrapers.py
====================

For every .ovpn config in `vpnbook/`: bring up the VPN, run the brilliance
+ loosegrown smoke tests through it, disconnect, move on. Reports which
VPN configs actually unblock each scraper.

Use case
--------
- brilliance API works from your home IP
- loosegrown API returns 403 (Sucuri WAF) from your home IP
- you need to find which Proton exit IPs the loosegrown WAF doesn't block
  so you can route the scraper through one

Requirements
------------
- Administrator / root (OpenVPN needs to manage the TAP/TUN interface)
- vpnbook/proton_credentials.txt present (for ProtonVPN auth)
- OpenVPN installed

Typical runs
------------
    # Test ALL 43 configs (slow: ~45s/config = ~30 min) — but stops early
    # once 3 configs are fully working unless you raise --max-wins.
    python test_vpn_scrapers.py

    # Just hunt for a loosegrown-friendly exit (skips brilliance call):
    python test_vpn_scrapers.py --only loosegrown

    # Try only US/CA/MX:
    python test_vpn_scrapers.py --country us
    python test_vpn_scrapers.py --country ca --max-wins 2

    # First 10 configs, both scrapers, no early-stop:
    python test_vpn_scrapers.py --limit 10 --max-wins 0
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Optional

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

VPN_DIR = Path(__file__).resolve().parent / "vpnbook"
RESULTS_FILE = VPN_DIR / "vpn_scraper_results.json"
WINNERS_FILE = VPN_DIR / "working_configs.txt"


@dataclass
class PerConfigResult:
    name: str
    path: str
    connected: bool = False
    exit_ip: Optional[str] = None
    connect_error: Optional[str] = None
    duration_s: float = 0.0
    brilliance: Optional[dict[str, Any]] = None
    loosegrown: Optional[dict[str, Any]] = None

    def works_for(self, scraper: str) -> bool:
        res = getattr(self, scraper)
        return bool(res and res.get("ok"))


def list_configs(country: Optional[str], limit: Optional[int]) -> list[Path]:
    configs = sorted(VPN_DIR.glob("*.ovpn"))
    if country:
        token = country.lower()
        configs = [c for c in configs if c.name.lower().startswith(f"{token}-")]
    if limit:
        configs = configs[:limit]
    return configs


def run_scraper_tests(targets: list[str]) -> dict[str, dict[str, Any]]:
    """Re-import test_scrapers each time so it sees the new exit IP cleanly."""
    # test_brilliance / test_loosegrown both create a fresh session per call,
    # so re-import isn't strictly required — but it cheaply guards against
    # any module-level caching the libraries do.
    import importlib
    import test_scrapers
    importlib.reload(test_scrapers)

    out: dict[str, dict[str, Any]] = {}
    if "brilliance" in targets:
        out["brilliance"] = test_scrapers.test_brilliance()
    if "loosegrown" in targets:
        out["loosegrown"] = test_scrapers.test_loosegrown()
    return out


def format_scraper_cell(r: Optional[dict[str, Any]]) -> str:
    if r is None:
        return "    -    "
    if r.get("ok"):
        return "   OK    "
    code = r.get("status_code")
    return f"  FAIL {code}" if code else "  FAIL    "


def print_row(idx: int, total: int, r: PerConfigResult) -> None:
    bril = format_scraper_cell(r.brilliance)
    loose = format_scraper_cell(r.loosegrown)
    ip = (r.exit_ip or "-").ljust(15)
    name = r.name
    if not r.connected:
        print(f"  [{idx:>2}/{total}]  conn:FAIL  {name}  ({r.connect_error})")
    else:
        print(f"  [{idx:>2}/{total}]  bril:{bril}  loose:{loose}  exit={ip}  {name}")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Test which VPN configs let brilliance + loosegrown through.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--only",
        choices=["brilliance", "loosegrown"],
        help="Only run one scraper test through each VPN (faster).",
    )
    parser.add_argument(
        "--country",
        type=str,
        default=None,
        help="Only test configs with this country prefix (us, ca, mx, nl, no).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Only test the first N configs after country filter.",
    )
    parser.add_argument(
        "--max-wins",
        type=int,
        default=3,
        help="Stop after this many configs pass ALL requested scrapers. 0 = no early stop. Default: 3.",
    )
    parser.add_argument(
        "--connect-timeout",
        type=int,
        default=45,
        help="Seconds to wait for each tunnel to come up. Default: 45.",
    )
    args = parser.parse_args()

    targets = [args.only] if args.only else ["brilliance", "loosegrown"]

    from vpn_rotator import (
        VPNConnection,
        VPNError,
        get_public_ip,
        is_admin,
        parse_credentials_file,
        write_auth_file,
    )

    if not is_admin():
        sys.exit(
            "This script needs Administrator / root (OpenVPN requires it).\n"
            "  Windows: right-click PowerShell -> 'Run as Administrator', then\n"
            "           cd D:\\usa_scrapping\n"
            "           env\\Scripts\\python.exe test_vpn_scrapers.py\n"
            "  Linux:   sudo python3 test_vpn_scrapers.py"
        )

    configs = list_configs(args.country, args.limit)
    if not configs:
        sys.exit("No .ovpn configs match the filter.")

    # The .ovpn files are named ca-/us-/mx-/nl-/no-... so vpn_rotator's
    # prefix-based credential lookup never finds proton_credentials.txt.
    # Bootstrap a single auth.txt from it and pass it explicitly per connection.
    proton_creds = VPN_DIR / "proton_credentials.txt"
    proton_auth = VPN_DIR / "proton_auth.txt"
    if not proton_creds.exists():
        sys.exit(f"Missing {proton_creds}. Create it with:\n  Username: <user>\n  Password: <pass>")
    user, pwd = parse_credentials_file(proton_creds)
    write_auth_file(user, pwd, proton_auth)
    print(f"Using auth file: {proton_auth.name}")

    baseline_ip = get_public_ip()
    print(f"Baseline public IP: {baseline_ip}")
    print(f"Testing {len(configs)} configs against {targets}")
    if args.max_wins > 0:
        print(f"Will stop early after {args.max_wins} configs pass all scrapers.\n")
    else:
        print()

    results: list[PerConfigResult] = []
    wins = 0
    started = time.perf_counter()

    for i, cfg in enumerate(configs, 1):
        per = PerConfigResult(name=cfg.name, path=str(cfg))
        t0 = time.perf_counter()
        try:
            with VPNConnection(
                cfg,
                auth_file=proton_auth,
                connect_timeout=args.connect_timeout,
                verify_ip=True,
            ) as vpn:
                per.connected = True
                per.exit_ip = vpn.ip_after
                scrapers = run_scraper_tests(targets)
                per.brilliance = scrapers.get("brilliance")
                per.loosegrown = scrapers.get("loosegrown")
        except VPNError as exc:
            per.connect_error = str(exc).splitlines()[0]
        except Exception as exc:  # noqa: BLE001
            per.connect_error = f"{exc.__class__.__name__}: {exc}"
        per.duration_s = round(time.perf_counter() - t0, 1)

        results.append(per)
        print_row(i, len(configs), per)

        if per.connected and all(per.works_for(s) for s in targets):
            wins += 1
            if args.max_wins and wins >= args.max_wins:
                print(f"\nReached --max-wins={args.max_wins}; stopping early.")
                break

        # Brief pause so the tun interface tears down cleanly before the next config.
        time.sleep(2)

    elapsed = time.perf_counter() - started
    print_summary(results, targets, elapsed)
    write_outputs(results, targets)
    return 0 if any(all(r.works_for(s) for s in targets) for r in results) else 1


def print_summary(
    results: list[PerConfigResult],
    targets: list[str],
    elapsed: float,
) -> None:
    print(f"\n{'=' * 70}")
    print(f"Summary — tested {len(results)} configs in {elapsed:.0f}s")
    print(f"{'=' * 70}")
    for s in targets:
        good = [r for r in results if r.works_for(s)]
        print(f"  {s:<11}: {len(good):>2}/{len(results)} configs working")
        for r in good:
            ip = r.exit_ip or "-"
            print(f"      + {r.name:<45}  exit={ip}")
    winners = [r for r in results if all(r.works_for(s) for s in targets)]
    if len(targets) > 1 and winners:
        print(f"\n  Configs that unblock BOTH scrapers ({len(winners)}):")
        for r in winners:
            print(f"      ★ {r.name:<45}  exit={r.exit_ip}")


def write_outputs(results: list[PerConfigResult], targets: list[str]) -> None:
    RESULTS_FILE.write_text(
        json.dumps(
            {
                "generated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
                "targets": targets,
                "total": len(results),
                "results": [asdict(r) for r in results],
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    winners = [r for r in results if all(r.works_for(s) for s in targets)]
    WINNERS_FILE.write_text(
        "\n".join(r.name for r in winners) + ("\n" if winners else ""),
        encoding="utf-8",
    )
    print(f"\nJSON report : {RESULTS_FILE}")
    print(f"Winners list: {WINNERS_FILE}  ({len(winners)} configs)")


if __name__ == "__main__":
    sys.exit(main())
