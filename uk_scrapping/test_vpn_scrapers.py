"""
test_vpn_scrapers.py
====================

For every .ovpn config in the repo-root `vpnbook/`: bring up the VPN, run the
abelini + diamond-heaven smoke tests through it, disconnect, move on. Reports
which VPN exit IPs unblock EACH API — so you can route abelini through one set
of configs and diamond-heaven through another.

Use case
--------
- abelini / diamond-heaven return 403 (Cloudflare) from some exit IPs
- you have 40+ Proton configs and want to know which ones each API accepts
- output is two lists: configs that work for abelini, configs that work for
  diamond-heaven (plus a combined JSON report)

Requirements
------------
- Administrator / root (OpenVPN needs to manage the TAP/TUN interface)
- ../vpnbook/proton_credentials.txt present (for ProtonVPN auth)
- OpenVPN installed
- Run from uk_scrapping/ so `import abelini` / `diamond-heavon` resolve.

Typical runs
------------
    # Test ALL configs, both APIs (slow: ~45s/config) — full separation:
    env\\Scripts\\python.exe test_vpn_scrapers.py

    # Just hunt for an abelini-friendly exit (skips diamond-heaven call):
    env\\Scripts\\python.exe test_vpn_scrapers.py --only abelini

    # Try only CA exits, first 10:
    env\\Scripts\\python.exe test_vpn_scrapers.py --country ca --limit 10

    # Stop once 3 configs pass ALL requested scrapers:
    env\\Scripts\\python.exe test_vpn_scrapers.py --max-wins 3
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Optional

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

# Configs + shared ProtonVPN credentials live in the repo-root vpnbook/.
VPN_DIR = Path(__file__).resolve().parent.parent / "vpnbook"
RESULTS_FILE = VPN_DIR / "uk_vpn_scraper_results.json"

# scraper key -> winners-list filename
WINNERS_FILES = {
    "abelini": VPN_DIR / "working_configs_abelini.txt",
    "diamond-heaven": VPN_DIR / "working_configs_diamond_heaven.txt",
}

ALL_TARGETS = ["abelini", "diamond-heaven"]


@dataclass
class PerConfigResult:
    name: str
    path: str
    connected: bool = False
    exit_ip: Optional[str] = None
    connect_error: Optional[str] = None
    duration_s: float = 0.0
    abelini: Optional[dict[str, Any]] = None
    diamond_heaven: Optional[dict[str, Any]] = None

    # map a target key to its result attribute (handles the hyphen in the key)
    _ATTR = {"abelini": "abelini", "diamond-heaven": "diamond_heaven"}

    def works_for(self, scraper: str) -> bool:
        res = getattr(self, self._ATTR[scraper])
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
    """Run the requested smoke tests under the currently-active tunnel."""
    import test_scrapers

    out: dict[str, dict[str, Any]] = {}
    if "abelini" in targets:
        out["abelini"] = test_scrapers.test_abelini()
    if "diamond-heaven" in targets:
        out["diamond-heaven"] = test_scrapers.test_diamond_heaven()
    return out


def format_scraper_cell(r: Optional[dict[str, Any]]) -> str:
    if r is None:
        return "    -    "
    if r.get("ok"):
        return "   OK    "
    code = r.get("status_code")
    return f"  FAIL {code}" if code else "  FAIL    "


def print_row(idx: int, total: int, r: PerConfigResult) -> None:
    abe = format_scraper_cell(r.abelini)
    dh = format_scraper_cell(r.diamond_heaven)
    ip = (r.exit_ip or "-").ljust(15)
    if not r.connected:
        print(f"  [{idx:>2}/{total}]  conn:FAIL  {r.name}  ({r.connect_error})")
    else:
        print(f"  [{idx:>2}/{total}]  abelini:{abe}  dheaven:{dh}  exit={ip}  {r.name}")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Find which VPN exit IPs unblock abelini vs diamond-heaven.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--only",
        choices=ALL_TARGETS,
        help="Only run one scraper test through each VPN (faster).",
    )
    parser.add_argument(
        "--country",
        type=str,
        default=None,
        help="Only test configs with this country prefix (ca, us, mx, nl, no, ...).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Only test the first N configs after the country filter.",
    )
    parser.add_argument(
        "--max-wins",
        type=int,
        default=0,
        help="Stop after this many configs pass ALL requested scrapers. "
        "0 = no early stop (full scan). Default: 0.",
    )
    parser.add_argument(
        "--connect-timeout",
        type=int,
        default=45,
        help="Seconds to wait for each tunnel to come up. Default: 45.",
    )
    args = parser.parse_args()

    targets = [args.only] if args.only else list(ALL_TARGETS)

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
            "           cd D:\\diamond_data_scrapping\\uk_scrapping\n"
            "           ..\\env\\Scripts\\python.exe test_vpn_scrapers.py\n"
            "  Linux:   sudo python3 test_vpn_scrapers.py"
        )

    configs = list_configs(args.country, args.limit)
    if not configs:
        sys.exit(f"No .ovpn configs match the filter in {VPN_DIR}.")

    # ProtonVPN configs are named by country (ca-/us-/...), so vpn_rotator's
    # prefix-based credential lookup never finds proton_credentials.txt.
    # Bootstrap a single auth.txt from it and pass it explicitly per connection.
    proton_creds = VPN_DIR / "proton_credentials.txt"
    proton_auth = VPN_DIR / "proton_auth.txt"
    if not proton_creds.exists():
        sys.exit(f"Missing {proton_creds}. Create it with:\n  Username: <user>\n  Password: <pass>")
    user, pwd = parse_credentials_file(proton_creds)
    write_auth_file(user, pwd, proton_auth)
    openvpn_log = VPN_DIR / "openvpn.log"
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
                log_file=openvpn_log,
                connect_timeout=args.connect_timeout,
                verify_ip=True,
            ) as vpn:
                per.connected = True
                per.exit_ip = vpn.ip_after
                scrapers = run_scraper_tests(targets)
                per.abelini = scrapers.get("abelini")
                per.diamond_heaven = scrapers.get("diamond-heaven")
        except VPNError as exc:
            per.connect_error = str(exc).splitlines()[0]
        except Exception as exc:  # noqa: BLE001
            per.connect_error = f"{exc.__class__.__name__}: {exc}"
        per.duration_s = round(time.perf_counter() - t0, 1)

        results.append(per)
        print_row(i, len(configs), per)
        # Persist after every config so a long run is crash-safe.
        write_outputs(results, targets)

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
    return 0 if any(r.connected for r in results) else 1


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
        print(f"  {s:<15}: {len(good):>2}/{len(results)} configs working")
        for r in good:
            print(f"      + {r.name:<45}  exit={r.exit_ip or '-'}")
    if len(targets) > 1:
        winners = [r for r in results if all(r.works_for(s) for s in targets)]
        print(f"\n  Configs that unblock BOTH APIs ({len(winners)}):")
        for r in winners:
            print(f"      * {r.name:<45}  exit={r.exit_ip}")


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
    # One winners file per scraper so each API's working exits are separated.
    for s in targets:
        good = [r for r in results if r.works_for(s)]
        WINNERS_FILES[s].write_text(
            "\n".join(r.name for r in good) + ("\n" if good else ""),
            encoding="utf-8",
        )


if __name__ == "__main__":
    sys.exit(main())
