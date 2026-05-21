"""
run_with_vpn.py
===============

Launch a scraper (brilliance or loosegrown) through a VPN tunnel drawn
from its dedicated, pre-vetted folder of working .ovpn configs:

    vpnbook/brilliance/   ← used when --scraper brilliance
    vpnbook/loosegrown/   ← used when --scraper loosegrown

The folders are populated by organize_working_configs.py.

Flow per run:
    1. Pick a config (random by default; specific via --config NAME)
    2. Bring up the VPN tunnel
    3. Verify the public IP changed
    4. subprocess-launch the matching scraper script
    5. Stream its output to the terminal
    6. On exit, tear the VPN down (always, even on Ctrl-C)
    7. On non-zero exit + --rotate-on-fail, retry with the next config

Examples:
    # Random config, single attempt
    python run_with_vpn.py --scraper brilliance
    python run_with_vpn.py --scraper loosegrown

    # Pin to a specific config
    python run_with_vpn.py --scraper brilliance --config us-free-3.protonvpn.tcp.ovpn

    # Auto-rotate on failure (up to 3 different configs)
    python run_with_vpn.py --scraper loosegrown --rotate-on-fail --max-attempts 3

Requirements:
    - Administrator / root (OpenVPN needs to manage TAP/TUN)
    - vpnbook/proton_credentials.txt (your OpenVPN/IKEv2 credentials)
    - The scraper folders must be populated (run organize_working_configs.py first)
"""

from __future__ import annotations

import argparse
import random
import subprocess
import sys
from pathlib import Path

# Windows console defaults to cp1252 which can't encode em-dashes / arrows / stars
# present in our help text. Force UTF-8 so argparse --help never blows up.
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

PROJECT_ROOT = Path(__file__).resolve().parent
VPN_DIR = PROJECT_ROOT / "vpnbook"

SCRAPER_SCRIPTS = {
    "brilliance": PROJECT_ROOT / "brilliance_diamonds.py",
    "loosegrown": PROJECT_ROOT / "loosegrown_diamonds.py",
}


def pick_configs(scraper: str, specific: str | None, shuffle: bool) -> list[Path]:
    folder = VPN_DIR / scraper
    if not folder.exists():
        sys.exit(
            f"Missing folder: {folder}\n"
            f"Run `python organize_working_configs.py` first to populate it."
        )
    configs = sorted(folder.glob("*.ovpn"))
    if not configs:
        sys.exit(
            f"No .ovpn files in {folder}.\n"
            f"Re-run test_vpn_scrapers.py + organize_working_configs.py to refresh."
        )
    if specific:
        match = [c for c in configs if c.name == specific or c.stem == specific]
        if not match:
            sys.exit(
                f"--config '{specific}' not found in {folder}.\n"
                f"Available: {', '.join(c.name for c in configs)}"
            )
        return match
    if shuffle:
        random.shuffle(configs)
    return configs


def run_scraper(scraper_path: Path) -> int:
    """Run the scraper as a subprocess using the same Python interpreter."""
    cmd = [sys.executable, str(scraper_path)]
    print(f"\n=== launching scraper: {' '.join(cmd)} ===", flush=True)
    # No capture — let the scraper's stdout/stderr stream live to the user.
    try:
        proc = subprocess.run(cmd, cwd=str(PROJECT_ROOT), check=False)
        return proc.returncode
    except KeyboardInterrupt:
        print("\nInterrupted — scraper subprocess will be terminated.", flush=True)
        return 130


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run a scraper through one of its pre-vetted VPN configs.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--scraper", required=True, choices=list(SCRAPER_SCRIPTS), help="Which scraper to run.")
    parser.add_argument("--config", help="Use this specific .ovpn (filename in vpnbook/<scraper>/). Default: random.")
    parser.add_argument("--rotate-on-fail", action="store_true", help="On non-zero scraper exit, rotate to the next config and retry.")
    parser.add_argument("--max-attempts", type=int, default=1, help="Cap retries when --rotate-on-fail. Default 1 (no rotation).")
    parser.add_argument("--connect-timeout", type=int, default=45, help="Seconds to wait for each tunnel. Default 45.")
    args = parser.parse_args()

    from vpn_rotator import (
        VPNConnection,
        VPNError,
        is_admin,
        parse_credentials_file,
        write_auth_file,
    )

    if not is_admin():
        sys.exit(
            "OpenVPN requires Administrator / root.\n"
            "  Windows: right-click PowerShell -> 'Run as Administrator', then\n"
            "           cd D:\\usa_scrapping ; env\\Scripts\\python.exe run_with_vpn.py ..."
        )

    # Bootstrap proton_auth.txt from proton_credentials.txt (same trick as test_vpn_scrapers.py).
    proton_creds = VPN_DIR / "proton_credentials.txt"
    proton_auth = VPN_DIR / "proton_auth.txt"
    if not proton_creds.exists():
        sys.exit(f"Missing {proton_creds}. Add Username:/Password: from ProtonVPN OpenVPN/IKEv2 page.")
    user, pwd = parse_credentials_file(proton_creds)
    write_auth_file(user, pwd, proton_auth)

    scraper_path = SCRAPER_SCRIPTS[args.scraper]
    if not scraper_path.exists():
        sys.exit(f"Scraper script not found: {scraper_path}")

    candidates = pick_configs(
        args.scraper,
        specific=args.config,
        shuffle=(args.config is None),
    )
    max_attempts = max(1, args.max_attempts if args.rotate_on_fail else 1)
    print(f"Scraper       : {args.scraper}")
    print(f"Available pool: {len(candidates)} configs in vpnbook/{args.scraper}/")
    print(f"Attempts      : up to {max_attempts}")

    last_exit = 1
    for attempt in range(1, max_attempts + 1):
        cfg = candidates[(attempt - 1) % len(candidates)]
        print(f"\n--- attempt {attempt}/{max_attempts} — config: {cfg.name} ---")
        try:
            with VPNConnection(
                cfg,
                auth_file=proton_auth,
                connect_timeout=args.connect_timeout,
                verify_ip=True,
            ) as vpn:
                print(f"Tunnel up. exit IP = {vpn.ip_after}")
                last_exit = run_scraper(scraper_path)
        except VPNError as exc:
            print(f"[vpn error] {exc}", flush=True)
            last_exit = 2
        except KeyboardInterrupt:
            print("\nInterrupted by user — disconnecting VPN.", flush=True)
            return 130

        if last_exit == 0:
            print(f"\nScraper exited cleanly (rc=0). Done.")
            return 0
        if not args.rotate_on_fail:
            return last_exit
        print(f"Scraper exited rc={last_exit}. Rotating to next config…")

    print(f"\nExhausted {max_attempts} attempts. Last exit code: {last_exit}")
    return last_exit


if __name__ == "__main__":
    sys.exit(main())
