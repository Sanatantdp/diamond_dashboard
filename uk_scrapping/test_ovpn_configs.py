"""
test_ovpn_configs.py
====================

Test every .ovpn config in `vpnbook/` to see which servers are currently
working. Two modes:

    Fast (default) — TCP-probe every `remote host port` in each config,
    concurrently. Catches dead/unreachable servers in ~30 seconds. No
    Administrator privileges required.

        python test_ovpn_configs.py
        python test_ovpn_configs.py --timeout 4 --concurrency 30

    Full           — Actually run OpenVPN against each config, wait for
    "Initialization Sequence Completed", verify the exit IP changed,
    then disconnect. Catches auth / overload / routing issues too.
    Slow (~30s per config) and requires Administrator.

        python test_ovpn_configs.py --full
        python test_ovpn_configs.py --full --country us

Output:
    - Pretty progress + final summary printed to the console.
    - JSON report written to `vpnbook/test_results.json`.
"""

from __future__ import annotations

import argparse
import json
import re
import socket
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Optional

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

VPN_DIR = Path(__file__).resolve().parent / "vpnbook"
RESULTS_FILE = VPN_DIR / "test_results.json"

_REMOTE_RX = re.compile(r"^\s*remote\s+(\S+)\s+(\d+)", re.MULTILINE)


@dataclass
class Endpoint:
    host: str
    port: int
    ok: bool = False
    latency_ms: Optional[float] = None
    error: Optional[str] = None


@dataclass
class ConfigResult:
    name: str
    path: str
    endpoints: list[Endpoint] = field(default_factory=list)
    reachable: bool = False
    # Full-mode-only fields:
    tunneled: Optional[bool] = None
    ip_before: Optional[str] = None
    ip_after: Optional[str] = None
    duration_s: Optional[float] = None
    error: Optional[str] = None


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------
def parse_remotes(config_path: Path) -> list[tuple[str, int]]:
    """Extract every `remote <host> <port>` from a .ovpn file."""
    text = config_path.read_text(encoding="utf-8", errors="ignore")
    return [(host, int(port)) for host, port in _REMOTE_RX.findall(text)]


# ---------------------------------------------------------------------------
# Fast mode — TCP probe
# ---------------------------------------------------------------------------
def probe_endpoint(host: str, port: int, timeout: float) -> Endpoint:
    ep = Endpoint(host=host, port=port)
    start = time.perf_counter()
    try:
        with socket.create_connection((host, port), timeout=timeout):
            ep.ok = True
            ep.latency_ms = round((time.perf_counter() - start) * 1000, 1)
    except (socket.timeout, OSError) as exc:
        ep.error = str(exc) or exc.__class__.__name__
    return ep


def test_config_fast(config_path: Path, timeout: float) -> ConfigResult:
    result = ConfigResult(name=config_path.name, path=str(config_path))
    remotes = parse_remotes(config_path)
    if not remotes:
        result.error = "no 'remote' lines found in config"
        return result
    for host, port in remotes:
        result.endpoints.append(probe_endpoint(host, port, timeout))
    result.reachable = any(ep.ok for ep in result.endpoints)
    return result


def run_fast_mode(
    configs: list[Path],
    timeout: float,
    concurrency: int,
) -> list[ConfigResult]:
    results: list[ConfigResult] = []
    total = len(configs)
    print(f"[fast] probing {total} configs (timeout={timeout}s, concurrency={concurrency})")
    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        futures = {pool.submit(test_config_fast, c, timeout): c for c in configs}
        for i, fut in enumerate(as_completed(futures), 1):
            res = fut.result()
            status = "OK " if res.reachable else "DEAD"
            best = min(
                (ep.latency_ms for ep in res.endpoints if ep.ok),
                default=None,
            )
            latency_str = f"{best:6.1f}ms" if best is not None else "   --   "
            print(f"  [{i:>2}/{total}] {status}  {latency_str}  {res.name}")
            results.append(res)
    return results


# ---------------------------------------------------------------------------
# Full mode — actually connect via OpenVPN
# ---------------------------------------------------------------------------
def run_full_mode(configs: list[Path], connect_timeout: int) -> list[ConfigResult]:
    # Imported here so fast mode doesn't pull in OpenVPN / requests.
    from vpn_rotator import VPNConnection, VPNError, get_public_ip, is_admin

    if not is_admin():
        sys.exit(
            "Full mode requires Administrator / root.\n"
            "  Windows: right-click PowerShell -> 'Run as Administrator', then\n"
            "           cd D:\\usa_scrapping ; .\\env\\Scripts\\activate ; "
            "python test_ovpn_configs.py --full\n"
            "  Linux:   sudo python3 test_ovpn_configs.py --full"
        )

    baseline_ip = get_public_ip()
    print(f"[full] baseline public IP: {baseline_ip}")
    results: list[ConfigResult] = []
    total = len(configs)

    for i, cfg in enumerate(configs, 1):
        res = ConfigResult(name=cfg.name, path=str(cfg), ip_before=baseline_ip)
        # Still fill in endpoints so the JSON report has remote details.
        res.endpoints = [Endpoint(host=h, port=p) for h, p in parse_remotes(cfg)]
        start = time.perf_counter()
        print(f"  [{i:>2}/{total}] connecting  {cfg.name} ...", flush=True)
        try:
            with VPNConnection(
                cfg,
                connect_timeout=connect_timeout,
                verify_ip=True,
            ) as vpn:
                res.ip_after = vpn.ip_after
                res.tunneled = bool(vpn.ip_after and vpn.ip_after != baseline_ip)
                res.reachable = True
        except VPNError as exc:
            res.tunneled = False
            res.error = str(exc).splitlines()[0]
        except Exception as exc:  # noqa: BLE001
            res.tunneled = False
            res.error = f"{exc.__class__.__name__}: {exc}"
        res.duration_s = round(time.perf_counter() - start, 1)

        status = "OK  " if res.tunneled else "FAIL"
        ip = res.ip_after or "—"
        err = f"  ({res.error})" if res.error else ""
        print(
            f"  [{i:>2}/{total}] {status}  {res.duration_s:5.1f}s  "
            f"exit={ip:<15}  {cfg.name}{err}"
        )
        results.append(res)
        # Brief pause so the tun/tap interface fully tears down before next config.
        time.sleep(2)

    return results


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------
def write_report(results: list[ConfigResult], mode: str) -> None:
    payload = {
        "mode": mode,
        "generated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "total": len(results),
        "ok": sum(1 for r in results if (r.tunneled if mode == "full" else r.reachable)),
        "results": [asdict(r) for r in results],
    }
    RESULTS_FILE.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"\nJSON report: {RESULTS_FILE}")


def print_summary(results: list[ConfigResult], mode: str) -> None:
    key = (lambda r: r.tunneled) if mode == "full" else (lambda r: r.reachable)
    good = [r for r in results if key(r)]
    bad = [r for r in results if not key(r)]
    print(f"\n{'=' * 70}")
    print(f"Summary: {len(good)}/{len(results)} configs working  ({mode} mode)")
    print(f"{'=' * 70}")
    if bad:
        print("\nNot working:")
        for r in bad:
            reason = r.error or "no endpoint reachable"
            print(f"  - {r.name}: {reason}")
    if good:
        print("\nWorking:")
        for r in good:
            print(f"  + {r.name}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main() -> int:
    parser = argparse.ArgumentParser(
        description="Test every .ovpn config in vpnbook/ for reachability.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--full",
        action="store_true",
        help="Actually connect via OpenVPN (slow, needs Admin). "
        "Default is a fast TCP probe.",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=5.0,
        help="Per-endpoint TCP timeout in seconds for fast mode "
        "(or per-config connect timeout in full mode). Default: 5.",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=20,
        help="Parallel TCP probes in fast mode (ignored in --full). Default: 20.",
    )
    parser.add_argument(
        "--country",
        type=str,
        default=None,
        help="Only test configs matching this country code (e.g. us, ca, mx).",
    )
    parser.add_argument(
        "--dir",
        type=Path,
        default=VPN_DIR,
        help=f"Directory containing .ovpn files (default: {VPN_DIR}).",
    )
    args = parser.parse_args()

    if not args.dir.exists():
        sys.exit(f"Config directory not found: {args.dir}")

    configs = sorted(args.dir.glob("*.ovpn"))
    if args.country:
        token = args.country.lower()
        configs = [c for c in configs if c.name.lower().startswith(f"{token}-")]
    if not configs:
        sys.exit("No .ovpn configs found to test.")

    start = time.perf_counter()
    if args.full:
        results = run_full_mode(configs, connect_timeout=int(args.timeout) or 45)
        mode = "full"
    else:
        results = run_fast_mode(configs, args.timeout, args.concurrency)
        mode = "fast"
    elapsed = time.perf_counter() - start

    print_summary(results, mode)
    print(f"\nTotal time: {elapsed:.1f}s")
    write_report(results, mode)
    return 0


if __name__ == "__main__":
    sys.exit(main())
