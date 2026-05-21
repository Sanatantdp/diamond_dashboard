"""
test_scrapers.py
================

Smoke-test the brilliance + loosegrown scraper APIs with a single, minimal
request each. Confirms the upstream APIs respond and aren't blocking us
(403 / Cloudflare / geo-block) before kicking off a full scrape run.

Reuses the real headers / payloads / sessions from each scraper module so
the test request looks identical to a production fetch.

Modes
-----
Direct (default) — call the APIs from your current public IP. Fast (~2s).

    python test_scrapers.py
    python test_scrapers.py --only brilliance
    python test_scrapers.py --only loosegrown

Via VPN          — bring up an OpenVPN tunnel (using a .ovpn from vpnbook/),
                   call the APIs through it, then disconnect. Requires Admin.

    python test_scrapers.py --vpn vpnbook/us-free-3.protonvpn.tcp.ovpn
    python test_scrapers.py --vpn us-free-3.protonvpn.tcp.ovpn --only brilliance

Exit code 0 if both scrapers succeed, 1 if any fail.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from contextlib import nullcontext
from pathlib import Path
from typing import Any

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass


def test_brilliance() -> dict[str, Any]:
    """Call the brilliance COUNT endpoint — the cheapest legitimate request."""
    import brilliance_diamonds as bd

    start = time.perf_counter()
    result: dict[str, Any] = {"scraper": "brilliance", "url": bd.COUNT_URL}
    try:
        # bd has a module-level `session` impersonating chrome124 + HEADERS dict.
        resp = bd.session.post(
            bd.COUNT_URL,
            headers=bd.HEADERS,
            json=bd.BASE_PAYLOAD,
            timeout=30,
        )
        result["status_code"] = resp.status_code
        result["elapsed_ms"] = round((time.perf_counter() - start) * 1000, 1)
        if not resp.ok:
            result["ok"] = False
            result["error"] = f"HTTP {resp.status_code}"
            result["body_preview"] = resp.text[:300]
            return result
        data = resp.json()
        # Expected shape: {"count": "576099"}
        raw_count = data.get("count")
        if raw_count is None:
            result["ok"] = False
            result["error"] = "missing 'count' in response"
            result["body_preview"] = json.dumps(data)[:300]
            return result
        result["ok"] = True
        result["total_diamonds"] = int(raw_count)
    except Exception as exc:  # noqa: BLE001
        result["ok"] = False
        result["error"] = f"{exc.__class__.__name__}: {exc}"
        result["elapsed_ms"] = round((time.perf_counter() - start) * 1000, 1)
    return result


def test_loosegrown() -> dict[str, Any]:
    """Call the loosegrown inventory endpoint asking for a single row."""
    import loosegrown_diamonds as lg

    start = time.perf_counter()
    result: dict[str, Any] = {"scraper": "loosegrown", "url": lg.URL}
    try:
        # Build a 1-row probe payload — same shape the scraper sends, just tiny.
        payload = {"ls_start": 1, "ls_per_page": 1, "shape": []}
        with lg._build_session() as session:
            resp = session.post(lg.URL, json=payload, timeout=lg.REQUEST_TIMEOUT)
        result["status_code"] = resp.status_code
        result["elapsed_ms"] = round((time.perf_counter() - start) * 1000, 1)
        if not resp.ok:
            result["ok"] = False
            result["error"] = f"HTTP {resp.status_code}"
            result["body_preview"] = resp.text[:300]
            return result
        data = resp.json()
        # Look for a list of rows — exact key varies, so probe likely keys.
        rows = None
        for key in ("data", "rows", "items", "results", "inventory", "diamonds"):
            v = data.get(key) if isinstance(data, dict) else None
            if isinstance(v, list):
                rows = v
                result["rows_key"] = key
                break
        result["ok"] = True
        result["sample_keys"] = list(data.keys())[:10] if isinstance(data, dict) else None
        result["rows_returned"] = len(rows) if rows is not None else None
    except Exception as exc:  # noqa: BLE001
        result["ok"] = False
        result["error"] = f"{exc.__class__.__name__}: {exc}"
        result["elapsed_ms"] = round((time.perf_counter() - start) * 1000, 1)
    return result


def resolve_vpn_config(arg: str) -> Path:
    """Accept either a full path or just the filename inside vpnbook/."""
    p = Path(arg)
    if p.exists():
        return p
    in_vpnbook = Path("vpnbook") / arg
    if in_vpnbook.exists():
        return in_vpnbook
    raise SystemExit(f"VPN config not found: {arg} (also tried {in_vpnbook})")


def print_result(r: dict[str, Any]) -> None:
    label = r["scraper"].upper()
    status = "OK " if r.get("ok") else "FAIL"
    print(f"\n[{status}] {label}")
    print(f"  url         : {r.get('url')}")
    print(f"  status_code : {r.get('status_code')}")
    print(f"  elapsed_ms  : {r.get('elapsed_ms')}")
    if r["scraper"] == "brilliance" and r.get("ok"):
        print(f"  total_diamonds: {r.get('total_diamonds'):,}")
    if r["scraper"] == "loosegrown" and r.get("ok"):
        print(f"  rows_key    : {r.get('rows_key')}")
        print(f"  rows_returned: {r.get('rows_returned')}")
        print(f"  sample_keys : {r.get('sample_keys')}")
    if not r.get("ok"):
        print(f"  error       : {r.get('error')}")
        if r.get("body_preview"):
            print(f"  body        : {r['body_preview']}")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Smoke-test brilliance + loosegrown scraper APIs.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--only",
        choices=["brilliance", "loosegrown"],
        help="Test only one scraper instead of both.",
    )
    parser.add_argument(
        "--vpn",
        type=str,
        default=None,
        help="Path to a .ovpn config (or just its filename inside vpnbook/). "
        "Brings up a tunnel before testing. Requires Administrator.",
    )
    args = parser.parse_args()

    targets: list[str] = [args.only] if args.only else ["brilliance", "loosegrown"]
    tests = {"brilliance": test_brilliance, "loosegrown": test_loosegrown}

    # --- optional VPN context ----------------------------------------------
    if args.vpn:
        from vpn_rotator import (
            VPNConnection,
            get_public_ip,
            is_admin,
            parse_credentials_file,
            write_auth_file,
        )
        if not is_admin():
            sys.exit(
                "--vpn requires Administrator. Re-run PowerShell as Admin "
                "and try again."
            )
        cfg = resolve_vpn_config(args.vpn)
        # .ovpn filenames start with country code (ca-/us-/mx-/...), so
        # vpn_rotator's prefix-based credential lookup won't find
        # proton_credentials.txt. Convert it once and pass explicitly.
        proton_creds = Path("vpnbook") / "proton_credentials.txt"
        proton_auth = Path("vpnbook") / "proton_auth.txt"
        if not proton_creds.exists():
            sys.exit(f"Missing {proton_creds}. Create it with Username:/Password: lines.")
        user, pwd = parse_credentials_file(proton_creds)
        write_auth_file(user, pwd, proton_auth)
        print(f"Bringing up VPN: {cfg.name}  (auth={proton_auth.name})")
        before_ip = get_public_ip()
        print(f"  before exit IP: {before_ip}")
        vpn_ctx = VPNConnection(cfg, auth_file=proton_auth, verify_ip=True)
    else:
        vpn_ctx = nullcontext()

    results: list[dict[str, Any]] = []
    with vpn_ctx as vpn:
        if args.vpn:
            print(f"  tunnel up — exit IP: {vpn.ip_after}\n")
        for name in targets:
            results.append(tests[name]())

    for r in results:
        print_result(r)

    all_ok = all(r.get("ok") for r in results)
    print(f"\n{'=' * 60}")
    print(f"Result: {sum(r.get('ok', False) for r in results)}/{len(results)} scrapers OK")
    print(f"{'=' * 60}")
    return 0 if all_ok else 1


if __name__ == "__main__":
    sys.exit(main())
