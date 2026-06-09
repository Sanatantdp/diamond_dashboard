"""
test_scrapers.py
================

Smoke-test the abelini + diamond-heaven scraper APIs with a single, minimal
request each. Confirms the upstream APIs respond and aren't blocking us
(403 / Cloudflare / geo-block) before kicking off a full scrape run.

Reuses the real headers / payloads / cookies from each scraper module so the
test request looks identical to a production fetch.

Modes
-----
Direct (default) — call the APIs from your current public IP. Fast (~2s).

    python test_scrapers.py
    python test_scrapers.py --only abelini
    python test_scrapers.py --only diamond-heaven

Via VPN          — bring up an OpenVPN tunnel (using a .ovpn from ../vpnbook/),
                   call the APIs through it, then disconnect. Requires Admin.

    python test_scrapers.py --vpn ca-free-3.protonvpn.tcp.ovpn
    python test_scrapers.py --vpn ca-free-3.protonvpn.tcp.ovpn --only abelini

Exit code 0 if all requested scrapers succeed, 1 if any fail.
"""

from __future__ import annotations

import argparse
import importlib
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

# Configs + shared ProtonVPN credentials live in the repo-root vpnbook/, one
# level above uk_scrapping/.
VPN_DIR = Path(__file__).resolve().parent.parent / "vpnbook"

# scraper key -> module name (diamond-heaven's file has a hyphen, so it must be
# imported via importlib rather than a plain `import`).
SCRAPER_MODULES = {
    "abelini": "abelini",
    "diamond-heaven": "diamond-heavon",
}


def test_abelini() -> dict[str, Any]:
    """GET one abelini listing page — the cheapest legitimate request."""
    from urllib.parse import urlencode

    from curl_cffi import requests as cffi_requests

    ab = importlib.import_module("abelini")

    start = time.perf_counter()
    result: dict[str, Any] = {"scraper": "abelini", "url": ab.BASE_URL}
    try:
        params = {**ab.BASE_PARAMS, "stone_shape": "RND", "page": "1"}
        url = f"{ab.BASE_URL}?{urlencode(params)}"
        with cffi_requests.Session(impersonate=ab.CHROME_VERSION) as session:
            resp = session.get(url, headers=ab.make_headers(), timeout=ab.REQUEST_TIMEOUT)
        result["status_code"] = resp.status_code
        result["elapsed_ms"] = round((time.perf_counter() - start) * 1000, 1)
        if resp.status_code != 200:
            result["ok"] = False
            result["error"] = f"HTTP {resp.status_code}"
            result["body_preview"] = resp.text[:300]
            return result
        data = resp.json()
        listing = data.get("listing", "")
        result["ok"] = bool(listing)
        result["stone_total"] = data.get("stone_total")
        if not listing:
            result["error"] = "empty 'listing' in response"
            result["body_preview"] = json.dumps(data)[:300]
    except Exception as exc:  # noqa: BLE001
        result["ok"] = False
        result["error"] = f"{exc.__class__.__name__}: {exc}"
        result["elapsed_ms"] = round((time.perf_counter() - start) * 1000, 1)
    return result


def test_diamond_heaven() -> dict[str, Any]:
    """POST a tiny diamond-heaven listing request (itemsPerPage=10)."""
    from curl_cffi import requests as cffi_requests

    dh = importlib.import_module("diamond-heavon")

    start = time.perf_counter()
    result: dict[str, Any] = {"scraper": "diamond-heaven", "url": dh.API_URL}
    try:
        form = {**dh.BASE_FORM, "p": "1", "itemsPerPage": "10"}
        cookies = dh._build_cookies()
        with cffi_requests.Session(impersonate=dh.CHROME_VERSION) as session:
            resp = session.post(
                dh.API_URL,
                headers=dh.make_headers(),
                cookies=cookies,
                data=form,
                timeout=40,
            )
        result["status_code"] = resp.status_code
        result["elapsed_ms"] = round((time.perf_counter() - start) * 1000, 1)
        if resp.status_code != 200:
            result["ok"] = False
            result["error"] = f"HTTP {resp.status_code}"
            result["body_preview"] = resp.text[:300]
            return result
        body = resp.json()
        diamonds = (
            body.get("items") or body.get("diamonds") or body.get("data")
            or body.get("products") or (body if isinstance(body, list) else [])
        )
        total = int(
            body.get("total_count") or body.get("totalCount") or body.get("total")
            or body.get("count") or (body.get("pagination") or {}).get("totalItems") or 0
        )
        result["ok"] = True
        result["total_count"] = total
        result["items_returned"] = len(diamonds)
    except Exception as exc:  # noqa: BLE001
        result["ok"] = False
        result["error"] = f"{exc.__class__.__name__}: {exc}"
        result["elapsed_ms"] = round((time.perf_counter() - start) * 1000, 1)
    return result


# scraper key -> smoke-test function
TESTS = {
    "abelini": test_abelini,
    "diamond-heaven": test_diamond_heaven,
}


def resolve_vpn_config(arg: str) -> Path:
    """Accept either a full path or just the filename inside ../vpnbook/."""
    p = Path(arg)
    if p.exists():
        return p
    in_vpnbook = VPN_DIR / arg
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
    if r["scraper"] == "abelini" and r.get("ok"):
        print(f"  stone_total : {r.get('stone_total')}")
    if r["scraper"] == "diamond-heaven" and r.get("ok"):
        print(f"  total_count    : {r.get('total_count')}")
        print(f"  items_returned : {r.get('items_returned')}")
    if not r.get("ok"):
        print(f"  error       : {r.get('error')}")
        if r.get("body_preview"):
            print(f"  body        : {r['body_preview']}")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Smoke-test abelini + diamond-heaven scraper APIs.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--only",
        choices=list(TESTS.keys()),
        help="Test only one scraper instead of both.",
    )
    parser.add_argument(
        "--vpn",
        type=str,
        default=None,
        help="Path to a .ovpn config (or just its filename inside ../vpnbook/). "
        "Brings up a tunnel before testing. Requires Administrator.",
    )
    args = parser.parse_args()

    targets: list[str] = [args.only] if args.only else list(TESTS.keys())

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
                "--vpn requires Administrator. Re-run PowerShell as Admin and try again."
            )
        cfg = resolve_vpn_config(args.vpn)
        # ProtonVPN configs are named by country (ca-/us-/...), so vpn_rotator's
        # prefix-based credential lookup won't find proton_credentials.txt.
        # Convert it once and pass it explicitly.
        proton_creds = VPN_DIR / "proton_credentials.txt"
        proton_auth = VPN_DIR / "proton_auth.txt"
        if not proton_creds.exists():
            sys.exit(f"Missing {proton_creds}. Create it with Username:/Password: lines.")
        user, pwd = parse_credentials_file(proton_creds)
        write_auth_file(user, pwd, proton_auth)
        print(f"Bringing up VPN: {cfg.name}  (auth={proton_auth.name})")
        before_ip = get_public_ip()
        print(f"  before exit IP: {before_ip}")
        vpn_ctx = VPNConnection(
            cfg,
            auth_file=proton_auth,
            log_file=VPN_DIR / "openvpn.log",
            verify_ip=True,
        )
    else:
        vpn_ctx = nullcontext()

    results: list[dict[str, Any]] = []
    with vpn_ctx as vpn:
        if args.vpn:
            print(f"  tunnel up — exit IP: {vpn.ip_after}\n")
        for name in targets:
            results.append(TESTS[name]())

    for r in results:
        print_result(r)

    all_ok = all(r.get("ok") for r in results)
    print(f"\n{'=' * 60}")
    print(f"Result: {sum(r.get('ok', False) for r in results)}/{len(results)} scrapers OK")
    print(f"{'=' * 60}")
    return 0 if all_ok else 1


if __name__ == "__main__":
    sys.exit(main())
