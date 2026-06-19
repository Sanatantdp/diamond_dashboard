# -*- coding: utf-8 -*-
"""
test_all_apis.py
================

Smoke-test all 9 diamond scraper APIs with a single minimal request each.
Confirms the upstream APIs respond and aren't blocked (403/Cloudflare/geo-block)
before running a full scrape.

APIs covered:
  brilliance     — USA  — worker.brilliance.com
  loosegrown     — USA  — loosegrowndiamond.com
  abelini        — UK   — abelini.com
  diamond-heaven — UK   — diamond-heaven.co.uk
  cullen         — ROA  — api.cullenjewellery.com
  gs-diamonds    — ROA  — gsdiamonds.com.au
  novita         — ROG  — novitadiamonds.de
  renesim        — ROG  — py.yorxs.de/live/graphql/
  77diamonds     — ROG  — 77diamonds.com

Modes
-----
Direct (default) — call from current public IP. Fast (~2-5s each).

    python test_all_apis.py
    python test_all_apis.py --only brilliance
    python test_all_apis.py --only diamond-heaven

Via single VPN config:

    python test_all_apis.py --vpn vpnbook/ca-free-3.protonvpn.tcp.ovpn
    python test_all_apis.py --vpn ca-free-3.protonvpn.tcp.ovpn --only novita

Test every .ovpn in a directory (slow, requires Admin):

    python test_all_apis.py --test-all-vpns
    python test_all_apis.py --test-all-vpns --vpn-dir vpnbook/uk
    python test_all_apis.py --test-all-vpns --vpn-dir vpnbook --only brilliance

Exit code 0 if all targeted scrapers succeed, 1 if any fail.
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

# ── Shared UA / chrome impersonation ─────────────────────────────────────────
_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/148.0.0.0 Safari/537.36"
)
_SEC_CH = '"Chromium";v="148", "Google Chrome";v="148", "Not/A)Brand";v="99"'
_CHROME_IMPERSONATE = "chrome"


def _session():
    from curl_cffi import requests as cffi
    return cffi.Session(impersonate=_CHROME_IMPERSONATE)


# ══════════════════════════════════════════════════════════════════════════════
# API SMOKE TESTS
# Each function returns a dict with at minimum: scraper, url, ok, elapsed_ms
# ══════════════════════════════════════════════════════════════════════════════

# ── 1. Brilliance (USA) ───────────────────────────────────────────────────────
def test_brilliance() -> dict[str, Any]:
    URL = "https://worker.brilliance.com/api/v1/lab-grown-diamond-search"
    headers = {
        "accept":             "application/json, text/plain, */*",
        "accept-language":    "en-GB,en-US;q=0.9,en;q=0.8",
        "content-type":       "application/json",
        "origin":             "https://www.brilliance.com",
        "priority":           "u=1, i",
        "sec-ch-ua":          _SEC_CH,
        "sec-ch-ua-mobile":   "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest":     "empty",
        "sec-fetch-mode":     "cors",
        "sec-fetch-site":     "same-site",
        "user-agent":         _UA,
    }
    # Minimal payload — ask for exactly 1 result
    payload = {
        "data": {
            "imgOnly": True,
            "view": "grid",
            "priceMin": 750,
            "priceMax": 100000,
            "caratMin": 1.5,
            "caratMax": 12,
            "colorMin": 6,
            "colorMax": 9,
            "clarityMin": 4,
            "clarityMax": 9,
            "depthMin": 0,
            "depthMax": 90,
            "tableMin": 0,
            "tableMax": 90,
            "shapeList": [],
            "certificateList": [],
            "pager": 1,
            "cutMin": 0,
            "cutMax": 4,
            "polishMin": 0,
            "polishMax": 3,
            "symmetryMin": 0,
            "symmetryMax": 3,
            "fluorMin": 0,
            "fluorMax": 3,
            "fastShipping": 0,
        }
    }
    result: dict[str, Any] = {"scraper": "brilliance", "url": URL}
    start = time.perf_counter()
    try:
        with _session() as s:
            resp = s.post(URL, headers=headers, json=payload, timeout=30)
        result["status_code"] = resp.status_code
        result["elapsed_ms"] = round((time.perf_counter() - start) * 1000, 1)
        if not resp.ok:
            result["ok"] = False
            result["error"] = f"HTTP {resp.status_code}"
            result["body_preview"] = resp.text[:300]
            return result
        data = resp.json()
        # Response has "data" with diamond list OR direct list
        diamonds = (
            data.get("data") if isinstance(data.get("data"), list)
            else data.get("diamonds") or data.get("items") or []
        )
        total = data.get("total") or data.get("count") or len(diamonds)
        result["ok"] = True
        result["total"] = total
        result["returned"] = len(diamonds)
    except Exception as exc:
        result["ok"] = False
        result["error"] = f"{exc.__class__.__name__}: {exc}"
        result["elapsed_ms"] = round((time.perf_counter() - start) * 1000, 1)
    return result


# ── 2. Loosegrown (USA) ───────────────────────────────────────────────────────
def test_loosegrown() -> dict[str, Any]:
    URL = "https://www.loosegrowndiamond.com/wp-json/ls/v1/inventory-filter"
    headers = {
        "accept":             "application/json, text/plain, */*",
        "accept-language":    "en-GB,en-US;q=0.9,en;q=0.8",
        "content-type":       "application/json",
        "origin":             "https://www.loosegrowndiamond.com",
        "priority":           "u=1, i",
        "referer":            "https://www.loosegrowndiamond.com/lab-diamonds/",
        "sec-ch-ua":          _SEC_CH,
        "sec-ch-ua-mobile":   "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest":     "empty",
        "sec-fetch-mode":     "cors",
        "sec-fetch-site":     "same-origin",
        "user-agent":         _UA,
    }
    cookies = {
        "sucuri_cloudproxy_uuid_c8b233ecb": "90a6959cc369bb3e741891a68114b00e",
        "sucuri_cloudproxy_uuid_3e5001f56":  "0d0a99b2606fd58fdb69f006941e717e",
        "PHPSESSID": "9du2j7n681ej2n8m410klq23fh",
    }
    payload = {"ls_start": 0, "ls_per_page": 1, "shape": []}
    result: dict[str, Any] = {"scraper": "loosegrown", "url": URL}
    start = time.perf_counter()
    try:
        with _session() as s:
            resp = s.post(URL, headers=headers, cookies=cookies,
                          json=payload, timeout=30)
        result["status_code"] = resp.status_code
        result["elapsed_ms"] = round((time.perf_counter() - start) * 1000, 1)
        if not resp.ok:
            result["ok"] = False
            result["error"] = f"HTTP {resp.status_code}"
            result["body_preview"] = resp.text[:300]
            return result
        data = resp.json()
        result["ok"] = True
        result["sample_keys"] = list(data.keys())[:10] if isinstance(data, dict) else None
        # Common response keys
        for k in ("data", "rows", "items", "results", "inventory", "diamonds", "products"):
            v = data.get(k) if isinstance(data, dict) else None
            if isinstance(v, list):
                result["rows_key"] = k
                result["returned"] = len(v)
                break
    except Exception as exc:
        result["ok"] = False
        result["error"] = f"{exc.__class__.__name__}: {exc}"
        result["elapsed_ms"] = round((time.perf_counter() - start) * 1000, 1)
    return result


# ── 3. Abelini (UK) ───────────────────────────────────────────────────────────
def test_abelini() -> dict[str, Any]:
    from urllib.parse import urlencode
    BASE_URL = "https://www.abelini.com/index.php"
    params = {
        "route":           "extension/opencart/product/option|diamondListing",
        "stone_shape":     "RND",
        "stone_type":      "LBG",
        "product_id":      "1240",
        "order":           "asc",
        "stone_carat_min": "1.00",
        "stone_carat_max": "1.20",
        "stone_price_min": "100",
        "stone_price_max": "5000000",
        "category_id":     "1",
        "page":            "1",
        "limit":           "20",
        "last_url_string": "choose-diamond",
        "markup":          "0",
        "discount":        "0",
    }
    url = f"{BASE_URL}?{urlencode(params)}"
    headers = {
        "accept":             "application/json, text/javascript, */*; q=0.01",
        "accept-language":    "en-GB,en-US;q=0.9,en;q=0.8",
        "referer":            "https://www.abelini.com/choose-diamond",
        "sec-ch-ua":          _SEC_CH,
        "sec-ch-ua-arch":     '"x86"',
        "sec-ch-ua-bitness":  '"64"',
        "sec-ch-ua-mobile":   "?0",
        "sec-ch-ua-model":    '""',
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest":     "empty",
        "sec-fetch-mode":     "cors",
        "sec-fetch-site":     "same-origin",
        "user-agent":         _UA,
        "x-requested-with":   "XMLHttpRequest",
        "priority":           "u=1, i",
    }
    cookies = {
        "OCSESSID": "b7234f02952368b5e0b7332548",
        "currency": "GBP",
        "cf_clearance": (
            "C5eNe0Z710kMsUk_BKWiMoH9efTeoRizlhfkR4GM2VE-1780660572-1.2.1.1"
            "-0.Hd2A40bDxsntw5QHy0BIxN_kKLOSjEJf_ZnKbXRLqGLtm10e04W1uBsgJXpy"
            "SsIQoUkyUL4jNGiiKQTXKAdI3uQA.aYY1unM17QX3PSZxoaYlJ5gp2yx6yk2Uf"
            "JSk8yDBp59Ss2HtVkJKEHwYv1zBC6MCjWtjyAtMTJXktB.vrP6gldmJNcJazDrh"
            ".UEhl1djgpOkxxo6KQcLAlpmCm7Y0z7AkCAKJ3LjlMQ.tlUsjUIlDjCv4oMMJWo"
            "KotauuHwtLhqpGsx7vrlosPskc6R3WBtPOrn5QHXw2VSUUeQ3yWeIHq8hYFtMlF"
            "BledAS1KU0F7GNekS4a.k1vCMujiPgsbrjj_KPSk3pR78CX5dTUnNBxg6hx3Q50"
            "L1quD9xCUpLjqYazWAtm3Bu1AxQ9OgsNScWS6fqyqgn_3XS0.AA"
        ),
    }
    result: dict[str, Any] = {"scraper": "abelini", "url": url}
    start = time.perf_counter()
    try:
        with _session() as s:
            resp = s.get(url, headers=headers, cookies=cookies, timeout=45)
        result["status_code"] = resp.status_code
        result["elapsed_ms"] = round((time.perf_counter() - start) * 1000, 1)
        if not resp.ok:
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
            result["body_preview"] = json.dumps(data)[:200]
    except Exception as exc:
        result["ok"] = False
        result["error"] = f"{exc.__class__.__name__}: {exc}"
        result["elapsed_ms"] = round((time.perf_counter() - start) * 1000, 1)
    return result


# ── 4. Diamond Heaven (UK) ────────────────────────────────────────────────────
def test_diamond_heaven() -> dict[str, Any]:
    URL = "https://www.diamond-heaven.co.uk/diamonds/index/listt"
    headers = {
        "accept":             "application/json, text/javascript, */*; q=0.01",
        "accept-language":    "en-GB,en-US;q=0.9,en;q=0.8",
        "content-type":       "application/x-www-form-urlencoded; charset=UTF-8",
        "origin":             "https://www.diamond-heaven.co.uk",
        "priority":           "u=1, i",
        "referer":            "https://www.diamond-heaven.co.uk/build-your-ring/by-stone/lab-grown-diamond",
        "sec-ch-ua":          _SEC_CH,
        "sec-ch-ua-arch":     '"x86"',
        "sec-ch-ua-bitness":  '"64"',
        "sec-ch-ua-mobile":   "?0",
        "sec-ch-ua-model":    '""',
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest":     "empty",
        "sec-fetch-mode":     "cors",
        "sec-fetch-site":     "same-origin",
        "user-agent":         _UA,
        "x-requested-with":   "XMLHttpRequest",
    }
    cookies = {
        "PHPSESSID":               "18bded02235548f862181ebfc870b28c",
        "form_key":                "Pu6BVx8dPzRkKeJT",
        "private_content_version": "1c3bc3f7dee93467920fe61d0cb7f04e",
        "cf_clearance": (
            "a6eGmwBzBmNTrWhZjqT.Ax0yz273g1k63O7esVx51_E-1780660790-1.2.1.1"
            "-ONbbkM1ps1JYa_eq6g9R7z5hom6PHxp35Xqh9LkWlQVmq3LsoK.kfcfqBuYT9wdg"
            "xjLp81wi23mZ31PLubdTT4B4mecrg0Mu8BFKIqydMDboQB9o8ZpJHlkOhVpeBwxCy"
            "wm5vCjeytBmTbokip5WLvshUKDfL6wFKBOPY54I0YRLQfPL0Z5k1NY9Pf7up.bgFA"
            "JbSUP1FxdiCrjq5xBCyofHQCZFHLIrdH.wD8cdYurL2NTw2a02X.4C6pOHaMavKkm"
            "pxfqx_pfHjD4vwpNseHDBVTTPep6UGrKbCBvNNdSHVpksYem7Oisvp0yMZ7VL5pYU"
            "B11ZxLx_wpjGfmpR2bRsjogwUJW9Oq4LLnHF.OE3FTM0yIeVPRVYbVlZSSmMlSo8"
            "Vr_tokQRobPDqcnxDg9hytr6qV9suEsgmhClhfs"
        ),
        "inova_p_sid":    "6c3sdoj_260605045946",
        "section_data_ids": "{}",
        "mage-messages":  "",
    }
    form = {
        "ajax":              "1",
        "is_product_page":   "1",
        "diamond_type":      "labgrown",
        "clarity":           "SI2,SI1,VS2,VS1,VVS2,VVS1,IF,FL",
        "colour":            "H,G,F,E,D",
        "cut":               "Good,Very Good,Excellent,Ideal",
        "p":                 "1",
        "itemsPerPage":      "9",
        "viewtype":          "grid",
        "carat":             "0.3-10",
        "price":             "100-100000",
        "polish":            "Good,Very Good,Excellent",
        "symmetry":          "Good,Very Good,Excellent",
        "flou":              "Very Strong,Strong,Medium,Faint,None",
        "withimagevideo":    "false",
        "available_instore": "false",
        "search":            "",
        "gemtype":           "",
        "ring_builder":      "1",
        "pagetype":          "ringbuilder",
    }
    result: dict[str, Any] = {"scraper": "diamond-heaven", "url": URL}
    start = time.perf_counter()
    try:
        with _session() as s:
            resp = s.post(URL, headers=headers, cookies=cookies,
                          data=form, timeout=40)
        result["status_code"] = resp.status_code
        result["elapsed_ms"] = round((time.perf_counter() - start) * 1000, 1)
        if not resp.ok:
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
            or body.get("count") or 0
        )
        result["ok"] = True
        result["total"] = total
        result["returned"] = len(diamonds)
    except Exception as exc:
        result["ok"] = False
        result["error"] = f"{exc.__class__.__name__}: {exc}"
        result["elapsed_ms"] = round((time.perf_counter() - start) * 1000, 1)
    return result


# ── 5. Cullen Jewellery (ROA) ─────────────────────────────────────────────────
def test_cullen() -> dict[str, Any]:
    URL = "https://api.cullenjewellery.com/api/stone_preference_data/feed/v2"
    headers = {
        "accept":             "application/json",
        "accept-language":    "en-GB,en-US;q=0.9,en;q=0.8",
        "content-type":       "application/json",
        "origin":             "https://cullenjewellery.com",
        "priority":           "u=1, i",
        "referer":            "https://cullenjewellery.com/",
        "sec-ch-ua":          _SEC_CH,
        "sec-ch-ua-mobile":   "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest":     "empty",
        "sec-fetch-mode":     "cors",
        "sec-fetch-site":     "same-site",
        "user-agent":         _UA,
    }
    # Minimal payload — narrow carat range, 10 items
    payload = {
        "filter": {
            "type": "diamond",
            "shapes": ["round"],
            "fancyColours": [],
            "moissaniteColours": [],
            "sapphireColours": [],
            "minColour": "g",
            "maxColour": "h",
            "minClarity": "vs1",
            "maxClarity": "vs2",
            "minCut": "ex",
            "maxCut": "ex",
            "minCarat": 1.0,
            "maxCarat": 1.1,
            "minLwRatio": 0.5,
            "maxLwRatio": 2.5,
            "minCsScore": 0,
            "maxCsScore": 1,
            "inStock": False,
            "cullenSelectStatus": "non-select-only",
        },
        "currency": "USD",
        "sort": "recommended",
        "start": 0,
        "addedRanOut": False,
    }
    result: dict[str, Any] = {"scraper": "cullen", "url": URL}
    start = time.perf_counter()
    try:
        with _session() as s:
            resp = s.post(URL, headers=headers, json=payload, timeout=30)
        result["status_code"] = resp.status_code
        result["elapsed_ms"] = round((time.perf_counter() - start) * 1000, 1)
        if not resp.ok:
            result["ok"] = False
            result["error"] = f"HTTP {resp.status_code}"
            result["body_preview"] = resp.text[:300]
            return result
        data = resp.json()
        result["ok"] = True
        result["sample_keys"] = list(data.keys())[:10] if isinstance(data, dict) else None
        for k in ("stones", "data", "items", "diamonds", "results"):
            v = data.get(k) if isinstance(data, dict) else None
            if isinstance(v, list):
                result["rows_key"] = k
                result["returned"] = len(v)
                break
        result["total"] = (
            data.get("total") or data.get("totalCount") or data.get("count")
            if isinstance(data, dict) else None
        )
    except Exception as exc:
        result["ok"] = False
        result["error"] = f"{exc.__class__.__name__}: {exc}"
        result["elapsed_ms"] = round((time.perf_counter() - start) * 1000, 1)
    return result


# ── 6. GS Diamonds (ROA) ──────────────────────────────────────────────────────
def test_gs_diamonds() -> dict[str, Any]:
    from urllib.parse import urlencode
    BASE_URL = "https://www.gsdiamonds.com.au/lab-diamonds"
    params = {
        "creation_method": "Lab-White",
        "max-carat":        "20.1",
        "min-carat":        "1.5",
        "shape":            "Round",
        "max-clarity_numeric": "75.5",
        "min-clarity_numeric": "0",
        "max-color_numeric":   "30",
        "min-color_numeric":   "0",
        "page":                "1",
    }
    url = f"{BASE_URL}?{urlencode(params)}"
    headers = {
        "accept":             "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "accept-language":    "en-GB,en-US;q=0.9,en;q=0.8",
        "sec-ch-ua":          _SEC_CH,
        "sec-ch-ua-mobile":   "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest":     "document",
        "sec-fetch-mode":     "navigate",
        "sec-fetch-site":     "none",
        "user-agent":         _UA,
    }
    result: dict[str, Any] = {"scraper": "gs-diamonds", "url": url}
    start = time.perf_counter()
    try:
        with _session() as s:
            resp = s.get(url, headers=headers, timeout=30)
        result["status_code"] = resp.status_code
        result["elapsed_ms"] = round((time.perf_counter() - start) * 1000, 1)
        if not resp.ok:
            result["ok"] = False
            result["error"] = f"HTTP {resp.status_code}"
            result["body_preview"] = resp.text[:300]
            return result
        # GS Diamonds may return HTML with embedded JSON or a JSON API
        body = resp.text
        if resp.headers.get("content-type", "").startswith("application/json"):
            data = resp.json()
            result["ok"] = True
            result["sample_keys"] = list(data.keys())[:10]
        else:
            # HTML response — check it loaded without block page
            ok = len(body) > 1000 and ("diamond" in body.lower() or "price" in body.lower())
            result["ok"] = ok
            result["content_length"] = len(body)
            if not ok:
                result["error"] = "Response too short or missing diamond data"
                result["body_preview"] = body[:300]
    except Exception as exc:
        result["ok"] = False
        result["error"] = f"{exc.__class__.__name__}: {exc}"
        result["elapsed_ms"] = round((time.perf_counter() - start) * 1000, 1)
    return result


# ── 7. Novita (ROG / DE) ──────────────────────────────────────────────────────
def test_novita() -> dict[str, Any]:
    URL = "https://novitadiamonds.de/en/api/product/diamonds"
    # API accepts application/json (not form-urlencoded)
    headers = {
        "accept":             "application/json, text/javascript, */*; q=0.01",
        "accept-language":    "en-GB,en-US;q=0.9,en;q=0.8",
        "content-type":       "application/json",
        "origin":             "https://novitadiamonds.de",
        "priority":           "u=1, i",
        "referer":            "https://novitadiamonds.de/en/create/diamanten/buy-lab-grown-diamonds",
        "sec-ch-ua":          '"Google Chrome";v="149", "Chromium";v="149", "Not)A;Brand";v="24"',
        "sec-ch-ua-mobile":   "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest":     "empty",
        "sec-fetch-mode":     "cors",
        "sec-fetch-site":     "same-origin",
        "user-agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/149.0.0.0 Safari/537.36"
        ),
        "x-requested-with":   "XMLHttpRequest",
    }
    cookies = {
        "cookie_preferences": (
            "%7B%22analytics%22%3Atrue%2C%22tag_manager%22%3Atrue%2C"
            "%22calendly%22%3Atrue%2C%22trustpilot%22%3Atrue%2C%22youtube%22%3Atrue%7D"
        ),
        "PHPSESSID": "bkeeq8cbndopeu5sskq68p95v0",
    }
    payload = {
        "shapes":                ["1", "4", "3", "2", "6", "5", "9", "8", "7", "10", "11"],
        "color_from":            1,
        "color_to":              16,
        "cut_from":              1,
        "cut_to":                3,
        "clarity_from":          2,
        "clarity_to":            8,
        "polish_from":           1,
        "polish_to":             2,
        "symmetry_from":         1,
        "symmetry_to":           2,
        "carat_from":            0.3,
        "carat_to":              20,
        "price_from":            150,
        "price_to":              13500,
        "location_description":  "all",
        "certificates":          [],
        "ratio_from":            1,
        "ratio_to":              3,
        "is_carat_infitinitive": 1,
        "is_price_infitinitive": 1,
        "is_ratio_infitinitive": 1,
        "maxResults":            32,
        "page":                  1,
        "order_by":              "priceToSellWoLocalTax",
        "order_type":            "ASC",
        "preferred_currency":    "EUR",
        "current_tab":           "result_tab",
        "fetch_count":           False,
        "token": (
            "ebaf63ab251c8e4a1c5050bc.14CP1gnK2syhHPXF6Tvg_-Z6nEkDpshgRLLHZDcoZeU"
            ".hsbfvEKPi4eXRpydiFi2zb896gFV4_4oL-vwAQFhB4Du39-HSqOPoNh2ng"
        ),
    }
    result: dict[str, Any] = {"scraper": "novita", "url": URL}
    start = time.perf_counter()
    try:
        with _session() as s:
            resp = s.post(URL, headers=headers, cookies=cookies,
                          json=payload, timeout=30)
        result["status_code"] = resp.status_code
        result["elapsed_ms"] = round((time.perf_counter() - start) * 1000, 1)
        if not resp.ok:
            result["ok"] = False
            result["error"] = f"HTTP {resp.status_code}"
            result["body_preview"] = resp.text[:300]
            return result
        data = resp.json()
        items = (data.get("response") or {}).get("items") or []
        total = (data.get("response") or {}).get("total_count") or 0
        result["ok"] = True
        result["total"] = total
        result["returned"] = len(items)
    except Exception as exc:
        result["ok"] = False
        result["error"] = f"{exc.__class__.__name__}: {exc}"
        result["elapsed_ms"] = round((time.perf_counter() - start) * 1000, 1)
    return result


# ── 8. Renesim (ROG) ──────────────────────────────────────────────────────────
def test_renesim() -> dict[str, Any]:
    URL = "https://py.yorxs.de/live/graphql/"
    headers = {
        "Accept":             "*/*",
        "Accept-Language":    "en-GB,en-US;q=0.9,en;q=0.8",
        "Connection":         "keep-alive",
        "Content-Type":       "application/json",
        "Origin":             "https://www.renesim.com",
        "Referer":            "https://www.renesim.com/",
        "Sec-Fetch-Dest":     "empty",
        "Sec-Fetch-Mode":     "cors",
        "Sec-Fetch-Site":     "cross-site",
        "User-Agent":         _UA,
        "sec-ch-ua":          _SEC_CH,
        "sec-ch-ua-mobile":   "?0",
        "sec-ch-ua-platform": '"Windows"',
    }
    # Minimal GraphQL query for 1 page
    gql = """{
  diamondData(
    page: "1",
    shapes: "",
    color: "",
    carat: "1.0, 1.1",
    clarity: "",
    symmetry: "",
    polish: "",
    price: "100, 2000000",
    table: "5.1,93",
    depthPer: "5.26,99.10",
    cut: "",
    fluoroscence: "",
    lab: "",
    diamondType: "lab-white",
    currencyFlag: "EUR",
    bulkPageData: "no",
    orderBy: "final_price_eur",
    langCode: "EN",
    taxPercentage: "0",
    sortBy: "asc"
  ) {
    dataCount
    diamondsReturned
    pageNo
    diamond {
      diamondId
      shape
      caratWeight
      color
      clarity
      finalPriceEur
      certificateNumber
    }
  }
}"""
    payload = {"query": gql}
    result: dict[str, Any] = {"scraper": "renesim", "url": URL}
    start = time.perf_counter()
    try:
        with _session() as s:
            resp = s.post(URL, headers=headers, json=payload, timeout=60)
        result["status_code"] = resp.status_code
        result["elapsed_ms"] = round((time.perf_counter() - start) * 1000, 1)
        if not resp.ok:
            result["ok"] = False
            result["error"] = f"HTTP {resp.status_code}"
            result["body_preview"] = resp.text[:300]
            return result
        data = resp.json()
        dd = (data.get("data") or {}).get("diamondData") or {}
        total = dd.get("dataCount", 0)
        diamonds = dd.get("diamond") or []
        result["ok"] = True
        result["total"] = total
        result["returned"] = len(diamonds)
    except Exception as exc:
        result["ok"] = False
        result["error"] = f"{exc.__class__.__name__}: {exc}"
        result["elapsed_ms"] = round((time.perf_counter() - start) * 1000, 1)
    return result


# ── 9. 77 Diamonds (ROG) ──────────────────────────────────────────────────────
def test_77diamonds() -> dict[str, Any]:
    URL = "https://www.77diamonds.com/api/bff/shop/diamond-list"
    headers = {
        "accept":             "application/json",
        "accept-language":    "en-GB,en-US;q=0.9,en;q=0.8",
        "content-type":       "application/json",
        "domain":             "https://www.77diamonds.com",
        "origin":             "https://www.77diamonds.com",
        "priority":           "u=1, i",
        "referer":            "https://www.77diamonds.com/loose-lab-grown-diamonds",
        "sec-ch-ua":          _SEC_CH,
        "sec-ch-ua-mobile":   "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest":     "empty",
        "sec-fetch-mode":     "cors",
        "sec-fetch-site":     "same-origin",
        "user-agent":         _UA,
    }
    cookies = {
        "session":           "a1544c92-9a79-4bb6-a1e6-7eccaa1d6565",
        "currentCountryId":  "528",
        "currentCurrencyId": "2",
        "addDiscount":       "LATESPRING2026",
        "showCookieBar":     "true",
    }
    payload = {
        "categoryId":       7,
        "clarities":        [44, 39, 38, 47, 266],
        "colors":           [19, 15, 17],
        "country":          0,
        "currency":         0,
        "currentPage":      1,
        "cuts":             [56, 510],
        "diamondType":      -1,
        "isGroupedShapes":  False,
        "itemId":           -1,
        "language":         0,
        "maxCarat":         30,
        "maxDepth":         0,
        "maxPrice":         0,
        "maxRatio":         5,
        "maxTable":         0,
        "minCarat":         0.7,
        "minDepth":         0,
        "minPrice":         0,
        "minRatio":         1,
        "minTable":         0,
        "quickShipping":    False,
        "resultsPerPage":   20,
        "searchBlocked":    False,
        "showPairs":        False,
        "stoneType":        3,
        "withMedia":        False,
        "shapes":           [1],
        "CurrentPageNumber": 1,
        "ResultsPerPage":   20,
        "Intensities":      [],
        "Certificates":     [],
        "Polishes":         [],
        "Symmetries":       [],
        "Fluorescences":    [],
        "GemClarity":       [],
        "GemIntensity":     [],
        "GemTreatment":     [],
        "GemClarityTreatment": [],
        "WithMedia":        False,
        "QuickShipping":    False,
        "CountryId":        528,
        "CurrencyId":       2,
        "LanguageId":       1,
        "UserPreference": {
            "CountryId":    528,
            "CurrencyId":   2,
            "LanguageId":   1,
            "DiscountCode": "LATESPRING2026",
        },
        "Url": "https://www.77diamonds.com/loose-lab-grown-diamonds",
    }
    result: dict[str, Any] = {"scraper": "77diamonds", "url": URL}
    start = time.perf_counter()
    try:
        with _session() as s:
            resp = s.post(URL, headers=headers, cookies=cookies,
                          json=payload, timeout=30)
        result["status_code"] = resp.status_code
        result["elapsed_ms"] = round((time.perf_counter() - start) * 1000, 1)
        if not resp.ok:
            result["ok"] = False
            result["error"] = f"HTTP {resp.status_code}"
            result["body_preview"] = resp.text[:300]
            return result
        data = resp.json()
        diamonds = data.get("Diamonds") or data.get("diamonds") or []
        total = data.get("Total") or data.get("total") or 0
        result["ok"] = True
        result["total"] = total
        result["returned"] = len(diamonds)
    except Exception as exc:
        result["ok"] = False
        result["error"] = f"{exc.__class__.__name__}: {exc}"
        result["elapsed_ms"] = round((time.perf_counter() - start) * 1000, 1)
    return result


# ── 10. Brilliant Earth (USA) ─────────────────────────────────────────────────
def test_brilliant_earth() -> dict[str, Any]:
    from urllib.parse import urlencode
    BASE = "https://www.brilliantearth.com/api/v1/plp/products/"
    params = {
        "display":     50,
        "page":        1,
        "currency":    "USD",
        "product_class": "Lab Created Colorless Diamonds",
        "cuts":        "Fair,Good,Very Good,Ideal,Super Ideal",
        "colors":      "J,I,H,G,F,E,D",
        "clarities":   "SI2,SI1,VS2,VS1,VVS2,VVS1,IF,FL",
        "polishes":    "Good,Very Good,Excellent",
        "symmetries":  "Good,Very Good,Excellent",
        "fluorescences": "Very Strong,Strong,Medium,Faint,None",
        "min_price":   180,   "max_price":   1396180,
        "MIN_PRICE":   180,   "MAX_PRICE":   1396180,
        "min_carat":   0.25,  "max_carat":   70.83,
        "MIN_CARAT":   0.25,  "MAX_CARAT":   70.83,
        "min_ratio":   1,     "max_ratio":   2.75,
        "MIN_RATIO":   1,     "MAX_RATIO":   2.75,
        "min_table":   45,    "max_table":   85,
        "MIN_TABLE":   45,    "MAX_TABLE":   85,
        "min_depth":   0.6,   "max_depth":   83.3,
        "MIN_DEPTH":   0.6,   "MAX_DEPTH":   83.3,
        "order_by":    "most_popular",
        "order_method": "asc",
    }
    url = f"{BASE}?{urlencode(params)}"
    headers = {
        "accept":             "application/json, text/javascript, */*; q=0.01",
        "accept-language":    "en-GB,en-US;q=0.9,en;q=0.8",
        "origin":             "https://www.brilliantearth.com",
        "referer":            "https://www.brilliantearth.com/loose-diamonds/search/",
        "sec-ch-ua":          '"Google Chrome";v="149", "Chromium";v="149", "Not)A;Brand";v="24"',
        "sec-ch-ua-mobile":   "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest":     "empty",
        "sec-fetch-mode":     "cors",
        "sec-fetch-site":     "same-origin",
        "user-agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/149.0.0.0 Safari/537.36"
        ),
        "x-requested-with":   "XMLHttpRequest",
    }
    result: dict[str, Any] = {"scraper": "brilliant-earth", "url": url[:80]}
    start = time.perf_counter()
    try:
        with _session() as s:
            resp = s.get(url, headers=headers, timeout=30)
        result["status_code"] = resp.status_code
        result["elapsed_ms"] = round((time.perf_counter() - start) * 1000, 1)
        if not resp.ok:
            result["ok"] = False
            result["error"] = f"HTTP {resp.status_code}"
            result["body_preview"] = resp.text[:300]
            return result
        data = resp.json()
        diamonds = []
        for key in ("products", "diamonds", "items", "data", "results"):
            v = data.get(key) if isinstance(data, dict) else None
            if isinstance(v, list):
                diamonds = v
                result["rows_key"] = key
                break
        total = (
            data.get("total_count") or data.get("totalCount") or
            data.get("total") or data.get("count") or 0
        ) if isinstance(data, dict) else 0
        result["ok"] = len(diamonds) > 0
        result["total"] = total
        result["returned"] = len(diamonds)
        if not diamonds:
            result["error"] = "Empty products list"
            result["sample_keys"] = list(data.keys())[:10] if isinstance(data, dict) else None
    except Exception as exc:
        result["ok"] = False
        result["error"] = f"{exc.__class__.__name__}: {exc}"
        result["elapsed_ms"] = round((time.perf_counter() - start) * 1000, 1)
    return result


# ── Registry ──────────────────────────────────────────────────────────────────
ALL_TESTS: dict[str, Any] = {
    "brilliance":     test_brilliance,
    "loosegrown":     test_loosegrown,
    "brilliant-earth": test_brilliant_earth,
    "abelini":        test_abelini,
    "diamond-heaven": test_diamond_heaven,
    "cullen":         test_cullen,
    "gs-diamonds":    test_gs_diamonds,
    "novita":         test_novita,
    "renesim":        test_renesim,
    "77diamonds":     test_77diamonds,
}

REGIONS = {
    "brilliance":     "USA",
    "loosegrown":     "USA",
    "brilliant-earth": "USA",
    "abelini":        "UK",
    "diamond-heaven": "UK",
    "cullen":         "ROA",
    "gs-diamonds":    "ROA",
    "novita":         "ROG",
    "renesim":        "ROG",
    "77diamonds":     "ROG",
}


# ══════════════════════════════════════════════════════════════════════════════
# OUTPUT
# ══════════════════════════════════════════════════════════════════════════════
def print_result(r: dict[str, Any]) -> None:
    name   = r["scraper"]
    status = "OK  " if r.get("ok") else "FAIL"
    region = REGIONS.get(name, "?")
    print(f"\n[{status}] {name.upper():<16} ({region})")
    print(f"  url         : {r.get('url', '')[:80]}")
    print(f"  status_code : {r.get('status_code', '-')}")
    print(f"  elapsed_ms  : {r.get('elapsed_ms', '-')}")
    if r.get("ok"):
        for key in ("total", "returned", "stone_total", "rows_key",
                    "rows_returned", "sample_keys", "content_length"):
            if r.get(key) is not None:
                print(f"  {key:<12}: {r[key]}")
    else:
        print(f"  error       : {r.get('error', '?')}")
        if r.get("body_preview"):
            print(f"  body        : {r['body_preview'][:200]}")


# ══════════════════════════════════════════════════════════════════════════════
# VPN HELPERS
# ══════════════════════════════════════════════════════════════════════════════
def _find_vpn_dir(explicit: str | None) -> Path:
    if explicit:
        p = Path(explicit)
        if not p.exists():
            sys.exit(f"VPN directory not found: {p}")
        return p
    # Auto-detect: prefer repo-root vpnbook/
    here = Path.cwd()
    candidates = [here / "vpnbook", here.parent / "vpnbook"]
    for c in candidates:
        if c.exists():
            return c
    sys.exit("No vpnbook/ directory found. Pass --vpn-dir explicitly.")


def resolve_vpn_config(arg: str, vpn_dir: Path) -> Path:
    p = Path(arg)
    if p.exists():
        return p
    in_dir = vpn_dir / arg
    if in_dir.exists():
        return in_dir
    sys.exit(f"VPN config not found: {arg} (also tried {in_dir})")


def _setup_proton_auth(vpn_dir: Path) -> Path | None:
    from vpn_rotator import parse_credentials_file, write_auth_file  # type: ignore

    # Walk up from vpn_dir to find proton_credentials.txt
    for d in [vpn_dir, vpn_dir.parent, vpn_dir.parent.parent]:
        creds = d / "proton_credentials.txt"
        if creds.exists():
            user, pwd = parse_credentials_file(creds)
            auth = d / "proton_auth.txt"
            write_auth_file(user, pwd, auth)
            print(f"ProtonVPN credentials loaded from {creds}")
            return auth
    print("WARNING: proton_credentials.txt not found — will use per-config credential lookup.")
    return None


def _collect_ovpn_configs(vpn_dir: Path, recursive: bool = False) -> list[Path]:
    if recursive:
        return sorted(vpn_dir.rglob("*.ovpn"))
    return sorted(vpn_dir.glob("*.ovpn"))


# ══════════════════════════════════════════════════════════════════════════════
# TEST ALL VPN CONFIGS
# ══════════════════════════════════════════════════════════════════════════════
def test_all_vpns(targets: list[str], vpn_dir: Path, recursive: bool = False) -> int:
    from vpn_rotator import VPNConnection, VPNError, is_admin, kill_openvpn_processes  # type: ignore

    if not is_admin():
        sys.exit(
            "--test-all-vpns requires Administrator privileges.\n"
            "Re-run PowerShell as Administrator and try again."
        )

    configs = _collect_ovpn_configs(vpn_dir, recursive)
    if not configs:
        sys.exit(f"No .ovpn files found in {vpn_dir}/ (recursive={recursive})")

    proton_auth = _setup_proton_auth(vpn_dir)
    openvpn_log = vpn_dir / "openvpn.log"

    print(f"\n{'=' * 70}")
    print(f"Testing {len(configs)} VPN config(s) in {vpn_dir}")
    print(f"APIs per config : {', '.join(targets)}")
    print(f"{'=' * 70}\n")

    summary: list[dict] = []
    try:
        for idx, cfg in enumerate(configs, start=1):
            print(f"[{idx:>3}/{len(configs)}] {cfg.name}")
            entry: dict = {
                "config":     cfg.name,
                "connect_ok": False,
                "exit_ip":    None,
                "scrapers":   {},
                "all_ok":     False,
                "error":      None,
            }
            try:
                with VPNConnection(
                    cfg,
                    auth_file=proton_auth,
                    log_file=openvpn_log,
                    verify_ip=True,
                ) as vpn:
                    entry["connect_ok"] = True
                    entry["exit_ip"] = vpn.ip_after
                    print(f"             tunnel up  exit IP={vpn.ip_after}")
                    for name in targets:
                        r = ALL_TESTS[name]()
                        entry["scrapers"][name] = {
                            "ok":     bool(r.get("ok")),
                            "status": r.get("status_code"),
                            "ms":     r.get("elapsed_ms"),
                            "error":  r.get("error"),
                        }
                        marker = "OK  " if r.get("ok") else "FAIL"
                        info = (
                            f"HTTP {r.get('status_code')}"
                            if r.get("status_code") else
                            (r.get("error") or "?")
                        )
                        ms_str = f" [{r.get('elapsed_ms', '?')}ms]"
                        print(f"             {marker}  {name:<16}  {info}{ms_str}")
                entry["all_ok"] = all(s["ok"] for s in entry["scrapers"].values())
            except VPNError as exc:
                entry["error"] = f"VPNError: {str(exc)[:120]}"
                print(f"             CONNECT FAILED  {str(exc)[:100]}")
            except Exception as exc:
                entry["error"] = f"{exc.__class__.__name__}: {str(exc)[:120]}"
                print(f"             ERROR  {exc.__class__.__name__}: {str(exc)[:100]}")

            summary.append(entry)
            print()
    except KeyboardInterrupt:
        print("\nInterrupted — tearing down any lingering VPN tunnel.")
        try:
            kill_openvpn_processes()
        except Exception:
            pass

    # ── Final summary ─────────────────────────────────────────────
    w = 80
    print(f"\n{'=' * w}")
    print(f"  RESULTS  —  {len(summary)} config(s) tested")
    print(f"{'=' * w}")
    hdr = f"  {'#':<4} {'config':<40} {'conn':<5} {'exit IP':<16}"
    for name in targets:
        hdr += f" {name[:10]:<10}"
    print(hdr)
    print(f"  {'-' * (w-2)}")

    fully_ok = 0
    for i, e in enumerate(summary, start=1):
        conn = "OK" if e["connect_ok"] else "NO"
        ip   = e["exit_ip"] or "-"
        line = f"  {i:<4} {e['config']:<40} {conn:<5} {ip:<16}"
        for name in targets:
            info = e["scrapers"].get(name, {})
            line += f" {'OK' if info.get('ok') else 'NO':<10}"
        if e.get("error") and not e["connect_ok"]:
            line += f"  [{e['error'][:40]}]"
        print(line)
        if e["all_ok"]:
            fully_ok += 1

    print(f"  {'-' * (w-2)}")
    print(f"  Fully working: {fully_ok}/{len(summary)}")
    print(f"{'=' * w}\n")
    return 0 if fully_ok > 0 else 1


# ══════════════════════════════════════════════════════════════════════════════
# CLI
# ══════════════════════════════════════════════════════════════════════════════
def main() -> int:
    parser = argparse.ArgumentParser(
        description="Smoke-test all 9 diamond scraper APIs.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--only",
        choices=list(ALL_TESTS.keys()),
        help="Test only one specific scraper.",
    )
    parser.add_argument(
        "--vpn",
        type=str,
        default=None,
        help="Path to a .ovpn config (or filename inside vpnbook/). "
        "Brings up a tunnel before testing. Requires Administrator.",
    )
    parser.add_argument(
        "--vpn-dir",
        type=str,
        default=None,
        dest="vpn_dir",
        help="Directory containing .ovpn configs (default: vpnbook/).",
    )
    parser.add_argument(
        "--test-all-vpns",
        action="store_true",
        help="Test every .ovpn in --vpn-dir through each scraper. "
        "Requires Administrator. Slow (~30-60s × N configs).",
    )
    parser.add_argument(
        "--recursive",
        action="store_true",
        help="Search --vpn-dir recursively for .ovpn files (default: top-level only).",
    )
    args = parser.parse_args()

    if args.vpn and args.test_all_vpns:
        sys.exit("Choose one of --vpn or --test-all-vpns, not both.")

    targets: list[str] = [args.only] if args.only else list(ALL_TESTS.keys())
    vpn_dir = _find_vpn_dir(args.vpn_dir)

    if args.test_all_vpns:
        return test_all_vpns(targets, vpn_dir, recursive=args.recursive)

    # ── Optional single-VPN context ───────────────────────────────
    if args.vpn:
        from vpn_rotator import VPNConnection, get_public_ip, is_admin  # type: ignore

        if not is_admin():
            sys.exit("--vpn requires Administrator. Re-run as Admin and try again.")
        cfg = resolve_vpn_config(args.vpn, vpn_dir)
        proton_auth = _setup_proton_auth(vpn_dir)
        print(f"Bringing up VPN: {cfg.name}")
        before = get_public_ip()
        print(f"  exit IP before: {before}")
        vpn_ctx = VPNConnection(
            cfg,
            auth_file=proton_auth,
            log_file=vpn_dir / "openvpn.log",
            verify_ip=True,
        )
    else:
        vpn_ctx = nullcontext()

    print(f"\nTesting APIs: {', '.join(targets)}")
    if not args.vpn:
        print("Mode: direct (no VPN)")

    results: list[dict] = []
    with vpn_ctx as vpn:
        if args.vpn:
            print(f"  tunnel up — exit IP: {vpn.ip_after}\n")
        for name in targets:
            r = ALL_TESTS[name]()
            results.append(r)

    for r in results:
        print_result(r)

    n_ok = sum(1 for r in results if r.get("ok"))
    print(f"\n{'=' * 60}")
    print(f"Result: {n_ok}/{len(results)} APIs OK")
    print(f"{'=' * 60}")
    return 0 if n_ok == len(results) else 1


if __name__ == "__main__":
    sys.exit(main())
