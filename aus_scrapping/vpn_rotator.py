"""
vpn_rotator.py
==============

Connect, verify, disconnect, and cycle through downloaded VPNBook .ovpn
configs. Designed to wrap diamond scrapers (or any HTTP workload) so each
batch of requests goes through a different exit IP.

OpenVPN tunnels are *system-wide* — once connected, ALL outbound traffic
on the host routes through the VPN until disconnect. There is nothing to
change inside the scraper's `requests` code; just wrap the scrape call
in a `with VPNConnection(...)` block.

Prerequisites
-------------
- Python 3.9+
- OpenVPN community client installed
    Windows: https://openvpn.net/community-downloads/
             Installs to C:\\Program Files\\OpenVPN\\bin\\openvpn.exe
    Linux:   apt install openvpn   (or equivalent)
- Process must run with elevated privileges (Administrator / root)
  so OpenVPN can manage the TAP/TUN interface.
- `vpnbook/credentials.txt` from the downloader script.

Layout this module expects (relative to cwd):
    vpnbook/
        credentials.txt
        vpnbook-us-server-1-tcp443.ovpn
        vpnbook-us-server-1-tcp80.ovpn
        ... (40 total)
"""

from __future__ import annotations

import json as _jsonlib
import logging
import os
import random
import re
import shutil
import subprocess
import sys
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Callable, Iterator, Optional

import requests


# ---------------------------------------------------------------------------
# Logging (lightweight — uses stdlib so we don't couple to your project logger)
# ---------------------------------------------------------------------------
log = logging.getLogger("vpn_rotator")
if not log.handlers:
    _h = logging.StreamHandler()
    _h.setFormatter(
        logging.Formatter(
            "%(asctime)s [%(levelname)s] vpn — %(message)s",
            "%Y-%m-%d %H:%M:%S",
        )
    )
    log.addHandler(_h)
    log.setLevel(logging.INFO)

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
VPN_DIR = Path.cwd() / "vpnbook"
CREDENTIALS_FILE = VPN_DIR / "credentials.txt"
AUTH_FILE = VPN_DIR / "auth.txt"          # OpenVPN-format: user\npass\n
DEFAULT_LOG_FILE = VPN_DIR / "openvpn.log"

# Common install locations — used if `openvpn` isn't on PATH.
_OPENVPN_CANDIDATES = [
    r"C:\Program Files\OpenVPN\bin\openvpn.exe",
    r"C:\Program Files (x86)\OpenVPN\bin\openvpn.exe",
    "/usr/sbin/openvpn",
    "/usr/local/sbin/openvpn",
]

# How long to wait for "Initialization Sequence Completed" before giving up
CONNECT_TIMEOUT = 45      # seconds
ROUTE_SETTLE_SECONDS = 2  # let routes stabilise before verifying


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------
class VPNError(RuntimeError):
    """Raised on any VPN setup, connection, or verification failure."""


# ---------------------------------------------------------------------------
# Privilege check
# ---------------------------------------------------------------------------
def is_admin() -> bool:
    """True if the current process has the privileges OpenVPN needs."""
    if os.name == "nt":
        try:
            import ctypes
            return bool(ctypes.windll.shell32.IsUserAnAdmin())
        except Exception:
            return False
    return os.geteuid() == 0  # POSIX: root


# ---------------------------------------------------------------------------
# Discovery + auth helpers
# ---------------------------------------------------------------------------
def find_openvpn_executable() -> str:
    """Return a path to openvpn[.exe], raising VPNError if not found."""
    on_path = shutil.which("openvpn")
    if on_path:
        return on_path
    for candidate in _OPENVPN_CANDIDATES:
        if Path(candidate).exists():
            return candidate
    raise VPNError(
        "Could not locate the OpenVPN binary. Install it from "
        "https://openvpn.net/community-downloads/ (Windows) "
        "or via your package manager (Linux), or pass openvpn_exe= explicitly."
    )


def parse_credentials_file(path: Path = CREDENTIALS_FILE) -> tuple[str, str]:
    """Parse the labelled credentials.txt produced by the downloader / user."""
    if not path.exists():
        raise VPNError(f"Credentials file not found: {path}")
    found: dict[str, str] = {}
    # 'utf-8-sig' strips the BOM Notepad adds when saving as UTF-8.
    for line in path.read_text(encoding="utf-8-sig").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or ":" not in line:
            continue
        key, value = line.split(":", 1)
        found[key.strip().lower()] = value.strip()
    if "username" not in found or "password" not in found:
        raise VPNError(
            f"Username and/or Password missing in {path}. "
            f"File must contain lines:\n"
            f"  Username: <your username>\n"
            f"  Password: <your password>"
        )
    return found["username"], found["password"]


def write_auth_file(
    username: str,
    password: str,
    path: Path = AUTH_FILE,
) -> Path:
    """Write the user/pass file in the form OpenVPN's --auth-user-pass expects."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(f"{username}\n{password}\n", encoding="utf-8")
    if os.name != "nt":
        os.chmod(path, 0o600)
    return path


def ensure_auth_file() -> Path:
    """Create auth.txt from credentials.txt if needed; return its path."""
    if AUTH_FILE.exists():
        return AUTH_FILE
    user, pwd = parse_credentials_file()
    return write_auth_file(user, pwd)


def list_ovpn_configs(
    directory: Path = VPN_DIR,
    *,
    country: Optional[str] = None,
    protocol: Optional[str] = None,
) -> list[Path]:
    """
    All downloaded .ovpn configs, sorted alphabetically.

    Supported filename patterns:
      • VPNBook : vpnbook-<country>-server-<n>-<protocol>.ovpn
      • VPNGate : vpngate-<country>-<hostname>-<ip>.ovpn

    Country/protocol filters match the token as a delimited segment anywhere
    in the filename stem, so both naming conventions are handled.

    Examples
    --------
        list_ovpn_configs(country="us")              # all US exits, both sources
        list_ovpn_configs(country="jp")              # Japan (VPNGate only)
        list_ovpn_configs(protocol="udp53")          # VPNBook UDP-53
    """
    all_configs = sorted(directory.glob("*.ovpn"))
    if country:
        token = country.lower()
        pattern = re.compile(rf"(^|-){re.escape(token)}(-|$)")
        all_configs = [p for p in all_configs if pattern.search(p.stem.lower())]
    if protocol:
        token = protocol.lower()
        all_configs = [p for p in all_configs if p.stem.lower().endswith(f"-{token}")]
    return all_configs


# ---------------------------------------------------------------------------
# Public-IP probe (verification)
# ---------------------------------------------------------------------------
_IP_PROBES = (
    "https://api.ipify.org",
    "https://ifconfig.me/ip",
    "https://icanhazip.com",
)
_IP_RX = re.compile(r"^\d{1,3}(?:\.\d{1,3}){3}$")


def get_public_ip(timeout: float = 8.0) -> Optional[str]:
    """Return the host's current public IPv4, or None if all probes fail."""
    for url in _IP_PROBES:
        try:
            resp = requests.get(url, timeout=timeout)
            if resp.ok:
                ip = resp.text.strip()
                if _IP_RX.match(ip):
                    return ip
        except requests.RequestException:
            continue
    return None


# ---------------------------------------------------------------------------
# VPNConnection — context manager
# ---------------------------------------------------------------------------
class VPNConnection:
    """
    Bring up a single OpenVPN tunnel for the lifetime of a `with` block.

        with VPNConnection("vpnbook/vpnbook-us-server-1-tcp443.ovpn") as vpn:
            print("Tunnelled IP:", vpn.ip_after)
            ...  # all requests in this block exit through the VPN
        # tunnel torn down here

    Notes
    -----
    - Must run with admin privileges; otherwise OpenVPN exits with code 1.
    - `verify_ip=True` (default) compares public IP before/after and raises
      if the tunnel didn't actually take effect.
    """

    def __init__(
        self,
        config_path: Path | str,
        *,
        auth_file: Optional[Path] = None,
        openvpn_exe: Optional[str] = None,
        connect_timeout: int = CONNECT_TIMEOUT,
        verify_ip: bool = True,
        log_file: Optional[Path] = DEFAULT_LOG_FILE,
    ) -> None:
        self.config_path = Path(config_path).resolve()
        # Caller can force a specific auth file; otherwise resolved per-config.
        self.auth_file_override: Optional[Path] = (
            Path(auth_file) if auth_file is not None else None
        )
        self.openvpn_exe = openvpn_exe or find_openvpn_executable()
        self.connect_timeout = connect_timeout
        self.verify_ip = verify_ip
        self.log_file = Path(log_file) if log_file else None

        self.process: Optional[subprocess.Popen] = None
        self.ip_before: Optional[str] = None
        self.ip_after: Optional[str] = None

    def _needs_auth(self) -> bool:
        """True if the .ovpn file requires username/password (e.g. VPNBook).

        VPNGate configs are certificate-only and have no `auth-user-pass`
        directive — passing --auth-user-pass to them confuses OpenVPN.
        """
        try:
            text = self.config_path.read_text(errors="ignore")
        except OSError:
            return True
        for line in text.splitlines():
            stripped = line.strip()
            if not stripped or stripped.startswith("#"):
                continue
            if stripped == "auth-user-pass" or stripped.startswith("auth-user-pass "):
                return True
        return False

    def _resolve_auth_file(self) -> Path:
        """
        Pick the correct auth file for this config.

        Strict lookup — never falls back to a *different provider's*
        credentials, because doing so used to silently send (e.g.) VPNBook
        credentials to a VeePN server, which AUTH_FAILS.

        Provider detection (first match wins):
          • Filename contains "protonvpn" → use `proton_*` credentials.
            ProtonVPN's downloader names configs `<country>-free-N.protonvpn.tcp.ovpn`
            so the bare filename prefix is the country, not the provider.
          • Otherwise → use the filename prefix (vpnbook-, veepn-, vpngate-, ...).

        Lookup order per provider (first hit wins):
          1. <dir>/<provider>_auth.txt        (OpenVPN format: user\\npass\\n)
          2. <dir>/<provider>_credentials.txt (labelled, auto-converted)

        Legacy fallback for the `vpnbook` provider only:
          3. <dir>/auth.txt
          4. <dir>/credentials.txt            (labelled, auto-converted)
        """
        if self.auth_file_override is not None:
            return self.auth_file_override

        directory = self.config_path.parent
        stem_lower = self.config_path.stem.lower()
        if "protonvpn" in stem_lower:
            provider = "proton"
        elif "veepn" in stem_lower:
            provider = "veepn"
        else:
            provider = stem_lower.split("-", 1)[0]

        # Walk from the config's directory up to (and including) VPN_DIR so a
        # single shared credentials file (e.g. vpnbook/proton_credentials.txt)
        # works for every scraper-specific subfolder.
        search_dirs: list[Path] = [directory]
        try:
            vpn_root = VPN_DIR.resolve()
            cur = directory.resolve()
            while cur != vpn_root and vpn_root in cur.parents:
                cur = cur.parent
                search_dirs.append(cur)
        except OSError:
            pass

        provider_creds = directory / f"{provider}_credentials.txt"

        for d in search_dirs:
            cand_auth = d / f"{provider}_auth.txt"
            cand_creds = d / f"{provider}_credentials.txt"

            # 1: provider-specific raw auth file
            if cand_auth.exists():
                return cand_auth

            # 2: provider-specific labelled credentials file → auto-convert
            if cand_creds.exists():
                user, pwd = parse_credentials_file(cand_creds)
                target = d / f"{provider}_auth.txt"
                write_auth_file(user, pwd, target)
                log.info(f"Auto-converted {cand_creds} → {target.name}")
                return target

        # 3+4: legacy fallback — ONLY for `vpnbook` provider
        if provider == "vpnbook":
            for d in search_dirs:
                legacy_auth = d / "auth.txt"
                legacy_creds = d / "credentials.txt"
                if legacy_auth.exists():
                    return legacy_auth
                if legacy_creds.exists():
                    user, pwd = parse_credentials_file(legacy_creds)
                    target = d / "auth.txt"
                    write_auth_file(user, pwd, target)
                    log.info(f"Auto-converted credentials.txt → auth.txt")
                    return target

        # Strict failure — never send wrong provider's credentials
        raise VPNError(
            f"No credentials found for '{self.config_path.name}' "
            f"(detected provider: '{provider}').\n"
            f"\n"
            f"Create this file:\n"
            f"    {provider_creds}\n"
            f"\n"
            f"With this content (replace placeholders with real values):\n"
            f"\n"
            f"    Username: <your {provider} OpenVPN username>\n"
            f"    Password: <your {provider} OpenVPN password>\n"
            f"\n"
            f"For ProtonVPN: get these from account.protonvpn.com -> Account "
            f"-> OpenVPN / IKEv2 username (NOT your account login).\n"
            f"For VeePN:     get from veepn.com -> Overview -> Advanced "
            f"-> Linux -> 'Generate Login Credentials'."
        )

    # ── lifecycle ──────────────────────────────────────────────────────────
    def connect(self) -> "VPNConnection":
        if not self.config_path.exists():
            raise VPNError(f"Config not found: {self.config_path}")

        if not is_admin():
            raise VPNError(
                "OpenVPN requires elevated privileges to bring up the TAP/TUN "
                "interface.\n"
                "  Windows : right-click PowerShell -> 'Run as Administrator', "
                "then `cd D:\\usa_scrapping`, `.\\env\\Scripts\\activate`, and re-run.\n"
                "  Linux   : run with `sudo`."
            )

        if self.verify_ip:
            self.ip_before = get_public_ip()

        cmd = [
            self.openvpn_exe,
            "--config", str(self.config_path),
            "--disable-dco",            # avoid 'dco connect error errno=121' on OpenVPN 2.7+
            "--connect-retry-max", "2",
            "--verb", "3",
        ]
        if self._needs_auth():
            auth_path = self._resolve_auth_file()
            log.info(f"Using auth file: {auth_path.name}")
            cmd.extend([
                "--auth-user-pass", str(auth_path),
                "--auth-nocache",
            ])

        # On Windows we need a new process group to send CTRL_BREAK on shutdown.
        creationflags = 0
        if os.name == "nt":
            creationflags = subprocess.CREATE_NEW_PROCESS_GROUP  # type: ignore[attr-defined]

        stdout = subprocess.PIPE
        self.process = subprocess.Popen(
            cmd,
            stdout=stdout,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            bufsize=1,
            creationflags=creationflags,
        )

        self._wait_for_ready()
        time.sleep(ROUTE_SETTLE_SECONDS)

        if self.verify_ip:
            self.ip_after = get_public_ip()
            if not self.ip_after:
                self.disconnect()
                raise VPNError("Could not determine public IP after connect.")
            if self.ip_after == self.ip_before:
                self.disconnect()
                raise VPNError(
                    f"Public IP unchanged after connect ({self.ip_before}). "
                    "Tunnel may not be routing traffic."
                )
        return self

    def _wait_for_ready(self) -> None:
        assert self.process is not None
        log_fh = open(self.log_file, "a", encoding="utf-8") if self.log_file else None
        captured: list[str] = []
        deadline = time.time() + self.connect_timeout

        def _drain_remaining() -> None:
            """Pull any final output after the process has exited."""
            try:
                rest = self.process.stdout.read()  # type: ignore[union-attr]
            except Exception:
                return
            if not rest:
                return
            for ln in rest.splitlines():
                captured.append(ln)
                if log_fh:
                    log_fh.write(ln + "\n")

        def _tail() -> str:
            return "\n  ".join(captured[-20:]) if captured else "(no output captured)"

        try:
            while time.time() < deadline:
                line = self.process.stdout.readline()  # type: ignore[union-attr]
                if line:
                    captured.append(line.rstrip("\n"))
                    if log_fh:
                        log_fh.write(line)
                        log_fh.flush()
                    if "Initialization Sequence Completed" in line:
                        return
                    if "AUTH_FAILED" in line:
                        raise VPNError(
                            f"VPN authentication failed for {self.config_path.name}.\n"
                            "credentials.txt may be stale — re-run the downloader to "
                            "fetch the current password.\n"
                            f"\nOpenVPN tail:\n  {_tail()}"
                        )
                    continue

                # No new data; check whether OpenVPN has died.
                if self.process.poll() is not None:
                    _drain_remaining()
                    raise VPNError(
                        f"OpenVPN exited early (code {self.process.returncode}) while "
                        f"loading {self.config_path.name}.\n"
                        "\nCommon causes:\n"
                        "  • TAP-Windows adapter missing — reinstall OpenVPN and tick "
                        "'TAP Virtual Ethernet Adapter' during setup.\n"
                        "  • Another OpenVPN session is already running — kill it in "
                        "Task Manager or via `taskkill /F /IM openvpn.exe`.\n"
                        "  • Windows Defender / antivirus blocking the tunnel — allow "
                        "openvpn.exe in your firewall.\n"
                        "  • The .ovpn references an option your installed OpenVPN "
                        "version doesn't support (rare).\n"
                        f"\nOpenVPN tail:\n  {_tail()}\n"
                        f"\nFull log: {self.log_file}"
                    )

                time.sleep(0.1)

            # Hit the timeout without seeing the completion line.
            raise VPNError(
                f"Timed out after {self.connect_timeout}s waiting for "
                f"{self.config_path.name} to connect.\n"
                f"\nOpenVPN tail:\n  {_tail()}\n"
                f"\nFull log: {self.log_file}"
            )
        finally:
            if log_fh:
                log_fh.close()

    def disconnect(self) -> None:
        if self.process is None:
            return
        try:
            if os.name == "nt":
                # CTRL_BREAK_EVENT is the clean shutdown signal on Windows.
                self.process.send_signal(subprocess.signal.CTRL_BREAK_EVENT)  # type: ignore[attr-defined]
            else:
                self.process.terminate()
            try:
                self.process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=5)
        except Exception:
            try:
                self.process.kill()
            except Exception:
                pass
        finally:
            self.process = None

    # ── context manager protocol ───────────────────────────────────────────
    def __enter__(self) -> "VPNConnection":
        return self.connect()

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.disconnect()


# ---------------------------------------------------------------------------
# Block detection — heuristic for "did this exit IP get blocked?"
# ---------------------------------------------------------------------------
_BLOCK_KEYWORDS = (
    "blocked", "captcha", "forbidden", "denied", "cloudflare",
    "rate limit", "too many requests", "expecting value",
    "could not fetch", "jsondecodeerror", "challenge", "access denied",
)


def default_block_detector(exc: BaseException) -> bool:
    """
    Best-effort guess: did this exception fire because the VPN exit IP
    was blocked / rate-limited / served a challenge page?

    Recognises:
      • JSONDecodeError (target returned HTML instead of JSON)
      • HTTP 403 / 429 / 502 / 503 / 504
      • Connection / timeout errors (heavy WAFs sometimes RST the socket)
      • Exception messages containing any block-related keyword
    """
    if isinstance(exc, _jsonlib.JSONDecodeError):
        return True
    try:
        from requests.exceptions import (
            ConnectionError as _ReqConnErr,
            HTTPError as _HTTPError,
            JSONDecodeError as _ReqJSONErr,
            Timeout as _Timeout,
        )
    except Exception:
        _ReqConnErr = _HTTPError = _ReqJSONErr = _Timeout = ()  # type: ignore

    if isinstance(exc, _ReqJSONErr):
        return True
    if isinstance(exc, _HTTPError):
        status = getattr(getattr(exc, "response", None), "status_code", 0)
        return status in (403, 429, 502, 503, 504)
    if isinstance(exc, (_ReqConnErr, _Timeout)):
        return True

    msg = str(exc).lower()
    return any(kw in msg for kw in _BLOCK_KEYWORDS)


# ---------------------------------------------------------------------------
# VPNRotator — iterate through configs with auto-reconnect + failover
# ---------------------------------------------------------------------------
class VPNRotator:
    """
    Cycle through every .ovpn config in `vpnbook/`, yielding one active
    VPNConnection at a time, OR run a callable with auto-failover.

    Two usage modes:

    1. Manual cycle (one VPN per loop iteration):

        rotator = VPNRotator(country="us")
        for vpn in rotator.cycle():
            do_one_batch_of_requests()

    2. Failover wrapper (rotate on detected blockage, stop on success):

        rotator = VPNRotator(country="us")
        result = rotator.run_with_failover(brilliance_diamonds_scraper)

    Skips configs that fail to connect (auth issue, server down) and logs
    the failure but never raises mid-iteration.
    """

    def __init__(
        self,
        directory: Path = VPN_DIR,
        *,
        country: Optional[str] = None,
        protocol: Optional[str] = None,
        shuffle: bool = True,
        verify_ip: bool = True,
        on_error: Optional[Callable[[Path, Exception], None]] = None,
    ) -> None:
        self.configs = list_ovpn_configs(
            directory, country=country, protocol=protocol
        )
        if not self.configs:
            raise VPNError(
                f"No .ovpn files in {directory} matching "
                f"country={country!r}, protocol={protocol!r}"
            )
        if shuffle:
            random.shuffle(self.configs)
        self.verify_ip = verify_ip
        self.on_error = on_error or (lambda _p, _e: None)
        log.info(
            f"Rotator ready: {len(self.configs)} config(s)"
            + (f" country={country}" if country else "")
            + (f" protocol={protocol}" if protocol else "")
        )

    def cycle(self) -> Iterator[VPNConnection]:
        for cfg in self.configs:
            try:
                with VPNConnection(cfg, verify_ip=self.verify_ip) as vpn:
                    yield vpn
            except VPNError as exc:
                self.on_error(cfg, exc)
                continue

    def run_with_failover(
        self,
        func: Callable[..., Any],
        *args: Any,
        is_blocked: Optional[Callable[[BaseException], bool]] = None,
        max_attempts: Optional[int] = None,
        idle_seconds: float = 2.0,
        **kwargs: Any,
    ) -> Any:
        """
        Run `func(*args, **kwargs)` under successive VPN exits until it
        returns successfully.

        Behaviour:
          • Connect VPN #1 → run func() → if it raises and is_blocked(exc)
            is True, disconnect, rotate to next config, try again.
          • Non-blockage exceptions are RE-RAISED immediately so genuine
            scraper bugs don't get silently retried 40 times.
          • Stops when func() returns, max_attempts is hit, or all configs
            have been tried.

        Returns whatever func() returns on success.
        Raises VPNError if every config was tried and func still failed.
        """
        detector = is_blocked or default_block_detector
        cap = max_attempts if max_attempts is not None else len(self.configs)
        last_exc: Optional[BaseException] = None
        last_cfg: Optional[Path] = None
        attempts = 0

        for cfg in self.configs:
            if attempts >= cap:
                break
            attempts += 1
            log.info(
                f"[failover {attempts}/{cap}] connecting via {cfg.name}"
            )
            last_cfg = cfg

            # VPN connect
            try:
                vpn = VPNConnection(cfg, verify_ip=self.verify_ip).connect()
            except VPNError as exc:
                log.warning(
                    f"[failover {attempts}/{cap}] VPN connect failed: {exc}. "
                    "rotating to next config"
                )
                self.on_error(cfg, exc)
                continue

            # Run the user's function while the tunnel is up
            try:
                log.info(
                    f"[failover {attempts}/{cap}] tunnel up "
                    f"(exit IP {vpn.ip_after}) — running {func.__name__}()"
                )
                result = func(*args, **kwargs)
                log.info(
                    f"[failover {attempts}/{cap}] {func.__name__}() "
                    f"completed successfully"
                )
                return result

            except BaseException as exc:
                if isinstance(exc, KeyboardInterrupt):
                    log.info("[failover] interrupted by user")
                    raise
                if detector(exc):
                    log.warning(
                        f"[failover {attempts}/{cap}] BLOCKED — "
                        f"{type(exc).__name__}: {str(exc)[:200]}"
                    )
                    last_exc = exc
                else:
                    log.error(
                        f"[failover {attempts}/{cap}] non-blockage error in "
                        f"{func.__name__}() — re-raising"
                    )
                    raise

            finally:
                vpn.disconnect()
                log.info(f"[failover {attempts}/{cap}] tunnel down")

            if idle_seconds > 0:
                time.sleep(idle_seconds)

        raise VPNError(
            f"All {attempts} VPN failover attempt(s) exhausted "
            f"(last config: {last_cfg.name if last_cfg else 'n/a'}). "
            f"Last error: {type(last_exc).__name__ if last_exc else 'N/A'}: "
            f"{last_exc}"
        )


# ---------------------------------------------------------------------------
# Convenience: run a callable through every config in turn
# ---------------------------------------------------------------------------
@contextmanager
def random_vpn(
    verify_ip: bool = True,
    *,
    country: Optional[str] = None,
    protocol: Optional[str] = None,
) -> Iterator[VPNConnection]:
    """
    Pick one random .ovpn from vpnbook/ and run a block under it.

        with random_vpn() as vpn:                      # any country, any protocol
            ...

        with random_vpn(country="us") as vpn:          # US exits only
            ...

        with random_vpn(country="uk", protocol="tcp443") as vpn:
            ...
    """
    configs = list_ovpn_configs(country=country, protocol=protocol)
    if not configs:
        raise VPNError(
            f"No .ovpn files matched country={country!r}, protocol={protocol!r} in {VPN_DIR}"
        )
    choice = random.choice(configs)
    with VPNConnection(choice, verify_ip=verify_ip) as vpn:
        yield vpn


# ---------------------------------------------------------------------------
# Cleanup helper — kill any leftover openvpn processes. Safe to call repeatedly
# and when no openvpn process is running. Used by the scrapers' Ctrl+C handlers
# so an interrupted tunnel doesn't keep the system routed through the VPN.
# ---------------------------------------------------------------------------
def kill_openvpn_processes() -> None:
    import platform
    system = platform.system()
    try:
        if system == "Windows":
            result = subprocess.run(
                ["taskkill", "/F", "/IM", "openvpn.exe"],
                capture_output=True, text=True, timeout=15,
            )
            if result.returncode == 0:
                log.info("[VPN cleanup] openvpn.exe terminated")
            elif "not found" in (result.stderr + result.stdout).lower():
                log.info("[VPN cleanup] no openvpn.exe running")
            else:
                log.warning(f"[VPN cleanup] taskkill rc={result.returncode}")
        else:
            result = subprocess.run(
                ["pkill", "-f", "openvpn"],
                capture_output=True, text=True, timeout=15,
            )
            if result.returncode in (0, 1):
                log.info("[VPN cleanup] pkill openvpn done")
            else:
                log.warning(f"[VPN cleanup] pkill rc={result.returncode}")
    except Exception as exc:
        log.warning(f"[VPN cleanup] error: {exc}")


# ---------------------------------------------------------------------------
# CLI smoke test:  python vpn_rotator.py
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    print(f"Found {len(list_ovpn_configs())} configs in {VPN_DIR}")
    print(f"Real public IP (before tunnel): {get_public_ip()}")
    with random_vpn() as vpn:
        print(f"Connected via {vpn.config_path.name}")
        print(f"Tunnelled IP: {vpn.ip_after}")
    print(f"Real public IP (after tunnel):  {get_public_ip()}")