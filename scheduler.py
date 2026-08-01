"""
Root diamond-data scheduler — manages all regional scraping pipelines.

WHAT IT RUNS
────────────
  USA     2 vendor scrapers : brilliance, loosegrown    (Mon + Thu @ 02:00)
  UK      2 vendor scrapers : diamond-heaven, abelini   (Wed + Sat @ 02:00)
  AUS     2 vendor scrapers : cullen, gs                (Fri + Tue @ 02:00)
  GERMANY 3 vendor scrapers : 77diamond, novita, renesim (Sun @ 02:00)

Each region also runs its baseline scraper, compare script, and DB loader
in the correct order before the pipeline finishes.

HOW TO CHANGE THE SCHEDULE
───────────────────────────
  Edit WEEKLY_JOBS near the top of this file.
  Keys are (weekday_string, "HH:MM") → list of region names.
  Weekday strings: "monday" "tuesday" "wednesday" "thursday" "friday" "saturday" "sunday"

  Example — run germany on tuesday at 03:30 instead:
    ("tuesday", "03:30"): ["germany"],

HOW TO RUN (choose one)
────────────────────────
  1. Keep alive in background (Windows, no console):
       pythonw scheduler.py

  2. Keep alive with console output:
       python scheduler.py

  3. Register as a Windows Task Scheduler job that auto-starts at login:
       schtasks /create /tn "DiamondScheduler" ^
                /tr "pythonw D:\\diamond_data_scrapping\\scheduler.py" ^
                /sc onlogon /ru SYSTEM /f

  4. To run a region IMMEDIATELY without waiting for the schedule (testing):
       python scheduler.py --run aus
       python scheduler.py --run uk usa aus
       python scheduler.py --run germany

INSTALL DEPENDENCY (once)
──────────────────────────
  pip install schedule
"""

from __future__ import annotations

import argparse
import logging
import subprocess
import sys
import time
import json
from datetime import datetime, timedelta
from pathlib import Path

try:
    import schedule
except ImportError:
    print("ERROR: 'schedule' package not installed. Run:  pip install schedule")
    sys.exit(1)

# ─────────────────────────────────────────────────────────────────────────────
# Paths
# ─────────────────────────────────────────────────────────────────────────────
BASE        = Path(__file__).parent.resolve()
LOG_FILE    = BASE / "scheduler.log"
STATUS_FILE = BASE / "scheduler_status.json"
HTML_FILE   = BASE / "scheduler_status.html"

# ─────────────────────────────────────────────────────────────────────────────
# Pipeline definitions
#
# Each region is an ordered list of scripts that run one-by-one.
# Order matters: baseline first → vendors → compare → DB upload.
# If a script does not exist it is skipped with a warning (SKIP_MISSING=True).
# ─────────────────────────────────────────────────────────────────────────────
PIPELINES: dict[str, list[Path]] = {

    # ── AUS  (baseline: ROA | vendors: cullen · gs) ──────────────────────────
    "aus": [
        BASE / "aus_scrapping" / "roa_diamond.py",         # 1. baseline (ROA)
        BASE / "aus_scrapping" / "cullen_diamonds.py",     # 2. vendor 1 – cullen
        BASE / "aus_scrapping" / "gs_diamonds.py",         # 3. vendor 2 – GS Diamonds
        BASE / "aus_scrapping" / "diamond_compare.py",     # 4. compare
        BASE / "aus_scrapping" / "roa_compare_to_db.py",   # 5. DB upload
    ],

    # ── UK   (baseline: ROUK | vendors: diamond-heaven · abelini) ────────────
    "uk": [
        BASE / "uk_scrapping" / "rouk_diamonds.py",        # 1. baseline (ROUK)
        BASE / "uk_scrapping" / "diamond-heavon.py",       # 2. vendor 1 – Diamond Heaven
        BASE / "uk_scrapping" / "abelini.py",              # 3. vendor 2 – Abelini
        BASE / "uk_scrapping" / "diamond_compare_uk.py",   # 4. compare
        BASE / "uk_scrapping" / "uk_compare_to_db.py",     # 5. DB upload
    ],

    # ── USA  (baseline: PC | vendors: brilliance · loosegrown) ───────────────
    "usa": [
        BASE / "usa_compare" / "pc_diamonds.py",           # 1. baseline (Precious Carbon)
        BASE / "usa_compare" / "brilliance_diamonds.py",   # 2. vendor 1 – Brilliance
        BASE / "usa_compare" / "loosegrown_diamonds.py",   # 3. vendor 2 – Loosegrown
        BASE / "usa_compare" / "diamond_compare.py",       # 4. compare
        BASE / "usa_compare" / "compare_to_mysql.py",      # 5. DB upload
    ],

    # ── GERMANY  (baseline: ROG | vendors: 77diamond · novita · renesim) ─────
    "germany": [
        BASE / "germany_scrapping" / "rog_diamonds.py",          # 1. baseline (ROG)
        BASE / "germany_scrapping" / "77diamond.py",             # 2. vendor 1 – 77 Diamonds
        BASE / "germany_scrapping" / "novita_diamonds_de.py",    # 3. vendor 2 – Novita DE
        BASE / "germany_scrapping" / "renesim_diamond.py",       # 4. vendor 3 – Renesim
        BASE / "germany_scrapping" / "diamond_compare.py",       # 5. compare
        BASE / "germany_scrapping" / "germany_compare_to_db.py", # 6. DB upload
    ],
}

# ─────────────────────────────────────────────────────────────────────────────
# Weekly schedule
#
#   Key   : (weekday, "HH:MM")
#   Value : list of region names from PIPELINES to run in sequence
#
#   Current plan:
#     AUS + UK + USA  → Monday & Thursday at 02:00  (2× per week)
#     GERMANY         → Wednesday at 02:00           (1× per week)
# ─────────────────────────────────────────────────────────────────────────────
WEEKLY_JOBS: dict[tuple[str, str], list[str]] = {
    # USA  → 2× per week  (Mon start, Thu second run)
    ("monday",    "02:00"): ["usa"],
    # ("thursday",  "02:00"): ["usa"],

    # UK   → 2× per week  (Wed start, Sat second run)
    ("wednesday", "02:00"): ["uk"],
    # ("saturday",  "02:00"): ["uk"],

    # AUS  → 2× per week  (Fri start, Tue second run)
    ("friday",    "02:00"): ["aus"],
    # ("tuesday",   "02:00"): ["aus"],

    # GERMANY → 1× per week  (Sun)
    ("sunday",    "02:00"): ["germany"],
}

# ─────────────────────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────────────────────
TIMEOUT_PER_SCRIPT = 60 * 60 * 3   # 3 hours max per individual script
SKIP_MISSING       = True           # skip scripts that don't exist (warn, don't abort)
STALE_CSV_DAYS     = 2             # delete diamond CSVs older than this before pipeline runs

# CSV glob patterns per region (relative to BASE/diamond_files/<subdir>/)
REGION_CSV_GLOBS: dict[str, tuple[str, list[str]]] = {
    "usa": ("usa", [
        "precious_carbon.csv",
        "brilliance_diamonds.csv",
        "loosegrowndiamond.csv",
    ]),
    "uk": ("uk", [
        "rouk_diamonds.csv",
        "diamond_heaven.csv",
        "abelini.csv",
    ]),
    "aus": ("aus", [
        "roa_*.csv",            # date-stamped filenames
        "cullen_diamonds.csv",
        "gs_diamonds.csv",
    ]),
    "germany": ("germany", [
        "rog_diamonds.csv",
        "77diamonds.csv",
        "novita_diamonds.csv",
        "renesim_diamonds.csv",
    ]),
}

# ─────────────────────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("scheduler")


# ─────────────────────────────────────────────────────────────────────────────
# Status tracking  (writes scheduler_status.json + scheduler_status.html)
# ─────────────────────────────────────────────────────────────────────────────
_WEEKDAY = {"monday":0,"tuesday":1,"wednesday":2,"thursday":3,
            "friday":4,"saturday":5,"sunday":6}

def _compute_next_run(region: str) -> datetime | None:
    now  = datetime.now()
    best = None
    for (day_str, time_str), regions in WEEKLY_JOBS.items():
        if region not in regions:
            continue
        h, m       = map(int, time_str.split(":"))
        days_ahead = (_WEEKDAY[day_str] - now.weekday()) % 7
        candidate  = (now + timedelta(days=days_ahead)).replace(
            hour=h, minute=m, second=0, microsecond=0)
        if candidate <= now:                    # already passed today → next week
            candidate += timedelta(days=7)
        if best is None or candidate < best:
            best = candidate
    return best


def _schedule_label(region: str) -> str:
    """Return human-readable schedule string, e.g. 'Mon + Thu @ 02:00'."""
    DAY_SHORT = {"monday":"Mon","tuesday":"Tue","wednesday":"Wed","thursday":"Thu",
                 "friday":"Fri","saturday":"Sat","sunday":"Sun"}
    entries: list[str] = []
    for (day_str, time_str), regions in WEEKLY_JOBS.items():
        if region in regions:
            entries.append(f"{DAY_SHORT[day_str]} @ {time_str}")
    _ORDER = {"Mon":0,"Tue":1,"Wed":2,"Thu":3,"Fri":4,"Sat":5,"Sun":6}
    return "  ·  ".join(sorted(entries, key=lambda x: _ORDER.get(x[:3], 7)))


def _load_status() -> dict:
    if STATUS_FILE.exists():
        try:
            return json.loads(STATUS_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {"regions": {}}


def _update_region_status(region: str, start: datetime, end: datetime, ok: bool) -> None:
    data = _load_status()
    data.setdefault("regions", {})[region] = {
        "last_run_start":    start.strftime("%Y-%m-%d %H:%M:%S"),
        "last_run_end":      end.strftime("%Y-%m-%d %H:%M:%S"),
        "duration_seconds":  int((end - start).total_seconds()),
        "status":            "success" if ok else "failed",
    }
    data["last_updated"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    STATUS_FILE.write_text(json.dumps(data, indent=2), encoding="utf-8")


def _write_html() -> None:
    """Generate scheduler_status.html from the current status JSON."""
    data    = _load_status()
    regions_data = data.get("regions", {})
    last_upd     = data.get("last_updated", "—")

    REGION_COLOR = {"aus": "#fbbf24", "uk": "#60a5fa",
                    "usa": "#34d399",  "germany": "#f87171"}
    SCRAPER_LABELS = {
        "aus":     "ROA (baseline) · cullen · gs",
        "uk":      "ROUK (baseline) · diamond-heaven · abelini",
        "usa":     "PC (baseline) · brilliance · loosegrown",
        "germany": "ROG (baseline) · 77diamond · novita · renesim",
    }

    next_runs_js: dict[str, float] = {}
    rows_html = ""
    for region in ["usa", "uk", "aus", "germany"]:
        info      = regions_data.get(region, {})
        status    = info.get("status", "never")
        last_start = info.get("last_run_start", "—")
        last_end   = info.get("last_run_end",   "—")
        dur_s      = info.get("duration_seconds")
        color      = REGION_COLOR.get(region, "#e2e8f0")
        sched      = _schedule_label(region)
        scrapers   = SCRAPER_LABELS.get(region, "")
        next_dt    = _compute_next_run(region)

        # Duration string
        if dur_s is not None:
            m, s = divmod(dur_s, 60)
            h, m = divmod(m, 60)
            dur_str = (f"{h}h {m}m {s}s" if h else f"{m}m {s}s")
        else:
            dur_str = "—"

        # Badge
        if status == "success":
            badge = '<span style="background:#064e3b;color:#34d399;padding:3px 12px;border-radius:20px;font-size:.75rem;font-weight:700">SUCCESS</span>'
        elif status == "failed":
            badge = '<span style="background:#450a0a;color:#f87171;padding:3px 12px;border-radius:20px;font-size:.75rem;font-weight:700">FAILED</span>'
        else:
            badge = '<span style="background:#1e293b;color:#64748b;padding:3px 12px;border-radius:20px;font-size:.75rem;font-weight:700">NEVER RUN</span>'

        # Next run
        if next_dt:
            next_runs_js[region] = next_dt.timestamp()
            next_str = next_dt.strftime("%a, %d %b %Y @ %H:%M")
        else:
            next_str = "—"

        rows_html += f"""
    <tr>
      <td>
        <div style="font-weight:700;font-size:1rem;color:{color};letter-spacing:.05em">{region.upper()}</div>
        <div style="font-size:.72rem;color:#475569;margin-top:4px">{scrapers}</div>
      </td>
      <td style="color:#94a3b8;font-size:.82rem">{sched}</td>
      <td>
        <div style="color:#e2e8f0;font-size:.85rem">{last_start}</div>
        <div style="color:#475569;font-size:.75rem">ended&nbsp;{last_end}</div>
      </td>
      <td style="color:#94a3b8;font-size:.85rem">{dur_str}</td>
      <td>{badge}</td>
      <td style="color:#e2e8f0;font-size:.85rem">{next_str}</td>
      <td id="cd_{region}" style="color:#94a3b8;font-size:.85rem;font-variant-numeric:tabular-nums">…</td>
    </tr>"""

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta http-equiv="refresh" content="60">
<title>Diamond Scheduler Dashboard</title>
<style>
  *{{box-sizing:border-box;margin:0;padding:0}}
  body{{font-family:'Segoe UI',sans-serif;background:#0f1117;color:#e2e8f0;padding:40px 32px}}
  h1{{font-size:1.5rem;font-weight:700;color:#f1f5f9;margin-bottom:4px}}
  .sub{{color:#475569;font-size:.82rem;margin-bottom:28px}}
  table{{width:100%;border-collapse:collapse;background:#1a2030;border-radius:12px;overflow:hidden;box-shadow:0 4px 24px #0006}}
  th{{background:#1e2840;color:#64748b;font-size:.7rem;text-transform:uppercase;letter-spacing:.07em;padding:14px 20px;text-align:left;white-space:nowrap}}
  td{{padding:16px 20px;border-top:1px solid #1e2840;vertical-align:middle}}
  tr:hover td{{background:#1e2535}}
  .dot{{display:inline-block;width:8px;height:8px;border-radius:50%;margin-right:6px;vertical-align:middle}}
</style>
</head>
<body>
<h1>Diamond Scraper Schedule</h1>
<p class="sub">Auto-refreshes every 60 s &nbsp;&middot;&nbsp; Last updated: <b>{last_upd}</b></p>
<table>
  <thead>
    <tr>
      <th>Region</th>
      <th>Schedule</th>
      <th>Last Run</th>
      <th>Duration</th>
      <th>Status</th>
      <th>Next Run</th>
      <th>Countdown</th>
    </tr>
  </thead>
  <tbody>{rows_html}
  </tbody>
</table>
<script>
  const NR = {json.dumps(next_runs_js)};
  function fmt(s){{
    if(s<=0)return'<span style="color:#34d399">Running now</span>';
    const d=Math.floor(s/86400),h=Math.floor(s%86400/3600),m=Math.floor(s%3600/60),sc=s%60;
    if(d>0)return d+'d '+h+'h '+m+'m';
    if(h>0)return h+'h '+m+'m '+sc+'s';
    return m+'m '+sc+'s';
  }}
  function tick(){{
    const now=Date.now()/1000;
    for(const[r,ts]of Object.entries(NR)){{
      const el=document.getElementById('cd_'+r);
      if(el)el.innerHTML=fmt(Math.max(0,Math.round(ts-now)));
    }}
  }}
  tick(); setInterval(tick,1000);
</script>
</body>
</html>"""

    HTML_FILE.write_text(html, encoding="utf-8")
    log.info(f"Dashboard written → {HTML_FILE.name}")


# ─────────────────────────────────────────────────────────────────────────────
# CSV purge  (runs before every pipeline to delete stale diamond data files)
# ─────────────────────────────────────────────────────────────────────────────
def purge_old_csvs(region: str) -> None:
    """Delete diamond CSV files older than STALE_CSV_DAYS so scrapers start fresh."""
    entry = REGION_CSV_GLOBS.get(region)
    if not entry:
        return
    subdir, patterns = entry
    csv_dir = BASE / "diamond_files" / subdir
    if not csv_dir.exists():
        log.info(f"[{region.upper()}] CSV dir not found — nothing to purge: {csv_dir}")
        return

    cutoff  = datetime.now() - timedelta(days=STALE_CSV_DAYS)
    deleted = 0
    for pattern in patterns:
        for f in csv_dir.glob(pattern):
            try:
                mtime = datetime.fromtimestamp(f.stat().st_mtime)
                age_days = (datetime.now() - mtime).days
                if mtime < cutoff:
                    f.unlink()
                    log.info(f"[{region.upper()}] 🗑  Deleted stale CSV ({age_days}d old): {f.name}")
                    deleted += 1
                else:
                    log.info(f"[{region.upper()}] ✓  CSV is fresh ({age_days}d old): {f.name}")
            except Exception as exc:
                log.warning(f"[{region.upper()}] Could not delete {f.name}: {exc}")

    if deleted:
        log.info(f"[{region.upper()}] Purged {deleted} stale CSV file(s) from {csv_dir}")
    else:
        log.info(f"[{region.upper()}] No stale CSVs to purge (all < {STALE_CSV_DAYS}d old)")


# ─────────────────────────────────────────────────────────────────────────────
# Script runner
# ─────────────────────────────────────────────────────────────────────────────
def run_script(label: str, script: Path) -> bool | None:
    """
    Returns True on success, False on crash/bad exit-code, None on timeout.
    Callers treat None as "continue to next script" rather than "abort pipeline".
    """
    if not script.exists():
        if SKIP_MISSING:
            log.warning(f"[{label}] SKIP — script not found: {script}")
            return True
        log.error(f"[{label}] ABORT — script not found: {script}")
        return False

    log.info(f"[{label}] ▶  {script.name}")
    start = time.time()
    try:
        result = subprocess.run(
            [sys.executable, str(script)],
            cwd=str(script.parent),
            capture_output=False,
            timeout=TIMEOUT_PER_SCRIPT,
        )
        elapsed = round(time.time() - start, 1)
        if result.returncode == 0:
            log.info(f"[{label}] ✅  done in {elapsed}s")
            return True
        log.error(f"[{label}] ❌  exit code {result.returncode} after {elapsed}s")
        return False
    except subprocess.TimeoutExpired:
        log.error(f"[{label}] ⏰  timed out after {TIMEOUT_PER_SCRIPT}s — skipping to next script")
        return None
    except Exception as exc:
        log.error(f"[{label}] 💥  {exc}")
        return False


# ─────────────────────────────────────────────────────────────────────────────
# Pipeline runner  (one region)
# ─────────────────────────────────────────────────────────────────────────────
def run_pipeline(region: str) -> bool:
    scripts = PIPELINES.get(region)
    if not scripts:
        log.error(f"Unknown region '{region}' — not defined in PIPELINES.")
        return False

    log.info("=" * 60)
    log.info(f"PIPELINE  [{region.upper()}]  {len(scripts)} scripts")
    log.info("=" * 60)

    # Delete stale CSV files (> 2 days old) before running so scrapers start fresh
    purge_old_csvs(region)

    pipeline_start = datetime.now()
    ok = True
    for script in scripts:
        result = run_script(region.upper(), script)
        if result is None:
            # Timeout — move on, no retry
            log.warning(f"Pipeline [{region}] timed out at {script.name} — continuing with next script")
            ok = False
        elif not result:
            # Crash — retry once from where it left off (checkpoint resumes automatically)
            log.warning(f"Pipeline [{region}] crashed at {script.name} — retrying once from last checkpoint...")
            retry = run_script(region.upper(), script)
            if retry is True:
                log.info(f"Pipeline [{region}] retry succeeded for {script.name}")
            else:
                log.warning(f"Pipeline [{region}] retry also failed for {script.name} — continuing with next script")
                ok = False

    _update_region_status(region, pipeline_start, datetime.now(), ok)

    if ok:
        log.info(f"Pipeline [{region}] completed successfully.")
    return ok


# ─────────────────────────────────────────────────────────────────────────────
# Job runner  (called by schedule or --run CLI)
# ─────────────────────────────────────────────────────────────────────────────
def run_job(regions: list[str]) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log.info("=" * 60)
    log.info(f"JOB START  {ts}")
    log.info(f"Regions   : {', '.join(regions)}")
    log.info("=" * 60)

    results: dict[str, bool] = {}
    for region in regions:
        results[region] = run_pipeline(region)

    log.info("=" * 60)
    log.info("JOB SUMMARY")
    for region, ok in results.items():
        status = "✅ OK" if ok else "❌ FAILED"
        log.info(f"  {region:<10} {status}")
    log.info("=" * 60)

    _write_html()


# ─────────────────────────────────────────────────────────────────────────────
# Schedule setup
# ─────────────────────────────────────────────────────────────────────────────
def setup_schedule() -> None:
    """Register all jobs defined in WEEKLY_JOBS with the schedule library."""
    for (day, time_str), regions in WEEKLY_JOBS.items():
        day_job = getattr(schedule.every(), day)   # e.g. schedule.every().monday
        day_job.at(time_str).do(run_job, regions)
        log.info(f"  Scheduled [{', '.join(regions)}] → every {day} at {time_str}")


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────
def main() -> None:
    parser = argparse.ArgumentParser(description="Diamond data scheduler")
    parser.add_argument(
        "--run",
        nargs="+",
        metavar="REGION",
        help="Run one or more regions immediately and exit. "
             f"Available: {', '.join(PIPELINES)}",
    )
    args = parser.parse_args()

    log.info("=" * 60)
    log.info(f"Diamond Scheduler  started {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log.info(f"Base dir : {BASE}")
    log.info(f"Log file : {LOG_FILE}")

    # ── Immediate run mode (--run aus  or  --run uk usa aus) ─────────────────
    if args.run:
        unknown = [r for r in args.run if r not in PIPELINES]
        if unknown:
            log.error(f"Unknown region(s): {', '.join(unknown)}")
            log.error(f"Available: {', '.join(PIPELINES)}")
            sys.exit(1)
        run_job(args.run)
        sys.exit(0)

    # ── Normal mode: register weekly jobs and loop forever ───────────────────
    log.info("Registering weekly jobs:")
    setup_schedule()
    log.info("Scheduler is running.  Press Ctrl+C to stop.")
    log.info("=" * 60)

    try:
        while True:
            schedule.run_pending()
            time.sleep(60)          # check every minute
    except KeyboardInterrupt:
        log.info("Scheduler stopped by user.")


if __name__ == "__main__":
    main()
