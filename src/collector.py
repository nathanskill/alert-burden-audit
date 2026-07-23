#!/usr/bin/env python3
"""
collector.py — data collector for the alert-burden audit (REF-2026-017).

Implements the monitored-universe rule and data plan of
protocol/locked_protocol_v1.0.md (frozen at commit 0ac2bbd):

  * Monitored universe (protocol §3, mechanical, no manual curation):
      all Binance spot pairs quoted in USDT with status TRADING,
      excluding the top 50 by trailing 30-day quote volume.
      Membership tables are archived with SHA-256 hashes; refreshed weekly.
  * Data (protocol §4):
      daily aggTrades and 1-minute klines per monitored pair from the
      Binance official public archives (data.binance.vision), pulled daily
      with a 1-2 day publication lag; SHA-256 manifests for every file.
      Raw pulls are never committed (data/ is gitignored); universe tables
      and manifests are mirrored into artifacts/ for the public record.
  * Primary evaluation stream starts 2026-07-24 00:00 UTC (first UTC
      midnight after the freeze commit). `--daily` pulls nothing dated
      before that day.

Subcommands
-----------
  --update-universe        Build and archive a new universe membership table.
  --pull-day YYYY-MM-DD    Pull one UTC day of archives for the current universe.
  --daily                  Pull all uncollected published days in the evaluation
                           window; refresh the universe if it is >7 days old.

Dependencies: Python 3.9+ standard library + `requests`.

Determinism notes: the universe table is sorted by (vol30d desc, symbol asc)
so equal-volume ties break alphabetically; manifests record file SHA-256 and
whether the official *.zip.CHECKSUM matched. All timestamps are UTC.
"""

import argparse
import concurrent.futures
import csv
import datetime as dt
import hashlib
import io
import logging
import os
import shutil
import sys
import threading
import time
import zipfile

import requests

# --------------------------------------------------------------------------
# Constants
# --------------------------------------------------------------------------

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(REPO_ROOT, "data")
UNIVERSE_DIR = os.path.join(DATA_DIR, "universe")
RAW_AGG_DIR = os.path.join(DATA_DIR, "raw", "aggTrades")
RAW_K1M_DIR = os.path.join(DATA_DIR, "raw", "klines1m")
MANIFEST_DIR = os.path.join(DATA_DIR, "manifests")
MANIFEST_CSV = os.path.join(MANIFEST_DIR, "pull_manifest.csv")
COVERAGE_CSV = os.path.join(MANIFEST_DIR, "coverage_log.csv")
DAYS_DONE_TXT = os.path.join(MANIFEST_DIR, "days_completed.txt")
ART_UNIVERSE_DIR = os.path.join(REPO_ROOT, "artifacts", "universe")
ART_MANIFEST_DIR = os.path.join(REPO_ROOT, "artifacts", "manifests")

API_BASE = "https://api.binance.com"
VISION_BASE = "https://data.binance.vision"

# Protocol §4: primary evaluation stream = first 12 complete UTC weeks
# beginning at the first UTC midnight after the freeze commit (2026-07-23).
STREAM_START = dt.date(2026, 7, 24)

# Binance publishes daily archives with a 1-2 day lag; use 2 to be safe.
PUBLICATION_LAG_DAYS = 2

# Universe refresh cadence (protocol §3: weekly membership updates).
UNIVERSE_MAX_AGE_DAYS = 7

# Protocol §3 excludes the top N pairs by trailing 30-day quote volume.
TOP_N_EXCLUDED = 50
TRAILING_DAYS = 30

# ---- Mechanical exclusion lists (documented; no manual curation) ----------
# Leveraged-token bases end in one of these suffixes. Applied to the BASE
# asset (e.g. BTCUP, ETHDOWN, EOSBULL, XRPBEAR), not to ordinary symbols.
LEVERAGED_SUFFIXES = ("UP", "DOWN", "BULL", "BEAR")

# Stablecoin / fiat-pegged BASE assets: pairs such as USDCUSDT or EURUSDT are
# stable-vs-stable FX pairs, not small-cap crypto assets, and are excluded.
# Fixed mechanical list — entries not currently listed are harmless.
STABLE_FIAT_BASES = frozenset({
    "USDC", "FDUSD", "TUSD", "DAI", "BUSD", "USDP", "PAX", "PAXG",
    "EUR", "EURI", "AEUR", "GBP", "AUD", "BRL", "TRY", "RUB", "UAH",
    "NGN", "ZAR", "BIDR", "IDRT", "GYEN", "VAI", "UST", "USTC",
    "SUSD", "USDS", "USD1", "USDE", "XUSD", "BKRW", "COP", "ARS",
    "MXN", "JPY", "PLN", "RON", "CZK",
})

# Rate limiting for api.binance.com: stay well under 1000 request-weight/min.
# /api/v3/klines with limit<=100 costs weight 2; exchangeInfo costs 20.
KLINES_WEIGHT = 2
WEIGHT_BUDGET_PER_MIN = 1000
# seconds between klines calls: 60s / (budget/weight) with ~50% headroom
KLINES_SLEEP = 60.0 / (WEIGHT_BUDGET_PER_MIN / KLINES_WEIGHT) * 2.0  # 0.24 s

DOWNLOAD_THREADS = 8
HTTP_TIMEOUT = 60
RETRIES = 3
RETRY_BACKOFF = 5.0  # seconds, multiplied by attempt number

MANIFEST_FIELDS = ["date", "symbol", "file", "bytes", "sha256",
                   "source_checksum_ok"]
# Coverage log records EVERY attempt (incl. 404 = listing gap / delisting),
# so which pairs were unavailable on which day is auditable for the paper.
COVERAGE_FIELDS = ["date", "symbol", "file", "status", "bytes"]

log = logging.getLogger("collector")

# --------------------------------------------------------------------------
# Small helpers
# --------------------------------------------------------------------------


def utc_today():
    return dt.datetime.now(dt.timezone.utc).date()


def sha256_file(path, chunk=1 << 20):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(chunk), b""):
            h.update(block)
    return h.hexdigest()


def ensure_dirs():
    for d in (UNIVERSE_DIR, RAW_AGG_DIR, RAW_K1M_DIR, MANIFEST_DIR,
              ART_UNIVERSE_DIR, ART_MANIFEST_DIR):
        os.makedirs(d, exist_ok=True)


def http_get(session, url, **kw):
    """GET with simple bounded retries on transient errors. Returns the
    response (any status) or raises after RETRIES attempts."""
    last_exc = None
    for attempt in range(1, RETRIES + 1):
        try:
            resp = session.get(url, timeout=HTTP_TIMEOUT, **kw)
            if resp.status_code in (429, 418):  # rate-limited / banned
                wait = float(resp.headers.get("Retry-After", 60))
                log.warning("HTTP %s from %s; sleeping %.0fs",
                            resp.status_code, url, wait)
                time.sleep(wait)
                continue
            if resp.status_code >= 500:
                time.sleep(RETRY_BACKOFF * attempt)
                continue
            return resp
        except requests.RequestException as exc:
            last_exc = exc
            time.sleep(RETRY_BACKOFF * attempt)
    if last_exc:
        raise last_exc
    raise RuntimeError("exhausted retries for %s" % url)


# --------------------------------------------------------------------------
# Universe (protocol §3)
# --------------------------------------------------------------------------


def eligible_symbols(session):
    """All Binance spot symbols: status TRADING, quote USDT, spot-trading
    allowed, minus leveraged tokens and stable/fiat bases (mechanical
    lists above). Returns a sorted list of symbol strings."""
    resp = http_get(session, API_BASE + "/api/v3/exchangeInfo")
    resp.raise_for_status()
    out = []
    for s in resp.json()["symbols"]:
        if s.get("status") != "TRADING":
            continue
        if s.get("quoteAsset") != "USDT":
            continue
        if not s.get("isSpotTradingAllowed", False):
            continue
        base = s.get("baseAsset", "")
        if any(base.endswith(suf) for suf in LEVERAGED_SUFFIXES):
            continue
        if base in STABLE_FIAT_BASES:
            continue
        out.append(s["symbol"])
    return sorted(out)


def trailing_quote_volume(session, symbol):
    """Sum of quoteVolume over the last TRAILING_DAYS daily klines."""
    resp = http_get(session, API_BASE + "/api/v3/klines",
                    params={"symbol": symbol, "interval": "1d",
                            "limit": TRAILING_DAYS})
    resp.raise_for_status()
    return sum(float(k[7]) for k in resp.json())  # index 7 = quote volume


def cmd_update_universe():
    ensure_dirs()
    session = requests.Session()
    symbols = eligible_symbols(session)
    log.info("eligible symbols after mechanical filters: %d", len(symbols))

    vols = {}
    t0 = time.time()
    for i, sym in enumerate(symbols, 1):
        vols[sym] = trailing_quote_volume(session, sym)
        if i % 50 == 0:
            log.info("  klines %d/%d (%.0fs elapsed)",
                     i, len(symbols), time.time() - t0)
        time.sleep(KLINES_SLEEP)

    # Deterministic ranking: volume desc, then symbol asc for ties.
    ranked = sorted(vols.items(), key=lambda kv: (-kv[1], kv[0]))

    date_str = utc_today().isoformat()
    path = os.path.join(UNIVERSE_DIR, "universe_%s.csv" % date_str)
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["symbol", "vol30d", "rank", "included"])
        for rank, (sym, vol) in enumerate(ranked, 1):
            w.writerow([sym, "%.8f" % vol, rank,
                        int(rank > TOP_N_EXCLUDED)])
    digest = sha256_file(path)
    with open(path + ".sha256", "w") as f:
        f.write("%s  %s\n" % (digest, os.path.basename(path)))

    for p in (path, path + ".sha256"):
        shutil.copy2(p, os.path.join(ART_UNIVERSE_DIR, os.path.basename(p)))

    n_inc = max(0, len(ranked) - TOP_N_EXCLUDED)
    log.info("universe table %s written (sha256 %s)", path, digest[:16])
    print("universe date: %s" % date_str)
    print("eligible pairs: %d" % len(ranked))
    print("excluded (top %d by 30d quote volume): %d"
          % (TOP_N_EXCLUDED, min(TOP_N_EXCLUDED, len(ranked))))
    print("monitored universe size: %d" % n_inc)
    print("top-5 excluded: %s"
          % ", ".join("%s(%.0fM)" % (s, v / 1e6) for s, v in ranked[:5]))
    print("top-5 included: %s"
          % ", ".join("%s(%.0fM)" % (s, v / 1e6)
                      for s, v in ranked[TOP_N_EXCLUDED:TOP_N_EXCLUDED + 5]))
    return path


def latest_universe():
    """(path, date) of the newest universe table, or (None, None)."""
    if not os.path.isdir(UNIVERSE_DIR):
        return None, None
    tables = sorted(f for f in os.listdir(UNIVERSE_DIR)
                    if f.startswith("universe_") and f.endswith(".csv"))
    if not tables:
        return None, None
    name = tables[-1]
    date = dt.date.fromisoformat(name[len("universe_"):-len(".csv")])
    return os.path.join(UNIVERSE_DIR, name), date


def included_symbols(universe_path):
    with open(universe_path, newline="") as f:
        return [row["symbol"] for row in csv.DictReader(f)
                if row["included"] == "1"]


# --------------------------------------------------------------------------
# Daily archive pulls (protocol §4)
# --------------------------------------------------------------------------


def vision_targets(symbol, date_str):
    """(url, local_path) pairs for one symbol-day."""
    agg = "%s-aggTrades-%s.zip" % (symbol, date_str)
    k1m = "%s-1m-%s.zip" % (symbol, date_str)
    return [
        ("%s/data/spot/daily/aggTrades/%s/%s" % (VISION_BASE, symbol, agg),
         os.path.join(RAW_AGG_DIR, date_str, agg)),
        ("%s/data/spot/daily/klines/%s/1m/%s" % (VISION_BASE, symbol, k1m),
         os.path.join(RAW_K1M_DIR, date_str, k1m)),
    ]


def fetch_one(session, url, local_path):
    """Download one zip; verify against the published .CHECKSUM when
    available. Returns a result dict; status in
    {ok, skipped, 404, failed}. source_checksum_ok in {1, 0, ''}
    ('' = no official checksum published; our own sha256 recorded)."""
    fname = os.path.basename(local_path)
    res = {"file": fname, "status": "failed", "bytes": 0,
           "sha256": "", "source_checksum_ok": ""}

    # Resumability: skip files already present with matching remote size.
    if os.path.exists(local_path) and os.path.getsize(local_path) > 0:
        try:
            head = session.head(url, timeout=HTTP_TIMEOUT)
            if (head.status_code == 200 and
                    int(head.headers.get("Content-Length", -1))
                    == os.path.getsize(local_path)):
                res.update(status="skipped",
                           bytes=os.path.getsize(local_path))
                return res
        except requests.RequestException:
            pass  # fall through to re-download

    resp = http_get(session, url)
    if resp.status_code == 404:
        res["status"] = "404"
        return res
    if resp.status_code != 200:
        log.warning("HTTP %s for %s", resp.status_code, url)
        return res

    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    tmp = local_path + ".part"
    with open(tmp, "wb") as f:
        f.write(resp.content)
    digest = sha256_file(tmp)

    # Official checksum sidecar: "<sha256>  <filename>".
    try:
        csum = http_get(session, url + ".CHECKSUM")
        if csum.status_code == 200 and csum.text.strip():
            official = csum.text.split()[0].strip().lower()
            if official == digest:
                res["source_checksum_ok"] = 1
            else:
                res["source_checksum_ok"] = 0
                log.error("CHECKSUM MISMATCH %s: official %s != local %s",
                          fname, official, digest)
                os.remove(tmp)
                return res
    except requests.RequestException:
        pass  # checksum endpoint unreachable: record our own hash only

    os.replace(tmp, local_path)
    res.update(status="ok", bytes=os.path.getsize(local_path), sha256=digest)
    return res


def _append_csv(path, fieldnames, rows):
    new = not os.path.exists(path)
    with open(path, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        if new:
            w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in fieldnames})
    shutil.copy2(path, os.path.join(ART_MANIFEST_DIR, os.path.basename(path)))


def append_manifest(rows):
    """Append successful-download rows to the pull manifest (mirrored to
    artifacts/)."""
    _append_csv(MANIFEST_CSV, MANIFEST_FIELDS, rows)


def append_coverage(rows):
    """Append every attempt (ok/skipped/404/failed) to the coverage log, so
    per-day availability of every monitored pair is auditable."""
    _append_csv(COVERAGE_CSV, COVERAGE_FIELDS, rows)


def cmd_pull_day(date_str, mark_done=True, limit=None):
    ensure_dirs()
    dt.date.fromisoformat(date_str)  # validate format early
    upath, udate = latest_universe()
    if upath is None:
        log.error("no universe table found; run --update-universe first")
        return 2
    symbols = included_symbols(upath)
    if limit:
        # Validation pull over a subset; never counts as a completed day.
        symbols = symbols[:limit]
        mark_done = False
        log.info("LIMIT=%d (validation pull; day not marked complete)", limit)
    log.info("pulling %s for %d symbols (universe %s)",
             date_str, len(symbols), udate)

    jobs = []
    for sym in symbols:
        for url, local in vision_targets(sym, date_str):
            jobs.append((sym, url, local))

    stats = {"ok": 0, "skipped": 0, "404": 0, "failed": 0}
    total_bytes = 0
    manifest_rows = []
    coverage_rows = []
    lock = threading.Lock()
    tls = threading.local()
    t0 = time.time()

    def worker(job):
        sym, url, local = job
        if not hasattr(tls, "session"):
            tls.session = requests.Session()
        r = fetch_one(tls.session, url, local)
        r.update(date=date_str, symbol=sym)
        return r

    with concurrent.futures.ThreadPoolExecutor(DOWNLOAD_THREADS) as pool:
        for i, r in enumerate(pool.map(worker, jobs), 1):
            with lock:
                stats[r["status"]] += 1
                total_bytes += r["bytes"]
                coverage_rows.append(r)
                if r["status"] == "ok":
                    manifest_rows.append(r)
                elif r["status"] == "404":
                    log.info("404 (listing gap, continuing): %s", r["file"])
                elif r["status"] == "failed":
                    log.warning("FAILED: %s", r["file"])
            if i % 100 == 0:
                log.info("  %d/%d files (%.0fs)", i, len(jobs),
                         time.time() - t0)

    if manifest_rows:
        manifest_rows.sort(key=lambda r: (r["symbol"], r["file"]))
        append_manifest(manifest_rows)
    if coverage_rows:
        coverage_rows.sort(key=lambda r: (r["symbol"], r["file"]))
        append_coverage(coverage_rows)

    elapsed = time.time() - t0
    print("pull-day %s: attempted=%d ok=%d skipped=%d 404=%d failed=%d "
          "bytes=%d (%.1f MiB) elapsed=%.0fs"
          % (date_str, len(jobs), stats["ok"], stats["skipped"],
             stats["404"], stats["failed"], total_bytes,
             total_bytes / 2**20, elapsed))

    if stats["ok"] + stats["skipped"] == 0:
        # Every file 404'd: the day is almost certainly not yet published
        # (publication lag can exceed PUBLICATION_LAG_DAYS). Do not mark it
        # complete; the next --daily run retries it.
        log.warning("day %s: no files available yet (all 404); "
                    "will retry on next run", date_str)
        return 3  # sentinel: day not yet published

    if stats["failed"] == 0 and mark_done:
        done = set()
        if os.path.exists(DAYS_DONE_TXT):
            with open(DAYS_DONE_TXT) as f:
                done = set(f.read().split())
        if date_str not in done:
            with open(DAYS_DONE_TXT, "a") as f:
                f.write(date_str + "\n")
    return 0 if stats["failed"] == 0 else 1


# --------------------------------------------------------------------------
# Daily driver
# --------------------------------------------------------------------------


def completed_days():
    if not os.path.exists(DAYS_DONE_TXT):
        return set()
    with open(DAYS_DONE_TXT) as f:
        return set(f.read().split())


def cmd_daily():
    ensure_dirs()
    today = utc_today()

    # Weekly universe refresh (protocol §3).
    upath, udate = latest_universe()
    if upath is None or (today - udate).days > UNIVERSE_MAX_AGE_DAYS:
        log.info("universe table missing or older than %d days; refreshing",
                 UNIVERSE_MAX_AGE_DAYS)
        cmd_update_universe()
    else:
        log.info("universe table %s is fresh (%d days old)",
                 udate, (today - udate).days)

    latest_published = today - dt.timedelta(days=PUBLICATION_LAG_DAYS)
    if latest_published < STREAM_START:
        print("daily: no days to pull yet (latest published UTC day %s "
              "precedes stream start %s)" % (latest_published, STREAM_START))
        return 0

    done = completed_days()
    day = STREAM_START
    pulled = 0
    rc = 0
    while day <= latest_published:
        ds = day.isoformat()
        if ds in done:
            log.info("day %s already collected; skipping", ds)
        else:
            r = cmd_pull_day(ds)
            pulled += 1
            if r == 3:
                # Publication lag longer than expected: later days will not
                # be published either. Benign; retry on the next run.
                log.info("stopping at %s (not yet published)", ds)
                break
            rc |= r
        day += dt.timedelta(days=1)
    if pulled == 0:
        print("daily: all published days up to %s already collected"
              % latest_published)
    return rc


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------


def main(argv=None):
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S")
    p = argparse.ArgumentParser(
        description="Data collector for the alert-burden audit "
                    "(protocol/locked_protocol_v1.0.md, frozen at 0ac2bbd).")
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--update-universe", action="store_true",
                   help="build and archive a new universe membership table")
    g.add_argument("--pull-day", metavar="YYYY-MM-DD",
                   help="pull one UTC day of daily archives")
    g.add_argument("--daily", action="store_true",
                   help="pull all uncollected published days in the "
                        "evaluation window; refresh universe if stale")
    p.add_argument("--limit", type=int, default=None,
                   help="with --pull-day: validation pull over the first N "
                        "monitored symbols only (day not marked complete)")
    args = p.parse_args(argv)

    if args.update_universe:
        cmd_update_universe()
        return 0
    if args.pull_day:
        return cmd_pull_day(args.pull_day, limit=args.limit)
    return cmd_daily()


if __name__ == "__main__":
    sys.exit(main())
