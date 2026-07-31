#!/usr/bin/env python3
"""
backfill_symbols.py — symbol-scoped archive backfill (REF-2026-017).

Pulls daily aggTrades + 1-minute klines for explicitly named symbols over a
date range through collector.py's standard fetch path (official .CHECKSUM
verification, zip validation, stale-.part-safe), appending rows to the same
pull manifest and coverage log with the same dedup rule as --pull-day.

Written for the Amendment 2 repair — JUPUSDT / SYRUPUSDT were wrongly
excluded from both archived universe tables by the leveraged-suffix
heuristic, so day-keyed universe pulls can never reach them — and reusable
for any symbol-scoped gap of that kind. Never touches days_completed.txt
and never writes universe tables.

Usage:
  .venv/bin/python src/backfill_symbols.py \\
      --symbols JUPUSDT,SYRUPUSDT --start 2026-07-24 --end 2026-07-31
"""

import argparse
import datetime as dt
import logging
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import requests

import collector as C

log = logging.getLogger("backfill")


def backfill(symbols, start, end):
    C.ensure_dirs()
    already = C.load_manifested_keys()
    session = requests.Session()
    rc = 0
    day = start
    while day <= end:
        ds = day.isoformat()
        stats = {"ok": 0, "skipped": 0, "404": 0, "failed": 0}
        manifest_rows = []
        coverage_rows = []
        for sym in symbols:
            for url, local in C.vision_targets(sym, ds):
                try:
                    r = C.fetch_one(session, url, local)
                except requests.RequestException as exc:
                    log.warning("fetch failed for %s: %s",
                                os.path.basename(local), exc)
                    r = {"file": os.path.basename(local), "status": "failed",
                         "bytes": 0, "sha256": "", "source_checksum_ok": ""}
                r.update(date=ds, symbol=sym)
                stats[r["status"]] += 1
                coverage_rows.append(r)
                if C.manifest_row_wanted(r, already):
                    manifest_rows.append(r)
        if manifest_rows:
            manifest_rows.sort(key=lambda r: (r["symbol"], r["file"]))
            C.append_manifest(manifest_rows)
        if coverage_rows:
            coverage_rows.sort(key=lambda r: (r["symbol"], r["file"]))
            C.append_coverage(coverage_rows)
        print("backfill %s: attempted=%d ok=%d skipped=%d 404=%d failed=%d"
              % (ds, len(coverage_rows), stats["ok"], stats["skipped"],
                 stats["404"], stats["failed"]))
        if stats["failed"]:
            rc = 1
        day += dt.timedelta(days=1)
    return rc


def main(argv=None):
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S")
    p = argparse.ArgumentParser(
        description="Symbol-scoped archive backfill through the collector's "
                    "verified fetch path (see Amendment 2).")
    p.add_argument("--symbols", required=True,
                   help="comma-separated symbols, e.g. JUPUSDT,SYRUPUSDT")
    p.add_argument("--start", required=True, metavar="YYYY-MM-DD")
    p.add_argument("--end", required=True, metavar="YYYY-MM-DD")
    args = p.parse_args(argv)
    symbols = sorted({s.strip().upper()
                      for s in args.symbols.split(",") if s.strip()})
    start = dt.date.fromisoformat(args.start)
    end = dt.date.fromisoformat(args.end)
    if end < start:
        p.error("--end precedes --start")
    return backfill(symbols, start, end)


if __name__ == "__main__":
    sys.exit(main())
