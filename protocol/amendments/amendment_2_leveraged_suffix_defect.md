# Amendment 2 — Leveraged-suffix filter defect (JUPUSDT, SYRUPUSDT)

Date: 2026-07-31. Status: adopted; defect fixed forward, exclusion window
backfilled. Restores conformance with `locked_protocol_v1.0.md` §3, which
specifies no leveraged-token exclusion at all (see Amendment 1 for why the
exclusion exists and why it restricts rather than extends the frozen rule).

## 1. Defect

The leveraged-token filter was implemented as a suffix heuristic
(`base.endswith(("UP", "DOWN", "BULL", "BEAR"))`). This falsely excluded two
ordinary base assets whose names merely end in "UP":

- **JUPUSDT** (Jupiter)
- **SYRUPUSDT** (Maple Finance)

Verified against exchangeInfo on 2026-07-31: these are the only live
TRADING+spot USDT pairs the heuristic removed, and the heuristic had **zero
true positives** — every real Binance leveraged token (BLVT) was delisted in
2022 and every FTX-issued BULL/BEAR token in 2020, all long before the
evaluation window. The filter therefore excluded two legitimate universe
members and nothing else.

## 2. Fix

`LEVERAGED_SUFFIXES` replaced by `LEVERAGED_BASES`, an exact-match frozenset
of the historical BLVT and FTX leveraged-token base assets (fixed forward
2026-07-31). Each exclusion made by the leveraged and stable/fiat lists is
now logged at universe-refresh time, so mechanical removals are visible in
the ops record rather than silent.

## 3. Exclusion window and backfill

- Window affected: 2026-07-24 (stream start) through 2026-07-31 (fix date).
  Neither pair appears in `universe_2026-07-23.csv` or
  `universe_2026-07-31.csv`; both archived, hash-signed tables are retained
  **unmodified** for auditability.
- Backfill: JUPUSDT and SYRUPUSDT daily aggTrades + 1-minute klines for the
  window were pulled from the immutable official archives
  (data.binance.vision) with normal `.CHECKSUM` verification, appending to
  `pull_manifest.csv` and `coverage_log.csv` through the collector's
  standard fetch path (driver: `src/backfill_symbols.py`). Archive files are
  identical whenever downloaded, so the late pull is data-equivalent to an
  on-time pull.
- Because the (defective) `universe_2026-07-31.csv` governs stream days up
  to the next weekly refresh (Amendment 4), the same backfill driver is
  re-run for the days governed by that table (2026-08-01 through the first
  post-fix table's effective date) so the two pairs have no residual gap.
  They enter the archived universe automatically at the first post-fix
  weekly refresh; no archived table is regenerated (the same-day overwrite
  guard added 2026-07-31 also prevents this mechanically).

## 4. Analysis rules

The primary analysis includes JUPUSDT and SYRUPUSDT for the backfilled
window, with this amendment cited as disclosure. Burden endpoints are
additionally reported with and without the two pairs as a sensitivity check.
