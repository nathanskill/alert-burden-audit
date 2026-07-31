# Amendment 4 — Day-to-membership mapping

Date: 2026-07-31. Status: adopted. Makes the mapping from stream day to
universe membership table deterministic; the frozen §3 text (weekly
mechanical updates, archived tables) is unchanged.

## 1. Rule

**Stream day D (UTC) is monitored and scored under the most recent archived
membership table dated strictly before D.**

Rationale: a table dated D is computed *during* day D from trailing klines
that include the partial day-D candle, so it may not govern day D itself or
any earlier day. Under this rule the mapping is reproducible from archived
filenames alone: days 2026-07-24..31 are governed by
`universe_2026-07-23.csv` (405 pairs); days from 2026-08-01 until the next
refresh by `universe_2026-07-31.csv` (415 pairs). Implemented as
`universe_for_day()` in `src/collector.py` (fixed forward 2026-07-31);
previously `--pull-day` used the newest table regardless of day, which made
the collector's refresh-then-pull ordering day-dependent.

## 2. The 2026-07-30 incident

Day 2026-07-30 was initially pulled under the same-day-refreshed
`universe_2026-07-31.csv` instead of the governing 2026-07-23 table.
Consequences, both corrected by the 2026-07-31 re-pull under the governing
table (Amendment 3 describes the repair mechanics):

- **Five surging pairs went unmonitored until repair**: FETUSDT, KAITOUSDT,
  SKHYBUSDT, VANAUSDT and ZAMAUSDT had risen into the top-50 exclusion of
  the 07-31 table but are members under the governing 07-23 table. Their
  day-30 archives were recovered in full.
- **Two spurious pre-membership 404 rows**: 币安人生USDT (a 2026-07-30
  Binance listing, first present in the 07-31 table) was probed for day 30
  before its archives published, logging two 404 rows; a later pass fetched
  its day-30 files. All of this pair's day-30 rows are outside the governed
  405-pair universe for that day. They are retained in the append-only
  coverage log and annotated by this amendment; they enter no analysis.

## 3. Membership churn at the first refresh (to be reported in the paper)

Between `universe_2026-07-23.csv` and `universe_2026-07-31.csv` (8 days):

- **5 pairs dropped** from the monitored set into the top-50 exclusion:
  FETUSDT (rank 51→50), KAITOUSDT (66→47), SKHYBUSDT (104→43), VANAUSDT
  (141→37; 104 ranks in 8 days) and ZAMAUSDT (77→46).
- **15 pairs added**: new listings (including tokenized-equity pairs and
  币安人生USDT) plus BCHUSDT and CELOUSDT leaving the top-50.

This churn rate is material to the burden denominators and is reported in
the paper's universe section.

## 4. Refresh cadence

The first refresh ran at 8 days, not 7, because the staleness test used a
strict `>`; fixed forward to `>=` on 2026-07-31 so refresh runs on the
protocol's weekly cadence. The 8-day first interval is disclosed here; both
archived tables stand as-is.
