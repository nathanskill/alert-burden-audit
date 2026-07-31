# Amendment 1 — Universe eligibility filters: full disclosure

Date: 2026-07-31. Status: adopted. Amends the reporting of
`locked_protocol_v1.0.md` §3; the frozen text is unchanged. Cite this
amendment wherever the paper states the universe rule or the 405/415
membership counts.

## 1. What the implementation actually filters

Protocol §3 (frozen) defines the universe as "all Binance spot pairs quoted
in USDT, excluding the top 50 by trailing 30-day quote volume", with weekly
mechanical membership updates. The implementation (`src/collector.py`,
`eligible_symbols()`) applies four eligibility filters *before* the top-50
ranking:

1. `status == "TRADING"` (exchangeInfo);
2. `isSpotTradingAllowed == true` (exchangeInfo);
3. exclusion of historical leveraged-token base assets (exact-match list;
   originally an `endswith` suffix heuristic — see Amendment 2 for the
   defect and its repair);
4. exclusion of a fixed 38-entry list of stablecoin / fiat-pegged base
   assets, reproduced verbatim here and frozen by this amendment:

   ```
   USDC, FDUSD, TUSD, DAI, BUSD, USDP, PAX, PAXG,
   EUR, EURI, AEUR, GBP, AUD, BRL, TRY, RUB, UAH,
   NGN, ZAR, BIDR, IDRT, GYEN, VAI, UST, USTC,
   SUSD, USDS, USD1, USDE, XUSD, BKRW, COP, ARS,
   MXN, JPY, PLN, RON, CZK
   ```

## 2. Provenance

All four filters were introduced in commit `4595b85` (2026-07-23 15:31 UTC)
— after the freeze tag `v1.0-protocol-freeze` (commit `0ac2bbd`, 2026-07-23
10:06 UTC) but before the primary stream start at 2026-07-24 00:00 UTC. The
stable/fiat list has been byte-identical since that commit. No filter was
added or changed after the first data point.

## 3. Restriction proof

The filters run before the top-50-by-volume exclusion, so every filter can
only remove pairs: the implemented universe is a strict subset of the
frozen-text universe. Under the protocol's amendment rule ("amendments may
restrict claims but may not alter ... the monitored-universe rule"), this
disclosure restricts the claimed universe; it does not extend it.

## 4. Limitations acknowledged

- The fixed stable/fiat list qualifies the literal "no manual curation"
  wording of §3: the list's *contents* were chosen by the author (once,
  before the stream started). It is mechanical in application and is frozen
  by this amendment; any future change would require a further amendment.
- **Tokenized-equity bases** pass the mechanical rule and remain included:
  CRCLB and SKHYB are monitored in `universe_2026-07-23.csv`; DELLB, PYPLB
  and other tokenized-equity/ETF listings enter `universe_2026-07-31.csv`.
  They are *not* silently folded into "small-cap crypto" framing: the paper
  reports them as a stratification / sensitivity variable in the burden
  analyses.
- **Trailing-volume window (pre-fix behavior):** the rankings behind the two
  archived tables (`universe_2026-07-23.csv`, 405 monitored pairs;
  `universe_2026-07-31.csv`, 415) were computed from 29 complete daily
  candles plus the in-progress candle of the computation day. Fixed forward
  on 2026-07-31 (31 candles requested, partial candle dropped: exactly 30
  complete UTC days). Both archived tables are retained unmodified.
