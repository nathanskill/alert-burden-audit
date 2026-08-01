# Amendment 5 — Cross-exchange gate (§6): missed probe deadline, disclosed; probe run late, no-go

Date: 2026-07-31 (disclosure). Status: **resolved 2026-08-01 — Option A executed; gate verdict: no-go, module permanently dropped per the frozen ≥ 60% rule.**

## 1. What §6 requires

`locked_protocol_v1.0.md` §6 gates the companion cross-exchange module on a
probe to be run **within the first week after freeze** (freeze 2026-07-23,
so due by 2026-07-30): sample 30 events at random from published
pump-event lists and check ≥ 2-venue minute-kline availability, with a
pre-registered go/no-go at ≥ 60% clearance; below that the module is
permanently dropped and reported as such.

## 2. What happened

The probe did not run by 2026-07-30. This was an operational omission — no
probe artifact existed at disclosure time, so neither the go nor the no-go
branch of the frozen gate had been exercised. The frozen default (module
dropped) is triggered by < 60% *clearance*, not by a deadline slip, so the
gate's own default did not resolve this state; only an explicit, documented
decision could. Silence was the one indefensible option, hence this
amendment on the day the slip was found.

## 3. Why the late probe is still valid

The probe samples *historical* events from published pump-event lists on
other venues and checks archive/kline availability there. It uses no data
from this study's evaluation stream, so running it two days late cannot be
contaminated by the new stream and does not touch any frozen endpoint. The
only cost of lateness is the disclosed deviation itself (§2 above, kept in
full).

## 4. Resolution — Option A executed (probe run late)

The 30-event availability probe was executed exactly as §6 specifies on
**2026-08-01** (12:02 UTC), within this amendment's 2026-08-02 resolution
deadline. Artifacts in `artifacts/cross_exchange_gate_v1_20260802/`:

- `probe_cross_exchange_gate.py` — the exact probe code, including the
  sampling code;
- `sample_manifest.json` — seed (`20260723`), sort key, sampling code,
  event-list citation (La Morgia et al. pump-and-dump-dataset,
  `pump_telegram.csv` @ upstream commit `d71250d`, sha256 recorded), and
  the 30 sampled events;
- `probe_results.csv` — per-event × per-venue results;
- `summary.json` — clearance, verdict, run timestamp, lateness
  disclosure, and post-run sanity checks.

**Outcome: 0 of 30 sampled events (0%) had minute klines retrievable from
≥ 2 venues — far below the pre-registered 60% bar. Verdict: no-go. The
companion cross-exchange module is permanently dropped and is reported as
such, strictly per the frozen ≥ 60% rule.**

Per-venue picture (details in `probe_results.csv`): Binance monthly
1m-kline archives covered 19/30 events; KuCoin listed none of the sampled
pairs; Gate.io's API refuses 1m candlesticks older than 10 000 minutes
("Candlestick too long ago"), so it can serve no historical event; OKX's
deep 1m history covered only 2/30. No event reached two venues. Post-run
sanity checks (recorded in `summary.json`) confirmed the query formats
retrieve data where it exists, so the nulls reflect genuine
unavailability, not probe defects.

Consequently all cross-exchange claims are removed from the paper. The
primary endpoints are unaffected: §6 was a gated companion module, default
OFF, and no cross-exchange claim appears in any output.

Option B (drop by amendment without running the probe) was not needed and
was not used.
