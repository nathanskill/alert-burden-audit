# Alert-Burden Audit (REF-2026-017)

Status: `PROTOCOL FROZEN · ANALYSIS FROZEN · COLLECTION RUNNING · NO RESULTS YET`

## The question

Published market-data pump-and-dump detectors are evaluated on event-centred
benchmarks: known pump episodes plus sampled negatives. That design cannot say
what a detector would cost to operate, because it never exposes the detector to
an unfiltered stream. This study takes the detector replicated and frozen in
[pump-and-dump-replication-audit](https://github.com/nathanskill/pump-and-dump-replication-audit)
(REF-2026-016), adds two trivial baselines, and runs all three forward over
every small-capitalisation Binance spot USDT pair for twelve weeks. It measures
what event-centred benchmarks structurally cannot: alerts per day, alerts per
1,000 monitored pair-hours, and a benchmark-rule-relative precision proxy.
Endpoints, thresholds, detectors and the universe rule are fixed in
[`protocol/locked_protocol_v1.0.md`](protocol/locked_protocol_v1.0.md), frozen
before the first data point (tag `v1.0-protocol-freeze`, commit `0ac2bbd`,
2026-07-23).

## Where the study stands (5 August 2026)

Collection started at the first UTC midnight after the freeze. The primary
evaluation stream is the first 12 complete UTC weeks: 2026-07-24 to 2026-10-15,
84 days. Every number below is checkable from a committed file.

- 11 of the 84 days are collected in full (2026-07-24 through 2026-08-03;
  Binance publishes each day's archives up to two days late). 9,078 archive
  files pulled, each verified against Binance's own SHA-256 checksums:
  `artifacts/manifests/pull_manifest.csv`.
- The coverage log is append-only and holds 18,012 attempt rows, superseded
  probe rows included by design (Amendment 3 defines the deduplication rule):
  `artifacts/manifests/coverage_log.csv`.
- The monitored universe is mechanical — all TRADING spot USDT pairs minus the
  top 50 by trailing 30-day quote volume — and stands at 405 pairs under the
  first archived membership table, 415 under the second
  (`artifacts/universe/`, rows with `included=1`). The eligibility filters the
  implementation applies before that rule are disclosed in full in Amendment 1.
- The analysis pipeline was written, tested and frozen on 2026-08-02 (tag
  `v1.0-analysis-freeze`) while only 7 of the 84 days existed, so the analysis
  cannot have been shaped by the results. 69 unit tests pass under
  `.venv-analysis` (`tests/`).
- Exactly one pass has been run over the partial stream: a structural check on
  2026-08-01, committed at
  `artifacts/structural_checks/structural_check_20260801T142752Z/`. It verified
  2,849 pair-days on disk, 68,376 monitored pair-hours and 31,087 reconstructed
  feature rows with zero errors, and produced no endpoint value — mechanically,
  not as a promise (see the guard below).
- The protocol's §6 companion cross-exchange module has been dropped. Its
  pre-registered availability probe cleared 0 of 30 sampled events against the
  ≥ 60% go bar, so the frozen rule kills the module permanently. The probe ran
  two days past its deadline; the slip is disclosed rather than smoothed over
  (Amendment 5; sampling manifest, per-venue results and post-run sanity checks
  in `artifacts/cross_exchange_gate_v1_20260801/`).

No endpoint value has been computed on the partial stream. Weak or null results
will be reported in full when they exist.

## The rules the study runs under

Freeze order is the core of the design. The protocol froze before any data;
amendments may restrict claims but may not alter endpoints, thresholds or the
universe rule after collection begins. The analysis froze before the window
closes, for the same reason in the other direction: code written after seeing
results can be shaped by them.

Between the two freezes sits a mechanical guard. The only analysis invocation
permitted before 2026-10-15 is `--partial-structural-check`, which sets
`ABA_STRUCTURAL_ONLY=1`; under that flag every endpoint-producing function in
`burden.py` and `precision_proxy.py` raises. Its output is limited to
structural facts — days, pairs, pair-days, files, rows, denominators, schema
and timezone conformance, timings, error counts — and contains no alert count,
score, rate, burden value or precision-proxy count. The committed structural
check records the guard's self-test firing.

Everything protocol-facing that changed after the freeze is a numbered
amendment. There are five, all adopted 2026-07-31 (Amendment 5 resolved
2026-08-01), under `protocol/amendments/`:

1. Universe eligibility filters disclosed in full, with a proof that each one
   restricts rather than extends the frozen rule.
2. A real defect: the leveraged-token filter used `endswith("UP")` and falsely
   excluded JUPUSDT and SYRUPUSDT — two ordinary pairs whose names merely end
   in "UP" — while catching zero actual leveraged tokens (all delisted years
   ago). Fixed to an exact-match list; both pairs backfilled from the immutable
   archives; burden endpoints will be reported with and without them.
3. A second defect, the partial-publication incident: an "optimistic" lag
   reduction let four days (2026-07-27..30) be marked complete with 33–51% of
   universe files missing, a systematic bias in the making. Caught 2026-07-31,
   all four days re-pulled to full verified coverage within the recoverable
   window. The ops-level account is
   `protocol/incident_20260731_partial_publication.md`; the amendment fixes
   the analysis-facing rules it exposed.
4. A deterministic day-to-membership mapping, after day 2026-07-30 was first
   pulled under the wrong week's table (five surging pairs went unmonitored
   until repair — named in the amendment, with the membership churn figures the
   paper must report).
5. The cross-exchange gate: probe deadline missed, slip disclosed, probe run
   late under a documented option, verdict no-go, module dropped.

The defects are part of the record on purpose. Archived universe tables are
never regenerated, the coverage log is never rewritten, and each repair note
states what would have been biased had the defect gone uncaught.

## Reproducing

The frozen detector is fitted on the upstream *released* labelled matrices and
is never retrained on this study's stream. Point the pipeline at a local
checkout of the upstream dataset (pinned commit
`d71250d4cb055dde2d415c8cba38a0dcd6eb6e16`, containing
`labeled_features/features_{25S,15S,5S}.csv.gz`); no local path is committed
here.

```
python3 -m venv .venv-analysis
.venv-analysis/bin/pip install -r requirements-analysis.txt

export ABA_UPSTREAM_DATASET=/path/to/pump-and-dump-dataset   # required
export ABA_REF016_ROOT=/path/to/REF-2026-016-repository      # optional: verifies the frozen tau* constants

.venv-analysis/bin/python src/run_analysis.py --partial-structural-check   # before the window closes
.venv-analysis/bin/python src/run_analysis.py                              # only once all 84 days are complete
.venv-analysis/bin/python -m unittest discover -s tests                    # 69 tests
```

The pipeline rebuilds the upstream feature matrix from collected aggTrades at
5 s / 15 s / 25 s (`features.py`; reconstruction limits R1–R7 are documented in
its module docstring), applies the three frozen §5 detectors with the
30-minute per-pair cooldown (`apply_detectors.py`), and produces the §2
endpoints (`burden.py`, `precision_proxy.py`). `run_analysis.py` drives it;
every artifact it writes records the protocol freeze commit, as §7 requires.
Value-bearing behaviour — cooldown, both baselines, the pair-hours denominator,
the bootstrap's clustering, the precision-proxy rule and its three variants —
is verified against synthetic data with known answers in `tests/`.

Documented switches, all defaulting to what the protocol and amendments bind:
`--pair-hours-rule` (default `verified-archive`; `universe-membership` is the
sensitivity alternative), `--exclude-amendment2-pairs` (Amendment 2 §4's
with/without check), `--warmup-days` (default `auto`; limit R5).

## What is not claimed

- No results of any kind exist yet. Nothing in this repository states or
  implies an endpoint value.
- Precision language is benchmark-rule-relative throughout. The protocol
  prohibits "real false-alarm rate", "daily false alerts" in the real-world
  sense, and "analyst workload".
- No "first replication" claim. No cross-exchange claim of any kind (§6
  module dropped, above).
- Raw pulls are not redistributed; manifests, aggregates and code are the
  public record.
- No Telegram or social-media data. No employer data, systems or client
  information — an absolute line, with the conflict-of-interest disclosure in
  protocol §8.

## Operations

The collector runs unattended under launchd (`StartCalendarInterval` at 07:17,
12:17, 18:17 and 23:17 local plus a run at load; the installed spec is
`launchd.daily.plist.template`). Each run refreshes the weekly membership
table when due, pulls every uncollected published day in the window, then
commits and pushes the `artifacts/` mirror — the "artifacts: mirror update"
commits in the log are its heartbeat. `run_daily.sh` is the wrapper: atomic
mkdir lock with an owner-PID staleness check, monthly logs under `logs/`.

```
python3 -m venv .venv && .venv/bin/pip install requests
.venv/bin/python src/collector.py --update-universe   # build/refresh membership table
.venv/bin/python src/collector.py --daily             # pull all uncollected published days
.venv/bin/python src/collector.py --pull-day 2026-07-24 [--limit N]   # one day (N=validation subset)
```

`data/` (raw pulls, local manifests) stays on the collection machine;
`artifacts/` (universe tables, pull manifest, coverage log, structural checks,
gate probe) is the public record. The collector needs only `requests` and runs
in its own venv, kept separate so an analysis dependency can never disturb
unattended collection.

Related repositories by the same author:
[pump-and-dump-replication-audit](https://github.com/nathanskill/pump-and-dump-replication-audit) ·
[evidence-separated-trading-screening](https://github.com/nathanskill/evidence-separated-trading-screening)

License: MIT (code and documentation in this repository). Upstream materials
remain under their own terms.
