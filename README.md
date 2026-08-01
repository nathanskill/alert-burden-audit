# Alert-Burden Audit (REF-2026-017)

Status: `PROTOCOL FROZEN / COLLECTION RUNNING / NO RESULTS YET`

A deployment-condition **alert-burden audit** of published market-data pump-and-dump detectors: the detector replicated and frozen in [pump-and-dump-replication-audit](https://github.com/nathanskill/pump-and-dump-replication-audit) (plus two trivial baselines) is run forward on an unfiltered continuous stream of small-cap Binance spot pairs, measuring what event-centred benchmarks structurally cannot: alerts per day, alerts per 1,000 monitored pair-hours, and a benchmark-rule-relative precision proxy.

- **Protocol**: [`protocol/locked_protocol_v1.0.md`](protocol/locked_protocol_v1.0.md) — frozen before the first data point (tag `v1.0-protocol-freeze`); the primary evaluation stream is the first 12 complete UTC weeks after the freeze commit (from 2026-07-24 00:00 UTC).
- **Collector**: `src/collector.py` — daily pulls from Binance official public archives (`data.binance.vision`), SHA-256 manifests + full coverage log for every attempt; raw pulls are not redistributed. Monitored universe: all Binance spot USDT pairs with status TRADING and spot trading enabled, excluding historical leveraged-token bases and a fixed 38-entry stable/fiat base list, minus the top 50 by trailing-30-day quote volume (mechanical rule; 405 pairs in the first table, 415 as of 2026-07-31; see protocol/amendments/).
- **Analysis pipeline**: `src/features.py`, `src/apply_detectors.py`, `src/burden.py`, `src/precision_proxy.py`, `src/run_analysis.py` — **frozen (tag `v1.0-analysis-freeze`) before the evaluation window closed**, so the analysis cannot have been shaped by the results.
- **No results have been produced yet.** No endpoint value has been computed on the partial stream: the only run permitted before the window closes is `--partial-structural-check`, which is mechanically prevented from producing one. Weak or null results will be reported in full when they exist.

## Analysis

The pipeline reconstructs the upstream feature matrix from the collected aggTrades at 5 s / 15 s / 25 s (`features.py`, with the reconstruction limits R1–R7 documented in its module docstring), applies the three frozen §5 detectors with the 30-minute per-pair cooldown (`apply_detectors.py`), and produces the §2 primary alert-burden endpoint (`burden.py`) and the §2 secondary benchmark-rule-relative precision proxy (`precision_proxy.py`). `run_analysis.py` is the driver; every artifact it writes carries the protocol freeze commit, as §7 requires.

### Analysis setup

The frozen detector is fitted on the upstream **released** labelled matrices and is never retrained on this study's stream. Point the pipeline at a local checkout of the upstream dataset (pinned commit `d71250d4cb055dde2d415c8cba38a0dcd6eb6e16`, containing `labeled_features/features_{25S,15S,5S}.csv.gz`) through an environment variable; no local path is committed to this repository.

```
python3 -m venv .venv-analysis
.venv-analysis/bin/pip install -r requirements-analysis.txt

export ABA_UPSTREAM_DATASET=/path/to/pump-and-dump-dataset   # required
export ABA_REF016_ROOT=/path/to/REF-2026-016-repository      # optional: verifies the frozen tau* constants

.venv-analysis/bin/python src/run_analysis.py --partial-structural-check   # before the window closes
.venv-analysis/bin/python src/run_analysis.py                              # only once all 84 days are complete
.venv-analysis/bin/python -m unittest discover -s tests                    # unit tests
```

`--partial-structural-check` sets `ABA_STRUCTURAL_ONLY=1`, which makes every endpoint-producing function raise. Its output is limited to structural facts — days/pairs/pair-days/files/rows processed, denominators, schema and timezone conformance, timings, error counts — and contains no alert count, score, rate, burden value or precision-proxy count. Value-bearing behaviour (cooldown, both baselines, the pair-hours denominator, the bootstrap's clustering, the precision-proxy rule and its three variants) is verified against synthetic data with known answers in `tests/`.

Documented switches, all defaulting to the rule the protocol and its amendments bind: `--pair-hours-rule` (default `verified-archive`; `universe-membership` is the sensitivity alternative), `--exclude-amendment2-pairs` (Amendment 2 §4's with/without check), `--warmup-days` (default `auto`; see limit R5).

## Operations

The collector runs unattended. Each run refreshes the monitored universe weekly and pulls every uncollected published day in the evaluation window (Binance archives publish with a 1–2 day lag).

```
python3 -m venv .venv && .venv/bin/pip install requests
.venv/bin/python src/collector.py --update-universe   # build/refresh membership table
.venv/bin/python src/collector.py --daily             # pull all uncollected published days
.venv/bin/python src/collector.py --pull-day 2026-07-24 [--limit N]   # one day (N=validation subset)
```

`run_daily.sh` is a self-locating launchd wrapper (atomic mkdir lock with owner-PID staleness check, monthly logs under `logs/`). Scheduled via launchd `StartCalendarInterval` four times daily — 07:17, 12:17, 18:17, 23:17 local, plus a run at load — see `launchd.daily.plist.template` for the exact installed spec. Each run finishes by committing and pushing the `artifacts/` mirror. `data/` (raw pulls, manifests) is local-only; `artifacts/` (universe tables, manifests, coverage log) is the public record. Protocol-facing changes since the freeze are recorded as numbered amendments under `protocol/amendments/`.

Related repositories by the same author: [pump-and-dump-replication-audit](https://github.com/nathanskill/pump-and-dump-replication-audit) · [evidence-separated-trading-screening](https://github.com/nathanskill/evidence-separated-trading-screening)

License: MIT (code and documentation in this repository). Upstream materials remain under their own terms.
