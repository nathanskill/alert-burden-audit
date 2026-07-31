# Alert-Burden Audit (REF-2026-017)

Status: `PROTOCOL FROZEN / COLLECTION RUNNING / NO RESULTS YET`

A deployment-condition **alert-burden audit** of published market-data pump-and-dump detectors: the detector replicated and frozen in [pump-and-dump-replication-audit](https://github.com/nathanskill/pump-and-dump-replication-audit) (plus two trivial baselines) is run forward on an unfiltered continuous stream of small-cap Binance spot pairs, measuring what event-centred benchmarks structurally cannot: alerts per day, alerts per 1,000 monitored pair-hours, and a benchmark-rule-relative precision proxy.

- **Protocol**: [`protocol/locked_protocol_v1.0.md`](protocol/locked_protocol_v1.0.md) — frozen before the first data point (tag `v1.0-protocol-freeze`); the primary evaluation stream is the first 12 complete UTC weeks after the freeze commit (from 2026-07-24 00:00 UTC).
- **Collector**: `src/collector.py` — daily pulls from Binance official public archives (`data.binance.vision`), SHA-256 manifests + full coverage log for every attempt; raw pulls are not redistributed. Monitored universe: all Binance spot USDT pairs with status TRADING and spot trading enabled, excluding historical leveraged-token bases and a fixed 38-entry stable/fiat base list, minus the top 50 by trailing-30-day quote volume (mechanical rule; 405 pairs in the first table, 415 as of 2026-07-31; see protocol/amendments/).
- **No results have been produced yet.** Weak or null results will be reported in full when they exist.

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
