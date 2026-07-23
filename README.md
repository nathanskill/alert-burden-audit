# Alert-Burden Audit (REF-2026-017)

Status: `PROTOCOL FROZEN / COLLECTION RUNNING / NO RESULTS YET`

A deployment-condition **alert-burden audit** of published market-data pump-and-dump detectors: the detector replicated and frozen in [pump-and-dump-replication-audit](https://github.com/nathanskill/pump-and-dump-replication-audit) (plus two trivial baselines) is run forward on an unfiltered continuous stream of small-cap Binance spot pairs, measuring what event-centred benchmarks structurally cannot: alerts per day, alerts per 1,000 monitored pair-hours, and a benchmark-rule-relative precision proxy.

- **Protocol**: [`protocol/locked_protocol_v1.0.md`](protocol/locked_protocol_v1.0.md) — frozen before the first data point; the primary evaluation stream is the first 12 complete UTC weeks after the freeze commit.
- **Collector**: `src/` — daily pulls from Binance official public archives (`data.binance.vision`), SHA-256 manifests for every file; raw pulls are not redistributed.
- **No results have been produced yet.** Weak or null results will be reported in full when they exist.

Related repositories by the same author: [pump-and-dump-replication-audit](https://github.com/nathanskill/pump-and-dump-replication-audit) · [evidence-separated-trading-screening](https://github.com/nathanskill/evidence-separated-trading-screening)

License: MIT (code and documentation in this repository). Upstream materials remain under their own terms.
