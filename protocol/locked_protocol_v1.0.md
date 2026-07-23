# Locked Protocol v1.0 — Deployment-Condition Alert-Burden Audit of Published Pump-and-Dump Detectors

Status: **FROZEN at the commit that introduces this file.** Any change requires a numbered amendment; amendments may restrict claims but may not alter endpoints, thresholds, or the monitored-universe rule after collection begins.

Author: Zhennan (Nathan) Yu, independent researcher, Sydney. Upstream study: REF-2026-016 (github.com/nathanskill/pump-and-dump-replication-audit), protocol freeze `c2736ed`.

## 1. Research question

What alert burden — alerts per day and per 1,000 monitored pair-hours — and what benchmark-rule-relative precision proxy do published market-data pump-and-dump detectors produce when run forward on an unfiltered continuous stream of small-capitalisation exchange pairs, under configurations frozen before the evaluation period?

Contribution claim (frozen wording): *a deployment-condition alert-burden audit of published market-data pump-and-dump detectors*, explicitly distinguished from event-centred evaluations and from prior real-time pipelines on other venues. No "first replication" claim; no real-world false-positive-rate claim.

## 2. Endpoints (order fixed)

1. **Primary — alert burden**: distribution of alerts/day; alerts per 1,000 monitored pair-hours; burden curves as a function of threshold, at the anchor τ = 0.5 and at the τ* values frozen in the upstream study's artifacts (no re-tuning on the new stream).
2. **Secondary — precision proxy**: a pre-frozen post-hoc pump-signature rule — primary rule: price gain > 25% within 5 minutes with volume > 10× the trailing 24-hour median for that pair, followed by a ≥ 50% retracement of the gain within 60 minutes; three sensitivity variants (15%/5×/40%; 35%/20×/60%; window 10 min) — plus a manually verified random sample of n = 100 alerts (verified by the author against published pump-event lists used only as citation-level cross-checks).
3. Prohibited terms (inherited): "real false-alarm rate", "daily false alerts" in the real-world sense, "analyst workload". All precision language is benchmark-rule-relative.

## 3. Monitored universe (mechanical; no manual curation)

All Binance spot pairs quoted in USDT, excluding the top 50 by trailing 30-day quote volume as of each weekly membership update (computed from daily klines; first membership table computed at collection start and archived). Membership updates run weekly under the same rule; all membership tables are archived with hashes. Expected size ≈ 350–450 pairs (upper bound accepted as-is if larger).

KuCoin coverage is **secondary**: 1-minute-kline granularity only, used for burden-curve sensitivity, not for the primary endpoints (KuCoin trade history is not retroactively completable; Binance is primary because its full aggregate-trade archives make the feature pipeline exactly reconstructible and auditable).

## 4. Data

- Binance official public archives (`data.binance.vision`): daily aggTrades and 1-minute klines per monitored pair, pulled daily with a 1–2 day publication lag; REST backfill for gaps. SHA-256 manifests for every pulled file.
- Features are rebuilt from aggregate trades at the upstream study's 5 s / 15 s / 25 s frequencies using the upstream feature definitions (re-implementation tested against the upstream feature-generation semantics before the first scoring run; test artifacts committed).
- **Primary evaluation stream: the first 12 complete UTC weeks beginning at the first UTC midnight after this protocol's freeze commit.** Extended collection beyond 12 weeks is reported as sensitivity only.
- No Telegram or social-media data of any kind. No employer data, systems, or client information (absolute line). Raw pulls are not redistributed; manifests, aggregates, and code are public.

## 5. Detectors (all frozen)

1. The upstream released-configuration random forest (REF-2026-016 frozen re-implementation: 200 trees, max depth 5, `random_state=1`), trained exactly as released on the upstream released matrices — never retrained on the new stream.
2. Trivial baseline A: volume z-score rule (z > 4 on 5-minute rolling window vs trailing 24 h).
3. Trivial baseline B: price-jump rule (return > 5% within 5 minutes).
Thresholds: τ = 0.5 anchor and upstream-artifact τ* per frequency; 30-minute cooldown per pair (upstream paper's convention).

## 6. Companion cross-exchange module — gated, default OFF

Within the first week after freeze: probe 30 randomly sampled events from published pump-event lists for ≥ 2-venue minute-kline availability. Pre-registered go/no-go: proceed only if ≥ 60% of sampled events clear; otherwise the module is permanently dropped and reported as such.

## 7. Analysis and reporting

Burden distributions with per-pair-activity stratification; detector agreement (alert-window overlap); precision proxy ± sensitivity band across the three variant rules; event-cluster bootstrap (n = 2000, seed = 20260722) for interval estimates where episodes cluster. Every analysis run records this protocol's freeze commit. Weak or null results are reported in full.

## 8. Conflict-of-interest disclosure (to appear in all outputs)

The author is employed full-time at a retail FX/CFD brokerage in Sydney and operates independent Chinese-language trading-education web properties. No employer data, systems, or client information is used anywhere in this study; monitored assets are cryptocurrency exchange pairs unrelated to the employer's products. Employer's management is aware of and supportive of the author's independent academic research (author's record, 23 July 2026).

## 9. Venue targets

ConPro '27 / FC 2027 workshops (primary window), WEIS 2027 (backup); arXiv q-fin.TR + cs.CE preprint on completion.
