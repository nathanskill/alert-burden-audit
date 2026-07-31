# Amendment 3 — Partial-publication incident and coverage-log semantics

Date: 2026-07-31. Status: adopted. Companion to the ops record
`protocol/incident_20260731_partial_publication.md`; this amendment fixes
the analysis-facing rules. The frozen protocol text is unchanged except for
the formal substitution in §5 below, which restricts (strengthens) the data
plan.

## 1. Defect

Two interacting errors in `src/collector.py`:

- Commit `d66c365` (2026-07-25) reduced `PUBLICATION_LAG_DAYS` from 2 to 1,
  deviating from the protocol's stated "1-2 day publication lag" on the
  rationale that the all-404 guard made optimistic probing safe. That
  rationale was false: the guard only detects a *totally* unpublished day.
- The completion gate conflated "not yet published" with "listing gap": any
  mix of successes and 404s with zero transport failures marked the day
  complete. Binance publishes each day's archives incrementally over several
  hours, so an early probe sees a *partially* published day.

## 2. Impact

Days 2026-07-27..30 were falsely marked complete at probes ~02:17 UTC
(D+1), with 33-51% of universe files missing — 1,280 files / 640 pair-days
across the four days, including a stable core of 124 late-publishing
symbols. The missingness was systematic across pairs (late publishers, not
delistings), which would have biased alerts/day and the monitored
pair-hours denominator of the primary endpoint had it gone uncorrected.

## 3. Repair (all on 2026-07-31, within the recoverable window)

1. Live checks confirmed the previously-404 files still served (HTTP 200);
   no data was permanently lost.
2. Code: `PUBLICATION_LAG_DAYS` restored to 2; completion now requires every
   universe symbol to reach a definitive state — a day with any 404 younger
   than `LISTING_GAP_DEADLINE_DAYS = 5` returns the retry sentinel instead
   of completing; 404s persisting past the deadline are accepted as genuine
   listing gaps, with a logged acceptance.
3. Days 2026-07-27..30 were removed from `days_completed.txt` (via the new
   `--requeue-day` recovery command) and re-pulled incrementally with
   checksum-verified skips. An interim repair pass ran before the day-keyed
   universe selection of Amendment 4 was deployed and therefore briefly
   pulled the four days under `universe_2026-07-31.csv` (415 pairs); the
   definitive re-pull followed under the governing `universe_2026-07-23.csv`
   (405 pairs, 810 files/day). Post-repair, each of the four days reached
   810/810 files in a definitive state with 404 = 0 and re-entered
   `days_completed.txt`.
4. Surplus rows: the interim wrong-table pass fetched files for up to 15
   pairs added by the 2026-07-31 table (30 files/day at most, including
   tokenized-equity listings) that are *outside* the universe governing
   2026-07-27..30. These rows remain in the append-only logs and on disk as
   surplus archive data; they are outside the governed universe for those
   days and enter no analysis (disclosed here; harmless).

## 4. Coverage-log semantics (analysis rules, binding)

- `coverage_log.csv` is **append-only** and now, by design, carries
  superseded rows (already true for 2026-07-25/26 sentinel retries; now also
  for the repaired days). **Every coverage computation deduplicates per
  `(date, file)`, keeping the final (latest) status.**
- Status taxonomy: a 404 row is *pending publication* while the day is
  younger than `LISTING_GAP_DEADLINE_DAYS`, and a *listing gap* only once
  the day has aged past the deadline without the file appearing. Only the
  latter may be treated as a true absence in analysis.
- The 2026-07-20 validation pull (16 files, commit `4595b85`, predating the
  committed coverage log and the stream window) is excluded from all
  coverage computations.

## 5. Formal substitution in the data plan

Protocol §4's "REST backfill for gaps" is replaced by: **"archive re-pull;
REST backfill only if the official archives remain unavailable."** Archive
files are immutable and checksum-signed, so a re-pull is data-identical to
an on-time pull, whereas REST reconstruction is not bit-auditable. This
substitution restricts the data plan to the stronger source and was applied
to the repair above.
