# Incident note — partial-publication days wrongly marked complete

Date recorded: 2026-07-31. Class: collector defect, fixed forward. The frozen
protocol is unchanged: `locked_protocol_v1.0.md` §4 states a "1-2 day
publication lag; REST backfill for gaps", which the defective configuration
deviated from and this repair restores.

## What happened

Commit `d66c365` (2026-07-25) reduced `PUBLICATION_LAG_DAYS` from 2 to 1,
reasoning that the all-404 sentinel made optimistic probing safe. The sentinel
only covers the case where a day is entirely unpublished. It does not cover
**partial publication**: probes at ~02:17 UTC (about 2.3 h after UTC day
close) on 2026-07-27..30 found 49-69% of universe files published, recorded
the remainder as 404 "listing gaps", and — because zero transfers *failed* —
marked all four days complete. `--daily` then skipped them permanently.

Observed coverage at the defective probes (universe 810 files/day; day 30's
universe refresh raised it to 830):

| day | ok/skipped | 404 |
|---|---|---|
| 2026-07-27 | 394 | 416 |
| 2026-07-28 | 522 | 288 |
| 2026-07-29 | 508 | 302 |
| 2026-07-30 | 556 | 274 |

A stable core of 124 symbols was missing on all four days (late publishers,
not delistings): the missingness is systematic across pairs, which would have
biased alerts/day and the pair-hours denominator of the primary endpoint had
it gone uncorrected. Days 2026-07-25/26 escaped only because their first
probes were all-404 (sentinel retry); 2026-07-24 was probed after full
publication.

## Repair (2026-07-31, within the recoverable window)

1. Live checks confirmed the "missing" files were still served by
   data.binance.vision (HTTP 200, sampled). No data was permanently lost.
2. `PUBLICATION_LAG_DAYS` restored to 2. New completion rule: a day with any
   404 younger than `LISTING_GAP_DEADLINE_DAYS = 5` returns the retry
   sentinel instead of completing; a 404 that persists past the deadline is
   accepted as a genuine listing gap, with a warning logged.
3. 2026-07-27..30 removed from `days_completed.txt` and re-pulled
   incrementally (existing files skipped by size check; only missing files
   downloaded). Post-repair coverage is recorded in `coverage_log.csv` by the
   repair-run rows; completion required 404 = 0 or deadline-aged gaps only.

## Residual effect on the study

None on the primary window, provided repair rows show full coverage: the
primary evaluation stream consumes archived daily files, which are identical
whenever downloaded. `coverage_log.csv` retains the defective probe rows —
they document probe timing, not the final dataset state. The lesson recorded
for the paper's limitations section: completion of a collection day must
require every universe member to reach a definitive state, and "optimistic"
lag reductions must be tested against partial publication, not only total
absence.
