# Partial-window structural check (REF-2026-017)

Mode: **structural only**. This run computed **no endpoint value** —
no alerts/day, no alerts per 1 000 pair-hours, no burden curve, no
precision-proxy count. `ABA_STRUCTURAL_ONLY=1` makes every endpoint
function in `burden.py` and `precision_proxy.py` raise; the guard
self-test in `structural_check.json` records that it fired.

Purpose: the analysis code is frozen *before* the evaluation window
closes, so it cannot have been shaped by the results. This check
verifies only that the pipeline runs end to end and that counts,
denominators, schemas and timestamps are right.

| fact | value |
|---|---|
| protocol freeze commit | `0ac2bbd026915b1ac09acf649638d28e637d0289` |
| evaluation window | 2026-07-24 .. 2026-10-15 (84 days) |
| days complete / outstanding | 7 / 77 |
| pair-days checked on disk | 2849 |
| files missing on disk | 0 |
| pair-day x frequency reconstructions | 36 / 36 |
| feature rows produced | 31087 |
| rows dropped non-finite | 0 |
| builds with a full rolling window | 29 |
| builds yielding zero scorable rows | 3 |
| errors | 0 |

- denominator `verified-archive`: 2849 pair-days, 68376 pair-hours, 407 distinct pairs
- denominator `universe-membership`: 2849 pair-days, 68376 pair-hours, 407 distinct pairs
