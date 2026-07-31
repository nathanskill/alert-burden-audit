# Amendment 5 — Cross-exchange gate (§6): missed probe deadline, disclosed

Date: 2026-07-31. Status: adopted (disclosure); resolution due 2026-08-02.

## 1. What §6 requires

`locked_protocol_v1.0.md` §6 gates the companion cross-exchange module on a
probe to be run **within the first week after freeze** (freeze 2026-07-23,
so due by 2026-07-30): sample 30 events at random from published
pump-event lists and check ≥ 2-venue minute-kline availability, with a
pre-registered go/no-go at ≥ 60% clearance; below that the module is
permanently dropped and reported as such.

## 2. What happened

The probe did not run by 2026-07-30. This was an operational omission — no
probe artifact exists, so neither the go nor the no-go branch of the frozen
gate has been exercised. The frozen default (module dropped) is triggered by
< 60% *clearance*, not by a deadline slip, so the gate's own default does
not resolve this state; only an explicit, documented decision can. Silence
is the one indefensible option, hence this amendment on the day the slip
was found.

## 3. Why a late probe would still be valid (if run)

The probe samples *historical* events from published pump-event lists on
other venues and checks archive/kline availability there. It uses no data
from this study's evaluation stream, so running it one or two days late
cannot be contaminated by the new stream and does not touch any frozen
endpoint. The only cost of lateness is the disclosed deviation itself.

## 4. Resolution commitment

By **2026-08-02** one of the following will be committed, each citing this
amendment:

- **Option A — run the probe late**: execute the 30-event availability
  probe exactly as §6 specifies, commit the sampling seed, event list
  citation, per-event results, and the resulting go/no-go verdict; the
  module then proceeds or is dropped strictly per the frozen ≥ 60% rule; or
- **Option B — drop by amendment**: permanently drop the companion module,
  reported as "dropped because the gating probe was not run by its
  deadline" (not as a gate outcome), and remove all cross-exchange claims
  from the paper.

Either way the primary endpoints are unaffected: §6 is a gated companion
module, default OFF, and no cross-exchange claim appears in any output
unless Option A runs and clears.
