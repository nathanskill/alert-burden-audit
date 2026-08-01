#!/usr/bin/env python3
"""precision_proxy.py — the §2 secondary endpoint (REF-2026-017).

Protocol reference: `protocol/locked_protocol_v1.0.md` §2.2 and §2.3.
Protocol freeze commit: 0ac2bbd026915b1ac09acf649638d28e637d0289.

§2.2 fixes a **pre-frozen post-hoc pump-signature rule**:

    primary rule: price gain > 25 % within 5 minutes, with volume > 10× the
    trailing 24-hour median for that pair, followed by a ≥ 50 % retracement of
    the gain within 60 minutes;

and three sensitivity variants: 15 % / 5× / 40 %; 35 % / 20× / 60 %; and the
primary rule with a 10-minute gain window. All four are implemented here and
none may be altered after the freeze.

§2.3 is binding on every use of this module: the quantity produced is a
**benchmark-rule-relative precision proxy**. It is not a real-world precision,
not a false-alarm rate, and not an analyst workload. The prohibited terms
("real false-alarm rate", "daily false alerts" in the real-world sense,
"analyst workload") must not appear in any output derived from it.

Frozen operationalisation (fixed before the window closed)
----------------------------------------------------------
For an alert episode at UTC minute ``t`` on pair ``P``, evaluated on the
complete 1-minute UTC kline grid of ``P``:

* reference price ``ref`` = close of minute ``t``;
* gain window = the ``gain_window_minutes`` minutes strictly after ``t``;
  ``peak`` = the maximum high in that window and ``t_peak`` its minute;
  the gain condition is ``peak / ref - 1 > gain_threshold``;
* volume condition: the maximum 1-minute quote volume inside the gain window
  exceeds ``volume_multiple`` × the median 1-minute quote volume over the
  1440 minutes strictly preceding ``t``. The trailing window must be complete
  and its median strictly positive, otherwise the alert is *not evaluable*;
* retracement condition: within the ``retracement_window_minutes`` minutes
  strictly after ``t_peak``, the minimum low falls to or below
  ``peak - retracement_fraction * (peak - ref)``;
* an alert *matches the signature* only when all three conditions hold; an
  alert whose forward windows are not fully covered by archived klines is
  reported as not evaluable and is excluded from the denominator rather than
  counted as a non-match.

Manual n = 100 sample
---------------------
§2.2 also requires a manually verified random sample of 100 alerts, verified
by the author against published pump-event lists used only as citation-level
cross-checks. That verification is a human step. This module provides the
draw (`draw_manual_sample`, deterministic under the protocol's frozen seed)
and the reader (`load_manual_sample`), and reports the sample as *not
performed* until a completed verdict file exists. It never synthesises,
imputes or defaults a verdict.
"""

from __future__ import annotations

import csv
import os
from dataclasses import dataclass, asdict

import numpy as np
import pandas as pd

from burden import assert_endpoints_allowed, BOOTSTRAP_SEED

#: Trailing window for the volume comparison (protocol §2.2: 24 hours).
TRAILING_MEDIAN_MINUTES = 1440

#: Seed for the manual-sample draw. The protocol fixes exactly one seed
#: (§7, 20260722); it is reused here so the draw is reproducible from the
#: frozen protocol alone.
MANUAL_SAMPLE_SEED = BOOTSTRAP_SEED
MANUAL_SAMPLE_N = 100

MANUAL_SAMPLE_FIELDS = [
    "sample_id",
    "pair",
    "ts_utc",
    "detector",
    "frequency",
    "threshold_label",
    "verdict",            # to be filled by the author: match / no-match / unclear
    "verifier",
    "verified_at_utc",
    "evidence_citation",  # citation-level cross-check only (§2.2)
    "notes",
]

VALID_VERDICTS = frozenset({"match", "no-match", "unclear"})


@dataclass(frozen=True)
class PumpSignatureRule:
    """One frozen pump-signature rule (§2.2)."""

    name: str
    role: str
    gain_threshold: float
    volume_multiple: float
    retracement_fraction: float
    gain_window_minutes: int = 5
    retracement_window_minutes: int = 60

    def as_dict(self) -> dict:
        return asdict(self)


PRIMARY_RULE = PumpSignatureRule(
    name="primary_25pct_10x_50pct_5min",
    role="primary",
    gain_threshold=0.25,
    volume_multiple=10.0,
    retracement_fraction=0.50,
)

VARIANT_LOOSE = PumpSignatureRule(
    name="variant_15pct_5x_40pct_5min",
    role="sensitivity",
    gain_threshold=0.15,
    volume_multiple=5.0,
    retracement_fraction=0.40,
)

VARIANT_STRICT = PumpSignatureRule(
    name="variant_35pct_20x_60pct_5min",
    role="sensitivity",
    gain_threshold=0.35,
    volume_multiple=20.0,
    retracement_fraction=0.60,
)

VARIANT_WINDOW10 = PumpSignatureRule(
    name="variant_25pct_10x_50pct_10min",
    role="sensitivity",
    gain_threshold=0.25,
    volume_multiple=10.0,
    retracement_fraction=0.50,
    gain_window_minutes=10,
)

#: The primary rule first, then the three §2.2 sensitivity variants.
ALL_RULES = (PRIMARY_RULE, VARIANT_LOOSE, VARIANT_STRICT, VARIANT_WINDOW10)


# --------------------------------------------------------------------------
# Rule evaluation
# --------------------------------------------------------------------------


def evaluate_alert(grid: pd.DataFrame, timestamp, rule: PumpSignatureRule) -> dict:
    """Evaluate one alert against one frozen rule.

    ``grid`` is a complete 1-minute UTC grid for the pair (see
    `features.complete_minute_grid`), which must extend far enough before and
    after ``timestamp`` to cover the trailing and forward windows; a grid that
    does not is reported as not evaluable.
    """

    stamp = pd.Timestamp(timestamp).floor("min")
    if stamp not in grid.index:
        return _not_evaluable(rule, "alert minute absent from kline grid")

    position = grid.index.get_loc(stamp)
    if isinstance(position, slice) or not isinstance(position, (int, np.integer)):
        return _not_evaluable(rule, "ambiguous alert minute in kline grid")
    position = int(position)

    gain_end = position + rule.gain_window_minutes
    if gain_end >= len(grid):
        return _not_evaluable(rule, "gain window not covered by archived klines")
    if position < TRAILING_MEDIAN_MINUTES:
        return _not_evaluable(rule, "trailing 24 h not covered by archived klines")

    window = grid.iloc[position + 1:gain_end + 1]
    reference = float(grid["close"].iloc[position])
    if not np.isfinite(reference) or reference <= 0:
        return _not_evaluable(rule, "non-positive reference close")

    peak = float(window["high"].max())
    peak_offset = int(np.argmax(window["high"].to_numpy()))
    peak_position = position + 1 + peak_offset
    gain = peak / reference - 1.0
    gain_ok = bool(gain > rule.gain_threshold)

    trailing = grid["quote_volume"].iloc[position - TRAILING_MEDIAN_MINUTES:position]
    trailing_median = float(np.median(trailing.to_numpy()))
    if not np.isfinite(trailing_median) or trailing_median <= 0:
        return _not_evaluable(rule, "trailing 24 h median volume is zero")
    window_volume = float(window["quote_volume"].max())
    volume_ok = bool(window_volume > rule.volume_multiple * trailing_median)

    retrace_end = peak_position + rule.retracement_window_minutes
    if retrace_end >= len(grid):
        return _not_evaluable(rule,
                              "retracement window not covered by archived klines")
    retrace_window = grid.iloc[peak_position + 1:retrace_end + 1]
    retrace_level = peak - rule.retracement_fraction * (peak - reference)
    trough = float(retrace_window["low"].min())
    retrace_ok = bool(trough <= retrace_level)

    return {
        "rule": rule.name,
        "role": rule.role,
        "evaluable": True,
        "reason": "",
        "gain_condition": gain_ok,
        "volume_condition": volume_ok,
        "retracement_condition": retrace_ok,
        "matches_signature": bool(gain_ok and volume_ok and retrace_ok),
    }


def _not_evaluable(rule: PumpSignatureRule, reason: str) -> dict:
    return {
        "rule": rule.name,
        "role": rule.role,
        "evaluable": False,
        "reason": reason,
        "gain_condition": False,
        "volume_condition": False,
        "retracement_condition": False,
        "matches_signature": False,
    }


def evaluate_episodes(episodes: pd.DataFrame, grids: dict,
                      rules=ALL_RULES) -> pd.DataFrame:
    """Evaluate every alert episode against every frozen rule.

    ``grids`` maps pair -> complete 1-minute grid covering the trailing and
    forward windows of that pair's alerts.
    """

    assert_endpoints_allowed()
    rows = []
    for episode in episodes.itertuples(index=False):
        grid = grids.get(episode.pair)
        for rule in rules:
            if grid is None:
                result = _not_evaluable(rule, "no kline grid for pair")
            else:
                result = evaluate_alert(grid, episode.ts_utc, rule)
            rows.append({
                "pair": episode.pair,
                "ts_utc": str(episode.ts_utc),
                "detector": episode.detector,
                "frequency": getattr(episode, "frequency", ""),
                "threshold_label": episode.threshold_label,
                **result,
            })
    return pd.DataFrame(rows)


def summarise(evaluations: pd.DataFrame) -> pd.DataFrame:
    """Benchmark-rule-relative precision proxy per detector stream and rule.

    The denominator is the number of *evaluable* alerts; alerts whose forward
    or trailing windows are not covered by archived klines are reported
    separately and never silently treated as non-matches.
    """

    assert_endpoints_allowed()
    rows = []
    if evaluations.empty:
        return pd.DataFrame(columns=[
            "detector", "frequency", "threshold_label", "rule", "role",
            "n_alerts", "n_evaluable", "n_matching",
            "precision_proxy_benchmark_rule_relative"])
    group_keys = ["detector", "frequency", "threshold_label", "rule", "role"]
    for key, group in evaluations.groupby(group_keys, sort=True, dropna=False):
        evaluable = group.loc[group["evaluable"]]
        matches = int(evaluable["matches_signature"].sum())
        rows.append({
            "detector": key[0],
            "frequency": key[1],
            "threshold_label": key[2],
            "rule": key[3],
            "role": key[4],
            "n_alerts": int(len(group)),
            "n_evaluable": int(len(evaluable)),
            "n_matching": matches,
            "precision_proxy_benchmark_rule_relative":
                (matches / len(evaluable)) if len(evaluable) else float("nan"),
        })
    return pd.DataFrame(rows)


# --------------------------------------------------------------------------
# Manual n = 100 sample (documented hook; never fabricated)
# --------------------------------------------------------------------------


def draw_manual_sample(episodes: pd.DataFrame, n: int = MANUAL_SAMPLE_N,
                       seed: int = MANUAL_SAMPLE_SEED) -> pd.DataFrame:
    """Deterministically draw the alerts to be verified by hand (§2.2).

    Returns a template with an empty ``verdict`` column. The draw is a simple
    random sample without replacement over the alert episodes, ordered
    canonically first so the result depends only on the episode table and the
    frozen seed.
    """

    assert_endpoints_allowed()
    if episodes.empty:
        return pd.DataFrame(columns=MANUAL_SAMPLE_FIELDS)
    ordered = episodes.sort_values(
        ["ts_utc", "pair", "detector", "frequency", "threshold_label"],
        kind="mergesort").reset_index(drop=True)
    rng = np.random.default_rng(seed)
    size = min(n, len(ordered))
    picks = np.sort(rng.choice(len(ordered), size=size, replace=False))
    rows = []
    for sample_id, position in enumerate(picks, 1):
        episode = ordered.iloc[int(position)]
        rows.append({
            "sample_id": sample_id,
            "pair": episode["pair"],
            "ts_utc": str(episode["ts_utc"]),
            "detector": episode["detector"],
            "frequency": episode.get("frequency", ""),
            "threshold_label": episode["threshold_label"],
            "verdict": "",
            "verifier": "",
            "verified_at_utc": "",
            "evidence_citation": "",
            "notes": "",
        })
    return pd.DataFrame(rows, columns=MANUAL_SAMPLE_FIELDS)


def write_manual_sample_template(sample: pd.DataFrame, path: str) -> str:
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    sample.to_csv(path, index=False)
    return path


def load_manual_sample(path: str) -> list:
    """Read a completed manual-verification file, validating every verdict."""

    with open(path, newline="") as handle:
        rows = list(csv.DictReader(handle))
    for row in rows:
        verdict = (row.get("verdict") or "").strip().lower()
        if verdict not in VALID_VERDICTS:
            raise ValueError(
                "manual sample row %s has verdict %r; expected one of %s. "
                "Incomplete verification files are never defaulted or imputed."
                % (row.get("sample_id"), row.get("verdict"),
                   sorted(VALID_VERDICTS)))
        row["verdict"] = verdict
    return rows


def manual_sample_status(path: str | None) -> dict:
    """Status of the manual n = 100 verification, without ever fabricating it."""

    assert_endpoints_allowed()
    record = {
        "required_by": "protocol/locked_protocol_v1.0.md §2.2",
        "n_required": MANUAL_SAMPLE_N,
        "draw_seed": MANUAL_SAMPLE_SEED,
        "path": path or "",
    }
    if not path or not os.path.exists(path):
        record.update({
            "performed": False,
            "note": ("manual verification not yet performed; the analysis "
                     "reports the rule-based precision proxy only and states "
                     "the manual sample as outstanding. No verdicts are "
                     "imputed."),
        })
        return record
    rows = load_manual_sample(path)
    counts = {v: sum(1 for r in rows if r["verdict"] == v)
              for v in sorted(VALID_VERDICTS)}
    record.update({
        "performed": True,
        "n_rows": len(rows),
        "verdict_counts": counts,
        "complete": len(rows) == MANUAL_SAMPLE_N,
    })
    return record
