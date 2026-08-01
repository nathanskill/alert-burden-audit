#!/usr/bin/env python3
"""apply_detectors.py — the three frozen detectors of §5 (REF-2026-017).

Protocol reference: `protocol/locked_protocol_v1.0.md` §5. Protocol freeze
commit: 0ac2bbd026915b1ac09acf649638d28e637d0289.

Detectors (all frozen before the evaluation window; nothing here is tuned on
this study's stream):

1. **Upstream released-configuration random forest.** The REF-2026-016 frozen
   re-implementation's configuration — ``RandomForestClassifier(
   n_estimators=200, max_depth=5, min_samples_leaf=1, random_state=1)`` —
   fitted **exactly once, on the upstream released labelled matrices**, and
   then applied forward. `fit_frozen_rf` is the only place a model is fitted
   and it accepts only the released matrix path; it has no code path that can
   see a row of this study's stream. §5's "never retrained on the new stream"
   is therefore a structural property of this module, not a convention.
   Thresholds: the replication anchor τ = 0.5 and the frozen per-frequency
   τ* taken from the upstream artifacts (:data:`TAU_STAR_FROZEN`).
2. **Trivial baseline A — volume z-score**: z > 4 on a 5-minute rolling
   window against the trailing 24 h.
3. **Trivial baseline B — price jump**: return > 5 % within 5 minutes.

A 30-minute per-pair cooldown (the upstream paper's convention, reproduced by
REF-2026-016's ``event_metrics.emit_alerts``) is applied to each alert stream:
a candidate crossing is suppressed when an alert was emitted less than 1800 s
earlier in the same stream. The cooldown clock runs continuously per pair
across day boundaries; it is never reset at midnight.

Frozen operationalisation of the two baselines
----------------------------------------------
§5 states the baseline rules but not every implementation detail, so the
details are fixed here, before the window closes, and are reported with the
results:

* both baselines consume the complete 1-minute UTC kline grid
  (`features.complete_minute_grid`), so "5 minutes" always means five
  calendar minutes;
* volume z-score: ``v5(t)`` is the sum of quote volume over the five minutes
  ending at minute ``t``; the baseline mean and sample standard deviation
  (ddof = 1) are taken over the 1440 values of ``v5`` strictly preceding
  ``t``; ``z(t) = (v5(t) - mean) / sd``; the rule fires on ``z > 4``. Where
  the trailing window is incomplete or ``sd == 0`` the rule cannot fire;
* price jump: ``r(t) = close(t) / close(t - 5 min) - 1``; the rule fires on
  ``r > 0.05``;
* both need warm-up from the preceding pair-day; warm-up minutes never
  themselves produce alerts.

Environment / setup (no absolute path is committed to this repository)
----------------------------------------------------------------------
``ABA_UPSTREAM_DATASET``  — path to a checkout of the upstream
    pump-and-dump dataset (pinned commit d71250d4…) containing
    ``labeled_features/features_{25S,15S,5S}.csv.gz``. Required to score with
    detector 1.
``ABA_REF016_ROOT``       — optional path to the REF-2026-016 frozen
    re-implementation. When set, the frozen τ* constants below are verified
    against that repository's archived selection artifacts. Nothing is read
    from it at scoring time and nothing in it is modified.

This module emits alert episodes. It computes no endpoint quantity: counting,
rating and stratifying episodes is `burden.py`'s job, and that module refuses
to run in structural-check mode.
"""

from __future__ import annotations

import csv
import hashlib
import os
from dataclasses import dataclass

import numpy as np
import pandas as pd

import features as F

# --------------------------------------------------------------------------
# Frozen constants
# --------------------------------------------------------------------------

RF_PARAMS = {
    "n_estimators": 200,
    "max_depth": 5,
    "min_samples_leaf": 1,
    "random_state": 1,
}

#: Replication anchor threshold (protocol §5).
TAU_ANCHOR = 0.5

#: Frozen per-frequency τ*, from the REF-2026-016 primary forward run
#: (artifact `artifacts/formal_forward_v1_20260722/summary.csv`, event
#: fraction 0.80 = the primary checkpoint; selection rule: chunk-level F1 on a
#: label-free expanding-time inner split of the outer training window, ties to
#: the higher threshold). Frozen before this study's window opened and never
#: re-selected on this stream.
TAU_STAR_FROZEN = {
    "25S": 0.7036605405579714,
    "15S": 0.5059013502406946,
    "5S": 0.3382249603958512,
}
TAU_STAR_SOURCE = ("REF-2026-016 artifacts/formal_forward_v1_20260722/"
                   "summary.csv, fraction=0.80 (primary)")

#: Protocol §5: 30-minute per-pair cooldown (upstream paper's convention).
COOLDOWN_SECONDS = 30 * 60

#: Trivial baseline A.
ZSCORE_THRESHOLD = 4.0
ZSCORE_WINDOW_MINUTES = 5
ZSCORE_TRAILING_MINUTES = 1440

#: Trivial baseline B.
PRICE_JUMP_THRESHOLD = 0.05
PRICE_JUMP_WINDOW_MINUTES = 5

DETECTOR_RF = "upstream_rf"
DETECTOR_ZSCORE = "volume_zscore"
DETECTOR_PRICE_JUMP = "price_jump"

#: Alert-episode table schema. `ts_utc` is the row's own timestamp (chunk
#: start for the RF, minute open for the baselines); `ts_available_utc` is the
#: earliest moment the information could exist (chunk start + chunk width, or
#: minute open + 60 s), following the REF-2026-016 interval-censoring
#: convention. The cooldown runs on `ts_utc`, exactly as upstream.
EPISODE_FIELDS = [
    "pair",
    "ts_utc",
    "detector",
    "threshold",
    "score",
    "threshold_label",
    "frequency",
    "ts_available_utc",
    "day",
]

_ENV_UPSTREAM = "ABA_UPSTREAM_DATASET"
_ENV_REF016 = "ABA_REF016_ROOT"


# --------------------------------------------------------------------------
# Frozen upstream random forest
# --------------------------------------------------------------------------


def upstream_dataset_root() -> str:
    """Path to the upstream released-matrix checkout, from the environment."""

    root = os.environ.get(_ENV_UPSTREAM, "").strip()
    if not root:
        raise RuntimeError(
            "%s is not set. Point it at a checkout of the upstream "
            "pump-and-dump dataset (pinned commit d71250d4…) containing "
            "labeled_features/features_{25S,15S,5S}.csv.gz. See README, "
            "'Analysis setup'." % _ENV_UPSTREAM)
    if not os.path.isdir(root):
        raise RuntimeError("%s=%r is not a directory" % (_ENV_UPSTREAM, root))
    return root


def released_matrix_path(frequency: str, upstream_root: str | None = None) -> str:
    root = upstream_root or upstream_dataset_root()
    return os.path.join(root, "labeled_features", "features_%s.csv.gz" % frequency)


def sha256_file(path: str, chunk: int = 1 << 20) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for block in iter(lambda: handle.read(chunk), b""):
            digest.update(block)
    return digest.hexdigest()


@dataclass
class FrozenModel:
    """A fitted frozen detector plus the provenance of its training matrix."""

    frequency: str
    model: object
    meta: dict


def fit_frozen_rf(frequency: str, upstream_root: str | None = None) -> FrozenModel:
    """Fit the released-configuration RF on the upstream released matrix.

    This is the only model fit in the study. Its sole input is the released
    labelled matrix identified by ``frequency``; no argument can introduce a
    row of this study's stream (protocol §5: never retrained on the new
    stream). The matrix SHA-256 and row/positive counts are recorded so that
    every output can show which frozen matrix produced the model.
    """

    from sklearn.ensemble import RandomForestClassifier  # local: heavy import

    path = released_matrix_path(frequency, upstream_root)
    if not os.path.exists(path):
        raise FileNotFoundError(
            "released matrix not found: %s (check %s)" % (path, _ENV_UPSTREAM))
    frame = pd.read_csv(path, usecols=F.FEATURES + ["gt"])
    x = frame[F.FEATURES].to_numpy(dtype=np.float64)
    y = frame["gt"].astype(int).to_numpy()
    model = RandomForestClassifier(**RF_PARAMS, n_jobs=-1)
    model.fit(x, y)
    meta = {
        "frequency": frequency,
        "training_matrix": "upstream:labeled_features/%s" % os.path.basename(path),
        "training_matrix_sha256": sha256_file(path),
        "training_rows": int(len(frame)),
        "training_positives": int(y.sum()),
        "rf_params": dict(RF_PARAMS),
        "feature_order": list(F.FEATURES),
        "retrained_on_study_stream": False,
    }
    return FrozenModel(frequency=frequency, model=model, meta=meta)


def score_frame(frozen: FrozenModel, frame: pd.DataFrame) -> np.ndarray:
    """Score reconstructed features with a frozen model (predict_proba[:, 1])."""

    if frame.empty:
        return np.empty(0, dtype="float64")
    missing = [c for c in F.FEATURES if c not in frame.columns]
    if missing:
        raise ValueError("feature frame is missing columns: %s" % missing)
    x = frame[F.FEATURES].to_numpy(dtype=np.float64)
    if not np.isfinite(x).all():
        raise ValueError("non-finite features reached the scorer; "
                         "features.drop_nonfinite must run first")
    return frozen.model.predict_proba(x)[:, 1]


def verify_tau_star_against_ref016(ref016_root: str | None = None,
                                   fraction: float = 0.80) -> dict:
    """Check :data:`TAU_STAR_FROZEN` against the REF-2026-016 artifacts.

    Optional integrity check; returns a per-frequency comparison. Reads only;
    nothing in the REF-2026-016 repository is modified.
    """

    root = ref016_root or os.environ.get(_ENV_REF016, "").strip()
    if not root:
        return {"checked": False, "reason": "%s not set" % _ENV_REF016}
    summary = os.path.join(root, "artifacts", "formal_forward_v1_20260722",
                           "summary.csv")
    if not os.path.exists(summary):
        return {"checked": False, "reason": "summary.csv not found under %s"
                % _ENV_REF016}
    table = pd.read_csv(summary)
    rows = table.loc[np.isclose(table["fraction"], fraction)]
    out = {"checked": True, "source": TAU_STAR_SOURCE, "matches": {}}
    for frequency, expected in TAU_STAR_FROZEN.items():
        got = rows.loc[rows["frequency"] == frequency, "tau_star"]
        out["matches"][frequency] = bool(
            len(got) == 1 and np.isclose(float(got.iloc[0]), expected, rtol=0,
                                         atol=1e-12))
    out["all_match"] = all(out["matches"].values())
    return out


# --------------------------------------------------------------------------
# Trivial baselines (1-minute klines)
# --------------------------------------------------------------------------


def volume_zscore_series(grid: pd.DataFrame) -> pd.Series:
    """z of the 5-minute rolling quote volume against the trailing 24 h.

    ``grid`` is a complete 1-minute UTC grid (see `features.complete_minute_grid`),
    optionally with a warm-up prefix. The trailing window is strictly prior to
    the evaluated minute, so no future information enters.
    """

    v5 = grid["quote_volume"].rolling(window=ZSCORE_WINDOW_MINUTES).sum()
    prior = v5.shift(1)
    mean = prior.rolling(window=ZSCORE_TRAILING_MINUTES).mean()
    sd = prior.rolling(window=ZSCORE_TRAILING_MINUTES).std(ddof=1)
    z = (v5 - mean) / sd.where(sd > 0)
    return z


def price_jump_series(grid: pd.DataFrame) -> pd.Series:
    """5-minute simple return of the 1-minute close."""

    close = grid["close"]
    return close / close.shift(PRICE_JUMP_WINDOW_MINUTES) - 1.0


def baseline_candidates(grid: pd.DataFrame, detector: str) -> pd.DataFrame:
    """Candidate crossings (pre-cooldown) for one baseline on one grid."""

    if detector == DETECTOR_ZSCORE:
        score = volume_zscore_series(grid)
        threshold = ZSCORE_THRESHOLD
    elif detector == DETECTOR_PRICE_JUMP:
        score = price_jump_series(grid)
        threshold = PRICE_JUMP_THRESHOLD
    else:
        raise KeyError("unknown baseline detector %r" % (detector,))
    hit = score > threshold
    hit = hit & score.notna()
    out = pd.DataFrame(
        {"ts_utc": grid.index[hit.to_numpy()],
         "score": score[hit.to_numpy()].to_numpy()}
    )
    out["threshold"] = threshold
    return out.reset_index(drop=True)


# --------------------------------------------------------------------------
# Cooldown
# --------------------------------------------------------------------------


def apply_cooldown(timestamps, scores=None,
                   cooldown_seconds: int = COOLDOWN_SECONDS,
                   last_emitted: pd.Timestamp | None = None):
    """Apply the 30-minute cooldown to an ordered candidate stream.

    Candidates are processed in strict timestamp order. A candidate is emitted
    only when no alert was emitted within the preceding ``cooldown_seconds``
    (strictly: a gap of exactly the cooldown is emitted, matching the
    REF-2026-016 ``emit_alerts`` comparison ``< cooldown``). ``last_emitted``
    carries the cooldown clock in from the previous day, so the clock never
    resets at midnight.

    Returns ``(kept_index, last_emitted)`` where ``kept_index`` is a list of
    positions into the input.
    """

    stamps = pd.to_datetime(pd.Series(list(timestamps)))
    if scores is not None and len(scores) != len(stamps):
        raise ValueError("scores must align with timestamps")
    order = np.argsort(stamps.to_numpy(), kind="mergesort")
    kept = []
    for position in order:
        current = pd.Timestamp(stamps.iloc[int(position)])
        if (last_emitted is not None and
                (current - last_emitted).total_seconds() < cooldown_seconds):
            continue
        kept.append(int(position))
        last_emitted = current
    return kept, last_emitted


# --------------------------------------------------------------------------
# Stream assembly
# --------------------------------------------------------------------------


def _episode_rows(pair, day, detector, frequency, threshold, threshold_label,
                  timestamps, scores, bin_seconds):
    rows = []
    for stamp, score in zip(timestamps, scores):
        stamp = pd.Timestamp(stamp)
        rows.append({
            "pair": pair,
            "ts_utc": stamp.isoformat(),
            "detector": detector,
            "threshold": float(threshold),
            "score": float(score),
            "threshold_label": threshold_label,
            "frequency": frequency,
            "ts_available_utc": (stamp + pd.Timedelta(seconds=bin_seconds)
                                 ).isoformat(),
            "day": day,
        })
    return rows


def rf_episodes_for_pair_day(pair_day: F.PairDayFeatures, scores: np.ndarray,
                             thresholds: dict, cooldown_state: dict):
    """Emit RF alert episodes for one pair-day at each frozen threshold."""

    rows = []
    bin_seconds = F.FREQUENCY_BIN_SECONDS[pair_day.frequency]
    frame = pair_day.frame
    for label, tau in thresholds.items():
        hit = np.asarray(scores) >= tau
        stamps = frame.loc[hit, "date"].tolist()
        hit_scores = np.asarray(scores)[hit]
        key = (pair_day.symbol, DETECTOR_RF, pair_day.frequency, label)
        kept, last = apply_cooldown(stamps, hit_scores,
                                    last_emitted=cooldown_state.get(key))
        cooldown_state[key] = last
        rows.extend(_episode_rows(
            pair_day.symbol, pair_day.day, DETECTOR_RF, pair_day.frequency,
            tau, label, [stamps[i] for i in kept], hit_scores[kept],
            bin_seconds))
    return rows


def baseline_episodes_for_pair_day(pair, day, grid, cooldown_state: dict,
                                   target_day_only: bool = True):
    """Emit baseline alert episodes for one pair-day from a 1-minute grid."""

    rows = []
    day_start = pd.Timestamp(day)
    day_end = day_start + pd.Timedelta(days=1)
    for detector, label in ((DETECTOR_ZSCORE, "fixed"),
                            (DETECTOR_PRICE_JUMP, "fixed")):
        candidates = baseline_candidates(grid, detector)
        if target_day_only and not candidates.empty:
            in_day = ((candidates["ts_utc"] >= day_start) &
                      (candidates["ts_utc"] < day_end))
            candidates = candidates.loc[in_day].reset_index(drop=True)
        key = (pair, detector, "", label)
        kept, last = apply_cooldown(candidates["ts_utc"].tolist(),
                                    candidates["score"].tolist(),
                                    last_emitted=cooldown_state.get(key))
        cooldown_state[key] = last
        threshold = (ZSCORE_THRESHOLD if detector == DETECTOR_ZSCORE
                     else PRICE_JUMP_THRESHOLD)
        rows.extend(_episode_rows(
            pair, day, detector, "", threshold, label,
            [candidates["ts_utc"].iloc[i] for i in kept],
            [candidates["score"].iloc[i] for i in kept],
            60))
    return rows


def rf_thresholds(frequency: str) -> dict:
    """The two frozen RF thresholds for a frequency."""

    return {"tau_anchor": TAU_ANCHOR, "tau_star": TAU_STAR_FROZEN[frequency]}


#: Pre-cooldown candidate table. Burden-vs-threshold curves must re-apply the
#: cooldown at every threshold (suppression depends on which crossings
#: survive), so the analysis retains all crossings down to a floor score
#: instead of only the episodes emitted at the frozen thresholds. Curves are
#: defined for thresholds at or above the corresponding floor, which is
#: recorded with the curve.
CANDIDATE_FIELDS = ["pair", "ts_utc", "detector", "frequency", "score", "day"]

CURVE_FLOORS = {
    DETECTOR_RF: 0.05,
    DETECTOR_ZSCORE: 2.0,
    DETECTOR_PRICE_JUMP: 0.01,
}


def rf_candidate_rows(pair_day: F.PairDayFeatures, scores) -> list:
    """Pre-cooldown RF crossings down to the curve floor."""

    scores = np.asarray(scores, dtype=float)
    floor = CURVE_FLOORS[DETECTOR_RF]
    hit = scores >= floor
    stamps = pair_day.frame.loc[hit, "date"]
    return [{"pair": pair_day.symbol, "ts_utc": pd.Timestamp(s).isoformat(),
             "detector": DETECTOR_RF, "frequency": pair_day.frequency,
             "score": float(v), "day": pair_day.day}
            for s, v in zip(stamps, scores[hit])]


def baseline_candidate_rows(pair, day, grid, target_day_only: bool = True) -> list:
    """Pre-cooldown baseline crossings down to the curve floors."""

    rows = []
    day_start = pd.Timestamp(day)
    day_end = day_start + pd.Timedelta(days=1)
    for detector, series in ((DETECTOR_ZSCORE, volume_zscore_series(grid)),
                             (DETECTOR_PRICE_JUMP, price_jump_series(grid))):
        floor = CURVE_FLOORS[detector]
        hit = (series > floor) & series.notna()
        stamps = grid.index[hit.to_numpy()]
        values = series[hit.to_numpy()].to_numpy()
        for stamp, value in zip(stamps, values):
            if target_day_only and not (day_start <= stamp < day_end):
                continue
            rows.append({"pair": pair, "ts_utc": pd.Timestamp(stamp).isoformat(),
                         "detector": detector, "frequency": "",
                         "score": float(value), "day": day})
    return rows


def write_candidates(rows, path: str) -> int:
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    with open(path, "w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=CANDIDATE_FIELDS)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in CANDIDATE_FIELDS})
    return len(rows)


def write_episodes(rows, path: str) -> int:
    """Write an alert-episode table. Returns the number of rows written."""

    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    with open(path, "w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=EPISODE_FIELDS)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in EPISODE_FIELDS})
    return len(rows)


# --------------------------------------------------------------------------
# Structural-only sink
# --------------------------------------------------------------------------


class StructuralSink:
    """Consume alert episodes without retaining or reporting any count.

    Used by `run_analysis.py --partial-structural-check`. The sink records
    only mechanical properties — whether timestamps are ordered, whether the
    cooldown separation holds, whether the schema conforms — and deliberately
    exposes no alert count, score, or rate. Neither the number of episodes nor
    the number of streams that produced one is stored: on partial data both
    are burden quantities. Emission behaviour is verified instead on synthetic
    data with known answers (`tests/test_detectors.py`).
    """

    def __init__(self):
        self.schema_ok = True
        self.cooldown_ok = True
        self.timestamps_utc_naive_ok = True
        self.score_range_ok = True

    def consume(self, rows, cooldown_seconds: int = COOLDOWN_SECONDS) -> None:
        by_stream = {}
        for row in rows:
            if set(row) != set(EPISODE_FIELDS):
                self.schema_ok = False
            key = (row["pair"], row["detector"], row["frequency"],
                   row["threshold_label"])
            by_stream.setdefault(key, []).append(row)
        for key, stream in by_stream.items():
            stamps = [pd.Timestamp(r["ts_utc"]) for r in stream]
            if any(s.tzinfo is not None for s in stamps):
                self.timestamps_utc_naive_ok = False
            ordered = sorted(stamps)
            if ordered != stamps:
                self.schema_ok = False
            for previous, current in zip(ordered, ordered[1:]):
                if (current - previous).total_seconds() < cooldown_seconds:
                    self.cooldown_ok = False
            if key[1] == DETECTOR_RF:
                for row in stream:
                    if not 0.0 <= float(row["score"]) <= 1.0:
                        self.score_range_ok = False

    def report(self) -> dict:
        return {
            "episode_schema_conformant": self.schema_ok,
            "cooldown_separation_respected": self.cooldown_ok,
            "timestamps_tz_naive_utc": self.timestamps_utc_naive_ok,
            "rf_scores_within_unit_interval": self.score_range_ok,
            "note": ("episode counts, scores, stream counts and rates are "
                     "endpoint-bearing and are deliberately not recorded by "
                     "this sink"),
        }
