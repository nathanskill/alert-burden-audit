#!/usr/bin/env python3
"""burden.py — the §2 primary endpoint: alert burden (REF-2026-017).

Protocol reference: `protocol/locked_protocol_v1.0.md` §2.1 (primary endpoint),
§3 (monitored universe), §7 (stratification, event-cluster bootstrap), and
Amendments 3 (coverage-log semantics) and 4 (day-to-membership mapping).
Protocol freeze commit: 0ac2bbd026915b1ac09acf649638d28e637d0289.

Endpoints implemented here
--------------------------
* the distribution of alerts/day;
* alerts per 1 000 monitored pair-hours;
* burden curves as a function of threshold, at the anchor τ = 0.5 and at the
  frozen τ* (`apply_detectors.TAU_STAR_FROZEN`) and across a threshold grid;
* per-pair-activity stratification (§7);
* interval estimates from an event-cluster bootstrap with n = 2000 and seed
  20260722 exactly as §7 specifies, resampling whole **pairs** (each pair
  carries all of its episodes and all of its pair-hours) — never rows.

The monitored pair-hours denominator
------------------------------------
Default rule — ``verified-archive``: a pair-day contributes 24 monitored
pair-hours only when (a) the pair is in the universe membership table
governing that UTC day (Amendment 4: the newest archived table dated strictly
before the day) **and** (b) the pull manifest records a checksum-verified
archive for *both* required files of that pair-day (the aggTrades archive and
the 1-minute kline archive). A pair-day whose archive never verified is not
monitored, because no detector could have run on it.

This was flagged as an author decision in an earlier audit, so the alternative
is implemented as a documented sensitivity switch rather than left implicit:
``--pair-hours-rule universe-membership`` credits 24 h to every governed
pair-day regardless of archive state. It always yields a denominator at least
as large as the default and is reported as a sensitivity row, never as the
headline. The rule in force is recorded in every output.

Coverage-log semantics follow Amendment 3 §4: rows are deduplicated per
``(date, file)`` keeping the final status, and the 2026-07-20 validation pull
is excluded from all coverage computations. Surplus rows for pairs outside the
governed universe of a day (Amendments 3 §3 and 4 §2) are dropped by the
universe intersection above and enter no denominator.

Endpoint guard
--------------
Every endpoint-producing function calls :func:`assert_endpoints_allowed`,
which raises when ``ABA_STRUCTURAL_ONLY=1`` is set in the environment.
`run_analysis.py --partial-structural-check` sets that variable, so a partial
run cannot compute a burden value even by mistake.
"""

from __future__ import annotations

import csv
import os
from collections import defaultdict

import numpy as np
import pandas as pd

import apply_detectors as D

# --------------------------------------------------------------------------
# Frozen analysis constants
# --------------------------------------------------------------------------

#: Protocol §7.
BOOTSTRAP_N = 2000
BOOTSTRAP_SEED = 20260722
BOOTSTRAP_ALPHA = 0.05

HOURS_PER_PAIR_DAY = 24.0
PAIR_HOURS_SCALE = 1000.0

PAIR_HOURS_RULES = ("verified-archive", "universe-membership")
DEFAULT_PAIR_HOURS_RULE = "verified-archive"

#: Amendment 3 §4: the pre-window validation pull is excluded from coverage.
EXCLUDED_COVERAGE_DATES = frozenset({"2026-07-20"})

#: Per-pair-activity strata (§7), assigned mechanically from the governing
#: membership table's own trailing-30-day volume ranking.
#: The two pairs reinstated by Amendment 2 carry no rank in the defective
#: tables, so they form their own reported bucket rather than being silently
#: dropped into the least-active tertile.
ACTIVITY_STRATA = ("high", "mid", "low", "unranked_amendment2")

#: Amendment 2 §4: JUPUSDT and SYRUPUSDT were removed from the archived
#: membership tables by a defective leveraged-token suffix heuristic, not by
#: the frozen §3 rule. Their archives were backfilled through the collector's
#: standard checksum-verified fetch path, and the amendment binds the primary
#: analysis to include them for the affected window, with a with/without
#: sensitivity check. They are reinstated for every stream day governed by one
#: of the two defective tables; from the first post-fix weekly refresh they are
#: in the archived table itself and this rule adds nothing.
AMENDMENT2_REINSTATED_PAIRS = ("JUPUSDT", "SYRUPUSDT")
AMENDMENT2_LAST_DEFECTIVE_TABLE = "2026-07-31"

_ENV_STRUCTURAL_ONLY = "ABA_STRUCTURAL_ONLY"


class EndpointsSuppressed(RuntimeError):
    """Raised when an endpoint computation is attempted in structural mode."""


def assert_endpoints_allowed() -> None:
    if os.environ.get(_ENV_STRUCTURAL_ONLY, "") == "1":
        raise EndpointsSuppressed(
            "endpoint computation is disabled: %s=1 (partial-window "
            "structural check). No primary or secondary endpoint value may be "
            "computed before the evaluation window closes."
            % _ENV_STRUCTURAL_ONLY)


# --------------------------------------------------------------------------
# Universe membership (protocol §3, Amendment 4)
# --------------------------------------------------------------------------


def load_universe_tables(universe_dir: str) -> dict:
    """{table date (str) -> [(symbol, rank, included)]} for archived tables."""

    tables = {}
    for name in sorted(os.listdir(universe_dir)):
        if not (name.startswith("universe_") and name.endswith(".csv")):
            continue
        date = name[len("universe_"):-len(".csv")]
        rows = []
        with open(os.path.join(universe_dir, name), newline="") as handle:
            for row in csv.DictReader(handle):
                rows.append((row["symbol"], int(row["rank"]),
                             row["included"] == "1"))
        tables[date] = rows
    return tables


def universe_for_day(tables: dict, day: str) -> str | None:
    """Table date governing ``day``: newest dated strictly before it."""

    candidates = [d for d in tables if d < day]
    return max(candidates) if candidates else None


def monitored_symbols(tables: dict, day: str,
                      include_amendment2_pairs: bool = True) -> list[str]:
    """Included symbols of the membership table governing ``day``.

    With ``include_amendment2_pairs`` (the default, binding under Amendment 2
    §4) the two pairs wrongly removed by the leveraged-suffix defect are
    reinstated for the days governed by a defective table. Passing False is
    the amendment's own with/without sensitivity variant.
    """

    table_date = universe_for_day(tables, day)
    if table_date is None:
        return []
    rows = [r for r in tables[table_date] if r[2]]
    rows.sort(key=lambda r: r[1])
    symbols = [r[0] for r in rows]
    if include_amendment2_pairs and table_date <= AMENDMENT2_LAST_DEFECTIVE_TABLE:
        for pair in AMENDMENT2_REINSTATED_PAIRS:
            if pair not in symbols:
                symbols.append(pair)
    return symbols


def activity_strata(tables: dict, day: str,
                    include_amendment2_pairs: bool = True) -> dict:
    """Map symbol -> activity stratum for one day (§7 stratification).

    Strata are mechanical: the governing membership table already ranks every
    pair by trailing-30-day quote volume, so the included pairs are split into
    three equal-size groups by that rank ('high' = most active third). No
    quantity from the evaluation stream enters the assignment. Pairs
    reinstated by Amendment 2 have no rank in the defective table and are
    reported in their own bucket.
    """

    symbols = monitored_symbols(tables, day, include_amendment2_pairs=False)
    n = len(symbols)
    out = {}
    if n:
        edges = [round(n * (i + 1) / 3) for i in range(3)]
        for position, symbol in enumerate(symbols):
            if position < edges[0]:
                out[symbol] = "high"
            elif position < edges[1]:
                out[symbol] = "mid"
            else:
                out[symbol] = "low"
    if include_amendment2_pairs:
        for symbol in monitored_symbols(tables, day, True):
            out.setdefault(symbol, "unranked_amendment2")
    return out


# --------------------------------------------------------------------------
# Manifests and coverage (protocol §4, Amendment 3)
# --------------------------------------------------------------------------


def _dedupe_last(rows, key_fields):
    """Keep the final row per key (Amendment 3 §4: append-only logs)."""

    out = {}
    for row in rows:
        out[tuple(row[f] for f in key_fields)] = row
    return list(out.values())


def load_pull_manifest(manifest_csv: str) -> list[dict]:
    with open(manifest_csv, newline="") as handle:
        rows = [r for r in csv.DictReader(handle)
                if r["date"] not in EXCLUDED_COVERAGE_DATES]
    return _dedupe_last(rows, ("date", "file"))


def load_coverage_log(coverage_csv: str) -> list[dict]:
    with open(coverage_csv, newline="") as handle:
        rows = [r for r in csv.DictReader(handle)
                if r["date"] not in EXCLUDED_COVERAGE_DATES]
    return _dedupe_last(rows, ("date", "file"))


def required_files(symbol: str, day: str) -> tuple[str, str]:
    return ("%s-aggTrades-%s.zip" % (symbol, day), "%s-1m-%s.zip" % (symbol, day))


def verified_pair_days(manifest_rows) -> set:
    """{(day, symbol)} whose *both* required archives are checksum-verified.

    "Verified" means the pull manifest carries a row for the file with
    ``source_checksum_ok == '1'``: the file's SHA-256 matched the official
    ``.CHECKSUM`` sidecar published alongside the archive.
    """

    seen = defaultdict(set)
    for row in manifest_rows:
        if row.get("source_checksum_ok") != "1":
            continue
        seen[(row["date"], row["symbol"])].add(row["file"])
    out = set()
    for (day, symbol), files in seen.items():
        need = set(required_files(symbol, day))
        if need <= files:
            out.add((day, symbol))
    return out


def monitored_pair_days(tables: dict, days, manifest_rows,
                        rule: str = DEFAULT_PAIR_HOURS_RULE,
                        include_amendment2_pairs: bool = True) -> dict:
    """{day -> sorted list of monitored symbols} under the chosen rule."""

    if rule not in PAIR_HOURS_RULES:
        raise ValueError("unknown pair-hours rule %r (expected one of %s)"
                         % (rule, ", ".join(PAIR_HOURS_RULES)))
    verified = verified_pair_days(manifest_rows) if rule == "verified-archive" else None
    out = {}
    for day in days:
        governed = monitored_symbols(tables, day, include_amendment2_pairs)
        if verified is None:
            out[day] = sorted(governed)
        else:
            out[day] = sorted(s for s in governed if (day, s) in verified)
    return out


def pair_hours(tables: dict, days, manifest_rows,
               rule: str = DEFAULT_PAIR_HOURS_RULE,
               include_amendment2_pairs: bool = True) -> dict:
    """Monitored pair-hours per day (denominator of the primary endpoint).

    Structural denominators are not endpoint values — the protocol's
    verification plan explicitly includes checking them — so this function is
    available in structural mode.
    """

    per_day = monitored_pair_days(tables, days, manifest_rows, rule,
                                  include_amendment2_pairs)
    return {day: len(symbols) * HOURS_PER_PAIR_DAY
            for day, symbols in per_day.items()}


def pair_hours_by_pair(tables: dict, days, manifest_rows,
                       rule: str = DEFAULT_PAIR_HOURS_RULE,
                       include_amendment2_pairs: bool = True) -> dict:
    """Monitored pair-hours per pair (bootstrap cluster denominators)."""

    per_day = monitored_pair_days(tables, days, manifest_rows, rule,
                                  include_amendment2_pairs)
    out = defaultdict(float)
    for symbols in per_day.values():
        for symbol in symbols:
            out[symbol] += HOURS_PER_PAIR_DAY
    return dict(out)


# --------------------------------------------------------------------------
# Episode tables
# --------------------------------------------------------------------------


def load_episodes(path: str) -> pd.DataFrame:
    frame = pd.read_csv(path)
    if not frame.empty:
        frame["ts_utc"] = pd.to_datetime(frame["ts_utc"])
    return frame


def _stream_key(frame: pd.DataFrame) -> pd.Series:
    return (frame["detector"].astype(str) + "|" +
            frame["frequency"].fillna("").astype(str) + "|" +
            frame["threshold_label"].astype(str))


def restrict_to_monitored(episodes: pd.DataFrame, per_day: dict) -> pd.DataFrame:
    """Drop episodes for pair-days outside the monitored set.

    Surplus archive rows outside the governed universe (Amendments 3 §3 and
    4 §2) and any pair-day excluded by the pair-hours rule must not contribute
    to a numerator whose denominator excludes them.
    """

    if episodes.empty:
        return episodes
    allowed = {(day, symbol) for day, symbols in per_day.items()
               for symbol in symbols}
    mask = [(row.day, row.pair) in allowed
            for row in episodes.itertuples(index=False)]
    return episodes.loc[mask].reset_index(drop=True)


# --------------------------------------------------------------------------
# Primary endpoint
# --------------------------------------------------------------------------


def alerts_per_day(episodes: pd.DataFrame, days) -> pd.DataFrame:
    """Alerts/day per detector stream, with every day in the window present."""

    assert_endpoints_allowed()
    days = list(days)
    rows = []
    if episodes.empty:
        return pd.DataFrame(columns=["detector", "frequency", "threshold_label",
                                     "day", "alerts"])
    episodes = episodes.assign(_stream=_stream_key(episodes))
    for stream, group in episodes.groupby("_stream", sort=True):
        detector, frequency, label = stream.split("|")
        counts = group.groupby("day").size().to_dict()
        for day in days:
            rows.append({"detector": detector, "frequency": frequency,
                         "threshold_label": label, "day": day,
                         "alerts": int(counts.get(day, 0))})
    return pd.DataFrame(rows)


def alerts_per_day_distribution(per_day: pd.DataFrame) -> pd.DataFrame:
    """Distribution summary of alerts/day for each detector stream."""

    assert_endpoints_allowed()
    rows = []
    for (detector, frequency, label), group in per_day.groupby(
            ["detector", "frequency", "threshold_label"], sort=True, dropna=False):
        values = group["alerts"].to_numpy(dtype=float)
        rows.append({
            "detector": detector,
            "frequency": frequency,
            "threshold_label": label,
            "n_days": int(len(values)),
            "mean": float(values.mean()),
            "sd": float(values.std(ddof=1)) if len(values) > 1 else float("nan"),
            "min": float(values.min()),
            "p25": float(np.percentile(values, 25)),
            "median": float(np.median(values)),
            "p75": float(np.percentile(values, 75)),
            "p90": float(np.percentile(values, 90)),
            "max": float(values.max()),
            "total": float(values.sum()),
        })
    return pd.DataFrame(rows)


def alerts_per_1000_pair_hours(episodes: pd.DataFrame, total_pair_hours: float
                               ) -> pd.DataFrame:
    """Alerts per 1 000 monitored pair-hours for each detector stream."""

    assert_endpoints_allowed()
    if total_pair_hours <= 0:
        raise ValueError("monitored pair-hours denominator is zero")
    rows = []
    if episodes.empty:
        return pd.DataFrame(columns=["detector", "frequency", "threshold_label",
                                     "alerts", "pair_hours",
                                     "alerts_per_1000_pair_hours"])
    episodes = episodes.assign(_stream=_stream_key(episodes))
    for stream, group in episodes.groupby("_stream", sort=True):
        detector, frequency, label = stream.split("|")
        rows.append({
            "detector": detector,
            "frequency": frequency,
            "threshold_label": label,
            "alerts": int(len(group)),
            "pair_hours": float(total_pair_hours),
            "alerts_per_1000_pair_hours":
                PAIR_HOURS_SCALE * len(group) / total_pair_hours,
        })
    return pd.DataFrame(rows)


def burden_curve(candidates: pd.DataFrame, detector: str, thresholds,
                 frequency: str = "",
                 cooldown_seconds: int = D.COOLDOWN_SECONDS) -> pd.DataFrame:
    """Burden as a function of threshold, recomputing the cooldown per τ.

    ``candidates`` is the pre-cooldown candidate table (pair, ts_utc,
    detector, frequency, score) retained down to a floor score; the cooldown
    must be re-applied at every threshold because suppression depends on which
    crossings survive. Curves are therefore only defined for thresholds at or
    above the retained floor, which is recorded alongside the curve.
    """

    assert_endpoints_allowed()
    selected = candidates.loc[(candidates["detector"] == detector) &
                              (candidates["frequency"].fillna("") == frequency)]
    rows = []
    for threshold in thresholds:
        total = 0
        for pair, group in selected.groupby("pair", sort=True):
            hits = group.loc[group["score"] >= threshold].sort_values(
                "ts_utc", kind="mergesort")
            kept, _ = D.apply_cooldown(hits["ts_utc"].tolist(),
                                       hits["score"].tolist(),
                                       cooldown_seconds=cooldown_seconds)
            total += len(kept)
        rows.append({"detector": detector, "frequency": frequency,
                     "threshold": float(threshold), "alerts": int(total)})
    return pd.DataFrame(rows)


def stratified_burden(episodes: pd.DataFrame, tables: dict, days,
                      manifest_rows, rule: str = DEFAULT_PAIR_HOURS_RULE,
                      include_amendment2_pairs: bool = True) -> pd.DataFrame:
    """Alerts and pair-hours per activity stratum (§7 stratification)."""

    assert_endpoints_allowed()
    per_day = monitored_pair_days(tables, days, manifest_rows, rule,
                                  include_amendment2_pairs)
    hours = defaultdict(float)
    for day, symbols in per_day.items():
        strata = activity_strata(tables, day, include_amendment2_pairs)
        for symbol in symbols:
            hours[strata.get(symbol, "unassigned")] += HOURS_PER_PAIR_DAY
    counts = defaultdict(int)
    episodes = restrict_to_monitored(episodes, per_day)
    if not episodes.empty:
        episodes = episodes.assign(_stream=_stream_key(episodes))
        strata_cache = {day: activity_strata(tables, day, include_amendment2_pairs)
                        for day in days}
        for row in episodes.itertuples(index=False):
            stratum = strata_cache.get(row.day, {}).get(row.pair, "unassigned")
            counts[(row._stream, stratum)] += 1
    rows = []
    streams = sorted({k[0] for k in counts})
    for stream in streams:
        detector, frequency, label = stream.split("|")
        for stratum in ACTIVITY_STRATA:
            denominator = hours.get(stratum, 0.0)
            alerts = counts.get((stream, stratum), 0)
            rows.append({
                "detector": detector,
                "frequency": frequency,
                "threshold_label": label,
                "stratum": stratum,
                "alerts": int(alerts),
                "pair_hours": float(denominator),
                "alerts_per_1000_pair_hours":
                    (PAIR_HOURS_SCALE * alerts / denominator)
                    if denominator else float("nan"),
            })
    return pd.DataFrame(rows)


def detector_agreement(episodes: pd.DataFrame,
                       window_seconds: int = D.COOLDOWN_SECONDS) -> pd.DataFrame:
    """Alert-window overlap between detector streams (§7 detector agreement).

    For each ordered pair of streams, the share of stream A's alerts on a pair
    that have a stream-B alert on the same pair within ±``window_seconds``.
    The default window is the cooldown length, so agreement is measured at the
    resolution at which the detectors can emit.
    """

    assert_endpoints_allowed()
    if episodes.empty:
        return pd.DataFrame(columns=["stream_a", "stream_b", "n_alerts_a",
                                     "n_matched", "share_matched",
                                     "window_seconds"])
    frame = episodes.assign(_stream=_stream_key(episodes))
    streams = sorted(frame["_stream"].unique())
    by_stream_pair = {}
    for stream in streams:
        subset = frame.loc[frame["_stream"] == stream]
        by_stream_pair[stream] = {
            pair: np.sort(pd.to_datetime(group["ts_utc"]).astype("int64").to_numpy())
            for pair, group in subset.groupby("pair")}
    tolerance = int(window_seconds) * 1_000_000_000
    rows = []
    for a in streams:
        for b in streams:
            if a == b:
                continue
            total = 0
            matched = 0
            for pair, stamps_a in by_stream_pair[a].items():
                stamps_b = by_stream_pair[b].get(pair)
                total += len(stamps_a)
                if stamps_b is None or len(stamps_b) == 0:
                    continue
                positions = np.searchsorted(stamps_b, stamps_a)
                for value, position in zip(stamps_a, positions):
                    nearest = []
                    if position < len(stamps_b):
                        nearest.append(abs(stamps_b[position] - value))
                    if position > 0:
                        nearest.append(abs(value - stamps_b[position - 1]))
                    if nearest and min(nearest) <= tolerance:
                        matched += 1
            rows.append({"stream_a": a, "stream_b": b, "n_alerts_a": int(total),
                         "n_matched": int(matched),
                         "share_matched": (matched / total) if total else float("nan"),
                         "window_seconds": int(window_seconds)})
    return pd.DataFrame(rows)


# --------------------------------------------------------------------------
# Event-cluster bootstrap (§7)
# --------------------------------------------------------------------------


def cluster_bootstrap_interval(values_by_cluster, n_boot: int = BOOTSTRAP_N,
                               seed: int = BOOTSTRAP_SEED,
                               alpha: float = BOOTSTRAP_ALPHA):
    """Percentile bootstrap of a mean, resampling whole clusters.

    Mirrors the REF-2026-016 frozen implementation: ``np.random.default_rng``
    seeded with 20260722, ``n_boot`` resamples of the cluster vector with
    replacement, percentile interval. Rows are never resampled.
    """

    values = np.asarray(list(values_by_cluster), dtype=float)
    if values.size == 0:
        raise ValueError("no cluster-level values to bootstrap")
    rng = np.random.default_rng(seed)
    n = values.size
    means = np.empty(n_boot)
    for b in range(n_boot):
        means[b] = values[rng.integers(0, n, size=n)].mean()
    return (float(np.percentile(means, 100 * (alpha / 2))),
            float(np.percentile(means, 100 * (1 - alpha / 2))))


def cluster_bootstrap_ratio(numerator_by_cluster, denominator_by_cluster,
                            scale: float = PAIR_HOURS_SCALE,
                            n_boot: int = BOOTSTRAP_N,
                            seed: int = BOOTSTRAP_SEED,
                            alpha: float = BOOTSTRAP_ALPHA):
    """Percentile bootstrap of ``scale * sum(num) / sum(den)`` over clusters.

    The resampling unit is the pair: a resampled pair carries *all* of its
    alert episodes and *all* of its monitored pair-hours, so within-pair
    clustering of episodes is preserved. Rows and individual episodes are
    never resampled independently.
    """

    num = np.asarray(list(numerator_by_cluster), dtype=float)
    den = np.asarray(list(denominator_by_cluster), dtype=float)
    if num.shape != den.shape:
        raise ValueError("numerator and denominator clusters must align")
    if num.size == 0:
        raise ValueError("no clusters to bootstrap")
    rng = np.random.default_rng(seed)
    n = num.size
    ratios = np.empty(n_boot)
    for b in range(n_boot):
        idx = rng.integers(0, n, size=n)
        denominator = den[idx].sum()
        ratios[b] = (scale * num[idx].sum() / denominator
                     if denominator > 0 else np.nan)
    finite = ratios[np.isfinite(ratios)]
    if finite.size == 0:
        raise ValueError("all bootstrap resamples had an empty denominator")
    return (float(np.percentile(finite, 100 * (alpha / 2))),
            float(np.percentile(finite, 100 * (1 - alpha / 2))))


def bootstrap_rate_by_pair(episodes: pd.DataFrame, hours_by_pair: dict,
                           n_boot: int = BOOTSTRAP_N,
                           seed: int = BOOTSTRAP_SEED) -> pd.DataFrame:
    """Per-stream alerts per 1 000 pair-hours with a pair-cluster interval."""

    assert_endpoints_allowed()
    pairs = sorted(hours_by_pair)
    denominators = [hours_by_pair[p] for p in pairs]
    rows = []
    if episodes.empty:
        return pd.DataFrame(columns=["detector", "frequency", "threshold_label",
                                     "alerts_per_1000_pair_hours",
                                     "ci95_low", "ci95_high", "n_pairs"])
    episodes = episodes.assign(_stream=_stream_key(episodes))
    for stream, group in episodes.groupby("_stream", sort=True):
        detector, frequency, label = stream.split("|")
        per_pair = group.groupby("pair").size().to_dict()
        numerators = [per_pair.get(p, 0) for p in pairs]
        low, high = cluster_bootstrap_ratio(numerators, denominators,
                                            n_boot=n_boot, seed=seed)
        total_hours = float(sum(denominators))
        rows.append({
            "detector": detector,
            "frequency": frequency,
            "threshold_label": label,
            "alerts_per_1000_pair_hours":
                PAIR_HOURS_SCALE * sum(numerators) / total_hours,
            "ci95_low": low,
            "ci95_high": high,
            "n_pairs": len(pairs),
        })
    return pd.DataFrame(rows)


# --------------------------------------------------------------------------
# Structural (non-endpoint) reporting
# --------------------------------------------------------------------------


def denominator_report(tables: dict, days, manifest_rows,
                       coverage_rows=None,
                       include_amendment2_pairs: bool = True) -> dict:
    """Structural facts about the denominator under both rules.

    Contains only counts of pairs, days and pair-hours — the quantities the
    protocol's verification plan calls for — and no alert quantity, so it is
    safe to produce during a partial-window structural check.
    """

    days = list(days)
    report = {"days": days, "n_days": len(days), "rules": {},
              "include_amendment2_pairs": include_amendment2_pairs,
              "amendment2_pairs": list(AMENDMENT2_REINSTATED_PAIRS)}
    for rule in PAIR_HOURS_RULES:
        per_day = monitored_pair_days(tables, days, manifest_rows, rule,
                                      include_amendment2_pairs)
        hours = pair_hours(tables, days, manifest_rows, rule,
                           include_amendment2_pairs)
        report["rules"][rule] = {
            "monitored_pairs_by_day": {d: len(per_day[d]) for d in days},
            "pair_hours_by_day": {d: hours[d] for d in days},
            "total_pair_days": int(sum(len(per_day[d]) for d in days)),
            "total_pair_hours": float(sum(hours.values())),
            "distinct_pairs": len({s for d in days for s in per_day[d]}),
        }
    governed = {d: monitored_symbols(tables, d, include_amendment2_pairs)
                for d in days}
    report["governing_table_by_day"] = {
        d: universe_for_day(tables, d) for d in days}
    report["governed_pairs_by_day"] = {d: len(governed[d]) for d in days}
    default_rule = report["rules"][DEFAULT_PAIR_HOURS_RULE]
    report["pair_days_excluded_by_default_rule"] = {
        d: len(governed[d]) - default_rule["monitored_pairs_by_day"][d]
        for d in days}
    if coverage_rows is not None:
        statuses = defaultdict(int)
        for row in coverage_rows:
            if row["date"] in days:
                statuses[row["status"]] += 1
        report["coverage_status_counts_deduped"] = dict(statuses)
    report["pair_hours_rule_default"] = DEFAULT_PAIR_HOURS_RULE
    return report
