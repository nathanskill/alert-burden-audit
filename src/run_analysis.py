#!/usr/bin/env python3
"""run_analysis.py — analysis driver for the alert-burden audit (REF-2026-017).

Protocol: `protocol/locked_protocol_v1.0.md`, frozen at commit
0ac2bbd026915b1ac09acf649638d28e637d0289 (tag `v1.0-protocol-freeze`).
Amendments 1-5 and `protocol/incident_20260731_partial_publication.md` apply.
The §6 companion cross-exchange module was permanently dropped by Amendment 5
(0/30 clearance against the pre-registered ≥ 60 % gate); nothing in this
pipeline implements or reports it.

The primary evaluation stream is the first 12 complete UTC weeks beginning
2026-07-24 (protocol §4): 84 UTC days, 2026-07-24 through 2026-10-15
inclusive. Extended collection is sensitivity only.

Two modes
---------
``run_analysis.py`` (default) runs the full analysis and produces every
artifact the paper cites. It **refuses to run before the evaluation window is
complete**: all 84 days must be present in the collector's
``days_completed.txt``.

``run_analysis.py --partial-structural-check`` is the only way to touch the
partially-collected stream. It sets ``ABA_STRUCTURAL_ONLY=1``, which makes
every endpoint-producing function in `burden.py` and `precision_proxy.py`
raise, and its output is limited to structural facts:

  * how many days, pairs, pair-days, files and feature rows were processed;
  * denominators under both pair-hours rules;
  * schema conformance, timestamp/timezone conformance, dropped-row counts,
    manifest/disk consistency, error counts and timings;
  * booleans for detector mechanics (cooldown separation respected, RF scores
    within the unit interval, episode schema conformant).

It deliberately records **no alert count, no score, no rate, no burden value
and no precision-proxy count**, because the point of freezing this code before
the window closes is that the analysis cannot have been shaped by the results.
Value-bearing behaviour is verified instead against synthetic data with known
answers in `tests/`.

§7 requires every analysis run to record the protocol freeze commit; every
JSON artifact carries a ``provenance`` block and every CSV artifact carries
``protocol_freeze_commit``, ``analysis_commit`` and ``run_id`` columns.

Analysis setup (see README, "Analysis setup"; no absolute path is committed)
----------------------------------------------------------------------------
``ABA_UPSTREAM_DATASET`` — checkout of the upstream pump-and-dump dataset
    (pinned commit d71250d4…) providing the released labelled matrices the
    frozen detector is fitted on. Required for detector 1.
``ABA_REF016_ROOT``      — optional; the REF-2026-016 frozen re-implementation,
    read only to verify the frozen τ* constants against its archived
    artifacts.
The interpreter must provide numpy / pandas / scikit-learn at the versions
pinned in `requirements-analysis.txt` (scikit-learn 1.6.1, matching the
REF-2026-016 environment in which the detector was frozen).
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import platform
import subprocess
import sys
import time

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(REPO_ROOT, "src"))

# --------------------------------------------------------------------------
# Frozen constants
# --------------------------------------------------------------------------

PROTOCOL_FREEZE_COMMIT = "0ac2bbd026915b1ac09acf649638d28e637d0289"
PROTOCOL_FREEZE_TAG = "v1.0-protocol-freeze"
PROTOCOL_FILE = "protocol/locked_protocol_v1.0.md"

#: Protocol §4: first 12 complete UTC weeks from the first UTC midnight after
#: the freeze commit.
STREAM_START = dt.date(2026, 7, 24)
EVALUATION_WEEKS = 12
EVALUATION_DAYS = EVALUATION_WEEKS * 7

DATA_DIR = os.path.join(REPO_ROOT, "data")
UNIVERSE_DIR = os.path.join(DATA_DIR, "universe")
MANIFEST_CSV = os.path.join(DATA_DIR, "manifests", "pull_manifest.csv")
COVERAGE_CSV = os.path.join(DATA_DIR, "manifests", "coverage_log.csv")
DAYS_DONE_TXT = os.path.join(DATA_DIR, "manifests", "days_completed.txt")
ARTIFACTS_DIR = os.path.join(REPO_ROOT, "artifacts")

#: Deterministic structural sample (structural mode only; it selects which
#: pair-days are mechanically exercised and influences no reported quantity).
STRUCTURAL_SAMPLE_SEED = 20260722
STRUCTURAL_SAMPLE_PAIR_DAYS = 12

_ENV_STRUCTURAL_ONLY = "ABA_STRUCTURAL_ONLY"


# --------------------------------------------------------------------------
# Window and provenance
# --------------------------------------------------------------------------


def window_days() -> list:
    return [(STREAM_START + dt.timedelta(days=n)).isoformat()
            for n in range(EVALUATION_DAYS)]


def window_bounds() -> tuple:
    days = window_days()
    return days[0], days[-1]


def completed_days() -> set:
    if not os.path.exists(DAYS_DONE_TXT):
        return set()
    with open(DAYS_DONE_TXT) as handle:
        return set(handle.read().split())


def _git(*args) -> str:
    try:
        out = subprocess.run(["git", "-C", REPO_ROOT, *args],
                             capture_output=True, text=True, timeout=30)
    except (OSError, subprocess.SubprocessError):
        return ""
    return out.stdout.strip() if out.returncode == 0 else ""


def provenance(mode: str, run_id: str, extra: dict | None = None) -> dict:
    """The §7 provenance block stamped into every artifact."""

    resolved = _git("rev-list", "-n", "1", PROTOCOL_FREEZE_TAG)
    record = {
        "study": "REF-2026-017",
        "protocol": PROTOCOL_FILE,
        "protocol_freeze_commit": PROTOCOL_FREEZE_COMMIT,
        "protocol_freeze_tag": PROTOCOL_FREEZE_TAG,
        "protocol_freeze_commit_verified": (
            resolved == PROTOCOL_FREEZE_COMMIT if resolved else None),
        "amendments_in_force": [
            "amendment_1_universe_eligibility",
            "amendment_2_leveraged_suffix_defect",
            "amendment_3_partial_publication_coverage",
            "amendment_4_day_membership_mapping",
            "amendment_5_cross_exchange_gate (module permanently dropped)",
        ],
        "analysis_commit": _git("rev-parse", "HEAD"),
        "analysis_tree_dirty": bool(_git("status", "--porcelain")),
        "run_id": run_id,
        "run_started_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "mode": mode,
        "evaluation_window": {
            "start": window_bounds()[0],
            "end": window_bounds()[1],
            "weeks": EVALUATION_WEEKS,
            "days": EVALUATION_DAYS,
        },
        "python": platform.python_version(),
        "platform": platform.platform(),
    }
    if extra:
        record.update(extra)
    return record


def write_json(payload: dict, path: str) -> str:
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    with open(path, "w") as handle:
        json.dump(payload, handle, indent=2, sort_keys=False, default=str)
        handle.write("\n")
    return path


def write_csv(frame, path: str, prov: dict) -> str:
    """Write a CSV artifact with the §7 provenance columns attached."""

    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    stamped = frame.copy()
    stamped["protocol_freeze_commit"] = prov["protocol_freeze_commit"]
    stamped["analysis_commit"] = prov["analysis_commit"]
    stamped["run_id"] = prov["run_id"]
    stamped.to_csv(path, index=False)
    return path


def run_id_now() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")


# --------------------------------------------------------------------------
# Shared loading
# --------------------------------------------------------------------------


def load_inputs(pair_hours_rule: str):
    import burden as B

    tables = B.load_universe_tables(UNIVERSE_DIR)
    manifest = B.load_pull_manifest(MANIFEST_CSV)
    coverage = B.load_coverage_log(COVERAGE_CSV)
    return tables, manifest, coverage


def _sorted_pair_days(per_day: dict) -> list:
    return sorted((day, symbol) for day, symbols in per_day.items()
                  for symbol in symbols)


# --------------------------------------------------------------------------
# Structural check (partial window)
# --------------------------------------------------------------------------


def structural_check(args) -> int:
    os.environ[_ENV_STRUCTURAL_ONLY] = "1"
    include_a2 = not args.exclude_amendment2_pairs

    import numpy as np

    import apply_detectors as D
    import burden as B
    import features as F

    run_id = run_id_now()
    prov = provenance("partial-structural-check", run_id, {
        "endpoint_policy": (
            "NO ENDPOINT VALUES. This run computes no alerts/day, no alerts "
            "per 1000 pair-hours, no burden curve and no precision-proxy "
            "count. %s=1 makes every endpoint function in burden.py and "
            "precision_proxy.py raise." % _ENV_STRUCTURAL_ONLY),
        "pair_hours_rule": args.pair_hours_rule,
        "include_amendment2_pairs": include_a2,
    })

    days = window_days()
    done = completed_days()
    completed_in_window = [d for d in days if d in done]
    report = {
        "provenance": prov,
        "window": {
            "start": days[0],
            "end": days[-1],
            "days_in_window": len(days),
            "days_completed_in_window": len(completed_in_window),
            "days_outstanding": len(days) - len(completed_in_window),
            "window_complete": len(completed_in_window) == len(days),
            "first_completed_day": completed_in_window[0] if completed_in_window else "",
            "last_completed_day": completed_in_window[-1] if completed_in_window else "",
            "days_completed_outside_window": sorted(done - set(days)),
        },
    }

    tables, manifest, coverage = load_inputs(args.pair_hours_rule)
    report["inputs"] = {
        "universe_tables": sorted(tables),
        "universe_table_sizes": {d: len(rows) for d, rows in sorted(tables.items())},
        "universe_included_sizes": {
            d: sum(1 for r in rows if r[2]) for d, rows in sorted(tables.items())},
        "pull_manifest_rows_deduped": len(manifest),
        "coverage_rows_deduped": len(coverage),
        "excluded_coverage_dates": sorted(B.EXCLUDED_COVERAGE_DATES),
    }

    t0 = time.time()
    report["denominators"] = B.denominator_report(
        tables, completed_in_window, manifest, coverage, include_a2)
    report["denominators"]["elapsed_s"] = round(time.time() - t0, 3)

    # ---- manifest vs disk ------------------------------------------------
    per_day = B.monitored_pair_days(tables, completed_in_window, manifest,
                                    args.pair_hours_rule, include_a2)
    pair_days = _sorted_pair_days(per_day)
    missing_files = []
    for day, symbol in pair_days:
        for path in (F.agg_trades_path(DATA_DIR, symbol, day),
                     F.klines_path(DATA_DIR, symbol, day)):
            if not os.path.exists(path):
                missing_files.append(os.path.basename(path))
    report["archive_presence"] = {
        "rule": args.pair_hours_rule,
        "pair_days_checked": len(pair_days),
        "files_expected": 2 * len(pair_days),
        "files_missing_on_disk": len(missing_files),
        "missing_examples": sorted(missing_files)[:10],
    }

    # ---- deterministic structural sample ---------------------------------
    rng = np.random.default_rng(STRUCTURAL_SAMPLE_SEED)
    size = min(args.sample_pair_days, len(pair_days))
    picks = sorted(rng.choice(len(pair_days), size=size, replace=False).tolist()) \
        if size else []
    sample = [pair_days[i] for i in picks]
    report["structural_sample"] = {
        "seed": STRUCTURAL_SAMPLE_SEED,
        "requested": args.sample_pair_days,
        "drawn": len(sample),
        "pair_days": ["%s/%s" % (day, symbol) for day, symbol in sample],
        "note": ("the sample determines which pair-days are mechanically "
                 "exercised; it influences no reported quantity"),
    }

    frequencies = args.frequencies or list(F.FREQUENCIES)

    # ---- feature reconstruction ------------------------------------------
    feature_rows = []
    errors = []
    built = {}
    t0 = time.time()
    for day, symbol in sample:
        for frequency in frequencies:
            try:
                result = F.build_pair_day_features(
                    DATA_DIR, symbol, day, frequency,
                    warmup_days=args.warmup_days)
            except Exception as exc:  # structural: record, never abort
                errors.append({"stage": "features", "pair_day": "%s/%s" % (day, symbol),
                               "frequency": frequency, "error": repr(exc)})
                continue
            built[(day, symbol, frequency)] = result
            meta = dict(result.meta)
            frame = result.frame
            meta["schema_columns_ok"] = (
                list(frame.columns) == ["date", "symbol"] + list(F.FEATURES))
            meta["all_finite"] = bool(
                frame.empty or np.isfinite(
                    frame[F.FEATURES].to_numpy(dtype=float)).all())
            meta["dates_tz_naive"] = bool(
                frame.empty or frame["date"].dt.tz is None)
            meta["dates_within_target_day"] = bool(
                frame.empty or
                ((frame["date"] >= np.datetime64(day)) &
                 (frame["date"] < np.datetime64(day) +
                  np.timedelta64(1, "D"))).all())
            meta["dates_monotonic"] = bool(
                frame.empty or frame["date"].is_monotonic_increasing)
            feature_rows.append(meta)
    feature_elapsed = time.time() - t0

    report["feature_reconstruction"] = {
        "frequencies": frequencies,
        "warmup_days": args.warmup_days,
        "pair_days_attempted": len(sample) * len(frequencies),
        "pair_days_built": len(built),
        "total_feature_rows_scored": int(sum(m["n_rows_scored"] for m in feature_rows)),
        "total_rows_dropped_nonfinite": int(
            sum(m["n_rows_dropped_nonfinite"] for m in feature_rows)),
        "total_trades_read_target_days": int(
            sum(m["n_trades_target_day"] for m in feature_rows)),
        # Scorable coverage (features.py limit R5): a pair-day can be
        # monitored (archive verified) and still yield no scorable row,
        # because the upstream rolling window spans 900 non-empty chunks
        # (700 at 5S) and a small-capitalisation pair may not supply them.
        "pair_day_frequency_with_zero_scorable_rows": int(
            sum(1 for m in feature_rows if m["n_rows_scored"] == 0)),
        "warmup_sufficient_count": int(
            sum(1 for m in feature_rows if m["warmup_sufficient"])),
        "warmup_days_used_max": int(
            max((m["warmup_days_used"] for m in feature_rows), default=0)),
        "zero_row_builds_by_frequency": {
            frequency: int(sum(1 for m in feature_rows
                               if m["frequency"] == frequency
                               and m["n_rows_scored"] == 0))
            for frequency in frequencies},
        "builds_by_frequency": {
            frequency: int(sum(1 for m in feature_rows
                               if m["frequency"] == frequency))
            for frequency in frequencies},
        "schema_columns_ok_all": all(m["schema_columns_ok"] for m in feature_rows),
        "all_finite_all": all(m["all_finite"] for m in feature_rows),
        "dates_tz_naive_all": all(m["dates_tz_naive"] for m in feature_rows),
        "dates_within_target_day_all": all(
            m["dates_within_target_day"] for m in feature_rows),
        "dates_monotonic_all": all(m["dates_monotonic"] for m in feature_rows),
        "timestamp_units_seen": sorted({m["timestamp_unit_target"]
                                        for m in feature_rows if m["timestamp_unit_target"]}),
        "elapsed_s": round(feature_elapsed, 3),
        "per_pair_day": feature_rows,
    }

    # ---- kline coverage cross-check --------------------------------------
    kline_meta = []
    for day, symbol in sample:
        try:
            kline_meta.append(F.kline_coverage_meta(DATA_DIR, symbol, day))
        except Exception as exc:
            errors.append({"stage": "klines", "pair_day": "%s/%s" % (day, symbol),
                           "error": repr(exc)})
    report["kline_coverage"] = {
        "pair_days_checked": len(kline_meta),
        "all_present": all(m.get("kline_archive_present") for m in kline_meta),
        "all_1440_minutes": all(m.get("n_kline_rows") == F.MINUTES_PER_DAY
                                for m in kline_meta),
        "total_minutes_filled_by_grid": int(
            sum(m.get("n_minutes_filled", 0) for m in kline_meta)),
        "any_duplicate_open_times": any(m.get("duplicated_open_times")
                                        for m in kline_meta),
        "all_monotonic": all(m.get("monotonic_increasing") for m in kline_meta),
    }

    # ---- detector mechanics (episodes discarded) --------------------------
    sink = D.StructuralSink()
    detector_report = {
        "tau_anchor": D.TAU_ANCHOR,
        "tau_star_frozen": D.TAU_STAR_FROZEN,
        "tau_star_source": D.TAU_STAR_SOURCE,
        "tau_star_verification": D.verify_tau_star_against_ref016(),
        "cooldown_seconds": D.COOLDOWN_SECONDS,
        "rf_params": dict(D.RF_PARAMS),
    }
    rows_scored = 0
    models = {}
    t0 = time.time()
    if not args.skip_detectors:
        try:
            for frequency in frequencies:
                models[frequency] = D.fit_frozen_rf(frequency)
            detector_report["frozen_models"] = {
                f: m.meta for f, m in models.items()}
        except Exception as exc:
            errors.append({"stage": "fit_frozen_rf", "error": repr(exc)})
        cooldown_state = {}
        for (day, symbol, frequency), result in sorted(built.items()):
            model = models.get(frequency)
            if model is None:
                continue
            try:
                scores = D.score_frame(model, result.frame)
                rows_scored += int(len(scores))
                episodes = D.rf_episodes_for_pair_day(
                    result, scores, D.rf_thresholds(frequency), cooldown_state)
                sink.consume(episodes)
            except Exception as exc:
                errors.append({"stage": "score_rf",
                               "pair_day": "%s/%s" % (day, symbol),
                               "frequency": frequency, "error": repr(exc)})
        baseline_state = {}
        for day, symbol in sample:
            try:
                path = F.klines_path(DATA_DIR, symbol, day)
                if not os.path.exists(path):
                    continue
                grid = F.complete_minute_grid(F.read_klines_1m(path), day)
                episodes = D.baseline_episodes_for_pair_day(
                    symbol, day, grid, baseline_state)
                sink.consume(episodes)
            except Exception as exc:
                errors.append({"stage": "baselines",
                               "pair_day": "%s/%s" % (day, symbol),
                               "error": repr(exc)})
    detector_report["feature_rows_scored"] = rows_scored
    detector_report["elapsed_s"] = round(time.time() - t0, 3)
    detector_report["mechanics"] = sink.report()
    report["detectors"] = detector_report

    # ---- endpoint guard self-test ----------------------------------------
    guard = {}
    try:
        B.alerts_per_day(__import__("pandas").DataFrame(), completed_in_window)
        guard["burden_guard_active"] = False
    except B.EndpointsSuppressed:
        guard["burden_guard_active"] = True
    try:
        import precision_proxy as P
        P.manual_sample_status(None)
        guard["precision_proxy_guard_active"] = False
    except B.EndpointsSuppressed:
        guard["precision_proxy_guard_active"] = True
    report["endpoint_guard"] = guard

    report["errors"] = {"count": len(errors), "detail": errors}

    out_dir = os.path.join(ARTIFACTS_DIR, "structural_checks",
                           "structural_check_%s" % run_id)
    write_json(report, os.path.join(out_dir, "structural_check.json"))
    _write_structural_markdown(report, os.path.join(out_dir, "README.md"))

    print("== REF-2026-017 partial-window structural check ==")
    print("mode: STRUCTURAL ONLY — no endpoint value computed or recorded")
    print("protocol freeze commit: %s (verified: %s)"
          % (PROTOCOL_FREEZE_COMMIT, prov["protocol_freeze_commit_verified"]))
    w = report["window"]
    print("window %s..%s: %d/%d days complete (%d outstanding)"
          % (w["start"], w["end"], w["days_completed_in_window"],
             w["days_in_window"], w["days_outstanding"]))
    for rule, block in report["denominators"]["rules"].items():
        print("denominator[%s]: %d pair-days, %.0f pair-hours, %d distinct pairs"
              % (rule, block["total_pair_days"], block["total_pair_hours"],
                 block["distinct_pairs"]))
    print("archives: %d pair-days checked, %d files missing on disk"
          % (report["archive_presence"]["pair_days_checked"],
             report["archive_presence"]["files_missing_on_disk"]))
    fr = report["feature_reconstruction"]
    print("features: %d/%d pair-day x frequency built, %d rows scored, "
          "%d rows dropped non-finite, %.1fs"
          % (fr["pair_days_built"], fr["pair_days_attempted"],
             fr["total_feature_rows_scored"], fr["total_rows_dropped_nonfinite"],
             fr["elapsed_s"]))
    print("scorable coverage: %d/%d builds had a full rolling window, "
          "%d yielded zero rows %s"
          % (fr["warmup_sufficient_count"], fr["pair_days_built"],
             fr["pair_day_frequency_with_zero_scorable_rows"],
             fr["zero_row_builds_by_frequency"]))
    print("detectors: %d feature rows scored, mechanics %s, %.1fs"
          % (detector_report["feature_rows_scored"],
             detector_report["mechanics"], detector_report["elapsed_s"]))
    print("endpoint guard: %s" % guard)
    print("errors: %d" % len(errors))
    print("written: %s" % os.path.relpath(out_dir, REPO_ROOT))
    return 1 if errors else 0


def _write_structural_markdown(report: dict, path: str) -> None:
    w = report["window"]
    fr = report["feature_reconstruction"]
    lines = [
        "# Partial-window structural check (REF-2026-017)",
        "",
        "Mode: **structural only**. This run computed **no endpoint value** —",
        "no alerts/day, no alerts per 1 000 pair-hours, no burden curve, no",
        "precision-proxy count. `ABA_STRUCTURAL_ONLY=1` makes every endpoint",
        "function in `burden.py` and `precision_proxy.py` raise; the guard",
        "self-test in `structural_check.json` records that it fired.",
        "",
        "Purpose: the analysis code is frozen *before* the evaluation window",
        "closes, so it cannot have been shaped by the results. This check",
        "verifies only that the pipeline runs end to end and that counts,",
        "denominators, schemas and timestamps are right.",
        "",
        "| fact | value |",
        "|---|---|",
        "| protocol freeze commit | `%s` |" % report["provenance"]["protocol_freeze_commit"],
        "| evaluation window | %s .. %s (%d days) |" % (w["start"], w["end"], w["days_in_window"]),
        "| days complete / outstanding | %d / %d |" % (w["days_completed_in_window"], w["days_outstanding"]),
        "| pair-days checked on disk | %d |" % report["archive_presence"]["pair_days_checked"],
        "| files missing on disk | %d |" % report["archive_presence"]["files_missing_on_disk"],
        "| pair-day x frequency reconstructions | %d / %d |" % (fr["pair_days_built"], fr["pair_days_attempted"]),
        "| feature rows produced | %d |" % fr["total_feature_rows_scored"],
        "| rows dropped non-finite | %d |" % fr["total_rows_dropped_nonfinite"],
        "| builds with a full rolling window | %d |" % fr["warmup_sufficient_count"],
        "| builds yielding zero scorable rows | %d |"
        % fr["pair_day_frequency_with_zero_scorable_rows"],
        "| errors | %d |" % report["errors"]["count"],
        "",
    ]
    for rule, block in report["denominators"]["rules"].items():
        lines.append("- denominator `%s`: %d pair-days, %.0f pair-hours, %d distinct pairs"
                     % (rule, block["total_pair_days"], block["total_pair_hours"],
                        block["distinct_pairs"]))
    lines.append("")
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    with open(path, "w") as handle:
        handle.write("\n".join(lines))


# --------------------------------------------------------------------------
# Full analysis (complete window only)
# --------------------------------------------------------------------------


def full_analysis(args) -> int:
    os.environ.pop(_ENV_STRUCTURAL_ONLY, None)
    include_a2 = not args.exclude_amendment2_pairs

    import numpy as np
    import pandas as pd

    import apply_detectors as D
    import burden as B
    import features as F
    import precision_proxy as P

    days = window_days()
    done = completed_days()
    outstanding = [d for d in days if d not in done]
    if outstanding:
        sys.stderr.write(
            "refusing to run: the primary evaluation window is incomplete.\n"
            "  window: %s .. %s (%d UTC days, protocol §4)\n"
            "  outstanding days: %d (first: %s)\n"
            "Run with --partial-structural-check for a structural-only pass; "
            "its output carries no endpoint value.\n"
            % (days[0], days[-1], len(days), len(outstanding), outstanding[0]))
        return 2

    run_id = run_id_now()
    prov = provenance("full", run_id, {
        "pair_hours_rule": args.pair_hours_rule,
        "include_amendment2_pairs": include_a2,
    })
    out_dir = os.path.join(ARTIFACTS_DIR, "analysis_v1_%s" % run_id)
    os.makedirs(out_dir, exist_ok=False)

    tables, manifest, coverage = load_inputs(args.pair_hours_rule)
    per_day = B.monitored_pair_days(tables, days, manifest,
                                    args.pair_hours_rule, include_a2)
    pair_days = _sorted_pair_days(per_day)
    frequencies = args.frequencies or list(F.FREQUENCIES)

    # ---- stage 1: features, scores, episodes, candidates ------------------
    episodes = []
    candidates = []
    build_log = []
    cooldown_state = {}
    baseline_state = {}
    models = {f: D.fit_frozen_rf(f) for f in frequencies}

    for day, symbol in pair_days:
        for frequency in frequencies:
            result = F.build_pair_day_features(
                DATA_DIR, symbol, day, frequency, warmup_days=args.warmup_days)
            build_log.append(result.meta)
            scores = D.score_frame(models[frequency], result.frame)
            episodes.extend(D.rf_episodes_for_pair_day(
                result, scores, D.rf_thresholds(frequency), cooldown_state))
            candidates.extend(D.rf_candidate_rows(result, scores))
        kline_file = F.klines_path(DATA_DIR, symbol, day)
        if os.path.exists(kline_file):
            grid = F.complete_minute_grid(F.read_klines_1m(kline_file), day)
            episodes.extend(D.baseline_episodes_for_pair_day(
                symbol, day, grid, baseline_state))
            candidates.extend(D.baseline_candidate_rows(symbol, day, grid))

    D.write_episodes(episodes, os.path.join(out_dir, "alert_episodes.csv"))
    D.write_candidates(candidates, os.path.join(out_dir, "alert_candidates.csv"))
    pd.DataFrame(build_log).to_csv(
        os.path.join(out_dir, "feature_build_log.csv"), index=False)

    episode_frame = B.load_episodes(os.path.join(out_dir, "alert_episodes.csv"))
    episode_frame = B.restrict_to_monitored(episode_frame, per_day)
    candidate_frame = pd.read_csv(os.path.join(out_dir, "alert_candidates.csv"))
    if not candidate_frame.empty:
        candidate_frame["ts_utc"] = pd.to_datetime(candidate_frame["ts_utc"])
        candidate_frame["frequency"] = candidate_frame["frequency"].fillna("")

    # ---- stage 2: primary endpoint ---------------------------------------
    hours_by_day = B.pair_hours(tables, days, manifest, args.pair_hours_rule,
                                include_a2)
    hours_by_pair = B.pair_hours_by_pair(tables, days, manifest,
                                         args.pair_hours_rule, include_a2)
    total_hours = float(sum(hours_by_day.values()))

    per_day_counts = B.alerts_per_day(episode_frame, days)
    write_csv(per_day_counts, os.path.join(out_dir, "alerts_per_day.csv"), prov)
    write_csv(B.alerts_per_day_distribution(per_day_counts),
              os.path.join(out_dir, "alerts_per_day_distribution.csv"), prov)
    write_csv(B.alerts_per_1000_pair_hours(episode_frame, total_hours),
              os.path.join(out_dir, "alerts_per_1000_pair_hours.csv"), prov)
    write_csv(B.bootstrap_rate_by_pair(episode_frame, hours_by_pair),
              os.path.join(out_dir, "alerts_per_1000_pair_hours_bootstrap.csv"), prov)
    write_csv(B.stratified_burden(episode_frame, tables, days, manifest,
                                  args.pair_hours_rule, include_a2),
              os.path.join(out_dir, "burden_by_activity_stratum.csv"), prov)
    write_csv(B.detector_agreement(episode_frame),
              os.path.join(out_dir, "detector_agreement.csv"), prov)

    # Sensitivity: the alternative pair-hours rule (documented switch).
    alt_rule = ("universe-membership"
                if args.pair_hours_rule == "verified-archive"
                else "verified-archive")
    alt_hours = float(sum(B.pair_hours(tables, days, manifest, alt_rule,
                                       include_a2).values()))
    alt = B.alerts_per_1000_pair_hours(
        B.restrict_to_monitored(
            B.load_episodes(os.path.join(out_dir, "alert_episodes.csv")),
            B.monitored_pair_days(tables, days, manifest, alt_rule,
                                  include_a2)),
        alt_hours)
    alt["pair_hours_rule"] = alt_rule
    write_csv(alt, os.path.join(out_dir,
                                "alerts_per_1000_pair_hours_sensitivity_rule.csv"), prov)

    # ---- stage 3: burden curves ------------------------------------------
    curves = []
    rf_grid = sorted(set(np.round(np.arange(0.05, 1.0, 0.05), 4).tolist()) |
                     {D.TAU_ANCHOR} |
                     set(D.TAU_STAR_FROZEN.values()))
    for frequency in frequencies:
        grid = [t for t in rf_grid if t >= D.CURVE_FLOORS[D.DETECTOR_RF]]
        curves.append(B.burden_curve(candidate_frame, D.DETECTOR_RF, grid,
                                     frequency=frequency))
    curves.append(B.burden_curve(candidate_frame, D.DETECTOR_ZSCORE,
                                 np.round(np.arange(2.0, 10.5, 0.5), 3).tolist()))
    curves.append(B.burden_curve(candidate_frame, D.DETECTOR_PRICE_JUMP,
                                 np.round(np.arange(0.01, 0.155, 0.005), 4).tolist()))
    curve_frame = pd.concat([c for c in curves if not c.empty], ignore_index=True) \
        if any(not c.empty for c in curves) else pd.DataFrame(
            columns=["detector", "frequency", "threshold", "alerts"])
    curve_frame["pair_hours"] = total_hours
    curve_frame["alerts_per_1000_pair_hours"] = (
        B.PAIR_HOURS_SCALE * curve_frame["alerts"] / total_hours)
    write_csv(curve_frame, os.path.join(out_dir, "burden_curves.csv"), prov)

    # ---- stage 4: secondary endpoint -------------------------------------
    grids = {}
    for pair in sorted(episode_frame["pair"].unique()) if not episode_frame.empty else []:
        frames = []
        for day in days:
            path = F.klines_path(DATA_DIR, pair, day)
            if os.path.exists(path):
                frames.append(F.complete_minute_grid(F.read_klines_1m(path), day))
        if frames:
            grids[pair] = pd.concat(frames).sort_index()
    evaluations = P.evaluate_episodes(episode_frame, grids)
    write_csv(evaluations, os.path.join(out_dir, "precision_proxy_evaluations.csv"), prov)
    write_csv(P.summarise(evaluations),
              os.path.join(out_dir, "precision_proxy_summary.csv"), prov)

    sample = P.draw_manual_sample(episode_frame)
    P.write_manual_sample_template(
        sample, os.path.join(out_dir, "manual_sample_template.csv"))
    manual_status = P.manual_sample_status(args.manual_sample)

    # ---- stage 5: run summary --------------------------------------------
    summary = {
        "provenance": prov,
        "conflict_of_interest": (
            "protocol §8 disclosure applies to this output; see "
            "protocol/locked_protocol_v1.0.md §8."),
        "terminology": (
            "protocol §2.3: all precision language is benchmark-rule-relative; "
            "the prohibited terms must not be applied to these outputs."),
        "cross_exchange_module": "permanently dropped (Amendment 5; 0/30 clearance)",
        "pair_hours_rule": args.pair_hours_rule,
        "pair_hours_rule_alternative_reported": alt_rule,
        "include_amendment2_pairs": include_a2,
        "amendment2_pairs": list(B.AMENDMENT2_REINSTATED_PAIRS),
        "monitored_pair_days": len(pair_days),
        "monitored_pair_hours": total_hours,
        "frequencies": frequencies,
        "thresholds": {"tau_anchor": D.TAU_ANCHOR,
                       "tau_star_frozen": D.TAU_STAR_FROZEN,
                       "tau_star_source": D.TAU_STAR_SOURCE},
        "bootstrap": {"n_boot": B.BOOTSTRAP_N, "seed": B.BOOTSTRAP_SEED,
                      "cluster": "pair"},
        "frozen_models": {f: m.meta for f, m in models.items()},
        "manual_sample": manual_status,
        "feature_reconstruction_limits": (
            "see the module docstring of src/features.py, limits R1-R7"),
    }
    write_json(summary, os.path.join(out_dir, "run_summary.json"))
    print("analysis complete: %s" % os.path.relpath(out_dir, REPO_ROOT))
    return 0


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description=("Analysis driver for REF-2026-017 (protocol "
                     "%s, freeze %s)." % (PROTOCOL_FILE, PROTOCOL_FREEZE_COMMIT)))
    parser.add_argument(
        "--partial-structural-check", action="store_true",
        help=("run a structural-only pass over the partially collected "
              "stream: counts, denominators, schema/timezone conformance, "
              "timings and error counts only. Computes and records NO "
              "endpoint value."))
    parser.add_argument(
        "--pair-hours-rule", default="verified-archive",
        choices=["verified-archive", "universe-membership"],
        help=("denominator rule for monitored pair-hours. Default "
              "'verified-archive': only pair-days whose aggTrades and 1m "
              "kline archives both verified against the official checksum "
              "contribute. 'universe-membership' is the documented "
              "sensitivity alternative."))
    parser.add_argument("--frequencies", nargs="*", default=None,
                        help="subset of 25S/15S/5S (default: all three)")
    parser.add_argument("--warmup-days", default="auto",
                        help=("preceding pair-days prepended to warm the "
                              "rolling window (features.py limit R5). "
                              "'auto' (default) extends backwards until the "
                              "upstream rolling window is full, capped at 14 "
                              "days; an integer fixes the warm-up length."))
    parser.add_argument("--sample-pair-days", type=int,
                        default=STRUCTURAL_SAMPLE_PAIR_DAYS,
                        help=("structural check only: how many pair-days to "
                              "exercise mechanically"))
    parser.add_argument("--skip-detectors", action="store_true",
                        help=("structural check only: skip fitting/scoring the "
                              "frozen models (feature and denominator checks "
                              "only)"))
    parser.add_argument("--exclude-amendment2-pairs", action="store_true",
                        help=("sensitivity variant required by Amendment 2 §4: "
                              "run without JUPUSDT and SYRUPUSDT, which the "
                              "leveraged-suffix defect wrongly removed from "
                              "the archived membership tables. The default "
                              "includes them, as the amendment binds."))
    parser.add_argument("--manual-sample", default=None,
                        help=("path to the completed manual n=100 verification "
                              "file (protocol §2.2). Absent means 'not "
                              "performed'; verdicts are never imputed."))
    args = parser.parse_args(argv)

    import features as F

    if args.frequencies:
        unknown = [f for f in args.frequencies if f not in F.UPSTREAM_ROLLING]
        if unknown:
            parser.error("unknown frequency/frequencies: %s" % unknown)
    if args.warmup_days != F.WARMUP_AUTO:
        try:
            args.warmup_days = int(args.warmup_days)
        except ValueError:
            parser.error("--warmup-days must be 'auto' or an integer")

    if args.partial_structural_check:
        return structural_check(args)
    return full_analysis(args)


if __name__ == "__main__":
    sys.exit(main())
