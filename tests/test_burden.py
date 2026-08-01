"""Burden-endpoint tests (REF-2026-017), on synthetic data with known answers.

Covers protocol §2.1, §7 and Amendments 3/4:

  * the monitored pair-hours denominator under the default verified-archive
    rule and under the documented universe-membership sensitivity switch,
    including a pair-day whose archive is missing, one whose archive is only
    half verified, and a surplus pair outside the governed universe;
  * the day-to-membership mapping (a table dated D never governs day D);
  * the append-only coverage/manifest dedup rule (final status wins) and the
    exclusion of the 2026-07-20 validation pull;
  * the event-cluster bootstrap: whole pairs are resampled with all of their
    episodes and all of their pair-hours, never rows, with the frozen
    n = 2000 / seed = 20260722;
  * the endpoint guard that makes a partial-window run unable to compute a
    burden value.
"""

import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))

import numpy as np
import pandas as pd

import burden as B


def universe_rows(included, excluded=()):
    """Build (symbol, rank, included) rows like an archived membership table."""

    rows = [(symbol, rank, False) for rank, symbol in enumerate(excluded, 1)]
    start = len(excluded) + 1
    rows += [(symbol, rank, True)
             for rank, symbol in enumerate(included, start)]
    return rows


def manifest_row(day, symbol, kind, ok="1"):
    name = ("%s-aggTrades-%s.zip" % (symbol, day) if kind == "agg"
            else "%s-1m-%s.zip" % (symbol, day))
    return {"date": day, "symbol": symbol, "file": name, "bytes": "10",
            "sha256": "aa", "source_checksum_ok": ok}


class UniverseMappingTest(unittest.TestCase):
    def test_table_dated_d_never_governs_day_d(self):
        tables = {"2026-07-23": universe_rows(["AAAUSDT"]),
                  "2026-07-31": universe_rows(["AAAUSDT", "BBBUSDT"])}
        self.assertEqual(B.universe_for_day(tables, "2026-07-24"), "2026-07-23")
        self.assertEqual(B.universe_for_day(tables, "2026-07-31"), "2026-07-23")
        self.assertEqual(B.universe_for_day(tables, "2026-08-01"), "2026-07-31")
        self.assertIsNone(B.universe_for_day(tables, "2026-07-23"))
        self.assertEqual(
            B.monitored_symbols(tables, "2026-08-01",
                                include_amendment2_pairs=False),
            ["AAAUSDT", "BBBUSDT"])

    def test_activity_strata_are_mechanical_tertiles_of_the_table_rank(self):
        tables = {"2026-07-23": universe_rows(
            ["A", "B", "C", "D", "E", "F"], excluded=["TOP"])}
        strata = B.activity_strata(tables, "2026-07-24")
        self.assertEqual([strata[s] for s in ["A", "B", "C", "D", "E", "F"]],
                         ["high", "high", "mid", "mid", "low", "low"])
        self.assertNotIn("TOP", strata)


class PairHoursDenominatorTest(unittest.TestCase):
    """The default rule credits only verified pair-days; a missing archive
    reduces the denominator and the sensitivity rule does not."""

    def setUp(self):
        # Table dates after the Amendment 2 fix, so the reinstatement rule
        # (tested separately) does not interact with these assertions.
        self.tables = {"2026-09-01": universe_rows(["AAAUSDT", "BBBUSDT"],
                                                   excluded=["BTCUSDT"])}
        self.days = ["2026-09-02", "2026-09-03"]
        self.manifest = [
            # Day 24: both pairs fully verified.
            manifest_row("2026-09-02", "AAAUSDT", "agg"),
            manifest_row("2026-09-02", "AAAUSDT", "k1m"),
            manifest_row("2026-09-02", "BBBUSDT", "agg"),
            manifest_row("2026-09-02", "BBBUSDT", "k1m"),
            # Day 25: BBBUSDT's aggTrades archive never arrived (missing), and
            # AAAUSDT's kline archive is present but failed its checksum.
            manifest_row("2026-09-03", "AAAUSDT", "agg"),
            manifest_row("2026-09-03", "AAAUSDT", "k1m", ok="0"),
            manifest_row("2026-09-03", "BBBUSDT", "k1m"),
            # Surplus row for a pair outside the governed universe.
            manifest_row("2026-09-03", "ZZZUSDT", "agg"),
            manifest_row("2026-09-03", "ZZZUSDT", "k1m"),
        ]

    def test_verified_archive_rule_is_the_default(self):
        self.assertEqual(B.DEFAULT_PAIR_HOURS_RULE, "verified-archive")

    def test_verified_archive_rule(self):
        per_day = B.monitored_pair_days(self.tables, self.days, self.manifest,
                                        "verified-archive")
        self.assertEqual(per_day["2026-09-02"], ["AAAUSDT", "BBBUSDT"])
        # Day 2: AAAUSDT half-verified, BBBUSDT missing an archive, ZZZUSDT
        # outside the governed universe -> nothing is monitored.
        self.assertEqual(per_day["2026-09-03"], [])
        hours = B.pair_hours(self.tables, self.days, self.manifest,
                             "verified-archive")
        self.assertEqual(hours, {"2026-09-02": 48.0, "2026-09-03": 0.0})
        self.assertEqual(
            B.pair_hours_by_pair(self.tables, self.days, self.manifest,
                                 "verified-archive"),
            {"AAAUSDT": 24.0, "BBBUSDT": 24.0})

    def test_universe_membership_sensitivity_rule(self):
        hours = B.pair_hours(self.tables, self.days, self.manifest,
                             "universe-membership")
        # Every governed pair-day counts, whatever the archive state; the
        # surplus out-of-universe pair still never counts.
        self.assertEqual(hours, {"2026-09-02": 48.0, "2026-09-03": 48.0})
        per_day = B.monitored_pair_days(self.tables, self.days, self.manifest,
                                        "universe-membership")
        self.assertNotIn("ZZZUSDT", per_day["2026-09-03"])

    def test_unknown_rule_raises(self):
        with self.assertRaises(ValueError):
            B.pair_hours(self.tables, self.days, self.manifest, "whatever")

    def test_denominator_report_is_structural_only(self):
        report = B.denominator_report(self.tables, self.days, self.manifest)
        self.assertEqual(report["rules"]["verified-archive"]["total_pair_days"], 2)
        self.assertEqual(report["rules"]["universe-membership"]["total_pair_days"], 4)
        self.assertEqual(report["pair_days_excluded_by_default_rule"],
                         {"2026-09-02": 0, "2026-09-03": 2})
        text = repr(report)
        for forbidden in ("alerts_per", "alerts/day", "precision"):
            self.assertNotIn(forbidden, text)


class LogDedupTest(unittest.TestCase):
    """Append-only logs: the final status per (date, file) wins (Amendment 3)."""

    def setUp(self):
        self.tmp = tempfile.mkdtemp()

    def _write(self, name, header, rows):
        path = os.path.join(self.tmp, name)
        with open(path, "w") as handle:
            handle.write(header + "\n")
            for row in rows:
                handle.write(row + "\n")
        return path

    def test_coverage_dedup_and_validation_pull_exclusion(self):
        path = self._write(
            "coverage_log.csv", "date,symbol,file,status,bytes",
            ["2026-07-20,AAAUSDT,AAAUSDT-1m-2026-07-20.zip,ok,10",
             "2026-07-27,AAAUSDT,AAAUSDT-1m-2026-07-27.zip,404,0",
             "2026-07-27,AAAUSDT,AAAUSDT-1m-2026-07-27.zip,ok,99"])
        rows = B.load_coverage_log(path)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["status"], "ok")
        self.assertEqual(rows[0]["bytes"], "99")

    def test_manifest_dedup(self):
        path = self._write(
            "pull_manifest.csv",
            "date,symbol,file,bytes,sha256,source_checksum_ok",
            ["2026-07-27,AAAUSDT,AAAUSDT-1m-2026-07-27.zip,10,aa,0",
             "2026-07-27,AAAUSDT,AAAUSDT-1m-2026-07-27.zip,10,bb,1"])
        rows = B.load_pull_manifest(path)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["source_checksum_ok"], "1")


class ClusterBootstrapTest(unittest.TestCase):
    """Whole pairs are resampled, never rows or individual episodes."""

    def test_frozen_parameters(self):
        self.assertEqual(B.BOOTSTRAP_N, 2000)
        self.assertEqual(B.BOOTSTRAP_SEED, 20260722)

    def test_resamples_whole_clusters(self):
        # Two pairs, 10 episodes on one and none on the other, 24 h each. With
        # cluster resampling the only attainable ratios are 0, 10/48 and 20/48
        # per 1000 h. Row-level resampling would produce many other values.
        low, high = B.cluster_bootstrap_ratio([10, 0], [24.0, 24.0])
        attainable = {0.0, 1000 * 10 / 48.0, 1000 * 20 / 48.0}
        for value in (low, high):
            self.assertTrue(
                any(abs(value - a) < 1e-9 for a in attainable),
                "bootstrap produced %r, which no whole-cluster resample can "
                "attain (attainable: %s)" % (value, sorted(attainable)))
        self.assertLess(low, high)

    def test_is_deterministic_under_the_frozen_seed(self):
        first = B.cluster_bootstrap_ratio([3, 1, 0, 7], [24.0, 48.0, 24.0, 72.0])
        second = B.cluster_bootstrap_ratio([3, 1, 0, 7], [24.0, 48.0, 24.0, 72.0])
        self.assertEqual(first, second)
        other = B.cluster_bootstrap_ratio([3, 1, 0, 7], [24.0, 48.0, 24.0, 72.0],
                                          seed=B.BOOTSTRAP_SEED + 1)
        self.assertNotEqual(first, other)

    def test_mean_interval_brackets_the_point_estimate(self):
        values = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
        low, high = B.cluster_bootstrap_interval(values)
        self.assertLess(low, float(np.mean(values)))
        self.assertGreater(high, float(np.mean(values)))

    def test_empty_input_raises(self):
        with self.assertRaises(ValueError):
            B.cluster_bootstrap_interval([])
        with self.assertRaises(ValueError):
            B.cluster_bootstrap_ratio([], [])


class EndpointGuardTest(unittest.TestCase):
    """A partial-window run cannot compute a burden value."""

    def setUp(self):
        self.previous = os.environ.get("ABA_STRUCTURAL_ONLY")
        os.environ["ABA_STRUCTURAL_ONLY"] = "1"

    def tearDown(self):
        if self.previous is None:
            os.environ.pop("ABA_STRUCTURAL_ONLY", None)
        else:
            os.environ["ABA_STRUCTURAL_ONLY"] = self.previous

    def test_endpoint_functions_raise(self):
        empty = pd.DataFrame(columns=["pair", "ts_utc", "detector", "frequency",
                                      "threshold_label", "day"])
        for call in (
            lambda: B.alerts_per_day(empty, ["2026-07-24"]),
            lambda: B.alerts_per_1000_pair_hours(empty, 24.0),
            lambda: B.burden_curve(empty, "upstream_rf", [0.5]),
            lambda: B.detector_agreement(empty),
            lambda: B.bootstrap_rate_by_pair(empty, {"AAAUSDT": 24.0}),
        ):
            with self.assertRaises(B.EndpointsSuppressed):
                call()

    def test_denominators_remain_available(self):
        tables = {"2026-07-23": universe_rows(["AAAUSDT"])}
        manifest = [manifest_row("2026-07-24", "AAAUSDT", "agg"),
                    manifest_row("2026-07-24", "AAAUSDT", "k1m")]
        hours = B.pair_hours(tables, ["2026-07-24"], manifest)
        self.assertEqual(hours, {"2026-07-24": 24.0})


class Amendment2ReinstatementTest(unittest.TestCase):
    """JUPUSDT/SYRUPUSDT are reinstated for days governed by a defective table."""

    def setUp(self):
        self.tables = {"2026-07-23": universe_rows(["AAAUSDT", "BBBUSDT",
                                                    "CCCUSDT"]),
                       "2026-09-08": universe_rows(["AAAUSDT", "JUPUSDT"])}

    def test_reinstated_under_a_defective_table(self):
        got = B.monitored_symbols(self.tables, "2026-07-24")
        self.assertEqual(sorted(got), ["AAAUSDT", "BBBUSDT", "CCCUSDT",
                                       "JUPUSDT", "SYRUPUSDT"])

    def test_sensitivity_variant_excludes_them(self):
        got = B.monitored_symbols(self.tables, "2026-07-24",
                                  include_amendment2_pairs=False)
        self.assertEqual(got, ["AAAUSDT", "BBBUSDT", "CCCUSDT"])

    def test_post_fix_table_needs_no_reinstatement(self):
        # A table produced after the fix carries JUPUSDT itself; the rule adds
        # nothing and never duplicates it.
        got = B.monitored_symbols(self.tables, "2026-09-09")
        self.assertEqual(got, ["AAAUSDT", "JUPUSDT"])

    def test_reinstated_pairs_form_their_own_activity_bucket(self):
        strata = B.activity_strata(self.tables, "2026-07-24")
        self.assertEqual(strata["JUPUSDT"], "unranked_amendment2")
        self.assertEqual(strata["SYRUPUSDT"], "unranked_amendment2")
        self.assertEqual(strata["AAAUSDT"], "high")
        self.assertIn("unranked_amendment2", B.ACTIVITY_STRATA)

    def test_denominator_reports_the_choice(self):
        manifest = [manifest_row("2026-07-24", s, k)
                    for s in ("AAAUSDT", "BBBUSDT", "CCCUSDT", "JUPUSDT",
                              "SYRUPUSDT")
                    for k in ("agg", "k1m")]
        report = B.denominator_report(self.tables, ["2026-07-24"], manifest)
        self.assertTrue(report["include_amendment2_pairs"])
        self.assertEqual(report["rules"]["verified-archive"]["total_pair_days"], 5)
        without = B.denominator_report(self.tables, ["2026-07-24"], manifest,
                                       include_amendment2_pairs=False)
        self.assertEqual(without["rules"]["verified-archive"]["total_pair_days"], 3)


class EpisodeRestrictionTest(unittest.TestCase):
    def test_surplus_pair_days_are_dropped(self):
        episodes = pd.DataFrame([
            {"pair": "AAAUSDT", "day": "2026-07-24", "detector": "price_jump",
             "frequency": "", "threshold_label": "fixed",
             "ts_utc": pd.Timestamp("2026-07-24 00:00:00")},
            {"pair": "ZZZUSDT", "day": "2026-07-24", "detector": "price_jump",
             "frequency": "", "threshold_label": "fixed",
             "ts_utc": pd.Timestamp("2026-07-24 00:00:00")},
        ])
        kept = B.restrict_to_monitored(episodes, {"2026-07-24": ["AAAUSDT"]})
        self.assertEqual(list(kept["pair"]), ["AAAUSDT"])


if __name__ == "__main__":
    unittest.main()
