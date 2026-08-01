"""Precision-proxy tests (REF-2026-017), on synthetic data with known answers.

Covers protocol §2.2: the pre-frozen pump-signature rule and each of its
three sensitivity variants, with cases constructed so that exactly one
condition separates them:

  * a clean pump signature matches the primary rule;
  * a 6× volume multiple matches the 5× variant but not the primary 10×;
  * a 30 % gain matches the primary but not the 35 % strict variant;
  * a peak at +8 minutes is missed by every 5-minute rule and caught only by
    the 10-minute-window variant;
  * a gain that never retraces matches no rule;
  * alerts whose trailing 24 h or forward windows are not covered by archived
    klines are reported as *not evaluable* rather than as non-matches;
  * the manual n = 100 sample is never fabricated.
"""

import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))

import pandas as pd

import precision_proxy as P


TRAILING = P.TRAILING_MEDIAN_MINUTES
BASE_PRICE = 1.0
BASE_VOLUME = 100.0


def build_grid(n_after=200):
    total = TRAILING + 1 + n_after
    index = pd.date_range("2026-07-24", periods=total, freq="min")
    frame = pd.DataFrame(
        {"open": BASE_PRICE, "high": BASE_PRICE, "low": BASE_PRICE,
         "close": BASE_PRICE, "volume": BASE_VOLUME,
         "quote_volume": BASE_VOLUME, "trades": 1.0},
        index=index)
    frame.index.name = "open_time"
    return frame


def stage_pump(grid, peak_offset=3, peak=1.30, window_volume=1500.0,
               trough=1.10, trough_offset=20, hold_minutes=120):
    """Write a pump signature into a grid at the alert minute.

    The price rises to ``peak`` at ``peak_offset`` minutes after the alert and
    then *holds* there, so a retracement only exists where one is written
    explicitly (``trough`` at ``trough_offset`` minutes after the peak).
    """

    alert = TRAILING
    peak_position = alert + peak_offset
    volume_column = grid.columns.get_loc("quote_volume")
    for position in range(alert + 1, peak_position + 1):
        grid.iloc[position, volume_column] = window_volume
    hold_end = min(len(grid) - 1, peak_position + hold_minutes)
    for column in ("open", "high", "low", "close"):
        index = grid.columns.get_loc(column)
        for position in range(peak_position, hold_end + 1):
            grid.iloc[position, index] = peak
    if trough is not None:
        grid.iloc[peak_position + trough_offset,
                  grid.columns.get_loc("low")] = trough
    return grid.index[alert]


class FrozenRuleTest(unittest.TestCase):
    def test_the_four_frozen_rules(self):
        self.assertEqual(len(P.ALL_RULES), 4)
        self.assertIs(P.ALL_RULES[0], P.PRIMARY_RULE)
        self.assertEqual(P.PRIMARY_RULE.role, "primary")
        self.assertEqual(
            (P.PRIMARY_RULE.gain_threshold, P.PRIMARY_RULE.volume_multiple,
             P.PRIMARY_RULE.retracement_fraction,
             P.PRIMARY_RULE.gain_window_minutes,
             P.PRIMARY_RULE.retracement_window_minutes),
            (0.25, 10.0, 0.50, 5, 60))
        self.assertEqual(
            (P.VARIANT_LOOSE.gain_threshold, P.VARIANT_LOOSE.volume_multiple,
             P.VARIANT_LOOSE.retracement_fraction),
            (0.15, 5.0, 0.40))
        self.assertEqual(
            (P.VARIANT_STRICT.gain_threshold, P.VARIANT_STRICT.volume_multiple,
             P.VARIANT_STRICT.retracement_fraction),
            (0.35, 20.0, 0.60))
        self.assertEqual(
            (P.VARIANT_WINDOW10.gain_threshold,
             P.VARIANT_WINDOW10.volume_multiple,
             P.VARIANT_WINDOW10.retracement_fraction,
             P.VARIANT_WINDOW10.gain_window_minutes),
            (0.25, 10.0, 0.50, 10))
        for rule in P.ALL_RULES[1:]:
            self.assertEqual(rule.role, "sensitivity")


class PrimaryRuleTest(unittest.TestCase):
    def test_clean_signature_matches_primary_and_loose_but_not_strict(self):
        grid = build_grid()
        stamp = stage_pump(grid)  # +30 %, 15x volume, retrace to 1.10
        primary = P.evaluate_alert(grid, stamp, P.PRIMARY_RULE)
        self.assertTrue(primary["evaluable"])
        self.assertTrue(primary["gain_condition"])
        self.assertTrue(primary["volume_condition"])
        self.assertTrue(primary["retracement_condition"])
        self.assertTrue(primary["matches_signature"])
        self.assertTrue(
            P.evaluate_alert(grid, stamp, P.VARIANT_LOOSE)["matches_signature"])
        strict = P.evaluate_alert(grid, stamp, P.VARIANT_STRICT)
        self.assertFalse(strict["gain_condition"])   # 30 % < 35 %
        self.assertFalse(strict["volume_condition"])  # 15x < 20x
        self.assertFalse(strict["matches_signature"])
        self.assertTrue(
            P.evaluate_alert(grid, stamp, P.VARIANT_WINDOW10)["matches_signature"])

    def test_volume_multiple_separates_primary_from_the_loose_variant(self):
        grid = build_grid()
        stamp = stage_pump(grid, window_volume=600.0)  # 6x the trailing median
        primary = P.evaluate_alert(grid, stamp, P.PRIMARY_RULE)
        self.assertTrue(primary["gain_condition"])
        self.assertFalse(primary["volume_condition"])   # 6x < 10x
        self.assertFalse(primary["matches_signature"])
        loose = P.evaluate_alert(grid, stamp, P.VARIANT_LOOSE)
        self.assertTrue(loose["volume_condition"])      # 6x > 5x
        self.assertTrue(loose["matches_signature"])

    def test_gain_threshold_is_strict(self):
        grid = build_grid()
        # A gain of exactly 25 % must not satisfy "gain > 25 %".
        stamp = stage_pump(grid, peak=BASE_PRICE * 1.25, trough=1.0)
        primary = P.evaluate_alert(grid, stamp, P.PRIMARY_RULE)
        self.assertFalse(primary["gain_condition"])
        self.assertFalse(primary["matches_signature"])

    def test_strict_variant_matches_a_bigger_pump(self):
        grid = build_grid()
        stamp = stage_pump(grid, peak=1.50, window_volume=2500.0, trough=1.10)
        strict = P.evaluate_alert(grid, stamp, P.VARIANT_STRICT)
        self.assertTrue(strict["gain_condition"])        # 50 % > 35 %
        self.assertTrue(strict["volume_condition"])      # 25x > 20x
        # retrace level = 1.50 - 0.6 * 0.50 = 1.20; the trough at 1.10 clears it
        self.assertTrue(strict["retracement_condition"])
        self.assertTrue(strict["matches_signature"])


class WindowVariantTest(unittest.TestCase):
    def test_late_peak_is_caught_only_by_the_ten_minute_variant(self):
        grid = build_grid()
        stamp = stage_pump(grid, peak_offset=8)
        for rule in (P.PRIMARY_RULE, P.VARIANT_LOOSE, P.VARIANT_STRICT):
            result = P.evaluate_alert(grid, stamp, rule)
            self.assertFalse(result["gain_condition"],
                             "%s should not see a peak at +8 min" % rule.name)
            self.assertFalse(result["matches_signature"])
        window10 = P.evaluate_alert(grid, stamp, P.VARIANT_WINDOW10)
        self.assertTrue(window10["gain_condition"])
        self.assertTrue(window10["matches_signature"])


class RetracementTest(unittest.TestCase):
    def test_no_retracement_means_no_match(self):
        grid = build_grid()
        stamp = stage_pump(grid, trough=None)
        for rule in P.ALL_RULES:
            result = P.evaluate_alert(grid, stamp, rule)
            self.assertFalse(result["retracement_condition"])
            self.assertFalse(result["matches_signature"])

    def test_retracement_must_fall_inside_the_sixty_minute_window(self):
        grid = build_grid(n_after=300)
        stamp = stage_pump(grid, trough=1.10, trough_offset=61)
        self.assertFalse(
            P.evaluate_alert(grid, stamp, P.PRIMARY_RULE)["retracement_condition"])
        grid = build_grid(n_after=300)
        stamp = stage_pump(grid, trough=1.10, trough_offset=60)
        self.assertTrue(
            P.evaluate_alert(grid, stamp, P.PRIMARY_RULE)["retracement_condition"])


class EvaluabilityTest(unittest.TestCase):
    def test_incomplete_trailing_window_is_not_evaluable(self):
        grid = build_grid()
        early = grid.index[10]
        result = P.evaluate_alert(grid, early, P.PRIMARY_RULE)
        self.assertFalse(result["evaluable"])
        self.assertIn("trailing", result["reason"])
        self.assertFalse(result["matches_signature"])

    def test_truncated_forward_window_is_not_evaluable(self):
        grid = build_grid(n_after=3)
        stamp = grid.index[TRAILING]
        result = P.evaluate_alert(grid, stamp, P.PRIMARY_RULE)
        self.assertFalse(result["evaluable"])
        self.assertIn("gain window", result["reason"])

    def test_alert_minute_absent_from_grid(self):
        grid = build_grid()
        result = P.evaluate_alert(grid, pd.Timestamp("2027-01-01 00:00:00"),
                                  P.PRIMARY_RULE)
        self.assertFalse(result["evaluable"])

    def test_summary_denominator_excludes_non_evaluable_alerts(self):
        grid = build_grid()
        good = stage_pump(grid)
        episodes = pd.DataFrame([
            {"pair": "AAAUSDT", "ts_utc": good, "detector": "upstream_rf",
             "frequency": "25S", "threshold_label": "tau_anchor"},
            {"pair": "AAAUSDT", "ts_utc": grid.index[5], "detector": "upstream_rf",
             "frequency": "25S", "threshold_label": "tau_anchor"},
        ])
        evaluations = P.evaluate_episodes(episodes, {"AAAUSDT": grid})
        summary = P.summarise(evaluations)
        primary = summary.loc[summary["rule"] == P.PRIMARY_RULE.name].iloc[0]
        self.assertEqual(int(primary["n_alerts"]), 2)
        self.assertEqual(int(primary["n_evaluable"]), 1)
        self.assertEqual(int(primary["n_matching"]), 1)
        self.assertEqual(
            float(primary["precision_proxy_benchmark_rule_relative"]), 1.0)


class ManualSampleTest(unittest.TestCase):
    """The manual n = 100 sample is a hook, never a fabrication."""

    def setUp(self):
        self.tmp = tempfile.mkdtemp()

    def test_absent_file_reports_not_performed(self):
        status = P.manual_sample_status(None)
        self.assertFalse(status["performed"])
        self.assertEqual(status["n_required"], 100)
        self.assertNotIn("verdict_counts", status)

    def test_incomplete_verdicts_raise(self):
        path = os.path.join(self.tmp, "manual.csv")
        with open(path, "w") as handle:
            handle.write(",".join(P.MANUAL_SAMPLE_FIELDS) + "\n")
            handle.write("1,AAAUSDT,2026-07-24T00:00:00,upstream_rf,25S,"
                         "tau_anchor,,,,,\n")
        with self.assertRaises(ValueError):
            P.load_manual_sample(path)

    def test_draw_is_deterministic_and_unverdicted(self):
        episodes = pd.DataFrame([
            {"pair": "P%02d" % i, "ts_utc": pd.Timestamp("2026-07-24") +
             pd.Timedelta(minutes=i), "detector": "upstream_rf",
             "frequency": "25S", "threshold_label": "tau_anchor"}
            for i in range(50)])
        first = P.draw_manual_sample(episodes, n=10)
        second = P.draw_manual_sample(episodes, n=10)
        pd.testing.assert_frame_equal(first, second)
        self.assertEqual(len(first), 10)
        self.assertEqual(list(first.columns), P.MANUAL_SAMPLE_FIELDS)
        self.assertTrue((first["verdict"] == "").all())
        self.assertEqual(P.MANUAL_SAMPLE_SEED, 20260722)


if __name__ == "__main__":
    unittest.main()
