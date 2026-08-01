"""Detector tests (REF-2026-017), all on synthetic data with known answers.

Covers protocol §5:

  * the 30-minute per-pair cooldown, including the boundary case (a gap of
    exactly the cooldown emits), out-of-order input, and the carry-in of the
    cooldown clock across a day boundary;
  * trivial baseline A (volume z-score > 4 on a 5-minute rolling window
    against the trailing 24 h), against an independent computation, including
    the strictness of the comparison and the undefined-when-sd-zero case;
  * trivial baseline B (return > 5 % within 5 minutes), including strictness;
  * the frozen thresholds and RF configuration;
  * the structural sink used by the partial-window check records no count.

No endpoint quantity is computed anywhere in this file.
"""

import os
import sys
import unittest
import unittest.mock

sys.path.insert(0, os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))

import numpy as np
import pandas as pd

import apply_detectors as D
import features as F


def minute_grid(volumes, closes=None, start="2026-07-24"):
    stamps = pd.date_range(start, periods=len(volumes), freq="min")
    closes = [1.0] * len(volumes) if closes is None else closes
    frame = pd.DataFrame(
        {"open": closes, "high": closes, "low": closes, "close": closes,
         "volume": volumes, "quote_volume": volumes,
         "trades": [1.0] * len(volumes), "filled": [False] * len(volumes)},
        index=stamps)
    frame.index.name = "open_time"
    return frame


class FrozenDetectorConstantsTest(unittest.TestCase):
    def test_frozen_constants(self):
        self.assertEqual(D.COOLDOWN_SECONDS, 1800)
        self.assertEqual(D.TAU_ANCHOR, 0.5)
        self.assertEqual(D.ZSCORE_THRESHOLD, 4.0)
        self.assertEqual(D.ZSCORE_WINDOW_MINUTES, 5)
        self.assertEqual(D.ZSCORE_TRAILING_MINUTES, 1440)
        self.assertEqual(D.PRICE_JUMP_THRESHOLD, 0.05)
        self.assertEqual(D.PRICE_JUMP_WINDOW_MINUTES, 5)
        self.assertEqual(D.RF_PARAMS, {"n_estimators": 200, "max_depth": 5,
                                       "min_samples_leaf": 1, "random_state": 1})
        self.assertEqual(sorted(D.TAU_STAR_FROZEN), ["15S", "25S", "5S"])
        self.assertAlmostEqual(D.TAU_STAR_FROZEN["25S"], 0.7036605405579714, places=15)
        self.assertAlmostEqual(D.TAU_STAR_FROZEN["15S"], 0.5059013502406946, places=15)
        self.assertAlmostEqual(D.TAU_STAR_FROZEN["5S"], 0.3382249603958512, places=15)


class CooldownTest(unittest.TestCase):
    """30-minute per-pair cooldown with a known-answer schedule."""

    def test_suppression_and_boundary(self):
        base = pd.Timestamp("2026-07-24 00:00:00")
        offsets_s = [0, 600, 1799, 1800, 1860, 3600, 3601]
        stamps = [base + pd.Timedelta(seconds=s) for s in offsets_s]
        kept, last = D.apply_cooldown(stamps, list(range(len(stamps))))
        # 0 emits; +600 and +1799 are inside the cooldown; +1800 is exactly the
        # cooldown and therefore emits; +1860 is inside the new cooldown;
        # +3600 is exactly 1800 after +1800 and emits; +3601 is suppressed.
        self.assertEqual(kept, [0, 3, 5])
        self.assertEqual(last, base + pd.Timedelta(seconds=3600))

    def test_unordered_input_is_processed_in_time_order(self):
        base = pd.Timestamp("2026-07-24 00:00:00")
        stamps = [base + pd.Timedelta(seconds=s) for s in (1800, 0, 600)]
        kept, _ = D.apply_cooldown(stamps, [0, 1, 2])
        # Positions into the input, in emission order: the 0 s candidate first.
        self.assertEqual(kept, [1, 0])

    def test_clock_carries_across_the_day_boundary(self):
        end_of_day = pd.Timestamp("2026-07-24 23:50:00")
        next_day = [pd.Timestamp("2026-07-25 00:10:00"),
                    pd.Timestamp("2026-07-25 00:21:00")]
        kept, last = D.apply_cooldown(next_day, [0, 1], last_emitted=end_of_day)
        # 00:10 is 20 minutes after 23:50 -> suppressed; 00:21 is 31 minutes
        # after 23:50 -> emitted. The clock is never reset at midnight.
        self.assertEqual(kept, [1])
        self.assertEqual(last, next_day[1])

    def test_empty_stream(self):
        kept, last = D.apply_cooldown([], [])
        self.assertEqual(kept, [])
        self.assertIsNone(last)


class VolumeZScoreBaselineTest(unittest.TestCase):
    """Baseline A against an independent computation."""

    def _grid(self):
        rng = np.random.default_rng(7)
        # 1440 trailing minutes + 5 for the first full window + the test tail.
        volumes = list(rng.integers(80, 120, size=1500).astype(float))
        return volumes

    def test_matches_independent_zscore(self):
        volumes = self._grid()
        grid = minute_grid(volumes)
        got = D.volume_zscore_series(grid).to_numpy()

        values = np.asarray(volumes, dtype=float)
        v5 = np.full(len(values), np.nan)
        for i in range(4, len(values)):
            v5[i] = values[i - 4:i + 1].sum()
        expected = np.full(len(values), np.nan)
        for i in range(len(values)):
            window = v5[i - 1440:i]
            if len(window) < 1440 or np.isnan(window).any():
                continue
            sd = window.std(ddof=1)
            if sd <= 0:
                continue
            expected[i] = (v5[i] - window.mean()) / sd
        np.testing.assert_allclose(got, expected, rtol=1e-12, atol=1e-12,
                                   equal_nan=True)

    def test_spike_fires_and_threshold_is_strict(self):
        volumes = [100.0] * 1445 + [101.0] * 5
        # Trailing v5 values vary only over the 100/101 transition, so sd is
        # small; a large spike must clear z > 4.
        volumes = volumes + [100000.0] + [100.0] * 10
        grid = minute_grid(volumes)
        z = D.volume_zscore_series(grid)
        candidates = D.baseline_candidates(grid, D.DETECTOR_ZSCORE)
        spike_minute = grid.index[1450]
        self.assertIn(spike_minute, list(candidates["ts_utc"]))
        self.assertTrue(float(z.loc[spike_minute]) > 4.0)
        for stamp in candidates["ts_utc"]:
            self.assertTrue(float(z.loc[stamp]) > D.ZSCORE_THRESHOLD)
        self.assertEqual(float(candidates["threshold"].iloc[0]), 4.0)
        # The comparison is strict: a score exactly equal to the threshold
        # does not fire.
        exact = float(z.loc[spike_minute])
        with unittest.mock.patch.object(D, "ZSCORE_THRESHOLD", exact):
            strict = D.baseline_candidates(grid, D.DETECTOR_ZSCORE)
        self.assertNotIn(spike_minute, list(strict["ts_utc"]))

    def test_zero_variance_trailing_window_cannot_fire(self):
        volumes = [100.0] * 1600
        grid = minute_grid(volumes)
        z = D.volume_zscore_series(grid)
        self.assertTrue(z.isna().all())
        self.assertTrue(D.baseline_candidates(grid, D.DETECTOR_ZSCORE).empty)


class PriceJumpBaselineTest(unittest.TestCase):
    """Baseline B: return > 5 % within 5 minutes, strict."""

    def test_known_jumps(self):
        closes = [1.0] * 15
        closes[14] = 1.06                 # +6 % over 5 minutes -> fires
        closes += [1.06] * 5
        closes[19] = 1.06 * 1.049         # +4.9 % -> does not fire
        volumes = [1.0] * len(closes)
        grid = minute_grid(volumes, closes)
        returns = D.price_jump_series(grid)
        self.assertAlmostEqual(float(returns.iloc[14]), 0.06, places=12)
        self.assertAlmostEqual(float(returns.iloc[19]), 0.049, places=12)
        candidates = D.baseline_candidates(grid, D.DETECTOR_PRICE_JUMP)
        fired = list(candidates["ts_utc"])
        self.assertIn(grid.index[14], fired)
        self.assertNotIn(grid.index[19], fired)
        self.assertEqual(float(candidates["threshold"].iloc[0]), 0.05)

    def test_threshold_comparison_is_strict(self):
        closes = [1.0] * 15
        closes[14] = 1.06
        grid = minute_grid([1.0] * len(closes), closes)
        exact = float(D.price_jump_series(grid).iloc[14])
        with unittest.mock.patch.object(D, "PRICE_JUMP_THRESHOLD", exact):
            strict = D.baseline_candidates(grid, D.DETECTOR_PRICE_JUMP)
        self.assertNotIn(grid.index[14], list(strict["ts_utc"]))


class BaselineEpisodeCooldownTest(unittest.TestCase):
    """Baseline candidates are reduced to episodes by the 30-minute cooldown."""

    def test_dense_jumps_collapse_to_one_episode_per_cooldown(self):
        # A step up every minute for 40 minutes: every minute from t+5 crosses
        # +5 %, but the cooldown admits at most one alert per 30 minutes.
        closes = [1.0] * 5 + [1.0 * (1.02 ** k) for k in range(1, 41)]
        grid = minute_grid([1.0] * len(closes), closes)
        state = {}
        episodes = D.baseline_episodes_for_pair_day(
            "AAAUSDT", "2026-07-24", grid, state)
        jumps = [e for e in episodes if e["detector"] == D.DETECTOR_PRICE_JUMP]
        stamps = [pd.Timestamp(e["ts_utc"]) for e in jumps]
        self.assertEqual(len(stamps), 2)
        self.assertEqual((stamps[1] - stamps[0]).total_seconds(), 1800)
        for episode in jumps:
            self.assertEqual(set(episode), set(D.EPISODE_FIELDS))
            self.assertEqual(episode["pair"], "AAAUSDT")
            self.assertEqual(episode["threshold"], 0.05)


class StructuralSinkTest(unittest.TestCase):
    """The partial-window sink exposes booleans only, never counts."""

    def test_reports_no_counts(self):
        sink = D.StructuralSink()
        base = pd.Timestamp("2026-07-24 00:00:00")
        rows = [{"pair": "AAAUSDT", "ts_utc": (base + pd.Timedelta(seconds=s)).isoformat(),
                 "detector": D.DETECTOR_RF, "threshold": 0.5, "score": 0.9,
                 "threshold_label": "tau_anchor", "frequency": "25S",
                 "ts_available_utc": base.isoformat(), "day": "2026-07-24"}
                for s in (0, 1800, 3600)]
        sink.consume(rows)
        report = sink.report()
        self.assertTrue(report["episode_schema_conformant"])
        self.assertTrue(report["cooldown_separation_respected"])
        self.assertTrue(report["rf_scores_within_unit_interval"])
        # Booleans and the explanatory note only: no numeric field may appear,
        # because on partial data any alert count is a burden quantity.
        for key, value in report.items():
            self.assertIsInstance(value, (bool, str),
                                  "sink leaked a numeric field in %r" % key)

    def test_detects_a_cooldown_violation(self):
        sink = D.StructuralSink()
        base = pd.Timestamp("2026-07-24 00:00:00")
        rows = [{"pair": "AAAUSDT", "ts_utc": (base + pd.Timedelta(seconds=s)).isoformat(),
                 "detector": D.DETECTOR_ZSCORE, "threshold": 4.0, "score": 9.0,
                 "threshold_label": "fixed", "frequency": "",
                 "ts_available_utc": base.isoformat(), "day": "2026-07-24"}
                for s in (0, 60)]
        sink.consume(rows)
        self.assertFalse(sink.report()["cooldown_separation_respected"])


class RfEpisodeTest(unittest.TestCase):
    """RF episode emission uses both frozen thresholds and the cooldown."""

    def test_thresholds_and_schema(self):
        stamps = pd.date_range("2026-07-24", periods=6, freq="10min")
        frame = pd.DataFrame({"date": stamps, "symbol": "AAAUSDT"})
        for name in F.FEATURES:
            frame[name] = 0.0
        pair_day = F.PairDayFeatures(symbol="AAAUSDT", day="2026-07-24",
                                     frequency="25S", frame=frame, meta={})
        scores = np.array([0.9, 0.9, 0.9, 0.6, 0.2, 0.9])
        state = {}
        episodes = D.rf_episodes_for_pair_day(
            pair_day, scores, D.rf_thresholds("25S"), state)
        anchor = [e for e in episodes if e["threshold_label"] == "tau_anchor"]
        star = [e for e in episodes if e["threshold_label"] == "tau_star"]
        # tau=0.5: crossings at 0,10,20,30,50 min -> cooldown keeps 0 and 30.
        self.assertEqual([pd.Timestamp(e["ts_utc"]).minute for e in anchor], [0, 30])
        # tau*=0.7036...: crossings at 0,10,20,50 -> cooldown keeps 0 and 50.
        self.assertEqual([pd.Timestamp(e["ts_utc"]).minute for e in star], [0, 50])
        for episode in episodes:
            self.assertEqual(set(episode), set(D.EPISODE_FIELDS))
            self.assertEqual(episode["detector"], D.DETECTOR_RF)
            self.assertEqual(episode["frequency"], "25S")
            available = pd.Timestamp(episode["ts_available_utc"])
            self.assertEqual((available - pd.Timestamp(episode["ts_utc"])
                              ).total_seconds(), 25)


if __name__ == "__main__":
    unittest.main()
