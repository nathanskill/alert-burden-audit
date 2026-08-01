"""Feature-reconstruction tests (REF-2026-017).

Synthetic data with known answers, plus schema/roundtrip conformance:

  * the frozen upstream constants (frequencies, rolling windows, feature
    order, released-matrix precision) are what the protocol §4/§5 pin;
  * every feature column matches an independent plain-Python reference
    computation on a small synthetic pair-day, including the upstream's
    rush-order semantics (buy side only, "more than one trade in the same
    millisecond") and its hour/23, minute/59 cyclical normalisation;
  * archives are parsed correctly: microsecond epochs are floored to the
    upstream millisecond grouping key, and `is_buyer_maker` maps to the
    upstream aggressor-side convention;
  * the emitted frame conforms to the scoring schema and survives a
    '%.3f' CSV roundtrip unchanged, which is the precision the frozen
    classifier was fitted at.

No endpoint quantity is computed anywhere in this file.

Run:  .venv-analysis/bin/python -m unittest discover -s tests
"""

import io
import math
import os
import statistics
import sys
import tempfile
import unittest
import unittest.mock
import zipfile

sys.path.insert(0, os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))

import numpy as np
import pandas as pd

import features as F


BASE = pd.Timestamp("2026-07-24 00:00:00")


def varied_spec(n, price_base=1.0, price_step=0.03):
    """A synthetic pair-day whose every upstream component varies.

    Trade counts cycle 1, 2, 3 and even chunks put all their buys in the same
    millisecond (a rush order) while odd chunks spread them, so no rolling
    window is constant — a constant window would make ``pct_change`` 0/0 and
    the upstream ``dropna`` would empty the frame.
    """

    spec = []
    for k in range(n):
        trades = []
        for j in range((k % 3) + 1):
            offset = 0 if k % 2 == 0 else 100 * j
            trades.append((offset, price_base + price_step * k + 0.01 * j,
                           5 + k + 2 * j, "buy"))
        spec.append(trades)
    return spec


def make_trades(spec, freq_seconds=25, start=None):
    """Build a trade frame from a per-chunk specification.

    ``spec[k]`` is a list of ``(offset_ms, price, qty, side)`` for chunk ``k``.
    """

    origin = BASE if start is None else pd.Timestamp(start)
    stamps, prices, volumes, sides = [], [], [], []
    for chunk, trades in enumerate(spec):
        chunk_start = origin + pd.Timedelta(seconds=freq_seconds * chunk)
        for offset_ms, price, qty, side in trades:
            stamps.append(chunk_start + pd.Timedelta(milliseconds=offset_ms))
            prices.append(float(price))
            volumes.append(float(price) * float(qty))
            sides.append(side)
    frame = pd.DataFrame({"price": prices, "btc_volume": volumes, "side": sides},
                         index=pd.DatetimeIndex(stamps, name="time"))
    return frame.sort_index(kind="mergesort")


def reference_features(spec, rolling_freq, price_window, freq_seconds=25,
                       start=None):
    """Independent plain-Python reference for the upstream definitions."""

    origin = BASE if start is None else pd.Timestamp(start)
    rush, counts, volumes, means, maxima, dates = [], [], [], [], [], []
    for chunk, trades in enumerate(spec):
        buys = [t for t in trades if t[3] == "buy"]
        if not buys:
            continue
        by_ms = {}
        for offset_ms, price, qty, _ in buys:
            by_ms[offset_ms] = by_ms.get(offset_ms, 0) + 1
        rush.append(float(sum(1 for n in by_ms.values() if n > 1)))
        counts.append(float(len(buys)))
        volumes.append(sum(p * q for _, p, q, _ in buys))
        means.append(statistics.fmean(p for _, p, _, _ in buys))
        maxima.append(max(p for _, p, _, _ in buys))
        dates.append(origin + pd.Timedelta(seconds=freq_seconds * chunk))

    def rolling(series, window, func):
        out = []
        for i in range(len(series)):
            if i + 1 < window:
                out.append(float("nan"))
            else:
                out.append(func(series[i + 1 - window:i + 1]))
        return out

    def pct_change(series):
        out = [float("nan")]
        for previous, current in zip(series, series[1:]):
            if isinstance(previous, float) and math.isnan(previous):
                out.append(float("nan"))
            elif previous == 0:
                out.append(float("inf") if current else float("nan"))
            else:
                out.append((current - previous) / previous)
        return out

    sd = lambda xs: statistics.stdev(xs)
    mean = lambda xs: statistics.fmean(xs)
    columns = {
        "std_rush_order": pct_change(rolling(rush, rolling_freq, sd)),
        "avg_rush_order": pct_change(rolling(rush, rolling_freq, mean)),
        "std_trades": pct_change(rolling(counts, rolling_freq, sd)),
        "std_volume": pct_change(rolling(volumes, rolling_freq, sd)),
        "avg_volume": pct_change(rolling(volumes, rolling_freq, mean)),
        "std_price": pct_change(rolling(means, rolling_freq, sd)),
        "avg_price": pct_change(rolling(means, price_window, mean)),
        "avg_price_max": pct_change(rolling(maxima, price_window, mean)),
        "hour_sin": [math.sin(2 * math.pi * d.hour / 23) for d in dates],
        "hour_cos": [math.cos(2 * math.pi * d.hour / 23) for d in dates],
        "minute_sin": [math.sin(2 * math.pi * d.minute / 59) for d in dates],
        "minute_cos": [math.cos(2 * math.pi * d.minute / 59) for d in dates],
    }
    frame = pd.DataFrame({"date": dates, **columns}).dropna().reset_index(drop=True)
    frame[F.FEATURES] = frame[F.FEATURES].round(F.RELEASED_MATRIX_DECIMALS)
    return frame


class FrozenConstantsTest(unittest.TestCase):
    """The upstream constants the protocol pins must not drift."""

    def test_frozen_upstream_constants(self):
        self.assertEqual(F.UPSTREAM_ROLLING, {"25S": 900, "15S": 900, "5S": 700})
        self.assertEqual(F.FREQUENCY_BIN_SECONDS, {"25S": 25, "15S": 15, "5S": 5})
        self.assertEqual(F.PRICE_LEVEL_WINDOW, 10)
        self.assertEqual(F.RELEASED_MATRIX_DECIMALS, 3)
        self.assertEqual(F.FEATURES, [
            "std_rush_order", "avg_rush_order", "std_trades", "std_volume",
            "avg_volume", "std_price", "avg_price", "avg_price_max",
            "hour_sin", "hour_cos", "minute_sin", "minute_cos"])


class KnownAnswerFeatureTest(unittest.TestCase):
    """Every column matches an independent reference on synthetic trades."""

    ROLLING = 3
    PRICE_WINDOW = 3

    def setUp(self):
        # 14 chunks of varied structure, plus a large sell trade in every
        # chunk that must be ignored entirely.
        spec = varied_spec(14, price_step=0.05)
        for trades in spec:
            trades.append((900, 999.0, 1000.0, "sell"))
        self.spec = spec
        self.patched = unittest.mock.patch.object(
            F, "PRICE_LEVEL_WINDOW", self.PRICE_WINDOW)
        self.patched.start()

    def tearDown(self):
        self.patched.stop()

    def test_matches_reference(self):
        trades = make_trades(self.spec)
        got = F.build_features(trades, "25S", rolling_freq=self.ROLLING)
        expected = reference_features(self.spec, self.ROLLING, self.PRICE_WINDOW)
        self.assertEqual(len(got), len(expected))
        self.assertTrue(len(got) > 0)
        pd.testing.assert_series_equal(got["date"], expected["date"],
                                       check_names=False)
        for column in F.FEATURES:
            np.testing.assert_allclose(
                got[column].to_numpy(), expected[column].to_numpy(),
                rtol=0, atol=1e-9, err_msg="column %s" % column)

    def test_sell_trades_are_ignored(self):
        trades = make_trades(self.spec)
        with_extra_sells = pd.concat([
            trades,
            make_trades([[(700, 5000.0, 77.0, "sell")] for _ in self.spec]),
        ]).sort_index(kind="mergesort")
        a = F.build_features(trades, "25S", rolling_freq=self.ROLLING)
        b = F.build_features(with_extra_sells, "25S", rolling_freq=self.ROLLING)
        pd.testing.assert_frame_equal(a, b)

    def test_rush_order_counts_same_millisecond_collisions(self):
        # Two chunks differing only in whether the two buys share a
        # millisecond must produce different rush-order inputs; the collision
        # variant has a strictly larger rush indicator.
        collide = [[(0, 1.0, 1, "buy"), (0, 1.0, 1, "buy")]]
        spread = [[(0, 1.0, 1, "buy"), (50, 1.0, 1, "buy")]]
        counts = []
        for spec in (collide, spread):
            frame = make_trades(spec)
            per_ts = frame[frame["side"] == "buy"].groupby(level=0).size()
            counts.append(float((per_ts > 1).sum()))
        self.assertEqual(counts, [1.0, 0.0])


class CyclicalTimeFeatureTest(unittest.TestCase):
    """Upstream normalises by 23 and 59, not 24 and 60."""

    def test_hour_and_minute_normalisation(self):
        trades = make_trades(varied_spec(30), start="2026-07-24 13:37:00")
        with unittest.mock.patch.object(F, "PRICE_LEVEL_WINDOW", 3):
            frame = F.build_features(trades, "25S", rolling_freq=3)
        self.assertTrue(len(frame) > 0)
        row = frame.iloc[0]
        hour = row["date"].hour
        minute = row["date"].minute
        self.assertAlmostEqual(row["hour_sin"],
                               round(math.sin(2 * math.pi * hour / 23), 3), places=9)
        self.assertAlmostEqual(row["hour_cos"],
                               round(math.cos(2 * math.pi * hour / 23), 3), places=9)
        self.assertAlmostEqual(row["minute_sin"],
                               round(math.sin(2 * math.pi * minute / 59), 3), places=9)
        self.assertAlmostEqual(row["minute_cos"],
                               round(math.cos(2 * math.pi * minute / 59), 3), places=9)


class ArchiveParsingTest(unittest.TestCase):
    """Microsecond epochs, header detection and the aggressor-side mapping."""

    def setUp(self):
        self.tmp = tempfile.mkdtemp()

    def _write_zip(self, name, body):
        path = os.path.join(self.tmp, name)
        with zipfile.ZipFile(path, "w") as zf:
            zf.writestr(name.replace(".zip", ".csv"), body)
        return path

    def test_epoch_unit_detection(self):
        self.assertEqual(F.detect_epoch_unit(1784851201), "s")
        self.assertEqual(F.detect_epoch_unit(1784851201570), "ms")
        self.assertEqual(F.detect_epoch_unit(1784851201570704), "us")
        self.assertEqual(F.detect_epoch_unit(1784851201570704000), "ns")

    def test_microsecond_archive_floors_to_milliseconds(self):
        # Two trades 400 microseconds apart must land on the SAME millisecond
        # key (upstream grouped at millisecond resolution) and one 2 ms later
        # must not.
        rows = [
            "1,2.0,3.0,10,10,1784851201570704,True,True",
            "2,2.5,1.0,11,11,1784851201570904,False,True",
            "3,3.0,2.0,12,12,1784851201573000,False,True",
        ]
        path = self._write_zip("AAAUSDT-aggTrades-2026-07-24.zip",
                               "\n".join(rows) + "\n")
        frame = F.read_agg_trades(path)
        self.assertEqual(list(frame["side"]), ["sell", "buy", "buy"])
        self.assertEqual(frame.index.nunique(), 2)
        self.assertAlmostEqual(frame["btc_volume"].iloc[0], 6.0)
        self.assertIsNone(frame.index.tz)

    def test_header_row_is_detected(self):
        header = ",".join(F.AGG_TRADE_COLUMNS)
        rows = [header, "1,2.0,3.0,10,10,1784851201570,False,True"]
        path = self._write_zip("BBBUSDT-aggTrades-2026-07-24.zip",
                               "\n".join(rows) + "\n")
        frame = F.read_agg_trades(path)
        self.assertEqual(len(frame), 1)
        self.assertEqual(frame["side"].iloc[0], "buy")


class SchemaAndRoundtripTest(unittest.TestCase):
    """Scoring-schema conformance and the '%.3f' released-matrix precision."""

    def _frame(self):
        trades = make_trades(varied_spec(20))
        with unittest.mock.patch.object(F, "PRICE_LEVEL_WINDOW", 3):
            return F.build_features(trades, "25S", rolling_freq=3)

    def test_schema(self):
        frame = self._frame()
        self.assertEqual(list(frame.columns), ["date"] + F.FEATURES)
        self.assertTrue(len(frame) > 0)
        self.assertTrue(frame["date"].is_monotonic_increasing)
        self.assertIsNone(frame["date"].dt.tz)
        for column in F.FEATURES:
            self.assertEqual(frame[column].dtype, np.dtype("float64"))
        self.assertFalse(frame[F.FEATURES].isna().any().any())

    def test_released_matrix_precision_roundtrip(self):
        frame = self._frame()
        buffer = io.StringIO()
        frame.to_csv(buffer, index=False, float_format="%.3f")
        buffer.seek(0)
        reloaded = pd.read_csv(buffer, parse_dates=["date"])
        for column in F.FEATURES:
            np.testing.assert_allclose(reloaded[column].to_numpy(),
                                       frame[column].to_numpy(),
                                       rtol=0, atol=0,
                                       err_msg="column %s" % column)

    def test_nonfinite_rows_are_dropped_and_counted(self):
        # Reconstruction limit R6: pct_change of a rolling statistic is +-inf
        # when the preceding value is exactly zero, and scikit-learn refuses
        # to score such a row, so it is dropped and counted.
        frame = pd.DataFrame(
            {"date": pd.date_range("2026-07-24", periods=5, freq="25s")})
        for name in F.FEATURES:
            frame[name] = 0.1
        frame.loc[0, "std_volume"] = np.inf
        frame.loc[1, "avg_price"] = -np.inf
        frame.loc[2, "std_price"] = np.nan
        cleaned, dropped = F.drop_nonfinite(frame)
        self.assertEqual(dropped, 3)
        self.assertEqual(len(cleaned), 2)
        self.assertTrue(np.isfinite(
            cleaned[F.FEATURES].to_numpy(dtype=float)).all())

    def test_component_misalignment_raises(self):
        # A chunk whose buy volume sums to exactly zero is dropped from the
        # volume components but not from the price components; upstream would
        # misalign positionally, we must raise instead.
        spec = [[(0, 1.0 + 0.02 * k, 5 + k, "buy")] for k in range(12)]
        spec[6] = [(0, 1.0, 0.0, "buy")]
        trades = make_trades(spec)
        with unittest.mock.patch.object(F, "PRICE_LEVEL_WINDOW", 3):
            with self.assertRaises(ValueError):
                F.build_features(trades, "25S", rolling_freq=3)


class MinuteGridTest(unittest.TestCase):
    """The complete 1-minute UTC grid used by the baselines."""

    def test_missing_minutes_are_filled_flat_with_zero_volume(self):
        stamps = pd.date_range("2026-07-24", periods=F.MINUTES_PER_DAY, freq="min")
        keep = stamps.delete([5, 6])
        klines = pd.DataFrame(
            {"open": 1.0, "high": 1.0, "low": 1.0, "close": 2.0,
             "volume": 3.0, "quote_volume": 6.0, "trades": 1.0},
            index=keep)
        klines.index.name = "open_time"
        grid = F.complete_minute_grid(klines, "2026-07-24")
        self.assertEqual(len(grid), F.MINUTES_PER_DAY)
        self.assertEqual(int(grid["filled"].sum()), 2)
        self.assertEqual(float(grid["quote_volume"].iloc[5]), 0.0)
        self.assertEqual(float(grid["close"].iloc[5]), 2.0)
        self.assertEqual(float(grid["high"].iloc[5]), 2.0)


if __name__ == "__main__":
    unittest.main()
