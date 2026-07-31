"""Smoke tests for collector.py (REF-2026-017).

Covers the regressions fixed on 2026-07-31:
  * eligible_symbols: JUP/SYRUP must survive the leveraged-token filter
    (Amendment 2), real BLVT/FTX bases and stable/fiat bases must not;
  * trailing_quote_volume: the in-progress daily candle is dropped;
  * universe_for_day: day D maps to the newest table dated strictly
    before D (Amendment 4);
  * manifest append/dedup: verified skips are manifested exactly once.

Run:  .venv/bin/python -m unittest discover -s tests
"""

import datetime as dt
import os
import shutil
import sys
import tempfile
import unittest
from unittest import mock

sys.path.insert(0, os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))

import collector as C


class FakeResp:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        pass

    def json(self):
        return self._payload


def sym(symbol, base, quote="USDT", status="TRADING", spot=True):
    return {"symbol": symbol, "baseAsset": base, "quoteAsset": quote,
            "status": status, "isSpotTradingAllowed": spot}


class EligibleSymbolsTest(unittest.TestCase):
    PAYLOAD = {"symbols": [
        sym("JUPUSDT", "JUP"),          # regression: ends in "UP", ordinary
        sym("SYRUPUSDT", "SYRUP"),      # regression: ends in "UP", ordinary
        sym("SUPUSDT", "SUP"),          # ends in "UP", ordinary
        sym("AAAUSDT", "AAA"),
        sym("BTCUPUSDT", "BTCUP"),      # real BLVT base: excluded
        sym("EOSBULLUSDT", "EOSBULL"),  # real FTX token base: excluded
        sym("USDCUSDT", "USDC"),        # stable base: excluded
        sym("HALTUSDT", "HALT", status="BREAK"),   # not TRADING
        sym("AAABTC", "AAA", quote="BTC"),         # not USDT-quoted
        sym("NOSPOTUSDT", "NOSPOT", spot=False),   # spot not allowed
    ]}

    def test_filters(self):
        with mock.patch.object(C, "http_get",
                               return_value=FakeResp(self.PAYLOAD)):
            got = C.eligible_symbols(session=None)
        self.assertEqual(
            got, ["AAAUSDT", "JUPUSDT", "SUPUSDT", "SYRUPUSDT"])


class TrailingVolumeTest(unittest.TestCase):
    def test_partial_candle_dropped(self):
        # 31 candles, quoteVolume (index 7) = 1.0 each except the final
        # in-progress candle = 1000.0, which must be dropped.
        klines = [[0] * 7 + [1.0] for _ in range(30)] + [[0] * 7 + [1000.0]]
        calls = {}

        def fake_get(session, url, **kw):
            calls.update(kw.get("params", {}))
            return FakeResp(klines)

        with mock.patch.object(C, "http_get", side_effect=fake_get):
            total = C.trailing_quote_volume(session=None, symbol="AAAUSDT")
        self.assertEqual(total, 30.0)
        self.assertEqual(calls.get("limit"), C.TRAILING_DAYS + 1)


class UniverseForDayTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        for d in ("2026-07-23", "2026-07-31"):
            with open(os.path.join(self.tmp, "universe_%s.csv" % d), "w"):
                pass
        self.patcher = mock.patch.object(C, "UNIVERSE_DIR", self.tmp)
        self.patcher.start()

    def tearDown(self):
        self.patcher.stop()
        shutil.rmtree(self.tmp)

    def test_strictly_before(self):
        self.assertEqual(C.universe_for_day("2026-07-24")[1],
                         dt.date(2026, 7, 23))
        # A table dated D never governs day D itself.
        self.assertEqual(C.universe_for_day("2026-07-31")[1],
                         dt.date(2026, 7, 23))
        self.assertEqual(C.universe_for_day("2026-08-01")[1],
                         dt.date(2026, 7, 31))
        self.assertEqual(C.universe_for_day("2026-07-23"), (None, None))


class ManifestDedupTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.manifest = os.path.join(self.tmp, "pull_manifest.csv")
        with open(self.manifest, "w") as f:
            f.write("date,symbol,file,bytes,sha256,source_checksum_ok\n"
                    "2026-07-27,AAAUSDT,AAAUSDT-1m-2026-07-27.zip,10,aa,1\n")
        self.patcher = mock.patch.object(C, "MANIFEST_CSV", self.manifest)
        self.patcher.start()

    def tearDown(self):
        self.patcher.stop()
        shutil.rmtree(self.tmp)

    def test_load_keys(self):
        self.assertEqual(C.load_manifested_keys(),
                         {("2026-07-27", "AAAUSDT-1m-2026-07-27.zip")})

    def test_row_wanted(self):
        already = C.load_manifested_keys()

        def row(status, file, sha="aa"):
            return {"status": status, "date": "2026-07-27", "file": file,
                    "sha256": sha}

        # Fresh downloads always manifested (re-download supersedes).
        self.assertTrue(C.manifest_row_wanted(
            row("ok", "AAAUSDT-1m-2026-07-27.zip"), already))
        # Verified skip of an already-manifested file: no duplicate row.
        self.assertFalse(C.manifest_row_wanted(
            row("skipped", "AAAUSDT-1m-2026-07-27.zip"), already))
        # Verified skip of a never-manifested file: append.
        self.assertTrue(C.manifest_row_wanted(
            row("skipped", "BBBUSDT-1m-2026-07-27.zip"), already))
        # Unverified skip (no sha256): never manifested.
        self.assertFalse(C.manifest_row_wanted(
            row("skipped", "BBBUSDT-1m-2026-07-27.zip", sha=""), already))
        # Non-success statuses: never manifested.
        self.assertFalse(C.manifest_row_wanted(
            row("404", "CCCUSDT-1m-2026-07-27.zip"), already))


if __name__ == "__main__":
    unittest.main()
