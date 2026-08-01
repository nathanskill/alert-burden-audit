#!/usr/bin/env python3
"""features.py — reconstruction of the upstream feature matrix (REF-2026-017).

Protocol reference: `protocol/locked_protocol_v1.0.md` §4 ("Features are
rebuilt from aggregate trades at the upstream study's 5 s / 15 s / 25 s
frequencies using the upstream feature definitions") and §5 (the detector is
applied exactly as released and is *never* retrained on this study's stream).
Protocol freeze commit: 0ac2bbd026915b1ac09acf649638d28e637d0289.

What this module does
---------------------
For one monitored pair and one UTC day it rebuilds the twelve upstream
features from the collected Binance daily ``aggTrades`` archive, at each of
the three upstream frequencies (5S / 15S / 25S), using the upstream feature
definitions:

    std_rush_order, avg_rush_order, std_trades, std_volume, avg_volume,
    std_price, avg_price, avg_price_max,
    hour_sin, hour_cos, minute_sin, minute_cos

Every definition below is a re-expression of the upstream reference
implementation (`features.py` in the upstream dataset repository, pinned
commit d71250d4cb055dde2d415c8cba38a0dcd6eb6e16, SHA-256
b02c14034f837bc625cb9df6baff7046d2186fb5f32cb5da056b8acaeedd083f, recorded in
the REF-2026-016 source-and-licence audit). No upstream code is copied; the
semantics are reproduced and are covered by tests in `tests/test_features.py`.
The upstream frequency/rolling-window pairing is likewise reproduced exactly:
25S and 15S use a 900-chunk rolling window, 5S uses 700.

1-minute klines are *not* an input to any upstream feature. They are read by
this module only for the structural coverage cross-check
(`kline_coverage_meta`) and are the basis of the two trivial baselines in
`apply_detectors.py`; this is stated here so that no reader assumes a
kline-derived quantity entered the random forest.

Reconstruction limits (each one is also repeated in the run report)
------------------------------------------------------------------
R1. **Trade granularity.** The upstream download path (`downloader.py`) used
    ccxt 1.36.1 `binance.fetch_trades`, whose Binance implementation of that
    era defaults to the *aggregate*-trades endpoint
    (``options['fetchTradesMethod'] = 'publicGetAggTrades'``). This study
    therefore feeds the official daily aggTrades archives, which is believed
    to be the matching granularity. It could not be verified byte-for-byte:
    the upstream raw per-event trade CSVs were never published, and the
    upstream download path cannot be re-run for 2018-2019 windows. If the
    upstream in fact used raw trades, the `*_rush_order` features would
    differ, because aggTrades merge same-price same-side same-millisecond
    fills of one taker order into a single row and the rush-order indicator
    is precisely "more than one buy trade in the same millisecond".
R2. **Timestamp resolution.** Upstream parsed millisecond epochs
    (``pd.to_datetime(ts, unit='ms')``). The current Binance daily archives
    publish *microsecond* epochs. Timestamps are therefore floored to
    milliseconds before the rush-order grouping, which restores the upstream
    grouping key exactly; sub-millisecond distinctions are discarded by
    design. The epoch unit is detected per file and recorded in the metadata.
R3. **Quote currency.** Upstream traded SYM/BTC and used
    ``btc_volume = price * amount``; this study monitors SYM/USDT, so the
    same product is a USDT quote volume. All eight non-cyclical features are
    percentage changes of rolling statistics of that quantity, which are
    invariant to a constant multiplicative rescaling of volume; the residual
    difference is only the second-order effect of BTC/USDT drift *within* a
    rolling window. No conversion is applied.
R4. **Released-matrix precision.** The upstream released matrices were
    written with ``float_format='%.3f'``; the frozen classifier was fitted on
    those 3-decimal values. Reconstructed features are therefore rounded to 3
    decimals by default (`round_dp=3`) so the scoring inputs carry the same
    precision as the training inputs. `round_dp=None` disables it.
R5. **Rolling warm-up across the day boundary.** Upstream computed features
    over a continuous multi-day window per event, so the rolling window was
    warm before the rows it kept. The upstream window is 900 *non-empty*
    chunks (700 at 5S), which on a small-capitalisation pair can span several
    calendar days: a pair-day computed in isolation, or with a single day of
    warm-up, can yield **no scorable rows at all**. This module therefore
    prepends preceding archived pair-days purely to warm the rolling state
    and then trims the output to the target UTC day.

    The default `warmup_days="auto"` extends backwards one archived day at a
    time until the prefix holds at least `rolling_freq` non-empty chunks,
    capped at :data:`MAX_WARMUP_DAYS`; it stops early at the first missing
    preceding archive, because a warm-up may not be carried across a gap in
    a continuous stream. An explicit integer is also accepted. The rule is
    mechanical and fixed before the evaluation window closed; it is not
    tuned on any result.

    Where the archives run out (notably at the stream start, where no day
    before 2026-07-24 was collected) the warm-up is short and the pair-day
    yields fewer rows, or none. This is recorded per pair-day as a structural
    fact (`warmup_days_used`, `warmup_chunks_available`, `warmup_sufficient`,
    `warmup_stop_reason`, `n_rows_target_day`) and is never imputed. Scorable
    coverage is a reporting obligation of the analysis, not a silent filter:
    a pair-day may be monitored (its archive verified) and still be
    unscorable, and the two counts must be reported side by side.
R6. **Non-finite rows.** ``pct_change`` of a rolling statistic is ±inf when
    the preceding value is exactly zero (e.g. a rolling standard deviation of
    a constant stretch). Upstream's terminal ``dropna()`` does not remove
    infinities; scikit-learn refuses to score them. Rows with any non-finite
    feature are therefore dropped and counted
    (`n_rows_dropped_nonfinite`); the count is a structural fact and is
    reported, because those chunks cannot raise an alert.
R7. **Component alignment.** Upstream assembles the feature columns
    positionally (``.values``) from series that were filtered independently
    (empty-chunk drops differ between the count/volume/price components in
    principle). This module asserts that the component indexes are identical
    and raises instead of silently misaligning. In all data seen so far the
    indexes coincide; a raise would signal a genuine anomaly.

This module computes no endpoint quantity of any kind.
"""

from __future__ import annotations

import io
import os
import zipfile
from dataclasses import dataclass, field

import numpy as np
import pandas as pd

# --------------------------------------------------------------------------
# Frozen upstream definitions
# --------------------------------------------------------------------------

#: The twelve upstream features, in the upstream column order.
FEATURES = [
    "std_rush_order",
    "avg_rush_order",
    "std_trades",
    "std_volume",
    "avg_volume",
    "std_price",
    "avg_price",
    "avg_price_max",
    "hour_sin",
    "hour_cos",
    "minute_sin",
    "minute_cos",
]

#: Upstream frequency label -> rolling window length in chunks
#: (upstream ``compute_features``: 25S/900, 15S/900, 5S/700).
UPSTREAM_ROLLING = {"25S": 900, "15S": 900, "5S": 700}

#: Upstream frequency label -> chunk width in seconds.
FREQUENCY_BIN_SECONDS = {"25S": 25, "15S": 15, "5S": 5}

#: Frequency labels in the upstream order.
FREQUENCIES = ("25S", "15S", "5S")

#: Upstream fixed rolling window for the two price-level features.
PRICE_LEVEL_WINDOW = 10

#: Upstream released matrices were written with '%.3f'.
RELEASED_MATRIX_DECIMALS = 3

#: Warm-up policy (limit R5). "auto" extends backwards until the rolling
#: window is full; the cap bounds the I/O for the most illiquid pairs.
WARMUP_AUTO = "auto"
MAX_WARMUP_DAYS = 14

#: Column layout of the Binance daily aggTrades archive (headerless variant).
AGG_TRADE_COLUMNS = [
    "agg_trade_id",
    "price",
    "quantity",
    "first_trade_id",
    "last_trade_id",
    "transact_time",
    "is_buyer_maker",
    "is_best_match",
]

#: Column layout of the Binance daily 1-minute kline archive.
KLINE_COLUMNS = [
    "open_time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "close_time",
    "quote_volume",
    "trades",
    "taker_buy_volume",
    "taker_buy_quote_volume",
    "ignore",
]

MINUTES_PER_DAY = 1440


# --------------------------------------------------------------------------
# Archive readers
# --------------------------------------------------------------------------


def detect_epoch_unit(value: int) -> str:
    """Return the pandas epoch unit for a Binance timestamp magnitude.

    Binance daily archives published millisecond epochs historically and
    microsecond epochs in the current generation (reconstruction limit R2).
    The magnitude bands below are unambiguous for any date after 1973 and
    before 5138.
    """

    v = abs(int(value))
    if v >= 10**17:
        return "ns"
    if v >= 10**14:
        return "us"
    if v >= 10**11:
        return "ms"
    return "s"


def _read_single_member_csv(zip_path: str, columns: list[str]) -> pd.DataFrame:
    """Read the single CSV member of a Binance daily archive.

    Handles both the headerless legacy layout and the current layout that
    carries a header row.
    """

    with zipfile.ZipFile(zip_path) as zf:
        names = zf.namelist()
        if len(names) != 1:
            raise ValueError("expected exactly one member in %s, found %d"
                             % (os.path.basename(zip_path), len(names)))
        raw = zf.read(names[0])
    if not raw.strip():
        return pd.DataFrame(columns=columns)
    first_field = raw.split(b"\n", 1)[0].split(b",", 1)[0].strip()
    has_header = not _looks_numeric(first_field)
    frame = pd.read_csv(
        io.BytesIO(raw),
        header=0 if has_header else None,
        names=None if has_header else columns,
    )
    if has_header:
        # Normalise to our canonical names positionally; the archive header
        # uses the same order.
        if len(frame.columns) != len(columns):
            raise ValueError("unexpected column count in %s: %d"
                             % (os.path.basename(zip_path), len(frame.columns)))
        frame.columns = columns
    return frame


def _looks_numeric(token: bytes) -> bool:
    try:
        float(token)
    except (TypeError, ValueError):
        return False
    return True


def read_agg_trades(zip_path: str) -> pd.DataFrame:
    """Read one daily aggTrades archive into the upstream trade schema.

    Returns a frame indexed by millisecond-floored UTC-naive ``time`` with
    columns ``price``, ``btc_volume`` (= price * quantity, the upstream
    quote-volume product; reconstruction limit R3) and ``side`` in
    {'buy', 'sell'}.

    ``side`` follows the upstream (ccxt) convention: the aggressor side.
    Binance's ``is_buyer_maker`` is True when the buyer was the maker, i.e.
    the taker sold, so ``side == 'buy'`` is exactly ``is_buyer_maker == False``.
    """

    raw = _read_single_member_csv(zip_path, AGG_TRADE_COLUMNS)
    if raw.empty:
        empty = pd.DataFrame(
            {"price": pd.Series(dtype="float64"),
             "btc_volume": pd.Series(dtype="float64"),
             "side": pd.Series(dtype="object")},
            index=pd.DatetimeIndex([], name="time"),
        )
        return empty
    unit = detect_epoch_unit(int(raw["transact_time"].iloc[0]))
    time = pd.to_datetime(raw["transact_time"].astype("int64"), unit=unit)
    # R2: restore the upstream millisecond grouping key.
    time = time.dt.floor("ms")
    price = raw["price"].astype("float64")
    quantity = raw["quantity"].astype("float64")
    maker = raw["is_buyer_maker"]
    if maker.dtype != bool:
        maker = maker.astype(str).str.strip().str.lower().isin(
            {"true", "1", "t", "yes"})
    frame = pd.DataFrame(
        {
            "price": price.to_numpy(),
            "btc_volume": (price * quantity).to_numpy(),
            "side": np.where(maker.to_numpy(), "sell", "buy"),
        },
        index=pd.DatetimeIndex(time.to_numpy(), name="time"),
    )
    return frame.sort_index(kind="mergesort")


def read_klines_1m(zip_path: str) -> pd.DataFrame:
    """Read one daily 1-minute kline archive.

    Returns a frame indexed by UTC-naive ``open_time`` with float OHLCV
    columns. No feature in this module derives from klines (see the module
    docstring); the baselines and the precision proxy consume this frame.
    """

    raw = _read_single_member_csv(zip_path, KLINE_COLUMNS)
    if raw.empty:
        return pd.DataFrame(
            columns=["open", "high", "low", "close", "volume",
                     "quote_volume", "trades"],
            index=pd.DatetimeIndex([], name="open_time"),
            dtype="float64",
        )
    unit = detect_epoch_unit(int(raw["open_time"].iloc[0]))
    open_time = pd.to_datetime(raw["open_time"].astype("int64"), unit=unit)
    out = pd.DataFrame(
        {
            "open": raw["open"].astype("float64").to_numpy(),
            "high": raw["high"].astype("float64").to_numpy(),
            "low": raw["low"].astype("float64").to_numpy(),
            "close": raw["close"].astype("float64").to_numpy(),
            "volume": raw["volume"].astype("float64").to_numpy(),
            "quote_volume": raw["quote_volume"].astype("float64").to_numpy(),
            "trades": raw["trades"].astype("float64").to_numpy(),
        },
        index=pd.DatetimeIndex(open_time.to_numpy(), name="open_time"),
    )
    return out.sort_index(kind="mergesort")


def complete_minute_grid(klines: pd.DataFrame, day: str) -> pd.DataFrame:
    """Reindex a day's klines onto the complete 1440-minute UTC grid.

    Binance daily kline archives observed for this study carry all 1440
    minutes, but a minute with no trades may in principle be absent. Missing
    minutes are filled with zero volume and a flat bar at the previous close
    (the standard no-trade convention), so that the baselines see a regular
    grid and a "5 minutes" window always means five calendar minutes. Minutes
    filled this way are flagged in the ``filled`` column.
    """

    start = pd.Timestamp(day)
    grid = pd.date_range(start, periods=MINUTES_PER_DAY, freq="min")
    out = klines.reindex(grid)
    out.index.name = "open_time"
    filled = out["close"].isna()
    if filled.any():
        out["close"] = out["close"].ffill()
        for col in ("open", "high", "low"):
            out[col] = out[col].where(~filled, out["close"])
        for col in ("volume", "quote_volume", "trades"):
            out[col] = out[col].fillna(0.0)
    out["filled"] = filled.to_numpy()
    return out


# --------------------------------------------------------------------------
# Upstream feature definitions
# --------------------------------------------------------------------------


def _pandas_freq(frequency: str) -> str:
    """Upstream label ('25S') -> pandas 2.x offset alias ('25s')."""

    if frequency not in UPSTREAM_ROLLING:
        raise KeyError("unknown upstream frequency %r" % (frequency,))
    return frequency.lower()


def _grouper(frequency: str) -> pd.Grouper:
    # origin='start_day' is the pandas default for a DatetimeIndex and is
    # stated explicitly here: 5/15/25 s all divide 86400 exactly, so chunk
    # boundaries are identical whether or not a warm-up prefix is present.
    return pd.Grouper(freq=_pandas_freq(frequency), origin="start_day")


def build_features(
    trades: pd.DataFrame,
    frequency: str,
    rolling_freq: int | None = None,
    round_dp: int | None = RELEASED_MATRIX_DECIMALS,
) -> pd.DataFrame:
    """Rebuild the upstream feature matrix from a trade frame.

    ``trades`` is the schema returned by :func:`read_agg_trades`: a
    DatetimeIndex of millisecond-floored UTC-naive timestamps with columns
    ``price``, ``btc_volume`` and ``side``.

    Returns a frame with columns ``date`` + :data:`FEATURES`, one row per
    non-empty chunk that survives the upstream ``dropna``, in chunk order.
    """

    if rolling_freq is None:
        rolling_freq = UPSTREAM_ROLLING[frequency]
    if not isinstance(trades.index, pd.DatetimeIndex):
        raise TypeError("trades must be indexed by a DatetimeIndex")
    if trades.index.tz is not None:
        raise ValueError("trades index must be tz-naive UTC (upstream parity)")

    buy = trades[trades["side"] == "buy"]
    if buy.empty:
        return _empty_feature_frame()

    grouper = _grouper(frequency)

    # ---- rush-order components -------------------------------------------
    # Upstream: count buy trades per identical timestamp, map count==1 -> 0
    # and count>1 -> 1, then per chunk sum the indicator and count the
    # distinct timestamps; chunks with no timestamp at all are dropped.
    per_timestamp = buy.groupby(level=0).size()
    rush_indicator = (per_timestamp > 1).astype("float64")
    rush_grouped = rush_indicator.groupby(grouper)
    rush_sum = rush_grouped.sum()
    rush_count = rush_grouped.count()
    rush_sum = rush_sum[rush_count != 0].dropna()

    # ---- per-chunk aggregates --------------------------------------------
    grouped = buy.groupby(grouper)
    trade_count = grouped["price"].count()
    trade_count = trade_count[trade_count != 0].dropna()
    volume_sum = grouped["btc_volume"].sum()
    volume_sum = volume_sum[volume_sum != 0].dropna()
    price_mean = grouped["price"].mean().dropna()
    price_max = grouped["price"].max().dropna()

    date = price_max.index
    # R7: upstream assembles positionally; we require exact alignment.
    for name, series in (
        ("std_rush_order/avg_rush_order", rush_sum),
        ("std_trades", trade_count),
        ("std_volume/avg_volume", volume_sum),
        ("std_price/avg_price", price_mean),
    ):
        if not series.index.equals(date):
            raise ValueError(
                "upstream component %s has a different chunk index than "
                "avg_price_max (%d vs %d rows); positional assembly would "
                "misalign" % (name, len(series), len(date)))

    def _pct_change(series: pd.Series) -> pd.Series:
        # fill_method=None is the pandas 2.x explicit form of the pandas 1.x
        # default behaviour on a series that contains no NaN (true here: all
        # component series were filtered/dropna'd above).
        return series.pct_change(fill_method=None)

    frame = pd.DataFrame(
        {
            "date": date,
            "std_rush_order": _pct_change(
                rush_sum.rolling(window=rolling_freq).std()).to_numpy(),
            "avg_rush_order": _pct_change(
                rush_sum.rolling(window=rolling_freq).mean()).to_numpy(),
            "std_trades": _pct_change(
                trade_count.rolling(window=rolling_freq).std()).to_numpy(),
            "std_volume": _pct_change(
                volume_sum.rolling(window=rolling_freq).std()).to_numpy(),
            "avg_volume": _pct_change(
                volume_sum.rolling(window=rolling_freq).mean()).to_numpy(),
            "std_price": _pct_change(
                price_mean.rolling(window=rolling_freq).std()).to_numpy(),
            "avg_price": _pct_change(
                price_mean.rolling(window=PRICE_LEVEL_WINDOW).mean()).to_numpy(),
            "avg_price_max": _pct_change(
                price_max.rolling(window=PRICE_LEVEL_WINDOW).mean()).to_numpy(),
            # Upstream normalises by 23 and 59, not 24 and 60. Reproduced
            # exactly: the frozen classifier was fitted on these values.
            "hour_sin": np.sin(2 * np.pi * date.hour / 23),
            "hour_cos": np.cos(2 * np.pi * date.hour / 23),
            "minute_sin": np.sin(2 * np.pi * date.minute / 59),
            "minute_cos": np.cos(2 * np.pi * date.minute / 59),
        }
    )
    frame = frame.dropna().reset_index(drop=True)
    if round_dp is not None:
        # R4: the released matrices carry 3 decimals; round after dropna, in
        # the same order as the upstream write path.
        frame[FEATURES] = frame[FEATURES].round(round_dp)
    return frame


def _empty_feature_frame() -> pd.DataFrame:
    data = {"date": pd.Series(dtype="datetime64[ns]")}
    for name in FEATURES:
        data[name] = pd.Series(dtype="float64")
    return pd.DataFrame(data)


def drop_nonfinite(frame: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """Drop rows with any non-finite feature (R6). Returns (frame, dropped)."""

    if frame.empty:
        return frame, 0
    finite = np.isfinite(frame[FEATURES].to_numpy(dtype="float64")).all(axis=1)
    dropped = int((~finite).sum())
    return frame.loc[finite].reset_index(drop=True), dropped


# --------------------------------------------------------------------------
# Pair-day driver
# --------------------------------------------------------------------------


@dataclass
class PairDayFeatures:
    """Reconstruction result and its structural metadata (no endpoint values)."""

    symbol: str
    day: str
    frequency: str
    frame: pd.DataFrame
    meta: dict = field(default_factory=dict)


def agg_trades_path(data_root: str, symbol: str, day: str) -> str:
    return os.path.join(data_root, "raw", "aggTrades", day,
                        "%s-aggTrades-%s.zip" % (symbol, day))


def klines_path(data_root: str, symbol: str, day: str) -> str:
    return os.path.join(data_root, "raw", "klines1m", day,
                        "%s-1m-%s.zip" % (symbol, day))


def _previous_days(day: str, count: int) -> list[str]:
    base = pd.Timestamp(day)
    return [(base - pd.Timedelta(days=n)).strftime("%Y-%m-%d")
            for n in range(count, 0, -1)]


def nonempty_chunk_count(trades: pd.DataFrame, frequency: str) -> int:
    """Number of chunks holding at least one buy trade (the rolling unit)."""

    buy = trades[trades["side"] == "buy"]
    if buy.empty:
        return 0
    return int(buy.groupby(_grouper(frequency))["price"].count().gt(0).sum())


def _collect_warmup(data_root, symbol, day, frequency, warmup_days,
                    rolling_freq):
    """Read preceding archived pair-days to warm the rolling window (R5).

    Returns ``(parts, used_days, chunks, sufficient, stop_reason)``.
    """

    if warmup_days == WARMUP_AUTO:
        limit = MAX_WARMUP_DAYS
        auto = True
    else:
        limit = max(0, int(warmup_days))
        auto = False

    parts, used = [], []
    chunks = 0
    stop_reason = "cap reached" if auto else "fixed warm-up"
    for previous in _previous_days(day, limit)[::-1]:  # nearest day first
        path = agg_trades_path(data_root, symbol, previous)
        if not os.path.exists(path):
            # A warm-up may not be carried across a gap in a continuous
            # stream (stream start, listing gap, unpublished day).
            stop_reason = "no archive for %s" % previous
            break
        parts.insert(0, read_agg_trades(path))
        used.insert(0, previous)
        if auto:
            chunks = nonempty_chunk_count(pd.concat(parts).sort_index(
                kind="mergesort"), frequency)
            if chunks >= rolling_freq:
                stop_reason = "rolling window full"
                break
    if not auto and parts:
        chunks = nonempty_chunk_count(
            pd.concat(parts).sort_index(kind="mergesort"), frequency)
    return parts, used, chunks, chunks >= rolling_freq, stop_reason


def build_pair_day_features(
    data_root: str,
    symbol: str,
    day: str,
    frequency: str,
    warmup_days=WARMUP_AUTO,
    round_dp: int | None = RELEASED_MATRIX_DECIMALS,
) -> PairDayFeatures:
    """Rebuild one pair-day's feature matrix at one frequency.

    Preceding archived pair-days are prepended to warm the rolling window
    (R5; ``warmup_days="auto"`` extends until the window is full, capped at
    :data:`MAX_WARMUP_DAYS`) and the returned frame is trimmed to the target
    UTC day. Metadata records only structural facts.
    """

    target_path = agg_trades_path(data_root, symbol, day)
    if not os.path.exists(target_path):
        raise FileNotFoundError(target_path)

    rolling_freq = UPSTREAM_ROLLING[frequency]
    parts, warmup_used, warmup_chunks, warmup_ok, stop_reason = _collect_warmup(
        data_root, symbol, day, frequency, warmup_days, rolling_freq)
    target = read_agg_trades(target_path)
    parts.append(target)
    trades = pd.concat(parts) if len(parts) > 1 else target
    trades = trades.sort_index(kind="mergesort")

    frame = build_features(trades, frequency, round_dp=round_dp)
    n_rows_all = len(frame)
    day_start = pd.Timestamp(day)
    day_end = day_start + pd.Timedelta(days=1)
    frame = frame.loc[(frame["date"] >= day_start) &
                      (frame["date"] < day_end)].reset_index(drop=True)
    n_rows_target = len(frame)
    frame, dropped = drop_nonfinite(frame)
    frame.insert(1, "symbol", symbol)

    meta = {
        "symbol": symbol,
        "day": day,
        "frequency": frequency,
        "rolling_freq": rolling_freq,
        "warmup_days_requested": warmup_days,
        "warmup_days_used": len(warmup_used),
        "warmup_chunks_available": int(warmup_chunks),
        "warmup_sufficient": bool(warmup_ok),
        "warmup_stop_reason": stop_reason,
        "n_trades_target_day": int(len(target)),
        "n_buy_trades_target_day": int((target["side"] == "buy").sum()),
        "n_rows_with_warmup": int(n_rows_all),
        "n_rows_target_day": int(n_rows_target),
        "n_rows_dropped_nonfinite": int(dropped),
        "n_rows_scored": int(len(frame)),
        "timestamp_unit_target": (
            detect_epoch_unit_of_file(target_path) if len(target) else ""),
        "tz_naive_utc": True,
        "round_dp": round_dp,
    }
    return PairDayFeatures(symbol=symbol, day=day, frequency=frequency,
                           frame=frame, meta=meta)


def detect_epoch_unit_of_file(zip_path: str) -> str:
    """Epoch unit of the first record of an archive (structural metadata)."""

    with zipfile.ZipFile(zip_path) as zf:
        names = zf.namelist()
        with zf.open(names[0]) as handle:
            first = handle.readline().decode("utf-8", "replace")
    fields = first.strip().split(",")
    if len(fields) < 6 or not _looks_numeric(fields[5].encode()):
        return ""
    return detect_epoch_unit(int(float(fields[5])))


def kline_coverage_meta(data_root: str, symbol: str, day: str) -> dict:
    """Structural cross-check of the 1-minute kline archive for a pair-day."""

    path = klines_path(data_root, symbol, day)
    if not os.path.exists(path):
        return {"symbol": symbol, "day": day, "kline_archive_present": False}
    klines = read_klines_1m(path)
    grid = complete_minute_grid(klines, day)
    return {
        "symbol": symbol,
        "day": day,
        "kline_archive_present": True,
        "n_kline_rows": int(len(klines)),
        "n_minutes_after_grid": int(len(grid)),
        "n_minutes_filled": int(grid["filled"].sum()),
        "first_open_time": klines.index.min().isoformat() if len(klines) else "",
        "last_open_time": klines.index.max().isoformat() if len(klines) else "",
        "monotonic_increasing": bool(klines.index.is_monotonic_increasing),
        "duplicated_open_times": int(klines.index.duplicated().sum()),
    }
