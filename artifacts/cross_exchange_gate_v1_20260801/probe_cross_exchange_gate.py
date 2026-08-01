#!/usr/bin/env python3
"""REF-2026-017 §6 cross-exchange gate probe (Amendment 5, Option A).

Frozen rule (locked_protocol_v1.0.md §6): probe 30 randomly sampled events
from published pump-event lists for >=2-venue minute-kline availability;
pre-registered go/no-go at >=60% clearance; otherwise the module is
permanently dropped and reported as such.

Event list: La Morgia et al. pump-and-dump-dataset (pump_telegram.csv),
github.com/SystemsLab-Sapienza/pump-and-dump-dataset @ d71250d.
Pair convention follows upstream downloader.py: <SYMBOL>/BTC.
Event timestamps (date + hour columns) are treated as UTC, matching the
upstream repository's naive-UTC handling.

Sampling: random.Random(20260723).sample(sorted_events, 30) where
sorted_events is the CSV rows sorted by the tuple
(symbol, group, date, hour, exchange) — i.e. CSV column order.

Venue checks (serial, >=0.5 s pause between HTTP requests):
  binance : HEAD data.binance.vision monthly 1m-kline zip for the pair and
            event month; HTTP 200 => minute klines retrievable.
  kucoin  : GET /api/v1/market/candles?type=1min for a 1 h window centred
            on the event; non-empty data => retrievable.
  gate    : GET /api/v4/spot/candlesticks interval=1m with from/to for the
            same 1 h window; non-empty array => retrievable.
  okx     : only attempted when both kucoin and gate fail; OKX
            history-candles has a limited lookback for 1m bars, so old
            events are expected to miss there (recorded, disclosed).

Clearance per event = minute klines actually retrievable from >=2 venues.

The script appends per-event rows to probe_results.csv as it goes and
skips events already present, so an interrupted run can be re-invoked
(synchronously) and will resume without re-querying finished events.
"""

import csv
import datetime as dt
import hashlib
import json
import os
import random
import sys
import time
import urllib.error
import urllib.request

SEED = 20260723
N_SAMPLE = 30
THRESHOLD = 0.60
PAUSE_S = 0.6
HTTP_TIMEOUT_S = 20

# Path to the local clone of the upstream event list. The input is pinned
# by upstream commit + sha256 (recorded in sample_manifest.json), so the
# local path itself is not part of the record.
EVENTS_CSV = os.environ.get("PUMP_EVENTS_CSV", "pump_telegram.csv")
UPSTREAM_URL = "https://github.com/SystemsLab-Sapienza/pump-and-dump-dataset"
UPSTREAM_COMMIT = "d71250d4cb055dde2d415c8cba38a0dcd6eb6e16"

OUT_DIR = os.path.dirname(os.path.abspath(__file__))
RESULTS_CSV = os.path.join(OUT_DIR, "probe_results.csv")
MANIFEST_JSON = os.path.join(OUT_DIR, "sample_manifest.json")
SUMMARY_JSON = os.path.join(OUT_DIR, "summary.json")

SAMPLING_CODE = (
    "rows = list(csv.DictReader(open(pump_telegram_csv)))\n"
    "sorted_events = sorted(rows, key=lambda r: (r['symbol'], r['group'],"
    " r['date'], r['hour'], r['exchange']))\n"
    "sample = random.Random(20260723).sample(sorted_events, 30)"
)

RESULT_FIELDS = [
    "event_idx", "symbol", "group", "date", "hour", "exchange", "pair",
    "event_utc",
    "binance_available", "binance_detail",
    "kucoin_available", "kucoin_detail",
    "gate_available", "gate_detail",
    "okx_available", "okx_detail",
    "venues_available", "cleared",
]

_last_request_t = [0.0]


def _http(url, method="GET"):
    """Rate-limited HTTP request. Returns (status_code, body_bytes_or_b'')."""
    wait = PAUSE_S - (time.monotonic() - _last_request_t[0])
    if wait > 0:
        time.sleep(wait)
    req = urllib.request.Request(
        url, method=method,
        headers={"User-Agent": "ref-2026-017-gate-probe/1.0"})
    try:
        with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT_S) as resp:
            body = b"" if method == "HEAD" else resp.read()
            return resp.status, body
    except urllib.error.HTTPError as e:
        try:
            body = e.read()
        except Exception:
            body = b""
        return e.code, body
    except Exception as e:  # DNS, timeout, TLS
        return -1, str(e).encode()
    finally:
        _last_request_t[0] = time.monotonic()


def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def event_epoch_utc(date_s, hour_s):
    t = dt.datetime.strptime(date_s + " " + hour_s, "%Y-%m-%d %H:%M")
    return int(t.replace(tzinfo=dt.timezone.utc).timestamp())


def check_binance(sym, date_s):
    pair = sym.upper() + "BTC"
    month = date_s[:7]
    url = ("https://data.binance.vision/data/spot/monthly/klines/"
           f"{pair}/1m/{pair}-1m-{month}.zip")
    status, _ = _http(url, method="HEAD")
    return status == 200, f"HEAD {status} {pair}-1m-{month}.zip"


def check_kucoin(sym, epoch):
    t0, t1 = epoch - 1800, epoch + 1800
    url = ("https://api.kucoin.com/api/v1/market/candles"
           f"?type=1min&symbol={sym.upper()}-BTC&startAt={t0}&endAt={t1}")
    status, body = _http(url)
    if status != 200:
        return False, f"HTTP {status}"
    try:
        j = json.loads(body)
    except Exception:
        return False, "bad json"
    data = j.get("data") or []
    if j.get("code") == "200000" and len(data) > 0:
        return True, f"{len(data)} candles"
    return False, f"code={j.get('code')} n=0"


def check_gate(sym, epoch):
    t0, t1 = epoch - 1800, epoch + 1800
    url = ("https://api.gateio.ws/api/v4/spot/candlesticks"
           f"?currency_pair={sym.upper()}_BTC&interval=1m&from={t0}&to={t1}")
    status, body = _http(url)
    if status != 200:
        try:
            label = json.loads(body).get("label", "")
        except Exception:
            label = ""
        return False, f"HTTP {status} {label}".strip()
    try:
        j = json.loads(body)
    except Exception:
        return False, "bad json"
    if isinstance(j, list) and len(j) > 0:
        return True, f"{len(j)} candles"
    return False, "n=0"


def check_okx(sym, epoch):
    after_ms = (epoch + 1800) * 1000
    url = ("https://www.okx.com/api/v5/market/history-candles"
           f"?instId={sym.upper()}-BTC&bar=1m&after={after_ms}&limit=60")
    status, body = _http(url)
    if status != 200:
        return False, f"HTTP {status}"
    try:
        j = json.loads(body)
    except Exception:
        return False, "bad json"
    data = j.get("data") or []
    if j.get("code") == "0" and len(data) > 0:
        # require candles actually inside the 1 h event window
        t0_ms, t1_ms = (epoch - 1800) * 1000, (epoch + 1800) * 1000
        in_win = [c for c in data if t0_ms <= int(c[0]) < t1_ms]
        if in_win:
            return True, f"{len(in_win)} candles in window"
        return False, f"{len(data)} candles all outside window (lookback)"
    return False, f"code={j.get('code')} n=0 (lookback limit likely)"


def load_sample():
    with open(EVENTS_CSV, newline="") as f:
        rows = list(csv.DictReader(f))
    sorted_events = sorted(
        rows, key=lambda r: (r["symbol"], r["group"], r["date"], r["hour"],
                             r["exchange"]))
    sample = random.Random(SEED).sample(sorted_events, N_SAMPLE)
    return rows, sorted_events, sample


def main():
    rows, _sorted_events, sample = load_sample()

    manifest = {
        "protocol_rule": "locked_protocol_v1.0.md §6",
        "amendment": "amendment_5_cross_exchange_gate.md (Option A: run late)",
        "seed": SEED,
        "n_sample": N_SAMPLE,
        "sort_key": "(symbol, group, date, hour, exchange) — CSV column order",
        "sampling_code": SAMPLING_CODE,
        "event_list": {
            "file": "pump_telegram.csv",
            "local_path": EVENTS_CSV,
            "upstream": UPSTREAM_URL,
            "upstream_commit": UPSTREAM_COMMIT,
            "raw_url": ("https://raw.githubusercontent.com/"
                        "SystemsLab-Sapienza/pump-and-dump-dataset/"
                        f"{UPSTREAM_COMMIT}/pump_telegram.csv"),
            "sha256": sha256_file(EVENTS_CSV),
            "n_events": len(rows),
        },
        "pair_convention": "upstream downloader.py: <SYMBOL>/BTC",
        "timestamp_convention": "date+hour treated as UTC",
        "sampled_events": [
            {"symbol": r["symbol"], "group": r["group"], "date": r["date"],
             "hour": r["hour"], "exchange": r["exchange"]}
            for r in sample
        ],
    }
    with open(MANIFEST_JSON, "w") as f:
        json.dump(manifest, f, indent=2)
        f.write("\n")

    done = {}
    if os.path.exists(RESULTS_CSV):
        with open(RESULTS_CSV, newline="") as f:
            for r in csv.DictReader(f):
                done[int(r["event_idx"])] = r
    new_file = not done
    out = open(RESULTS_CSV, "a", newline="")
    writer = csv.DictWriter(out, fieldnames=RESULT_FIELDS)
    if new_file:
        writer.writeheader()
        out.flush()

    results = []
    for idx, ev in enumerate(sample):
        if idx in done:
            results.append(done[idx])
            continue
        sym, date_s, hour_s = ev["symbol"], ev["date"], ev["hour"]
        epoch = event_epoch_utc(date_s, hour_s)
        b_ok, b_det = check_binance(sym, date_s)
        k_ok, k_det = check_kucoin(sym, epoch)
        g_ok, g_det = check_gate(sym, epoch)
        if not k_ok and not g_ok:
            o_ok, o_det = check_okx(sym, epoch)
        else:
            o_ok, o_det = False, "not attempted (kucoin/gate sufficed)"
        n_avail = sum([b_ok, k_ok, g_ok, o_ok])
        row = {
            "event_idx": idx, "symbol": sym, "group": ev["group"],
            "date": date_s, "hour": hour_s, "exchange": ev["exchange"],
            "pair": sym.upper() + "/BTC",
            "event_utc": dt.datetime.fromtimestamp(
                epoch, dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "binance_available": b_ok, "binance_detail": b_det,
            "kucoin_available": k_ok, "kucoin_detail": k_det,
            "gate_available": g_ok, "gate_detail": g_det,
            "okx_available": o_ok, "okx_detail": o_det,
            "venues_available": n_avail, "cleared": n_avail >= 2,
        }
        writer.writerow(row)
        out.flush()
        results.append(row)
        print(f"[{idx + 1:2d}/{N_SAMPLE}] {sym:>6} {date_s} "
              f"binance={b_ok} kucoin={k_ok} gate={g_ok} okx={o_ok} "
              f"cleared={n_avail >= 2}", flush=True)
    out.close()

    def truthy(v):
        return v is True or v == "True"

    n_clear = sum(1 for r in results if truthy(r["cleared"]))
    clearance = n_clear / N_SAMPLE
    verdict = "go" if clearance >= THRESHOLD else "no-go"
    summary = {
        "protocol_rule": "locked_protocol_v1.0.md §6",
        "amendment": "amendment_5_cross_exchange_gate.md (Option A: run late)",
        "n_sampled": N_SAMPLE,
        "n_cleared": n_clear,
        "clearance": round(clearance, 4),
        "threshold": THRESHOLD,
        "verdict": verdict,
        "verdict_meaning": (
            "proceed" if verdict == "go" else
            "module permanently dropped and reported as such, per the "
            "frozen >=60% rule"),
        "run_timestamp_utc": dt.datetime.now(dt.timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%SZ"),
        "lateness_disclosure": (
            "Probe was due 2026-07-30 (one week after the 2026-07-23 "
            "freeze). It was not run on time; the slip was disclosed in "
            "Amendment 5 on 2026-07-31 and the probe executed under its "
            "Option A within the amendment's 2026-08-02 resolution "
            "deadline. See run_timestamp_utc for the actual run time."),
    }
    with open(SUMMARY_JSON, "w") as f:
        json.dump(summary, f, indent=2)
        f.write("\n")
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
