"""
Spot price paths for the backtest: either loaded from a real CSV you supply,
or a synthetic GBM (geometric Brownian motion) path for smoke-testing the
harness. GBM has no momentum, no regimes, no fat tails — it exists only to
prove the plumbing works, not to estimate profitability.
"""
from __future__ import annotations

import csv
import math
import random
from dataclasses import dataclass
from typing import Dict, Iterator, List, Tuple


@dataclass
class SpotTick:
    ts: float                 # unix seconds
    spots: Dict[str, float]   # e.g. {"BTC": 65000.0, "ETH": 3400.0}


def load_csv(path: str, underlyings: List[str]) -> List[SpotTick]:
    """CSV columns expected: ts,BTC,ETH,... (ts = unix seconds, one row per
    snapshot, ascending time order). Extra columns are ignored."""
    ticks: List[SpotTick] = []
    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            ts = float(row["ts"])
            spots = {}
            for u in underlyings:
                if u in row and row[u] not in (None, ""):
                    spots[u] = float(row[u])
            if spots:
                ticks.append(SpotTick(ts=ts, spots=spots))
    ticks.sort(key=lambda t: t.ts)
    return ticks


def synthetic_gbm(
    underlyings: List[str],
    start_spots: Dict[str, float],
    n_ticks: int,
    interval_sec: float = 30.0,
    annual_vol: Dict[str, float] = None,
    annual_drift: Dict[str, float] = None,
    start_ts: float = None,
    seed: int = 7,
) -> List[SpotTick]:
    """Pure smoke-test data. Do not draw profitability conclusions from
    results on this — see chain_synth.py docstring."""
    rng = random.Random(seed)
    annual_vol = annual_vol or {u: 0.6 for u in underlyings}
    annual_drift = annual_drift or {u: 0.0 for u in underlyings}
    start_ts = start_ts if start_ts is not None else 1_700_000_000.0
    dt_years = interval_sec / (365.0 * 86400.0)

    spots = dict(start_spots)
    out: List[SpotTick] = []
    ts = start_ts
    for _ in range(n_ticks):
        for u in underlyings:
            vol = annual_vol.get(u, 0.6)
            mu = annual_drift.get(u, 0.0)
            z = rng.gauss(0, 1)
            spots[u] *= math.exp((mu - 0.5 * vol * vol) * dt_years + vol * math.sqrt(dt_years) * z)
        out.append(SpotTick(ts=ts, spots=dict(spots)))
        ts += interval_sec
    return out
