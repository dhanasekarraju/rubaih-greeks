"""
SnapshotLogger — append real live ticker snapshots to a JSONL file, so every
dry-run (or live) scan cycle builds a real historical dataset you can later
run through runner.py with actual Delta order-book data instead of a
synthetic chain.

This is the single highest-value thing you can do before trusting any
backtest number: Delta doesn't sell/offer historical options tick data for
free, so "real history" for this strategy only exists from the point you
start logging it. Wire this in now; every day you wait is a day of data
you'll wish you had in three months.

Wiring it into the live engine (one line, in main_loop right where tickers
are fetched — NOT changed for you automatically, since it touches the live
trading loop and you should review it first):

    from backtest.logger import SnapshotLogger
    _snap_logger = SnapshotLogger("data/live_snapshots.jsonl")
    ...
    tickers = await self.client.get_option_tickers(...)
    _snap_logger.log(time.time(), tickers)   # <-- add this line
    entries = self.cycle.pick_entries(tickers or [])
"""
from __future__ import annotations

import json
import os
from typing import Dict, List


class SnapshotLogger:
    def __init__(self, path: str):
        self.path = path
        d = os.path.dirname(path)
        if d:
            os.makedirs(d, exist_ok=True)

    def log(self, ts: float, tickers: List[Dict]) -> None:
        try:
            with open(self.path, "a") as f:
                f.write(json.dumps({"ts": ts, "tickers": tickers}) + "\n")
        except OSError:
            # Never let logging failures interrupt live trading.
            pass


def load_jsonl(path: str):
    """Yields (ts, tickers) tuples in file order. Caller must ensure the
    file is chronologically sorted (SnapshotLogger appends in order, so a
    file it produced always is)."""
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            row = json.loads(line)
            yield float(row["ts"]), row["tickers"]
