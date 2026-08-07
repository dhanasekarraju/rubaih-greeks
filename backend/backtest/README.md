# Rubaih Greeks — Backtest Harness

Replays historical (or synthetic) option-chain snapshots through your
**actual production strategy code** in `greeks_engine.OptionsCycle` —
`parse_ticker`, `filter_rank`, `pick_entries`, `arm`, `evaluate_exit`. This
is not a reimplementation. It's the same code, fed historical time instead
of wall-clock time, via three small backward-compatible edits to
`greeks_engine.py` (each new param defaults to `None` → `time.time()`, so
live behavior is byte-for-byte unchanged):

- `OptionsCycle.pick_entries(tickers, now=None)`
- `OptionsCycle.evaluate_exit(mark, trade=None, now=None)`
- `OptionsCycle.arm(sig, fill_premium=None, now=None)`
- `OptionsCycle._dte_days(row, symbol="", now=None)` / `parse_ticker(row, now=None)`
  — this one mattered most: DTE was being measured against real wall-clock
  time internally, which silently breaks the min/max DTE gate on any
  historical replay. Fixed and threaded through.

## Quick start

```bash
cd backend
pip install -r requirements.txt   # if not already installed
python3 -m backtest.run_demo
```

This runs ~7 simulated days of a synthetic GBM spot path through a
Black-Scholes option chain and your real strategy logic, then prints a
win rate / profit factor / expectancy / max drawdown report.

## What this proved (and a bug it caught)

The first version of the synthetic chain generator didn't encode expiry in
the instrument symbol — only strike. Two different-dated contracts at the
same rounded strike collided in the by-symbol lookup, so a held position
could get silently repriced against the wrong (longer-dated, far more
expensive) contract on the very next tick. Result: 100% win rate, every
trade hitting TP in one 30-second snapshot. That's the shape a backtest
bug takes — suspiciously good, not suspiciously bad. Fixed by keying
synthetic symbols on `(strike, absolute_expiry_ts)`.

After the fix, a fair-coin-flip random walk (GBM has no real momentum —
by construction) produces what it honestly should: win rate near 50%,
slightly negative expectancy once round-trip fees and spread are paid.
That's a good sign the harness itself is sound — it isn't manufacturing
edge that isn't there.

## What this does NOT prove

**Nothing here tells you whether the live strategy is profitable.** GBM
has no autocorrelation, no regimes, no volatility clustering, no fat
tails — real BTC/ETH have all four, and momentum strategies live or die
on exactly the structure GBM doesn't have. The synthetic option chain
uses a flat, slowly-drifting IV surface with no skew — real weekly crypto
options do not. Treat every synthetic-chain number as a plumbing check,
not a P&L estimate.

## Getting a real answer

Delta doesn't offer free historical options order-book data. Two paths:

1. **Start logging now.** `backtest/logger.py` has `SnapshotLogger` — wire
   one line into `main_loop` where tickers are already fetched (see the
   docstring in that file for the exact line and why it's not auto-wired
   for you — it touches the live trading loop and you should review it
   first). Every day you wait is a day of real data you won't have in
   three months.
2. **Once you have logged data**, replace the synthetic snapshot source in
   `run_demo.py` with `backtest.logger.load_jsonl("data/live_snapshots.jsonl")`
   and drop `reprice_fn` (real logged ticker data should carry your held
   symbol's live quote forward tick to tick, since Delta's feed doesn't
   stop listing a product just because it drifted off ATM).

You need on the order of 100+ closed trades from real data before the win
rate / expectancy numbers mean anything statistically. `report.py` prints
a reminder below that threshold.

## Files

| File | Purpose |
|---|---|
| `chain_synth.py` | Black-Scholes synthetic option chain generator |
| `spot_paths.py` | CSV loader for real spot history, or synthetic GBM |
| `runner.py` | `BacktestRunner` — the actual replay engine |
| `report.py` | Trade log → win rate / expectancy / drawdown stats |
| `logger.py` | `SnapshotLogger` — build a real dataset going forward |
| `run_demo.py` | End-to-end smoke test on synthetic data |
