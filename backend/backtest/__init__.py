"""
Rubaih Greeks — backtest harness.

Replays historical (or synthetic) option-chain snapshots through the *real*
production strategy code in greeks_engine.OptionsCycle — the same
parse_ticker / filter_rank / pick_entries / arm / evaluate_exit your live
bot uses. Nothing here is a re-implementation of the strategy; it is the
strategy, fed historical time instead of wall-clock time.

Modules:
    chain_synth.py  — Black-Scholes synthetic option chain generator, driven
                       by a spot price path you supply. Useful for (a)
                       proving the harness is wired correctly end-to-end and
                       (b) rough what-if sweeps. NOT a substitute for real
                       historical option order-book data — see README.md.
    spot_paths.py    — loads a real historical spot series from CSV, or
                       generates a synthetic GBM path for smoke-testing.
    runner.py        — BacktestRunner: feeds snapshots to OptionsCycle in
                       chronological order, applies slippage/fees, logs
                       every trade.
    report.py         — turns a trade log into win rate, expectancy (R),
                       profit factor, max drawdown, and an equity curve.
    logger.py         — SnapshotLogger: append real live ticker snapshots
                       to a JSONL file from the running bot, so you build a
                       real historical dataset over time.
"""
