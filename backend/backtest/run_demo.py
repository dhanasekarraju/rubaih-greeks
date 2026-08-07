"""
Smoke test: wires spot_paths -> chain_synth -> BacktestRunner -> report,
running your ACTUAL greeks_engine.OptionsCycle against a synthetic GBM
spot path and Black-Scholes option chain.

Run it:
    cd backend
    python3 -m backtest.run_demo

What this proves: the harness plumbs snapshots through parse_ticker /
filter_rank / pick_entries / arm / evaluate_exit correctly, fees and
slippage are charged, and stats compute correctly end-to-end.

What this does NOT prove: that the strategy makes money. GBM has no real
momentum structure and the option chain is a flat-IV Black-Scholes
surface, both of which are far kinder / far more random than real crypto
weeklies. See README.md before drawing any conclusion about profitability
from these numbers.
"""
from __future__ import annotations

import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import yaml

from backtest.chain_synth import ChainConfig, SyntheticChainGenerator
from backtest.runner import BacktestRunner, make_bs_reprice_fn
from backtest.report import compute_stats, print_report
from backtest.spot_paths import synthetic_gbm


def main():
    root = Path(__file__).resolve().parent.parent
    cfg = yaml.safe_load((root / "config.yaml").read_text())

    underlyings = [u.upper() for u in cfg["trading"]["underlyings"]]
    n_ticks = 20_000          # 20,000 x 30s ≈ 6.9 days of 30s snapshots
    interval_sec = 30.0

    spot_ticks = synthetic_gbm(
        underlyings=underlyings,
        start_spots={"BTC": 65000.0, "ETH": 3400.0},
        n_ticks=n_ticks,
        interval_sec=interval_sec,
        annual_vol={"BTC": 0.55, "ETH": 0.65},
        annual_drift={"BTC": 0.0, "ETH": 0.0},   # no drift: a fair coin flip on direction
        start_ts=time.time() - n_ticks * interval_sec,
        seed=7,
    )

    chain_gen = SyntheticChainGenerator(ChainConfig(underlyings=underlyings), seed=7)
    reprice_fn = make_bs_reprice_fn(lambda u, ts: chain_gen.current_iv(u))

    runner = BacktestRunner(cfg, reprice_fn=reprice_fn)

    def snapshots():
        for tick in spot_ticks:
            tickers = chain_gen.step(tick.ts, tick.spots)
            yield tick.ts, tickers

    runner.run(snapshots())

    stats = compute_stats(runner.trades, runner.equity_curve)
    print_report(stats)

    print(f"\nSnapshots processed: {n_ticks}")
    print(f"Sim duration: {n_ticks * interval_sec / 86400:.1f} days")
    if runner.trades:
        print("\nFirst 5 trades:")
        for t in runner.trades[:5]:
            print(
                f"  {t.symbol:20s} {t.option_type:4s} size={t.size:3d} "
                f"entry={t.entry_fill:.4f} exit={t.exit_fill:.4f} "
                f"net={t.net_pnl:+.4f} r={t.r_multiple:+.2f}R  {t.reason.split(':')[0]}"
            )


if __name__ == "__main__":
    main()
