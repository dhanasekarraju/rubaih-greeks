"""Turn a BacktestRunner's trade log + equity curve into the numbers that
actually answer 'does this have edge' — win rate, expectancy in R,
profit factor, max drawdown. Nothing here should be surprising; the point
is to stop eyeballing scrollback logs and get one honest printout."""
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Tuple

from .runner import TradeRecord


@dataclass
class Stats:
    n_trades: int
    n_wins: int
    n_losses: int
    win_rate: float
    gross_profit: float
    gross_loss: float
    profit_factor: Optional[float]
    net_pnl: float
    avg_r: Optional[float]
    expectancy_quote: float
    max_drawdown_quote: float
    max_drawdown_pct: float
    avg_hold_hours: float


def compute_stats(trades: List[TradeRecord], equity_curve: List[Tuple[float, float]]) -> Stats:
    n = len(trades)
    wins = [t for t in trades if t.net_pnl > 0]
    losses = [t for t in trades if t.net_pnl <= 0]
    gross_profit = sum(t.net_pnl for t in wins)
    gross_loss = -sum(t.net_pnl for t in losses)
    net_pnl = sum(t.net_pnl for t in trades)
    r_values = [t.r_multiple for t in trades if t.r_multiple is not None]

    peak = float("-inf")
    max_dd = 0.0
    max_dd_pct = 0.0
    for _, eq in equity_curve:
        peak = max(peak, eq)
        if peak > 0:
            dd = peak - eq
            dd_pct = dd / peak if peak > 0 else 0.0
            max_dd = max(max_dd, dd)
            max_dd_pct = max(max_dd_pct, dd_pct)

    avg_hold_hours = (sum(t.hold_sec for t in trades) / n / 3600.0) if n else 0.0

    return Stats(
        n_trades=n,
        n_wins=len(wins),
        n_losses=len(losses),
        win_rate=(len(wins) / n) if n else 0.0,
        gross_profit=gross_profit,
        gross_loss=gross_loss,
        profit_factor=(gross_profit / gross_loss) if gross_loss > 0 else None,
        net_pnl=net_pnl,
        avg_r=(sum(r_values) / len(r_values)) if r_values else None,
        expectancy_quote=(net_pnl / n) if n else 0.0,
        max_drawdown_quote=max_dd,
        max_drawdown_pct=max_dd_pct,
        avg_hold_hours=avg_hold_hours,
    )


def print_report(stats: Stats, quote_ccy: str = "USDT") -> None:
    print("=" * 60)
    print("BACKTEST REPORT")
    print("=" * 60)
    print(f"Trades:            {stats.n_trades}  ({stats.n_wins}W / {stats.n_losses}L)")
    print(f"Win rate:          {stats.win_rate:.1%}")
    pf = f"{stats.profit_factor:.2f}" if stats.profit_factor is not None else "n/a (no losses yet)"
    print(f"Profit factor:     {pf}")
    avg_r = f"{stats.avg_r:+.2f}R" if stats.avg_r is not None else "n/a"
    print(f"Avg R multiple:    {avg_r}")
    print(f"Net PnL:           {stats.net_pnl:+.4f} {quote_ccy}")
    print(f"Expectancy/trade:  {stats.expectancy_quote:+.4f} {quote_ccy}")
    print(f"Max drawdown:      {stats.max_drawdown_quote:.4f} {quote_ccy} ({stats.max_drawdown_pct:.1%})")
    print(f"Avg hold time:     {stats.avg_hold_hours:.1f}h")
    print("=" * 60)
    if stats.n_trades < 100:
        print(
            f"NOTE: {stats.n_trades} trades is not enough to trust this win rate. "
            "Treat this as a plumbing check until you have 100+ trades of real data."
        )
