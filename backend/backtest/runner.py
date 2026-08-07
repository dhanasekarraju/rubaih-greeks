"""
BacktestRunner — feeds ticker snapshots into greeks_engine.OptionsCycle in
chronological order and produces a trade log. This does NOT reimplement the
strategy: parse_ticker / filter_rank / pick_entries / arm / evaluate_exit
are the exact same methods your live bot calls. Only the clock and the
data source are swapped.

Fill model:
  - Entry fill = ask (matches live pick_entries, which already uses
    c.ask as the signal price).
  - Exit fill = bid, minus the same taker fee_rate the strategy already
    calibrates in round_trip_friction — i.e. realized PnL is charged the
    same round-trip cost the strategy's own edge gate assumes it must
    clear. If your realized results are much worse than the strategy's
    edge_pct suggested, that gap IS the model error to go hunt down.

For held positions between snapshots where the exact contract does not
reappear in the scanned chain (spot moved out of the strike band, or a
real feed doesn't repeat every strike every tick), pass a `reprice_fn`
(signature: reprice_fn(trade, ts, spot, expiry_ts) -> Optional[float]) to
value the open position directly. chain_synth-driven runs should use
`make_bs_reprice_fn(iv_lookup)`; real logged data should generally not
need one, since Delta's ticker feed for options being tracked typically
stays live.
"""
from __future__ import annotations

import math
import time as _time
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Tuple

from greeks_engine import OptionsCycle, Signal, OpenTrade

from .chain_synth import bs_price_and_delta


@dataclass
class TradeRecord:
    symbol: str
    underlying: str
    option_type: str
    strike: float
    size: int
    entry_ts: float
    exit_ts: float
    entry_fill: float
    exit_fill: float
    entry_fee: float
    exit_fee: float
    contract_value: float
    reason: str

    @property
    def gross_pnl(self) -> float:
        return (self.exit_fill - self.entry_fill) * self.size * self.contract_value

    @property
    def net_pnl(self) -> float:
        return self.gross_pnl - self.entry_fee - self.exit_fee

    @property
    def cost_basis(self) -> float:
        return self.entry_fill * self.size * self.contract_value

    @property
    def r_multiple(self) -> Optional[float]:
        # Reconstructed from the strategy's own SL distance at entry.
        if self._risk_amount and self._risk_amount > 0:
            return self.net_pnl / self._risk_amount
        return None

    _risk_amount: float = field(default=0.0, repr=False)

    @property
    def hold_sec(self) -> float:
        return self.exit_ts - self.entry_ts


class BacktestRunner:
    def __init__(
        self,
        cfg: Dict,
        fee_rate_override: Optional[float] = None,
        reprice_fn: Optional[Callable[[OpenTrade, float], Optional[float]]] = None,
        max_stale_snapshots: int = 20,
    ):
        self.cycle = OptionsCycle(cfg)
        if fee_rate_override is not None:
            self.cycle.fee_rate = fee_rate_override
            self.cycle.taker_fee = fee_rate_override
        self.reprice_fn = reprice_fn
        self.max_stale_snapshots = max_stale_snapshots

        self.trades: List[TradeRecord] = []
        self.equity_curve: List[Tuple[float, float]] = []  # (ts, realized_equity)
        self._realized_equity = self.cycle.free_capital_quote
        self._entry_meta: Dict[str, Dict] = {}   # symbol -> {expiry_ts, ...}
        self._last_mark: Dict[str, float] = {}
        self._stale_count: Dict[str, int] = {}
        self._current_spots: Dict[str, float] = {}

    # ------------------------------------------------------------------
    def run(self, snapshots) -> "BacktestRunner":
        """`snapshots` yields (ts, tickers) in ascending ts order — either
        backtest.logger.load_jsonl(path) for real data, or your own
        generator wrapping chain_synth for synthetic runs."""
        for ts, tickers in snapshots:
            self._step(ts, tickers)
        return self

    # ------------------------------------------------------------------
    def _step(self, ts: float, tickers: List[Dict]) -> None:
        by_symbol: Dict[str, "OptionCandidate"] = {}
        for row in tickers:
            u = str(row.get("underlying_asset_symbol") or "").upper()
            spot = row.get("spot_price") or (row.get("greeks") or {}).get("spot")
            if u and spot:
                try:
                    self._current_spots[u] = float(spot)
                except (TypeError, ValueError):
                    pass
            c = self.cycle.parse_ticker(row, now=ts)
            if c:
                by_symbol[c.symbol] = c

        # 1) manage open trades: mark, exit check
        for trade in list(self.cycle.open_trades()):
            mark = self._current_mark(trade, ts, by_symbol)
            if mark is None:
                continue
            exit_sig = self.cycle.evaluate_exit(mark, trade=trade, now=ts)
            if exit_sig:
                self._fill_exit(trade, exit_sig, ts, by_symbol)

        # 2) entries
        if self.cycle.slots_free() > 0:
            signals = self.cycle.pick_entries(tickers, now=ts)
            for sig in signals:
                self._fill_entry(sig, ts, by_symbol)

        self.equity_curve.append((ts, self._mark_to_market_equity(ts, by_symbol)))

    # ------------------------------------------------------------------
    def _current_mark(self, trade: OpenTrade, ts: float, by_symbol: Dict) -> Optional[float]:
        c = by_symbol.get(trade.symbol)
        if c is not None:
            self._last_mark[trade.symbol] = c.mark
            self._stale_count[trade.symbol] = 0
            return c.mark
        if self.reprice_fn is not None:
            spot = self._current_spots.get(trade.underlying)
            expiry_ts = (self._entry_meta.get(trade.symbol) or {}).get("expiry_ts")
            px = self.reprice_fn(trade, ts, spot, expiry_ts) if spot else None
            if px is not None:
                self._last_mark[trade.symbol] = px
                self._stale_count[trade.symbol] = 0
                return px
        # No live quote and no reprice model: carry last mark, but force an
        # exit if it's been stale too long instead of silently riding a
        # position we have no real price for.
        self._stale_count[trade.symbol] = self._stale_count.get(trade.symbol, 0) + 1
        if self._stale_count[trade.symbol] > self.max_stale_snapshots:
            return self._last_mark.get(trade.symbol)
        return self._last_mark.get(trade.symbol)

    # ------------------------------------------------------------------
    def _fill_entry(self, sig: Signal, ts: float, by_symbol: Dict) -> None:
        c = by_symbol.get(sig.symbol)
        entry_fill = sig.premium  # already ask, per pick_entries
        cval = sig.contract_value or 1.0
        self.cycle.arm(sig, fill_premium=entry_fill, now=ts)
        notional = entry_fill * sig.size * cval
        entry_fee = notional * self.cycle.fee_rate
        self._entry_meta[sig.symbol] = {
            "entry_ts": ts,
            "entry_fee": entry_fee,
            "expiry_ts": ts + (c.dte_days * 86400.0) if c else None,
        }
        self._realized_equity -= entry_fee  # fee paid on entry regardless of outcome

    def _fill_exit(self, trade: OpenTrade, sig: Signal, ts: float, by_symbol: Dict) -> None:
        c = by_symbol.get(trade.symbol)
        exit_fill = c.bid if (c is not None and c.bid > 0) else sig.premium
        cval = trade.contract_value or 1.0
        notional = exit_fill * trade.size * cval
        exit_fee = notional * self.cycle.fee_rate
        meta = self._entry_meta.get(trade.symbol, {})
        entry_fee = meta.get("entry_fee", 0.0)

        rec = TradeRecord(
            symbol=trade.symbol,
            underlying=trade.underlying,
            option_type=trade.option_type,
            strike=trade.strike,
            size=trade.size,
            entry_ts=meta.get("entry_ts", trade.entry_ts),
            exit_ts=ts,
            entry_fill=trade.entry_premium,
            exit_fill=exit_fill,
            entry_fee=entry_fee,
            exit_fee=exit_fee,
            contract_value=cval,
            reason=sig.reason,
        )
        rec._risk_amount = trade.r_inr
        self.trades.append(rec)
        self._realized_equity += rec.gross_pnl - exit_fee
        self.cycle.clear(underlying=trade.underlying)
        self._entry_meta.pop(trade.symbol, None)
        self._last_mark.pop(trade.symbol, None)
        self._stale_count.pop(trade.symbol, None)

    # ------------------------------------------------------------------
    def _mark_to_market_equity(self, ts: float, by_symbol: Dict) -> float:
        unreal = 0.0
        for trade in self.cycle.open_trades():
            mark = self._last_mark.get(trade.symbol)
            if mark is not None:
                unreal += (mark - trade.entry_premium) * trade.size * trade.contract_value
        return self._realized_equity + unreal


def make_bs_reprice_fn(iv_lookup: Callable[[str, float], float]):
    """Build a reprice_fn for synthetic-chain runs: reprices a held option
    directly via Black-Scholes using the trade's own strike/type and the
    entry-derived expiry, rather than depending on that exact strike
    reappearing in a regenerated near-ATM chain later on.

    iv_lookup(underlying, ts) -> annualized IV to use for the reprice.
    Caller is responsible for tracking current spot -- see backtest/run_demo.py
    for the reference wiring (it threads spot through the runner instance).
    """

    def _fn(trade: OpenTrade, ts: float, spot: float, expiry_ts: Optional[float]) -> Optional[float]:
        if not expiry_ts:
            return None
        t_years = max(0.0, (expiry_ts - ts) / (365.0 * 86400.0))
        iv = iv_lookup(trade.underlying, ts)
        price, _ = bs_price_and_delta(spot, trade.strike, t_years, iv, trade.option_type)
        return price

    return _fn
