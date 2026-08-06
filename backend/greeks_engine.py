"""
================================================================================
RUBAIH GREEKS — Delta options cycle engine
================================================================================
Same type as Rubaih futures: flat → scan → buy → TP/SL/trail → flat
Buy-side only (no premium selling) in v1.
LIVE_TRADING must be true for real Delta orders.
================================================================================
"""

from __future__ import annotations

import asyncio
import json
import math
import re
import os
import secrets
import time
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import asyncpg
import redis.asyncio as aioredis
import yaml
from dotenv import load_dotenv

from ai_advisor import GreeksAI, AIDecision, ai_configured
from command_bus import verify_command
from delta_client import DeltaClient, DeltaAPIError, env_delta_client

load_dotenv()

ROOT = Path(__file__).resolve().parent
CFG = yaml.safe_load((ROOT / "config.yaml").read_text())

LIVE_TRADING = os.getenv("LIVE_TRADING", "false").strip().lower() in ("1", "true", "yes")
COMMAND_SECRET = (
    os.getenv("RUBAIH_GREEKS_API_TOKEN")
    or os.getenv("RUBAIH_API_TOKEN")
    or ""
).strip()
FREE_SEED = float(
    os.getenv("RUBAIH_GREEKS_FREE_INR")
    or os.getenv("FREE_CAPITAL_INR")
    or 0
)


def _f(v, default=0.0) -> float:
    try:
        if v is None or v == "":
            return float(default)
        return float(v)
    except (TypeError, ValueError):
        return float(default)


@dataclass
class OptionCandidate:
    symbol: str
    product_id: int
    underlying: str
    option_type: str  # call / put
    strike: float
    mark: float
    bid: float
    ask: float
    spot: float
    dte_days: float
    spread_pct: float
    contract_value: float = 1.0
    unit_cost: float = 0.0  # approx quote currency cost for 1 contract
    delta: float = 0.0  # raw signed delta from exchange
    score: float = 0.0
    friction_pct: float = 0.0  # round-trip spread + fees, as a share of premium
    edge_pct: float = 0.0  # delta-implied premium move from current momentum


@dataclass
class OpenTrade:
    symbol: str
    product_id: int
    underlying: str
    option_type: str
    strike: float
    size: int
    entry_premium: float
    entry_ts: float
    tp: float
    sl: float
    r_inr: float
    premium_budget: float
    contract_value: float = 1.0
    peak_pnl: float = 0.0
    peak_premium: float = 0.0
    side: str = "buy"


@dataclass
class Signal:
    action: str  # BUY / SELL
    symbol: str
    product_id: int
    size: int
    reason: str
    premium: float = 0.0
    underlying: str = ""
    option_type: str = ""
    strike: float = 0.0
    contract_value: float = 1.0


class OptionsCycle:
    """Scan liquid near-ATM options; exit on premium TP/SL/trail/time."""

    def __init__(self, cfg: Dict):
        t = cfg.get("trading") or {}
        s = t.get("strategy") or {}
        r = cfg.get("risk") or {}
        self.underlyings = [u.upper() for u in (t.get("underlyings") or ["BTC", "ETH"])]
        self.allow_sell_premium = bool(t.get("allow_sell_premium", False))
        self.capital_inr = float(t.get("capital_inr", 1000))
        self.usdt_inr = float(t.get("usdt_inr", 87))
        # Canonical sizing unit is quote currency (USDT for Delta options).
        # free_capital_inr remains a synchronized legacy alias for API/mobile.
        self.free_capital_quote = self.capital_inr / max(self.usdt_inr, 1.0)
        self.free_capital_inr = self.free_capital_quote
        self.margin_use_frac = float(t.get("margin_use_frac", 0.20))
        self.margin_use_max_frac = float(t.get("margin_use_max_frac", 0.25))
        self.max_premium_budget = float(t.get("max_premium_budget_usdt", 5.0))
        self.max_open_underlyings = max(1, int(t.get("max_open_underlyings", 2)))
        self.one_per_underlying = bool(t.get("one_per_underlying", True))
        self.min_interval = float(t.get("min_entry_interval_sec", 120))
        self.entry_cooldown = float(s.get("entry_cooldown_sec", 180))
        self.lookback = int(s.get("momentum_lookback", 20))
        self.entry_move_pct = float(s.get("entry_move_pct", 0.0015))
        self.min_dte = float(s.get("min_dte_days", 1))
        self.max_dte = float(s.get("max_dte_days", 7))
        self.atm_band = float(s.get("atm_band_pct", 0.02))
        self.max_spread = float(s.get("max_spread_pct", 0.03))
        self.min_mark = float(s.get("min_mark_premium", 0.15))
        self.min_delta = float(s.get("min_delta", 0.25))
        self.max_delta = float(s.get("max_delta", 0.60))
        self.target_delta = float(s.get("target_delta", 0.40))
        self.tp_pct = float(s.get("take_profit_premium_pct", 0.50))
        self.sl_pct = float(s.get("stop_loss_premium_pct", 0.25))
        self.max_loss_frac = float(s.get("max_loss_frac", 0.30))
        self.min_edge_multiple = max(0.0, float(s.get("min_edge_multiple", 1.5)))
        self.trail_arm_r = float(s.get("trail_arm_r", 0.7))
        self.trail_giveback_r = float(s.get("trail_giveback_r", 0.4))
        self.trail_giveback_of_peak = float(s.get("trail_giveback_of_peak", 0.25))
        self.max_hold_sec = float(s.get("max_hold_sec", 14400))
        self.max_drawdown_pct = float(r.get("max_drawdown_pct", 0.15))
        self.max_daily_loss_frac = float(r.get("max_daily_loss_frac", 0.25))
        self.kill_on_drawdown = bool(r.get("kill_switch_on_drawdown", True))
        self.halt_confirm_readings = max(1, int(r.get("halt_confirm_readings", 2)))
        self.auto_resume = bool(r.get("auto_resume", True))
        self.halt_cooldown_sec = max(0.0, float(r.get("halt_cooldown_min", 60)) * 60.0)
        self.bot_allocation = float(r.get("bot_allocation_quote", 0) or 0)
        self.min_free_quote = max(0.0, float(r.get("min_free_quote", 1.0)))
        self.max_total_loss_frac = max(0.0, float(r.get("max_total_loss_frac", 0.35)))
        self.halt_recover_frac = min(1.0, max(0.0, float(r.get("halt_recover_frac", 0.7))))
        self.taker_fee = float((cfg.get("exchange") or {}).get("taker_fee", 0.014))
        # Recalibrated from real fills; the config value is only a seed.
        self.fee_rate = self.taker_fee
        self._fee_obs = 0
        # Product ids the operator holds manually. The bot must not size, exit,
        # or reconcile against positions it did not open.
        self.excluded_pids: Set[int] = set()
        self._spot_hist: Dict[str, List[float]] = {u: [] for u in self.underlyings}
        self._last_signal = 0.0
        self._last_signal_by_u: Dict[str, float] = {}
        self._last_hold_log = 0.0
        self._last_reject: Dict[str, int] = {}
        # One live option per underlying (BTC and/or ETH).
        self.trades: Dict[str, OpenTrade] = {}

    @property
    def trade(self) -> Optional[OpenTrade]:
        """Primary open trade (first by underlying name). Prefer open_trades()."""
        if not self.trades:
            return None
        for u in self.underlyings:
            if u in self.trades:
                return self.trades[u]
        return next(iter(self.trades.values()), None)

    def open_trades(self) -> List[OpenTrade]:
        return list(self.trades.values())

    def trade_for(self, underlying: str) -> Optional[OpenTrade]:
        return self.trades.get((underlying or "").upper())

    def trade_by_product(self, product_id: int) -> Optional[OpenTrade]:
        pid = int(product_id or 0)
        if pid <= 0:
            return None
        for t in self.trades.values():
            if int(t.product_id or 0) == pid:
                return t
        return None

    def trade_by_symbol(self, symbol: str) -> Optional[OpenTrade]:
        sym = str(symbol or "")
        for t in self.trades.values():
            if t.symbol == sym:
                return t
        return None

    def own_product_ids(self) -> Set[int]:
        return {int(t.product_id or 0) for t in self.trades.values() if int(t.product_id or 0)}

    def slots_open(self) -> int:
        return len(self.trades)

    def slots_free(self) -> int:
        return max(0, self.max_open_underlyings - len(self.trades))

    def note_fee_observation(self, fee: float, premium_notional: float):
        """Learn the true taker rate from settled fills.

        Delta charges options fees against underlying notional, so a rate
        expressed as a share of premium is only knowable after the fact.
        """
        if fee <= 0 or premium_notional <= 0:
            return
        rate = fee / premium_notional
        if not 0.0 < rate < 0.5:
            return
        self._fee_obs += 1
        # Converge fast off the config seed, then smooth.
        alpha = 0.5 if self._fee_obs <= 3 else 0.25
        self.fee_rate = (1.0 - alpha) * self.fee_rate + alpha * rate

    def round_trip_friction(self, spread_pct: float) -> float:
        """Cost of a full round trip as a share of premium.

        The spread is crossed on the way in and on the way out, and the taker
        fee is charged on both legs.
        """
        return max(0.0, spread_pct) + 2.0 * max(0.0, self.fee_rate)

    def set_free_capital(self, free: float):
        value = max(0.0, float(free or 0.0))
        self.free_capital_quote = value
        self.free_capital_inr = value

    def budget(self) -> float:
        """Largest single-coin budget available right now (SCAN display)."""
        caps = [self.budget_for(u) for u in self.underlyings]
        return max(caps) if caps else 0.0

    def budget_for(self, underlying: str) -> float:
        """Per-coin premium budget. Empty slot → up to max_premium_budget of free."""
        u = (underlying or "").upper()
        if not u:
            return 0.0
        if self.one_per_underlying and u in self.trades:
            return 0.0
        if self.slots_free() <= 0:
            return 0.0
        free = max(self.free_capital_quote, 0.0)
        cap = float(self.max_premium_budget or 0.0)
        if cap <= 0:
            return free
        return min(free, cap)

    def size_contracts(self, unit_cost: float, budget: Optional[float] = None) -> int:
        """Integer contracts whose premium notional fits budget."""
        b = self.budget() if budget is None else float(budget)
        if unit_cost <= 0 or b <= 0:
            return 0
        n = int(math.floor(b / unit_cost))
        return max(0, n)

    def note_spot(self, underlying: str, spot: float):
        if spot <= 0:
            return
        u = underlying.upper()
        hist = self._spot_hist.setdefault(u, [])
        hist.append(spot)
        if len(hist) > self.lookback * 3:
            del hist[: len(hist) - self.lookback * 3]

    def momentum(self, underlying: str) -> float:
        hist = self._spot_hist.get(underlying.upper()) or []
        need = max(5, self.lookback // 2)
        if len(hist) < need:
            return 0.0
        n = min(self.lookback, len(hist))
        a, b = hist[-n], hist[-1]
        if a <= 0:
            return 0.0
        return (b - a) / a

    @staticmethod
    def _expiry_ts_from_symbol(symbol: str) -> Optional[float]:
        # Delta symbols end with DDMMYY e.g. C-BTC-65000-010826
        # Expiry is 12:00 UTC (not midnight) — midnight DTE empties the 2–5d window.
        m = re.search(r"-(\d{6})$", symbol or "")
        if not m:
            return None
        try:
            dt = datetime.strptime(m.group(1), "%d%m%y").replace(
                tzinfo=timezone.utc, hour=12, minute=0, second=0, microsecond=0
            )
            return dt.timestamp()
        except ValueError:
            return None

    @staticmethod
    def _parse_expiry_ts(row: Dict, symbol: str = "") -> Optional[float]:
        for key in ("settlement_time", "expiry_time", "expiration_time", "expiry"):
            raw = row.get(key)
            if raw is None and isinstance(row.get("product"), dict):
                raw = row["product"].get(key)
            if raw is None:
                continue
            if isinstance(raw, (int, float)):
                v = float(raw)
                if v > 1e14:
                    return v / 1e9
                if v > 1e11:
                    return v / 1e3
                return v
            if isinstance(raw, str) and raw.strip():
                try:
                    return datetime.fromisoformat(raw.replace("Z", "+00:00")).timestamp()
                except ValueError:
                    pass
        return OptionsCycle._expiry_ts_from_symbol(symbol)

    def _dte_days(self, row: Dict, symbol: str = "") -> float:
        ts = self._parse_expiry_ts(row, symbol)
        if not ts:
            return -1.0
        return max(0.0, (ts - time.time()) / 86400.0)

    def parse_ticker(self, row: Dict) -> Optional[OptionCandidate]:
        if str(row.get("product_trading_status") or "operational").lower() not in (
            "operational",
            "active",
            "",
        ):
            return None
        quotes = row.get("quotes") or {}
        greeks = row.get("greeks") or {}
        product = row.get("product") or {}
        symbol = str(row.get("symbol") or product.get("symbol") or "")
        if not symbol:
            return None
        contract = str(row.get("contract_type") or product.get("contract_type") or "").lower()
        if "call" in contract:
            otype = "call"
        elif "put" in contract:
            otype = "put"
        else:
            return None
        underlying = str(row.get("underlying_asset_symbol") or "").upper()
        if underlying not in self.underlyings:
            for u in self.underlyings:
                if f"-{u}-" in symbol.upper() or symbol.upper().startswith(
                    ("C-" + u, "P-" + u)
                ):
                    underlying = u
                    break
        if underlying not in self.underlyings:
            return None
        mark = _f(row.get("mark_price") or row.get("close"))
        bid = _f(quotes.get("best_bid") or mark)
        ask = _f(quotes.get("best_ask") or mark)
        spot = _f(row.get("spot_price") or greeks.get("spot"))
        strike = _f(row.get("strike_price") or product.get("strike_price"))
        pid = int(row.get("product_id") or product.get("id") or 0)
        cval = _f(row.get("contract_value") or product.get("contract_value"), 1.0)
        if cval <= 0:
            cval = 1.0
        unit_cost = mark * cval
        if pid <= 0 or strike <= 0 or mark <= 0 or unit_cost < self.min_mark:
            return None
        mid = (bid + ask) / 2 if bid > 0 and ask > 0 else mark
        spread = abs(ask - bid) / mid if mid > 0 else 1.0
        dte = self._dte_days(row, symbol)
        if dte < 0:
            return None
        delta = _f(greeks.get("delta") or row.get("delta"))
        return OptionCandidate(
            symbol=symbol,
            product_id=pid,
            underlying=underlying,
            option_type=otype,
            strike=strike,
            mark=mark,
            bid=bid,
            ask=ask,
            spot=spot if spot > 0 else strike,
            dte_days=dte,
            spread_pct=spread,
            contract_value=cval,
            unit_cost=unit_cost,
            delta=delta,
        )

    def filter_rank(self, cands: List[OptionCandidate]) -> List[OptionCandidate]:
        ranked: List[OptionCandidate] = []
        self._last_reject = {
            "dte": 0,
            "spread": 0,
            "atm": 0,
            "delta": 0,
            "mom": 0,
            "side": 0,
            "held": 0,
            "edge": 0,
            "pass": 0,
        }
        for c in cands:
            if c.product_id in self.excluded_pids:
                self._last_reject["held"] += 1
                continue
            if c.dte_days < self.min_dte or c.dte_days > self.max_dte:
                self._last_reject["dte"] += 1
                continue
            if c.spread_pct > self.max_spread:
                self._last_reject["spread"] += 1
                continue
            if c.spot <= 0:
                self._last_reject["atm"] += 1
                continue
            moneyness = abs(c.strike - c.spot) / c.spot
            if moneyness > self.atm_band * 2.5:
                self._last_reject["atm"] += 1
                continue
            abs_delta = abs(c.delta) if c.delta != 0 else 0.0
            if abs_delta > 0 and (
                abs_delta < self.min_delta or abs_delta > self.max_delta
            ):
                self._last_reject["delta"] += 1
                continue
            mom = self.momentum(c.underlying)
            if abs(mom) < self.entry_move_pct:
                self._last_reject["mom"] += 1
                continue
            if mom >= self.entry_move_pct and c.option_type != "call":
                self._last_reject["side"] += 1
                continue
            if mom <= -self.entry_move_pct and c.option_type != "put":
                self._last_reject["side"] += 1
                continue
            c.friction_pct = self.round_trip_friction(c.spread_pct)
            eff_delta = abs(c.delta) if c.delta != 0 else self.target_delta
            c.edge_pct = eff_delta * abs(mom) * c.spot / max(c.mark, 1e-9)
            if c.edge_pct < c.friction_pct * self.min_edge_multiple:
                self._last_reject["edge"] += 1
                continue
            atm_score = 1.0 / (1e-6 + moneyness)
            spread_score = 1.0 / (1e-6 + c.spread_pct)
            dte_score = 1.0 - abs((c.dte_days - 3.0) / max(self.max_dte, 1))
            if abs_delta > 0:
                delta_score = 1.0 / (1e-6 + abs(abs_delta - self.target_delta))
            else:
                delta_score = 0.5
            c.score = (
                atm_score * 2
                + spread_score
                + max(0.0, dte_score)
                + delta_score * 1.5
                + abs(mom) * 40
                + (c.edge_pct / max(c.friction_pct, 1e-9)) * 3
            )
            ranked.append(c)
            self._last_reject["pass"] += 1
        ranked.sort(key=lambda x: x.score, reverse=True)
        return ranked

    def pick_entry(self, tickers: List[Dict]) -> Optional[Signal]:
        sigs = self.pick_entries(tickers)
        return sigs[0] if sigs else None

    def pick_entries(self, tickers: List[Dict]) -> List[Signal]:
        """Up to one new entry per free underlying when each clears the gates."""
        now = time.time()
        if self.slots_free() <= 0:
            return []
        cands = []
        spots = {}
        for row in tickers:
            c = self.parse_ticker(row)
            if c:
                spots[c.underlying] = c.spot
                cands.append(c)
        for u, spot in spots.items():
            self.note_spot(u, spot)
        ranked = self.filter_rank(cands)
        if not ranked:
            return []

        cooldown = max(self.min_interval, self.entry_cooldown)
        open_us = set(self.trades.keys())
        signals: List[Signal] = []
        taken_us: Set[str] = set()
        # Walk score order; take at most one contract per free underlying.
        for c in ranked:
            if len(signals) >= self.slots_free():
                break
            u = c.underlying.upper()
            if u in open_us or u in taken_us:
                continue
            if self.one_per_underlying and u in self.trades:
                continue
            last_u = self._last_signal_by_u.get(u, 0.0)
            if now - last_u < cooldown:
                continue
            budget = self.budget_for(u)
            if budget <= 0:
                continue
            px = c.ask if c.ask > 0 else c.mark
            unit_cost = px * c.contract_value
            if unit_cost <= 0 or unit_cost > budget * 1.01:
                continue
            size = self.size_contracts(unit_cost, budget)
            if size < 1:
                continue
            mom = self.momentum(u)
            signals.append(
                Signal(
                    action="BUY",
                    symbol=c.symbol,
                    product_id=c.product_id,
                    size=size,
                    premium=px,
                    underlying=u,
                    option_type=c.option_type,
                    strike=c.strike,
                    contract_value=c.contract_value,
                    reason=(
                        f"ENTRY_{c.option_type.upper()}: {c.symbol} mom={mom:+.2%} "
                        f"spot={c.spot:.2f} K={c.strike:.2f} dte={c.dte_days:.1f}d "
                        f"delta={c.delta:+.2f} mark={px:.4f} unit≈{unit_cost:.4f} "
                        f"size={size} budget={budget:.4f} "
                        f"TP=+{self.tp_pct:.0%} SL=-{self.sl_pct:.0%}"
                    ),
                )
            )
            taken_us.add(u)
            self._last_signal_by_u[u] = now
            self._last_signal = now
        return signals

    def arm(self, sig: Signal, fill_premium: Optional[float] = None):
        entry = float(fill_premium or sig.premium)
        cval = float(sig.contract_value or 1.0)
        tp = entry * (1.0 + self.tp_pct)
        sl = entry * (1.0 - self.sl_pct)
        r = abs(entry - sl) * sig.size * cval
        cost = entry * sig.size * cval
        u = (sig.underlying or "").upper()
        if not u:
            # Fall back to symbol parse C-BTC-... / P-ETH-...
            parts = (sig.symbol or "").split("-")
            u = parts[1].upper() if len(parts) >= 2 else "UNK"
        trade = OpenTrade(
            symbol=sig.symbol,
            product_id=sig.product_id,
            underlying=u,
            option_type=sig.option_type,
            strike=sig.strike,
            size=sig.size,
            entry_premium=entry,
            entry_ts=time.time(),
            tp=tp,
            sl=sl,
            r_inr=r,
            premium_budget=cost,
            contract_value=cval,
            peak_pnl=0.0,
            peak_premium=entry,
        )
        self.trades[u] = trade
        print(
            f"[PLAN] {sig.symbol} entry={entry:.4f} size={sig.size} cval={cval} "
            f"cost={cost:.4f} | TP=+{self.tp_pct:.0%}→{tp:.4f} "
            f"SL=-{self.sl_pct:.0%}→{sl:.4f} | 1R={r:.4f} trail={self.trail_arm_r}R "
            f"| slots={self.slots_open()}/{self.max_open_underlyings}"
        )

    def clear(self, underlying: Optional[str] = None, symbol: Optional[str] = None):
        if symbol:
            t = self.trade_by_symbol(symbol)
            if t:
                self.trades.pop(t.underlying.upper(), None)
            return
        if underlying:
            self.trades.pop(underlying.upper(), None)
            return
        self.trades.clear()

    def _trade_to_dict(self, t: OpenTrade) -> Dict:
        return {
            "symbol": t.symbol,
            "product_id": t.product_id,
            "underlying": t.underlying,
            "option_type": t.option_type,
            "strike": t.strike,
            "size": t.size,
            "entry_premium": t.entry_premium,
            "entry_ts": t.entry_ts,
            "tp": t.tp,
            "sl": t.sl,
            "r_inr": t.r_inr,
            "premium_budget": t.premium_budget,
            "contract_value": t.contract_value,
            "peak_pnl": t.peak_pnl,
            "peak_premium": t.peak_premium,
        }

    def trade_plan_dict(self) -> Dict:
        rows = [self._trade_to_dict(t) for t in self.open_trades()]
        if not rows:
            return {}
        # v2 multi-slot + legacy single-trade keys for older API/mobile readers
        primary = rows[0]
        return {
            "version": 2,
            "trades": rows,
            **primary,
        }

    def _restore_one(self, data: Dict) -> Optional[OpenTrade]:
        if not data or not data.get("symbol"):
            return None
        return OpenTrade(
            symbol=str(data["symbol"]),
            product_id=int(data.get("product_id") or 0),
            underlying=str(data.get("underlying") or ""),
            option_type=str(data.get("option_type") or ""),
            strike=_f(data.get("strike")),
            size=int(data.get("size") or 0),
            entry_premium=_f(data.get("entry_premium")),
            entry_ts=_f(data.get("entry_ts"), time.time()),
            tp=_f(data.get("tp")),
            sl=_f(data.get("sl")),
            r_inr=_f(data.get("r_inr")),
            premium_budget=_f(data.get("premium_budget")),
            contract_value=_f(data.get("contract_value"), 1.0) or 1.0,
            peak_pnl=_f(data.get("peak_pnl")),
            peak_premium=_f(data.get("peak_premium") or data.get("entry_premium")),
        )

    def restore(self, data: Dict):
        self.trades = {}
        if not data:
            return
        rows = data.get("trades")
        if isinstance(rows, list) and rows:
            for row in rows:
                t = self._restore_one(row if isinstance(row, dict) else {})
                if t and t.underlying:
                    self.trades[t.underlying.upper()] = t
            return
        # Legacy single-trade plan
        t = self._restore_one(data)
        if t and t.underlying:
            self.trades[t.underlying.upper()] = t
        elif t:
            self.trades["UNK"] = t

    def evaluate_exit(self, mark: float, trade: Optional[OpenTrade] = None) -> Optional[Signal]:
        t = trade if trade is not None else self.trade
        if not t or mark <= 0:
            return None
        cval = float(t.contract_value or 1.0)
        pnl = (mark - t.entry_premium) * t.size * cval
        t.peak_pnl = max(t.peak_pnl, pnl)
        t.peak_premium = max(t.peak_premium, mark)
        giveback = t.peak_pnl - pnl
        arm = t.r_inr * self.trail_arm_r
        fees = t.entry_premium * t.size * cval * self.fee_rate * 2
        giveback_need = max(
            t.r_inr * self.trail_giveback_r,
            t.peak_pnl * self.trail_giveback_of_peak if t.peak_pnl > 0 else 0.0,
        )
        max_loss = t.premium_budget * self.max_loss_frac
        now = time.time()
        if now - self._last_hold_log > 8:
            self._last_hold_log = now
            held = now - t.entry_ts
            open_n = self.slots_open()
            print(
                f"[HOLD] {t.symbol} mark={mark:.4f} pnl={pnl:.2f} peak={t.peak_pnl:.2f} "
                f"giveback={giveback:.2f}/{giveback_need:.2f} TP={t.tp:.4f} SL={t.sl:.4f} "
                f"maxloss={max_loss:.2f} held={held:.0f}s/{self.max_hold_sec:.0f}s "
                f"trail={'ON' if t.peak_pnl >= arm else 'off'} "
                f"slots={open_n}/{self.max_open_underlyings}"
            )

        def _sell(reason: str) -> Signal:
            self._last_signal = now
            self._last_signal_by_u[t.underlying.upper()] = now
            return Signal(
                action="SELL",
                symbol=t.symbol,
                product_id=t.product_id,
                size=t.size,
                premium=mark,
                underlying=t.underlying,
                option_type=t.option_type,
                strike=t.strike,
                contract_value=t.contract_value,
                reason=reason,
            )

        if self.max_hold_sec > 0 and (now - t.entry_ts) >= self.max_hold_sec:
            return _sell(
                f"EXIT_TIME: {t.symbol} held={now - t.entry_ts:.0f}s "
                f">= {self.max_hold_sec:.0f}s pnl={pnl:.2f}"
            )
        if mark >= t.tp:
            return _sell(f"EXIT_TP: {t.symbol} mark>={t.tp:.4f} pnl={pnl:.2f}")
        if mark <= t.sl:
            return _sell(f"EXIT_SL: {t.symbol} mark<={t.sl:.4f} pnl={pnl:.2f}")
        if pnl <= -abs(max_loss):
            return _sell(f"EXIT_MAXLOSS: {t.symbol} pnl={pnl:.2f}")
        if t.peak_pnl >= arm and pnl >= fees and giveback >= giveback_need:
            return _sell(
                f"EXIT_TRAIL: {t.symbol} peak={t.peak_pnl:.2f} now={pnl:.2f} "
                f"giveback={giveback:.2f}"
            )
        return None


class Store:
    def __init__(self):
        self.pg: Optional[asyncpg.Pool] = None
        self.rd: Optional[aioredis.Redis] = None

    async def connect(self):
        self.pg = await asyncpg.create_pool(
            host=os.getenv("DB_HOST", "127.0.0.1"),
            port=int(os.getenv("DB_PORT", "5438")),
            user=os.getenv("DB_USER", "greeks"),
            password=os.getenv("DB_PASSWORD", "greeks"),
            database=os.getenv("DB_NAME", "rubaih_greeks"),
            min_size=1,
            max_size=5,
        )
        self.rd = aioredis.Redis(
            host=os.getenv("REDIS_HOST", "127.0.0.1"),
            port=int(os.getenv("REDIS_PORT", "6388")),
            decode_responses=True,
        )
        await self.rd.ping()
        schema = (ROOT / "schema.sql").read_text()
        async with self.pg.acquire() as con:
            await con.execute(schema)

    async def close(self):
        if self.pg:
            await self.pg.close()
        if self.rd:
            await self.rd.aclose()

    async def save_trade_plan(self, plan: Dict):
        if not self.rd:
            return
        await self.rd.set("greeks:trade_plan", json.dumps(plan))

    async def load_trade_plan(self) -> Dict:
        if not self.rd:
            return {}
        raw = await self.rd.get("greeks:trade_plan")
        return json.loads(raw) if raw else {}

    async def save_capital(self, free: float, source: str, quote_ccy: str = "USDT", usdt_inr: float = 87.0):
        if not self.rd:
            return
        ccy = (quote_ccy or "USDT").upper()
        inr_approx = free * usdt_inr if ccy in ("USDT", "USD", "USDC") else free
        payload = {
            "free_inr": free,  # legacy: cycle free in quote units
            "free_quote": free,
            "quote_ccy": ccy,
            "free_inr_approx": inr_approx,
            "source": source,
            "ts": time.time(),
        }
        await self.rd.set("greeks:capital_ledger", json.dumps(payload))
        await self.rd.hset(
            "greeks:settings",
            mapping={
                "free_capital_inr": f"{free:.4f}",  # legacy quote-unit alias
                "free_capital_quote": f"{free:.4f}",
                "free_quote": f"{free:.4f}",
                "quote_ccy": ccy,
                "free_inr_approx": f"{inr_approx:.2f}",
                "capital_source": source,
            },
        )

    async def load_capital(self) -> Dict:
        if not self.rd:
            return {}
        raw = await self.rd.get("greeks:capital_ledger")
        return json.loads(raw) if raw else {}

    async def is_halted(self) -> bool:
        if not self.rd:
            return False
        return (await self.rd.get("greeks:halted") or "").strip() in ("1", "true", "yes")

    async def set_halted(self, halted: bool, reason: str = ""):
        if not self.rd:
            return
        if halted:
            await self.rd.set("greeks:halted", "1")
            await self.rd.set(
                "greeks:halt_reason",
                json.dumps({"reason": reason, "ts": time.time()}),
            )
            await self.set_status("halted", reason)
        else:
            await self.rd.delete("greeks:halted")
            await self.rd.delete("greeks:halt_reason")

    async def get_halt_reason(self) -> str:
        if not self.rd:
            return ""
        raw = await self.rd.get("greeks:halt_reason")
        if not raw:
            return ""
        try:
            return str(json.loads(raw).get("reason") or "")
        except Exception:
            return str(raw)

    async def load_risk_state(self) -> Dict:
        if not self.rd:
            return {}
        raw = await self.rd.get("greeks:risk_state")
        return json.loads(raw) if raw else {}

    async def save_risk_state(self, state: Dict):
        if not self.rd:
            return
        await self.rd.set("greeks:risk_state", json.dumps(state))

    async def publish_dashboard(self, snap: Dict):
        if not self.rd:
            return
        await self.rd.set("greeks:dashboard", json.dumps(snap))
        await self.rd.publish("greeks:dashboard", json.dumps(snap))

    async def push_log(self, line: str):
        """Append a live log line for the mobile Logs tab."""
        if not self.rd:
            return
        payload = {"ts": time.time(), "line": line}
        try:
            await self.rd.lpush("greeks:logs", json.dumps(payload))
            await self.rd.ltrim("greeks:logs", 0, 199)
            await self.rd.publish("greeks:log", json.dumps(payload))
        except Exception:
            pass

    async def save_wallet_snapshot(
        self,
        rows: List[Dict],
        free: float,
        source: str,
        quote_ccy: str = "USDT",
        usdt_inr: float = 87.0,
    ):
        if not self.rd:
            return
        ccy = (quote_ccy or "USDT").upper()
        inr_approx = free * usdt_inr if ccy in ("USDT", "USD", "USDC") else free
        payload = {
            "ts": time.time(),
            "free_capital": free,
            "free_quote": free,
            "quote_ccy": ccy,
            "free_inr_approx": inr_approx,
            "source": source,
            "balances": rows,
        }
        await self.rd.set("greeks:wallet", json.dumps(payload))

    async def set_status(self, status: str, detail: str = ""):
        if self.pg:
            await self.pg.execute(
                "INSERT INTO engine_status (status, detail) VALUES ($1,$2)",
                status,
                detail,
            )
        if self.rd:
            await self.rd.set("greeks:engine_status", status)

    async def save_trade(self, sig: Signal, ai: bool = False):
        if not self.pg:
            return
        await self.pg.execute(
            """INSERT INTO option_trades
               (symbol, product_id, side, size, premium, underlying, option_type, strike, reason, ai_augmented)
               VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)""",
            sig.symbol,
            str(sig.product_id),
            sig.action.lower(),
            sig.size,
            sig.premium,
            sig.underlying,
            sig.option_type,
            sig.strike,
            sig.reason,
            ai,
        )

    async def save_ai_decision(self, decision: AIDecision):
        if self.pg:
            await self.pg.execute(
                """INSERT INTO ai_decisions
                   (model, action, confidence, reasoning, risk_assessment, portfolio_delta)
                   VALUES ($1,$2,$3,$4,$5,$6)""",
                decision.model_used,
                decision.action,
                decision.confidence,
                decision.reasoning,
                decision.risk_assessment,
                0.0,
            )
        if self.rd:
            payload = {
                "ts": time.time(),
                "model": decision.model_used,
                "action": decision.action,
                "confidence": decision.confidence,
                "reasoning": decision.reasoning,
                "risk_assessment": decision.risk_assessment,
            }
            try:
                await self.rd.set("greeks:ai_last", json.dumps(payload))
                await self.rd.lpush("greeks:signals", json.dumps(payload))
                await self.rd.ltrim("greeks:signals", 0, 99)
                await self.rd.publish("greeks:signal", json.dumps(payload))
            except Exception:
                pass

    async def pop_command(self) -> Optional[str]:
        if not self.rd:
            return None
        return await self.rd.lpop("greeks:commands")


class GreeksEngine:
    def __init__(self):
        self.store = Store()
        self.cycle = OptionsCycle(CFG)
        self.client: Optional[DeltaClient] = None
        self.ai = GreeksAI()
        self._ai_enabled = ai_configured()
        self._live = LIVE_TRADING
        self._running = True
        self._last_fill_ts = 0.0
        self._last_flatten_ts = 0.0
        self._session_pnl = 0.0
        self._capital_source = "unset"
        self._quote_ccy = "USDT"
        self._last_capital_refresh = 0.0
        self._ai_last: Optional[Dict] = None
        self._mark_cache: Dict[str, float] = {}
        self._delta_cache: Dict[str, float] = {}
        self._halted = False
        self._halt_reason = ""
        self._day_key = ""
        self._drawdown_pct = 0.0
        self._halt_ts = 0.0
        self._breach_count = 0
        self._equity = 0.0
        self._order_flight_ttl_sec = 90
        # Single writer boundary for cycle.trade + canonical free quote balance.
        self._state_lock = asyncio.Lock()
        # Serializes exchange order lifecycles across BUY, SELL, halt, and panic.
        self._order_lock = asyncio.Lock()
        risk_cfg = CFG.get("risk") or {}
        self._settle_grace_sec = max(
            10.0, float(risk_cfg.get("position_settle_grace_sec", 30))
        )
        self._flat_confirm_count = 0
        self._flat_confirm_by_pid: Dict[int, int] = {}
        self._flat_confirms_needed = max(
            2, int(risk_cfg.get("flat_confirm_readings", 2))
        )
        self._entry_blocked = False
        # Risk is measured on the bot's own curve: allocation + its realised PnL
        # + its open position. Wallet balance is collateral availability only.
        self._bot_base = 0.0
        self._bot_realized = 0.0
        self._peak_equity = 0.0
        self._day_start_equity = 0.0
        self._halt_kind = ""
        # Non-halt entry block (e.g. wallet below the floor) — not a loss.
        self._risk_block = ""
        # Product ids Delta reports that this bot did not open.
        self._foreign_pids: Set[int] = set()

    async def _log(self, line: str):
        print(line)
        try:
            await self.store.push_log(line)
        except Exception:
            pass

    async def start(self):
        if len(COMMAND_SECRET) < 16:
            raise RuntimeError(
                "RUBAIH_GREEKS_API_TOKEN (or RUBAIH_API_TOKEN) must be >=16 chars"
            )
        await self.store.connect()
        self.client = env_delta_client(CFG)
        plan = await self.store.load_trade_plan()
        self.cycle.restore(plan)
        auth = False
        if self.client.api_key:
            auth = await self.client.ping_auth()
        await self._load_halt_state()
        if auth:
            # Learn the operator's manual inventory before the first entry.
            await self._scan_foreign_positions()
        # Authenticate first: the wallet is only readable once auth is established,
        # and an unauthenticated boot silently falls back to the stale ledger.
        await self._refresh_capital(force=True)
        await self._log("=" * 60)
        await self._log(" RUBAIH GREEKS — Delta options cycle (capital survival)")
        await self._log(f" LIVE_TRADING: {'ON' if self._live else 'OFF (dry-run)'}")
        await self._log(f" Delta auth: {'OK' if auth else 'NO / missing keys'}")
        await self._log(f" AI: {'ENABLED (OpenRouter→NVIDIA)' if self._ai_enabled else 'DISABLED'}")
        await self._log(
            f" Free: {self.cycle.free_capital_inr:.4f} {self._quote_ccy} "
            f"budget={self.cycle.max_premium_budget:.2f}/coin "
            f"slots=0/{self.cycle.max_open_underlyings} "
            f"(one per underlying)"
        )
        await self._log(
            f" Filters: DTE {self.cycle.min_dte:.0f}-{self.cycle.max_dte:.0f}d "
            f"delta {self.cycle.min_delta:.2f}-{self.cycle.max_delta:.2f} "
            f"mom>={self.cycle.entry_move_pct:.2%}"
        )
        await self._log(
            f" Exits: TP=+{self.cycle.tp_pct:.0%} SL=-{self.cycle.sl_pct:.0%} "
            f"trail={self.cycle.trail_arm_r}R max_hold={self.cycle.max_hold_sec:.0f}s"
        )
        await self._log(
            f" Risk: max_dd={self.cycle.max_drawdown_pct:.0%} of bot equity "
            f"daily_loss={self.cycle.max_daily_loss_frac:.0%} "
            f"ruin_stop={self.cycle.max_total_loss_frac:.0%} "
            f"floor={self.cycle.min_free_quote:.2f} {self._quote_ccy} "
            f"confirm={self.cycle.halt_confirm_readings} halted={self._halted}"
        )
        await self._log(
            f" Edge gate: need {self.cycle.min_edge_multiple:.1f}x round-trip "
            f"friction (spread<={self.cycle.max_spread:.1%} + "
            f"fee {self.cycle.fee_rate:.2%}/side)"
        )
        await self._log(
            f" Manual positions protected: "
            f"{sorted(self._foreign_pids) if self._foreign_pids else 'none detected'}"
        )
        await self._log(" AI conf: advisory only; EMERGENCY acts only if conf>0.95")
        await self._log("=" * 60)
        if self._live and not auth:
            await self._log("[WARN] LIVE on but Delta auth failed — orders blocked")
        if self._halted:
            await self._log(f"[HALT] Active — no new entries. Reason: {self._halt_reason or 'n/a'}")
        await self.store.set_status("halted" if self._halted else "running")
        tasks = [
            self.main_loop(),
            self.sync_loop(),
            self.command_loop(),
        ]
        if self._ai_enabled:
            tasks.append(self.ai_loop())
        await asyncio.gather(*tasks)

    async def _refresh_capital(self, force: bool = False):
        """Refresh canonical free collateral in USDT quote units."""
        free_quote = 0.0
        source = "unknown"
        quote_ccy = "USDT"
        wallet_rows: List[Dict] = []
        wallet_authoritative = False

        def available_value(row: Dict) -> float:
            for key in (
                "available_balance",
                "available_balance_for_margin",
                "balance",
            ):
                if key in row and row.get(key) is not None:
                    return max(0.0, _f(row.get(key)))
            return 0.0

        if self.client and self.client._auth_ok:
            try:
                bals = await self.client.get_balances()
                wallet_rows = []
                best_usdt: Optional[float] = None
                best_inr: Optional[float] = None
                for b in bals or []:
                    ccy = str(b.get("asset_symbol") or b.get("currency") or "").upper()
                    avail = available_value(b)
                    total = _f(
                        b.get("balance")
                        if b.get("balance") is not None
                        else b.get("wallet_balance"),
                        avail,
                    )
                    if ccy:
                        wallet_rows.append(
                            {
                                "asset": ccy,
                                "available": avail,
                                "balance": total,
                            }
                        )
                    if ccy in ("USDT", "USD", "USDC"):
                        best_usdt = max(best_usdt or 0.0, avail)
                    if ccy == "INR":
                        best_inr = max(best_inr or 0.0, avail)
                # Presence of a USDT row is authoritative even when exactly zero.
                if best_usdt is not None:
                    free_quote = best_usdt
                    quote_ccy = "USDT"
                    source = "wallet:USDT"
                    wallet_authoritative = True
                elif best_inr is not None:
                    # Premiums are USDT quoted; convert INR collateral before sizing.
                    free_quote = best_inr / max(self.cycle.usdt_inr, 1.0)
                    quote_ccy = "USDT"
                    source = "wallet:INR→USDT"
                    wallet_authoritative = True
            except Exception as e:
                await self._log(f"[CAPITAL] wallet error: {e}")

        ledger_present = False
        if not wallet_authoritative:
            ledger = await self.store.load_capital()
            if "free_quote" in ledger or "free_inr" in ledger:
                ledger_present = True
                free_quote = max(
                    0.0,
                    _f(
                        ledger.get("free_quote")
                        if ledger.get("free_quote") is not None
                        else ledger.get("free_inr")
                    ),
                )
                stored_ccy = str(ledger.get("quote_ccy") or "USDT").upper()
                if stored_ccy == "INR":
                    free_quote /= max(self.cycle.usdt_inr, 1.0)
                source = "ledger"
                quote_ccy = "USDT"

        if not wallet_authoritative and not ledger_present and FREE_SEED > 0:
            # Env seed is INR intent; convert once to USDT quote for sizing
            free_quote = FREE_SEED / max(self.cycle.usdt_inr, 1.0)
            quote_ccy = "USDT"
            source = "env_seed_inr_to_usdt"
            await self.store.save_capital(
                free_quote, source, quote_ccy, self.cycle.usdt_inr
            )
            await self._log(
                f"[CAPITAL] seeded ₹{FREE_SEED:.0f} → {free_quote:.4f} USDT "
                f"(÷ usdt_inr={self.cycle.usdt_inr:.0f}) day-1 posture"
            )
        if not wallet_authoritative and not ledger_present and FREE_SEED <= 0:
            free_quote = self.cycle.capital_inr / max(self.cycle.usdt_inr, 1.0)
            quote_ccy = "USDT"
            source = "config_seed"

        async with self._state_lock:
            self.cycle.set_free_capital(free_quote)
            self._capital_source = source
            self._quote_ccy = "USDT"
            self._last_capital_refresh = time.time()
            await self.store.save_capital(
                free_quote, source, "USDT", self.cycle.usdt_inr
            )
        await self.store.save_wallet_snapshot(
            wallet_rows, free_quote, source, "USDT", self.cycle.usdt_inr
        )
        if force:
            inr = free_quote * self.cycle.usdt_inr
            await self._log(
                f"[CAPITAL] free={free_quote:.4f} USDT "
                f"(≈₹{inr:.0f}) source={source}"
            )
        await self._update_risk_after_capital(free_quote)

    async def _load_halt_state(self):
        self._halted = await self.store.is_halted()
        self._halt_reason = await self.store.get_halt_reason() if self._halted else ""
        state = await self.store.load_risk_state()
        self._bot_base = _f(state.get("bot_base"))
        self._bot_realized = _f(state.get("bot_realized"))
        self._peak_equity = _f(state.get("peak_equity"))
        self._day_start_equity = _f(state.get("day_start_equity"))
        self._day_key = str(state.get("day_key") or "")
        self._drawdown_pct = _f(state.get("drawdown_pct"))
        self._halt_ts = _f(state.get("halt_ts"))
        self._halt_kind = str(state.get("halt_kind") or "")
        # Halts persisted before the bot had its own equity curve were computed
        # from wallet balance, which every manual trade moved. Drop them rather
        # than serve a cooldown for a loss the bot never took.
        if self._halted and (self._halt_ts <= 0 or not self._halt_kind):
            await self._clear_halt("halt predates bot-scoped equity curve")
        # Operator retuned bot_allocation_quote (deposit / withdrawal). Adopt it
        # when flat so Redis doesn't keep the old probe-sized base forever.
        declared = self.cycle.bot_allocation
        if declared > 0 and not self.cycle.open_trades():
            old = self._bot_base
            if old <= 0 or abs(declared - old) / max(old, 1e-9) > 0.02:
                self._bot_base = declared
                self._bot_realized = 0.0
                equity = self._bot_equity()
                self._peak_equity = max(self._peak_equity, equity)
                self._day_start_equity = equity
                self._day_key = datetime.now(timezone.utc).strftime("%Y-%m-%d")
                self._drawdown_pct = 0.0
                await self._persist_risk_state()
                await self._log(
                    f"[RISK] allocation retuned {old:.4f} → {declared:.4f} "
                    f"{self._quote_ccy} (operator config)"
                )

    async def _persist_risk_state(self):
        await self.store.save_risk_state(
            {
                "bot_base": self._bot_base,
                "bot_realized": self._bot_realized,
                "peak_equity": self._peak_equity,
                "day_start_equity": self._day_start_equity,
                "day_key": self._day_key,
                "drawdown_pct": self._drawdown_pct,
                "halt_ts": self._halt_ts,
                "halt_kind": self._halt_kind,
                "ts": time.time(),
            }
        )

    def _position_value(self) -> float:
        """Market value of open options (quote ccy)."""
        total = 0.0
        for t in self.cycle.open_trades():
            if t.size <= 0:
                continue
            mark = self._mark_cache.get(t.symbol, 0.0) or t.entry_premium
            total += max(0.0, mark * t.size * float(t.contract_value or 1.0))
        return total

    def _bot_equity(self) -> float:
        """The bot's own equity curve, independent of the wallet.

        Wallet balance moves whenever the operator opens a manual position or
        transfers funds. Measuring risk on it makes the operator's own trading
        look like bot losses, so performance is tracked from the allocation the
        bot was given plus only the PnL the bot itself produced.
        """
        return self._bot_base + self._bot_realized + self._unrealized()

    def _unrealized(self) -> float:
        total = 0.0
        for t in self.cycle.open_trades():
            if t.size <= 0:
                continue
            mark = self._mark_cache.get(t.symbol, 0.0) or t.entry_premium
            total += (mark - t.entry_premium) * t.size * float(t.contract_value or 1.0)
        return total


    def _seed_bot_base(self, free: float) -> float:
        declared = self.cycle.bot_allocation
        if declared <= 0:
            declared = self.cycle.capital_inr / max(self.cycle.usdt_inr, 1.0)
        return declared if declared > 0 else max(free, 0.0)

    async def _update_risk_after_capital(self, free: float):
        """Halt on the bot's own drawdown; block entries on collateral floors.

        These are two different questions. "Has the bot lost money?" is answered
        by its equity curve. "Can it afford a trade right now?" is answered by
        the wallet, which the operator's manual positions legitimately reduce.
        """
        if free < 0:
            return
        if self._bot_base <= 0:
            self._bot_base = self._seed_bot_base(free)
            await self._log(
                f"[RISK] bot allocation base={self._bot_base:.4f} {self._quote_ccy}"
            )

        equity = self._bot_equity()
        self._equity = equity

        # Collateral availability is a block, never a loss.
        if free < self.cycle.min_free_quote:
            self._risk_block = (
                f"free {free:.4f} < floor {self.cycle.min_free_quote:.4f} "
                f"{self._quote_ccy}"
            )
        else:
            self._risk_block = ""

        day_key = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        if day_key != self._day_key or self._day_start_equity <= 0:
            self._day_key = day_key
            self._day_start_equity = equity
            self._breach_count = 0
            await self._log(f"[RISK] day start equity={equity:.4f} {self._quote_ccy}")

        # High-water mark only ever rises. Nothing but an explicit operator
        # reset may lower it, or a 15% limit becomes an endless series of them.
        if equity > self._peak_equity:
            self._peak_equity = equity
        self._drawdown_pct = (
            (self._peak_equity - equity) / self._peak_equity
            if self._peak_equity > 0
            else 0.0
        )
        daily_loss_frac = (
            (self._day_start_equity - equity) / self._day_start_equity
            if self._day_start_equity > 0
            else 0.0
        )
        total_loss_frac = (
            max(0.0, -self._bot_realized) / self._bot_base
            if self._bot_base > 0
            else 0.0
        )
        await self._persist_risk_state()

        if self._halted:
            return
        if not self.cycle.kill_on_drawdown:
            return

        reason = ""
        kind = ""
        if total_loss_frac >= self.cycle.max_total_loss_frac:
            kind = "ruin"
            reason = (
                f"realised loss {total_loss_frac:.1%} >= "
                f"{self.cycle.max_total_loss_frac:.1%} of allocation "
                f"(realised={self._bot_realized:.4f} base={self._bot_base:.4f})"
            )
        elif self._drawdown_pct >= self.cycle.max_drawdown_pct:
            kind = "drawdown"
            reason = (
                f"drawdown {self._drawdown_pct:.1%} >= "
                f"{self.cycle.max_drawdown_pct:.1%} "
                f"(equity={equity:.4f} peak={self._peak_equity:.4f})"
            )
        elif daily_loss_frac >= self.cycle.max_daily_loss_frac:
            kind = "daily"
            reason = (
                f"daily loss {daily_loss_frac:.1%} >= "
                f"{self.cycle.max_daily_loss_frac:.1%} "
                f"(equity={equity:.4f} day_start={self._day_start_equity:.4f})"
            )
        if not reason:
            self._breach_count = 0
            return
        # Mark blips must not halt on a single reading
        self._breach_count += 1
        if self._breach_count < self.cycle.halt_confirm_readings:
            await self._log(
                f"[RISK] breach {self._breach_count}/{self.cycle.halt_confirm_readings}: {reason}"
            )
            return
        await self._trigger_halt(reason, kind)

    async def _trigger_halt(self, reason: str, kind: str = "drawdown"):
        if self._halted:
            return
        self._halted = True
        self._halt_kind = kind
        self._halt_reason = reason
        self._halt_ts = time.time()
        self._breach_count = 0
        await self.store.set_halted(True, reason)
        await self._persist_risk_state()
        cooldown = (
            f"auto-resume in {self.cycle.halt_cooldown_sec / 60:.0f}m"
            if self.cycle.auto_resume
            else "manual resume required"
        )
        await self._log(f"[HALT] {reason} — flatten open trade; block entries ({cooldown})")
        async with self._order_lock:
            async with self._state_lock:
                open_rows = [replace(t) for t in self.cycle.open_trades()]
            for t in open_rows:
                mark = self._mark_cache.get(t.symbol, t.entry_premium)
                sig = Signal(
                    action="SELL",
                    symbol=t.symbol,
                    product_id=t.product_id,
                    size=t.size,
                    premium=mark if mark > 0 else t.entry_premium,
                    underlying=t.underlying,
                    option_type=t.option_type,
                    strike=t.strike,
                    contract_value=t.contract_value,
                    reason=f"EXIT_HALT: {reason}",
                )
                await self._execute_serialized(sig)
        await self.store.set_status("halted", reason)

    async def _clear_halt(self, note: str = "operator resume", rebase: bool = True):
        """Resume trading. Only an operator may move the high-water mark.

        Rebasing on a timer is what turns a 15% drawdown limit into an unbounded
        sequence of 15% losses, so auto-resume passes rebase=False.
        """
        async with self._state_lock:
            self._halted = False
            self._halt_reason = ""
            self._halt_kind = ""
            self._halt_ts = 0.0
            self._breach_count = 0
            self._entry_blocked = False
            await self.store.set_halted(False)
            if rebase:
                # A human explicitly accepted the loss: fold realised PnL into a
                # new base so the ruin guard measures from here on.
                self._bot_base = max(0.0, self._bot_base + self._bot_realized)
                self._bot_realized = 0.0
                equity = self._bot_equity()
                self._peak_equity = equity
                self._day_start_equity = equity
                self._day_key = datetime.now(timezone.utc).strftime("%Y-%m-%d")
                self._drawdown_pct = 0.0
            equity = self._bot_equity()
            await self._persist_risk_state()
        await self.store.set_status("running")
        await self._log(
            f"[HALT] Cleared ({note}) — entries allowed again, "
            f"equity={equity:.4f} peak={self._peak_equity:.4f}"
        )

    async def _maybe_auto_resume(self) -> bool:
        """Resume only once the breached condition has actually cleared.

        A cooldown timer says nothing about whether the risk is gone, so it
        gates re-entry but never substitutes for recovery.
        """
        if not self._halted or not self.cycle.auto_resume:
            return False
        if self._halt_kind == "ruin":
            return False  # terminal: needs a human and probably a refund
        if self._halt_ts <= 0:
            self._halt_ts = time.time()
            await self._persist_risk_state()
            return False
        waited = time.time() - self._halt_ts
        if waited < self.cycle.halt_cooldown_sec:
            return False
        if self._halt_kind == "daily":
            today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            if self._day_key == today:
                return False  # a daily limit lasts the rest of the day
            cleared = "UTC day rolled over"
        else:
            recover_at = self.cycle.max_drawdown_pct * self.cycle.halt_recover_frac
            if self._drawdown_pct > recover_at:
                return False
            cleared = f"drawdown {self._drawdown_pct:.1%} <= {recover_at:.1%}"
        await self._clear_halt(f"auto-resume: {cleared}", rebase=False)
        return True

    async def _publish(self):
        async with self._state_lock:
            open_rows = [replace(t) for t in self.cycle.open_trades()]
            free = float(self.cycle.free_capital_quote)
            budget = self.cycle.budget()
            slots = f"{self.cycle.slots_open()}/{self.cycle.max_open_underlyings}"
        positions = []
        upnl_all = 0.0
        for t in open_rows:
            mark = self._mark_cache.get(t.symbol, 0.0)
            upnl = (
                (mark - t.entry_premium) * t.size * float(t.contract_value or 1.0)
                if mark > 0
                else 0.0
            )
            upnl_all += upnl
            positions.append(
                {
                    "symbol": t.symbol,
                    "size": t.size,
                    "entry": t.entry_premium,
                    "mark": mark,
                    "upnl": upnl,
                    "tp": t.tp,
                    "sl": t.sl,
                    "underlying": t.underlying,
                    "option_type": t.option_type,
                    "strike": t.strike,
                    "delta": self._delta_cache.get(t.symbol),
                }
            )
        t = open_rows[0] if open_rows else None
        mark = positions[0]["mark"] if positions else 0.0
        upnl = positions[0]["upnl"] if positions else 0.0
        ccy = getattr(self, "_quote_ccy", "USDT") or "USDT"
        inr_approx = free * float(self.cycle.usdt_inr) if ccy in ("USDT", "USD", "USDC") else free
        snap = {
            "ts": time.time(),
            "mode": "options_cycle",
            "live": self._live,
            "free_capital_inr": free,
            "free_capital_quote": free,
            "free_quote": free,
            "quote_ccy": ccy,
            "free_inr_approx": inr_approx,
            "usdt_inr": self.cycle.usdt_inr,
            "budget_inr": budget,
            "budget_quote": budget,
            "budget_per_coin": self.cycle.max_premium_budget,
            "open_slots": slots,
            "max_open_underlyings": self.cycle.max_open_underlyings,
            "capital_source": self._capital_source,
            "session_pnl": self._session_pnl + upnl_all,
            "halted": self._halted,
            "halt_reason": self._halt_reason,
            "drawdown_pct": self._drawdown_pct,
            "halt_kind": self._halt_kind,
            "risk_block": self._risk_block,
            "bot_base": self._bot_base,
            "bot_realized": self._bot_realized,
            "bot_equity": self._bot_equity(),
            "peak_free": self._peak_equity,
            "peak_equity": self._peak_equity,
            "day_start_free": self._day_start_equity,
            "day_start_equity": self._day_start_equity,
            "equity_quote": free + self._position_value(),
            "fee_rate": self.cycle.fee_rate,
            "manual_positions": sorted(self._foreign_pids),
            "auto_resume": self.cycle.auto_resume,
            "halt_resume_in_sec": max(
                0.0, self.cycle.halt_cooldown_sec - (time.time() - self._halt_ts)
            )
            if (
                self._halted
                and self.cycle.auto_resume
                and self._halt_ts > 0
                and self._halt_kind != "ruin"
            )
            else 0.0,
            "ai_enabled": self._ai_enabled,
            "ai_last_action": (self._ai_last or {}).get("action"),
            "ai_confidence": (self._ai_last or {}).get("confidence"),
            "ai_emergency_conf": 0.95,
            "position": positions[0] if positions else None,
            "positions": positions,
            "tp_display": f"Premium +{self.cycle.tp_pct*100:.0f}%",
            "sl_display": f"Premium −{self.cycle.sl_pct*100:.0f}%",
            "max_hold_sec": self.cycle.max_hold_sec,
        }
        await self.store.publish_dashboard(snap)

    async def _clear_external_flat(
        self,
        reason: str,
        refresh_wallet: bool = True,
        product_id: int = 0,
        symbol: str = "",
    ):
        """Delta already flat (manual close / reduce_only empty) — drop local ghost."""
        async with self._state_lock:
            if product_id:
                t = self.cycle.trade_by_product(int(product_id))
            elif symbol:
                t = self.cycle.trade_by_symbol(symbol)
            else:
                t = self.cycle.trade
            if not t:
                return
            mark = self._mark_cache.get(t.symbol, 0.0) or t.entry_premium
            self._bot_realized += (
                (mark - t.entry_premium) * t.size * float(t.contract_value or 1.0)
            )
            sym = t.symbol
            self.cycle.clear(underlying=t.underlying)
            self._last_flatten_ts = time.time()
            self._last_fill_ts = 0.0
            self._flat_confirm_count = 0
            plan = self.cycle.trade_plan_dict()
            await self.store.save_trade_plan(plan if plan else {})
            if not refresh_wallet:
                await self.store.save_capital(
                    self.cycle.free_capital_quote,
                    "ledger",
                    "USDT",
                    self.cycle.usdt_inr,
                )
        await self._log(f"[SYNC] Clearing local trade {sym} — {reason}")
        if refresh_wallet:
            await self._refresh_capital(force=True)
        await self._publish()

    def _new_client_order_id(self, sig: Signal) -> str:
        """Delta client ids are limited to 32 characters."""
        side = "b" if sig.action == "BUY" else "s"
        return f"rg{side}{int(time.time() * 1000)}{secrets.token_hex(5)}"[:32]

    async def _claim_order(self, sig: Signal, client_order_id: str) -> bool:
        """Fail closed unless Redis atomically claims id + product/side flight."""
        rd = self.store.rd
        if not rd:
            await self._log("[ORDER] Redis unavailable — idempotency claim blocked")
            return False
        flight_key = f"greeks:orderflight:{sig.product_id}:{sig.action.lower()}"
        try:
            claimed = await rd.set(
                f"greeks:coid:{client_order_id}",
                json.dumps(
                    {
                        "status": "pending",
                        "product_id": sig.product_id,
                        "side": sig.action.lower(),
                        "ts": time.time(),
                    }
                ),
                nx=True,
                ex=86400,
            )
            if not claimed:
                return False
            flight = await rd.set(
                flight_key,
                client_order_id,
                nx=True,
                ex=self._order_flight_ttl_sec,
            )
            if not flight:
                await rd.delete(f"greeks:coid:{client_order_id}")
                return False
            return True
        except Exception as exc:
            await self._log(f"[ORDER] Redis idempotency claim failed: {exc}")
            return False

    async def _finish_order_claim(
        self,
        sig: Signal,
        client_order_id: str,
        result: Dict,
        release_flight: bool,
    ):
        rd = self.store.rd
        if not rd:
            return
        flight_key = f"greeks:orderflight:{sig.product_id}:{sig.action.lower()}"
        try:
            await rd.set(
                f"greeks:coid:{client_order_id}",
                json.dumps(result, default=str),
                ex=86400,
            )
            if release_flight:
                current = await rd.get(flight_key)
                if current == client_order_id:
                    await rd.delete(flight_key)
        except Exception:
            pass

    async def _scan_foreign_positions(self):
        """Record Delta positions this bot did not open.

        Contracts the operator bought by hand are not the bot's inventory: it
        must never size into them, exit them, or count their margin as its own
        loss. Excluding the product outright is the only safe rule, because a
        reduce_only order nets against the combined position.
        """
        if not (self.client and self.client._auth_ok):
            return
        try:
            positions = await self.client.get_positions()
        except Exception as exc:
            await self._log(f"[GUARD] manual position scan failed: {exc}")
            return
        async with self._state_lock:
            own = self.cycle.own_product_ids()
        foreign: Set[int] = set()
        for row in positions or []:
            pid = int(
                row.get("product_id") or (row.get("product") or {}).get("id") or 0
            )
            size = int(_f(row.get("size") or row.get("position_size")))
            if pid and abs(size) > 0 and pid not in own:
                foreign.add(pid)
        appeared = foreign - self._foreign_pids
        if appeared:
            await self._log(
                f"[GUARD] manual positions protected — excluded product_ids "
                f"{sorted(appeared)}"
            )
        self._foreign_pids = foreign
        self.cycle.excluded_pids = set(foreign)

    async def _confirm_delta_flat(
        self,
        product_id: int,
        reads: int = 3,
        delay_sec: float = 0.8,
    ) -> Tuple[bool, int]:
        """Require consecutive authoritative position reads before declaring flat."""
        if not (self.client and self.client._auth_ok and product_id):
            return False, 0
        last_size = 0
        for idx in range(max(1, reads)):
            try:
                positions = await self.client.get_positions(product_id=product_id)
            except Exception as exc:
                await self._log(f"[ORDER] flat confirm failed: {exc}")
                return False, last_size
            last_size = 0
            for row in positions or []:
                pid = int(
                    row.get("product_id")
                    or (row.get("product") or {}).get("id")
                    or 0
                )
                if pid == int(product_id):
                    last_size = int(_f(row.get("size") or row.get("position_size")))
                    break
            if abs(last_size) > 0:
                return False, last_size
            if idx < reads - 1:
                await asyncio.sleep(delay_sec)
        return True, 0

    async def _execute(self, sig: Signal) -> bool:
        async with self._order_lock:
            return await self._execute_serialized(sig)

    async def _execute_serialized(self, sig: Signal) -> bool:
        await self._log(f"[SIGNAL] {sig.reason}")
        if sig.action == "BUY" and (
            not self._running or self._halted or self._entry_blocked
        ):
            await self._log("[ORDER] BUY blocked — engine halted/stopping")
            return False
        if sig.action == "BUY" and self.cycle.allow_sell_premium is False:
            pass  # buy path only
        live_ok = self._live and self.client and self.client._auth_ok
        fill_px = sig.premium
        filled_size = int(sig.size)
        actual_fee: Optional[float] = None
        if live_ok:
            client_order_id = self._new_client_order_id(sig)
            if not await self._claim_order(sig, client_order_id):
                await self._log(
                    f"[ORDER] blocked duplicate/in-flight {sig.action} "
                    f"product={sig.product_id}"
                )
                return False
            # Begin settle grace before POST so sync cannot ghost-clear while
            # the order lifecycle is still awaiting Delta/fill history.
            async with self._state_lock:
                self._last_fill_ts = time.time()
                self._flat_confirm_count = 0
            started_at_us = int(time.time() * 1_000_000)
            initial: Dict = {}
            submit_error = ""
            try:
                if sig.action == "BUY":
                    initial = await self.client.place_order(
                        sig.product_id,
                        sig.size,
                        "buy",
                        order_type="market_order",
                        client_order_id=client_order_id,
                    )
                else:
                    initial = await self.client.place_order(
                        sig.product_id,
                        sig.size,
                        "sell",
                        order_type="market_order",
                        reduce_only=True,
                        client_order_id=client_order_id,
                    )
            except Exception as e:
                submit_error = str(e)
                await self._log(
                    f"[ORDER] POST ambiguous coid={client_order_id}: {e}; "
                    "querying Delta by client id (no blind retry)"
                )
                code = getattr(e, "code", "") or ""
                text = str(e).lower()
                # Reduce-only error is not enough to clear SoT; sync/flat confirm must.
                if sig.action == "SELL" and (
                    code == "no_position_for_reduce_only"
                    or "no_position_for_reduce_only" in text
                ):
                    flat, _ = await self._confirm_delta_flat(sig.product_id)
                    if flat:
                        await self._clear_external_flat(
                            "Delta flat confirmed after no_position_for_reduce_only",
                            product_id=sig.product_id,
                        )
                        await self._finish_order_claim(
                            sig,
                            client_order_id,
                            {"status": "already_flat", "error": submit_error},
                            release_flight=True,
                        )
                        return True

            order_id = str(initial.get("id") or "") if isinstance(initial, dict) else ""
            try:
                resolved = await self.client.resolve_order_fill(
                    requested_size=sig.size,
                    product_id=sig.product_id,
                    order_id=order_id or None,
                    client_order_id=client_order_id,
                    initial=initial if isinstance(initial, dict) else None,
                    timeout_sec=12.0,
                    poll_sec=0.35,
                    started_at_us=started_at_us,
                )
            except Exception as exc:
                await self._log(
                    f"[ORDER] reconciliation failed coid={client_order_id}: {exc}"
                )
                await self._finish_order_claim(
                    sig,
                    client_order_id,
                    {"status": "ambiguous", "error": submit_error or str(exc)},
                    release_flight=False,
                )
                return False

            filled_size = int(resolved.get("filled_size") or 0)
            fill_px = _f(resolved.get("avg_price"))
            actual_fee = _f(resolved.get("fee"))
            terminal = bool(resolved.get("terminal"))
            await self._finish_order_claim(
                sig,
                client_order_id,
                {
                    "status": resolved.get("state"),
                    "order_id": resolved.get("order_id"),
                    "filled_size": filled_size,
                    "avg_price": fill_px,
                    "fee": actual_fee,
                    "requested_size": sig.size,
                    "ts": time.time(),
                },
                release_flight=terminal,
            )
            if not resolved.get("confirmed") or filled_size <= 0 or fill_px <= 0:
                await self._log(
                    f"[ORDER] no confirmed execution coid={client_order_id} "
                    f"state={resolved.get('state')} — local SoT unchanged"
                )
                return False

            async with self._state_lock:
                self._last_fill_ts = time.time()
                self._flat_confirm_count = 0
            partial = filled_size < int(sig.size)
            await self._log(
                f"[FILL] LIVE {sig.action} {sig.symbol} "
                f"filled={filled_size}/{sig.size} avg={fill_px:.6f} "
                f"state={resolved.get('state')} fee={actual_fee:.6f}"
                f"{' PARTIAL' if partial else ''}"
            )
        else:
            await self._log(
                f"[DRY] {sig.action} {sig.symbol} size={filled_size} @ {fill_px:.4f}"
            )
            async with self._state_lock:
                self._last_fill_ts = time.time()
                self._flat_confirm_count = 0

        applied, fully_closed, refresh_wallet = await self._apply_confirmed_execution(
            sig,
            filled_size,
            fill_px,
            actual_fee,
        )
        if refresh_wallet:
            # Network I/O is outside the state mutex; publish is locked internally.
            await self._refresh_capital(force=True)
        return applied if sig.action == "BUY" else fully_closed

    async def _apply_confirmed_execution(
        self,
        sig: Signal,
        filled_size: int,
        fill_px: float,
        actual_fee: Optional[float],
    ) -> Tuple[bool, bool, bool]:
        """Atomically mutate trade, free quote, and persisted plan/ledger."""
        async with self._state_lock:
            executed_sig = replace(sig, size=filled_size, premium=fill_px)
            await self.store.save_trade(executed_sig)
            if sig.action == "BUY":
                u = (sig.underlying or "").upper()
                if self.cycle.one_per_underlying and u and u in self.cycle.trades:
                    await self._log(
                        f"[ORDER] BUY blocked — already holding {u} slot"
                    )
                    return False, False, False
                if self.cycle.slots_free() <= 0:
                    await self._log("[ORDER] BUY blocked — no free underlying slots")
                    return False, False, False
                cval = float(sig.contract_value or 1.0)
                cost = fill_px * filled_size * cval
                fee = (
                    actual_fee
                    if actual_fee is not None and actual_fee > 0
                    else cost * self.cycle.taker_fee
                )
                self.cycle.note_fee_observation(fee, cost)
                self.cycle.arm(executed_sig, fill_px)
                self.cycle.set_free_capital(
                    self.cycle.free_capital_quote - cost - fee
                )
                # Entry fee is realised the moment it is charged; the position
                # itself is carried as unrealised against its mark.
                self._bot_realized -= fee
                await self.store.save_trade_plan(self.cycle.trade_plan_dict())
                await self.store.save_capital(
                    self.cycle.free_capital_quote,
                    "ledger",
                    "USDT",
                    self.cycle.usdt_inr,
                )
                self._capital_source = "ledger"
                self._flat_confirm_count = 0
                return True, False, False

            t = (
                self.cycle.trade_by_product(sig.product_id)
                or self.cycle.trade_by_symbol(sig.symbol)
                or self.cycle.trade
            )
            if not t:
                await self._log(
                    "[ORDER] confirmed SELL but no local trade; "
                    "wallet/position sync required"
                )
                return False, False, True

            original_size = max(1, int(t.size))
            closed_size = min(filled_size, original_size)
            cval = float(t.contract_value or 1.0)
            pnl = (fill_px - t.entry_premium) * closed_size * cval
            release = t.premium_budget * (closed_size / original_size)
            fee = (
                actual_fee
                if actual_fee is not None and actual_fee > 0
                else fill_px * closed_size * cval * self.cycle.taker_fee
            )
            self.cycle.note_fee_observation(fee, fill_px * closed_size * cval)
            self._session_pnl += pnl - fee
            self._bot_realized += pnl - fee
            self.cycle.set_free_capital(
                self.cycle.free_capital_quote + release + pnl - fee
            )
            remaining = original_size - closed_size
            if remaining <= 0:
                self.cycle.clear(underlying=t.underlying)
                self._last_flatten_ts = time.time()
                self._flat_confirm_count = 0
                plan = self.cycle.trade_plan_dict()
                await self.store.save_trade_plan(plan if plan else {})
            else:
                t.size = remaining
                t.premium_budget = max(0.0, t.premium_budget - release)
                t.r_inr = max(0.0, t.r_inr * (remaining / original_size))
                await self.store.save_trade_plan(self.cycle.trade_plan_dict())
                await self._log(
                    f"[ORDER] partial exit retained local risk size={remaining} "
                    f"on {t.symbol}"
                )
            await self.store.save_capital(
                self.cycle.free_capital_quote,
                "ledger",
                "USDT",
                self.cycle.usdt_inr,
            )
            self._capital_source = "ledger"
            await self._log(
                f"[{'FLAT' if remaining <= 0 else 'PARTIAL'}] "
                f"closed={closed_size} pnl≈{pnl:.4f} "
                f"free={self.cycle.free_capital_quote:.4f} USDT "
                f"slots={self.cycle.slots_open()}/{self.cycle.max_open_underlyings}"
            )
            return True, remaining <= 0, True

    async def main_loop(self):
        while self._running:
            try:
                if not self.client:
                    await asyncio.sleep(2)
                    continue
                if time.time() - self._last_capital_refresh > 60:
                    await self._refresh_capital(force=False)
                tickers = await self.client.get_option_tickers(self.cycle.underlyings)
                # Manage every open slot: mark refresh + exit, independently.
                async with self._state_lock:
                    active_trades = [replace(t) for t in self.cycle.open_trades()]
                for active_trade in active_trades:
                    sym = active_trade.symbol
                    try:
                        tk = await self.client.get_ticker(sym)
                        if tk:
                            mark = _f(
                                tk.get("mark_price")
                                or (tk.get("quotes") or {}).get("mark_price")
                            )
                            if mark > 0:
                                self._mark_cache[sym] = mark
                            g = tk.get("greeks") or {}
                            dlt = _f(g.get("delta") or tk.get("delta"))
                            if dlt != 0:
                                self._delta_cache[sym] = dlt
                            async with self._state_lock:
                                exit_sig = None
                                live = self.cycle.trade_by_symbol(sym)
                                if live:
                                    exit_sig = self.cycle.evaluate_exit(
                                        mark
                                        if mark > 0
                                        else self._mark_cache.get(sym, 0),
                                        trade=live,
                                    )
                                    await self.store.save_trade_plan(
                                        self.cycle.trade_plan_dict()
                                    )
                            if exit_sig:
                                await self._execute(exit_sig)
                    except Exception as e:
                        await self._log(f"[MARK] {sym}: {e}")

                # Entries fill free underlying slots (BTC and/or ETH).
                if self._halted and not await self._maybe_auto_resume():
                    if int(time.time()) % 90 < 3:
                        left = max(
                            0.0,
                            self.cycle.halt_cooldown_sec - (time.time() - self._halt_ts),
                        )
                        when = (
                            f"auto-resume in {left / 60:.0f}m"
                            if self.cycle.auto_resume
                            else "POST /api/resume to clear"
                        )
                        await self._log(
                            f"[HALT] blocked entries — {self._halt_reason or 'risk'} ({when})"
                        )
                elif self._risk_block:
                    if int(time.time()) % 90 < 3:
                        await self._log(f"[RISK] entries blocked — {self._risk_block}")
                elif self.cycle.slots_free() > 0:
                    async with self._state_lock:
                        entries = self.cycle.pick_entries(tickers or [])
                    for entry in entries:
                        await self._execute(entry)
                    if not entries and int(time.time()) % 60 < 3:
                        rej = getattr(self.cycle, "_last_reject", {}) or {}
                        mom_btc = self.cycle.momentum("BTC")
                        mom_eth = self.cycle.momentum("ETH")
                        await self._log(
                            f"[SCAN] tickers={len(tickers or [])} "
                            f"free={self.cycle.free_capital_quote:.4f} "
                            f"budget={self.cycle.budget():.4f}/coin "
                            f"slots={self.cycle.slots_open()}/"
                            f"{self.cycle.max_open_underlyings} "
                            f"pass={rej.get('pass', 0)} "
                            f"rej dte={rej.get('dte', 0)} spr={rej.get('spread', 0)} "
                            f"atm={rej.get('atm', 0)} δ={rej.get('delta', 0)} "
                            f"mom={rej.get('mom', 0)} side={rej.get('side', 0)} "
                            f"edge={rej.get('edge', 0)} held={rej.get('held', 0)} "
                            f"| mom BTC={mom_btc:+.3%} ETH={mom_eth:+.3%} "
                            f"fee={self.cycle.fee_rate:.2%}/side"
                        )
                await self._publish()
            except Exception as e:
                await self._log(f"[MAIN] {type(e).__name__}: {e}")
            await asyncio.sleep(3)

    async def _reconcile_size(self, pid: int, exchange_size: int):
        """Shrink to match Delta, but never grow.

        A position larger than the bot's own is the operator trading the same
        contract. Adopting the surplus would make the bot's next TP, SL, or
        emergency flatten sell contracts it never bought.
        """
        async with self._state_lock:
            t = self.cycle.trade_by_product(pid)
            if not t:
                return
            local_size = abs(int(t.size or 0))
            if exchange_size == local_size:
                return
            if exchange_size > local_size:
                await self._log(
                    f"[GUARD] Delta holds {exchange_size} vs bot {local_size} on "
                    f"{t.symbol} — surplus is manual, not adopted"
                )
                return
            scale = exchange_size / local_size if local_size else 0.0
            mark = self._mark_cache.get(t.symbol, 0.0) or t.entry_premium
            gone = local_size - exchange_size
            self._bot_realized += (
                (mark - t.entry_premium) * gone * float(t.contract_value or 1.0)
            )
            t.size = exchange_size
            t.premium_budget = max(0.0, t.premium_budget * scale)
            t.r_inr = max(0.0, t.r_inr * scale)
            await self.store.save_trade_plan(self.cycle.trade_plan_dict())
            await self._log(
                f"[SYNC] size reduced to Delta → {exchange_size} on {t.symbol} "
                f"({gone} closed externally)"
            )

    async def sync_loop(self):
        """Keep local open trades aligned with Delta (manual open/close)."""
        while self._running:
            try:
                if not (self.client and self.client._auth_ok):
                    await asyncio.sleep(5)
                    continue

                await self._scan_foreign_positions()

                async with self._state_lock:
                    locals_ = [replace(t) for t in self.cycle.open_trades()]
                for local_trade in locals_:
                    pid = int(local_trade.product_id or 0)
                    try:
                        positions = await self.client.get_positions(
                            product_id=pid or None
                        )
                    except Exception as e:
                        await self._log(f"[SYNC] positions: {e}")
                        continue

                    matched_size = 0
                    open_pids = set()
                    for p in positions or []:
                        p_pid = int(
                            p.get("product_id")
                            or (p.get("product") or {}).get("id")
                            or 0
                        )
                        size = int(_f(p.get("size") or p.get("position_size")))
                        if p_pid and abs(size) > 0:
                            open_pids.add(p_pid)
                        if pid and p_pid == pid:
                            matched_size = size

                    exchange_flat = abs(matched_size) == 0 and (
                        not pid or pid not in open_pids
                    )
                    should_clear = False
                    if exchange_flat:
                        age = (
                            float("inf")
                            if self._last_fill_ts <= 0
                            else time.time() - self._last_fill_ts
                        )
                        async with self._state_lock:
                            same_trade = bool(self.cycle.trade_by_product(pid))
                            if not same_trade:
                                self._flat_confirm_by_pid[pid] = 0
                            elif age < self._settle_grace_sec:
                                self._flat_confirm_by_pid[pid] = 0
                                await self._log(
                                    f"[SYNC] defer flat {local_trade.symbol} — "
                                    f"settle grace {age:.0f}/{self._settle_grace_sec:.0f}s"
                                )
                            else:
                                n = self._flat_confirm_by_pid.get(pid, 0) + 1
                                self._flat_confirm_by_pid[pid] = n
                                should_clear = n >= self._flat_confirms_needed
                                if not should_clear:
                                    await self._log(
                                        f"[SYNC] flat confirm {n}/"
                                        f"{self._flat_confirms_needed} "
                                        f"for {local_trade.symbol}"
                                    )
                    else:
                        async with self._state_lock:
                            self._flat_confirm_by_pid[pid] = 0

                    if should_clear:
                        self._flat_confirm_by_pid[pid] = 0
                        await self._clear_external_flat(
                            f"Delta flat confirmed {self._flat_confirms_needed}x "
                            f"after {self._settle_grace_sec:.0f}s settle grace",
                            product_id=pid,
                        )
                    elif abs(matched_size) > 0:
                        await self._reconcile_size(pid, abs(int(matched_size)))

                await asyncio.sleep(5)
            except Exception as e:
                await self._log(f"[SYNC] {e}")
                await asyncio.sleep(8)

    async def command_loop(self):
        while self._running:
            try:
                raw = await self.store.pop_command()
                if not raw:
                    await asyncio.sleep(1)
                    continue
                try:
                    payload = json.loads(raw)
                except (TypeError, json.JSONDecodeError):
                    await self._log("[CMD] rejected unsigned legacy command")
                    continue
                if not verify_command(COMMAND_SECRET, payload):
                    await self._log("[CMD] rejected invalid/expired HMAC command")
                    continue
                nonce = str(payload.get("nonce") or "")
                try:
                    fresh = await self.store.rd.set(
                        f"greeks:cmdnonce:{nonce}",
                        "1",
                        nx=True,
                        ex=300,
                    )
                except Exception as exc:
                    await self._log(
                        f"[CMD] nonce store unavailable — fail closed: {exc}"
                    )
                    continue
                if not fresh:
                    await self._log(f"[CMD] rejected replay nonce={nonce[:10]}…")
                    continue
                cmd = str(payload.get("command") or "").strip().lower()
                if cmd in ("kill", "flatten", "panic"):
                    await self._log(f"[CMD] {cmd}")
                    await self._emergency("operator")
                elif cmd == "refresh_capital":
                    await self._refresh_capital(force=True)
                    await self._publish()
                elif cmd in ("resume", "unhalt", "clear_halt"):
                    await self._log(f"[CMD] {cmd}")
                    await self._clear_halt()
                    await self._publish()
                elif cmd == "sync":
                    await self._log(f"[CMD] {cmd}")
                    async with self._state_lock:
                        locals_ = [replace(t) for t in self.cycle.open_trades()]
                    if not locals_:
                        await self._log("[SYNC] no local trade to sync")
                    elif self.client and self.client._auth_ok:
                        for local in locals_:
                            flat, matched = await self._confirm_delta_flat(
                                local.product_id,
                                reads=3,
                                delay_sec=0.8,
                            )
                            if flat:
                                await self._clear_external_flat(
                                    "signed forced sync — Delta flat confirmed 3x",
                                    product_id=local.product_id,
                                )
                            else:
                                await self._log(
                                    f"[SYNC] Delta still open size={matched} "
                                    f"on {local.symbol}"
                                )
                    await self._publish()
            except Exception as e:
                await self._log(f"[CMD] {e}")
                await asyncio.sleep(1)

    async def ai_loop(self):
        """Advisory overlay every ~3 minutes. Quant remains authority."""
        while self._running:
            try:
                async with self._state_lock:
                    rows = [replace(t) for t in self.cycle.open_trades()]
                    free_quote = self.cycle.free_capital_quote
                positions = []
                upnl_all = 0.0
                for t in rows:
                    mark = self._mark_cache.get(t.symbol, 0.0)
                    upnl = (
                        (mark - t.entry_premium)
                        * t.size
                        * float(t.contract_value or 1.0)
                        if mark > 0
                        else 0.0
                    )
                    upnl_all += upnl
                    positions.append(
                        {
                            "symbol": t.symbol,
                            "side": "long",
                            "option_type": t.option_type,
                            "strike": t.strike,
                            "size": t.size,
                            "entry": t.entry_premium,
                            "mark": mark,
                            "tp": t.tp,
                            "sl": t.sl,
                            "underlying": t.underlying,
                        }
                    )
                context = {
                    "free_capital": free_quote,
                    "quant_signal": "HOLD" if positions else "SCAN",
                    "position": positions[0] if positions else None,
                    "positions": positions,
                    "upnl": upnl_all,
                    "underlyings": self.cycle.underlyings,
                    "notes": (
                        "buy-side options cycle; up to one live option per "
                        "underlying (BTC+ETH); no premium selling"
                    ),
                }
                decision = await self.ai.analyze(context)
                if decision:
                    self._ai_last = {
                        "action": decision.action,
                        "confidence": decision.confidence,
                        "reasoning": decision.reasoning,
                        "model": decision.model_used,
                        "risk_assessment": decision.risk_assessment,
                    }
                    await self.store.save_ai_decision(decision)
                    await self._log(
                        f"[AI] {decision.model_used} → {decision.action} "
                        f"(conf={decision.confidence:.2f}) {decision.reasoning[:120]}"
                    )
                    await self._publish()
                    if decision.action == "EMERGENCY" and decision.confidence > 0.95:
                        await self._log(f"[AI] EMERGENCY: {decision.reasoning}")
                        await self._emergency(f"AI_EMERGENCY: {decision.reasoning}")
                        break
            except Exception as e:
                await self._log(f"[AI LOOP] {e}")
            await asyncio.sleep(180)

    async def _emergency(self, reason: str = "operator"):
        # Block any BUY signal already waiting behind the order mutex.
        self._entry_blocked = True
        async with self._order_lock:
            await self._emergency_serialized(reason)

    async def _emergency_serialized(self, reason: str = "operator"):
        await self.store.set_status("kill_switch", reason)
        async with self._state_lock:
            snapshots = [replace(t) for t in self.cycle.open_trades()]
        if not self._live:
            self._running = False
            return
        if not snapshots:
            self._running = False
            return

        any_failed = False
        for snapshot in snapshots:
            sig = Signal(
                action="SELL",
                symbol=snapshot.symbol,
                product_id=snapshot.product_id,
                size=snapshot.size,
                premium=self._mark_cache.get(
                    snapshot.symbol, snapshot.entry_premium
                ),
                underlying=snapshot.underlying,
                option_type=snapshot.option_type,
                strike=snapshot.strike,
                contract_value=snapshot.contract_value,
                reason=f"EXIT_EMERGENCY: {reason}",
            )
            execution_confirmed = await self._execute_serialized(sig)
            flat, exchange_size = await self._confirm_delta_flat(
                snapshot.product_id,
                reads=3,
                delay_sec=0.8,
            )
            if execution_confirmed and flat:
                async with self._state_lock:
                    remains = bool(self.cycle.trade_by_product(snapshot.product_id))
                if remains:
                    await self._clear_external_flat(
                        "emergency flatten confirmed by Delta positions",
                        product_id=snapshot.product_id,
                    )
                continue
            any_failed = True
            async with self._state_lock:
                live = self.cycle.trade_by_product(snapshot.product_id)
                if not live:
                    if exchange_size:
                        snapshot.size = abs(int(exchange_size))
                    self.cycle.trades[snapshot.underlying.upper()] = snapshot
                elif exchange_size:
                    live.size = abs(int(exchange_size))
                await self.store.save_trade_plan(self.cycle.trade_plan_dict())
            await self._log(
                f"[EMERGENCY] flatten NOT confirmed on {snapshot.symbol} — "
                f"retained; Delta size={exchange_size}"
            )

        if any_failed:
            await self.store.set_status(
                "flatten_failed",
                f"{reason}; one or more legs not confirmed flat",
            )
            return
        await self.store.set_status("stopped", f"flat confirmed: {reason}")
        await self._log(
            "[EMERGENCY] Delta explicitly confirmed flat — local SoT cleared"
        )
        self._running = False

    async def shutdown(self):
        self._running = False
        await self.store.set_status("stopped")
        await self.ai.close()
        if self.client:
            await self.client.close()
        await self.store.close()


async def main():
    eng = GreeksEngine()
    try:
        await eng.start()
    except KeyboardInterrupt:
        pass
    finally:
        await eng.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
