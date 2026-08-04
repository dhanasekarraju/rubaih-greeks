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
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import asyncpg
import redis.asyncio as aioredis
import yaml
from dotenv import load_dotenv

from ai_advisor import GreeksAI, AIDecision, ai_configured
from delta_client import DeltaClient, DeltaAPIError, env_delta_client

load_dotenv()

ROOT = Path(__file__).resolve().parent
CFG = yaml.safe_load((ROOT / "config.yaml").read_text())

LIVE_TRADING = os.getenv("LIVE_TRADING", "false").strip().lower() in ("1", "true", "yes")
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
        self.free_capital_inr = self.capital_inr
        self.margin_use_frac = float(t.get("margin_use_frac", 0.20))
        self.margin_use_max_frac = float(t.get("margin_use_max_frac", 0.25))
        self.max_premium_budget = float(t.get("max_premium_budget_usdt", 2.0))
        self.min_interval = float(t.get("min_entry_interval_sec", 120))
        self.entry_cooldown = float(s.get("entry_cooldown_sec", 300))
        self.lookback = int(s.get("momentum_lookback", 20))
        self.entry_move_pct = float(s.get("entry_move_pct", 0.003))
        self.min_dte = float(s.get("min_dte_days", 2))
        self.max_dte = float(s.get("max_dte_days", 5))
        self.atm_band = float(s.get("atm_band_pct", 0.015))
        self.max_spread = float(s.get("max_spread_pct", 0.05))
        self.min_mark = float(s.get("min_mark_premium", 0.5))
        self.min_delta = float(s.get("min_delta", 0.30))
        self.max_delta = float(s.get("max_delta", 0.55))
        self.target_delta = float(s.get("target_delta", 0.40))
        self.tp_pct = float(s.get("take_profit_premium_pct", 0.35))
        self.sl_pct = float(s.get("stop_loss_premium_pct", 0.18))
        self.max_loss_frac = float(s.get("max_loss_frac", 0.10))
        self.trail_arm_r = float(s.get("trail_arm_r", 0.7))
        self.trail_giveback_r = float(s.get("trail_giveback_r", 0.4))
        self.trail_giveback_of_peak = float(s.get("trail_giveback_of_peak", 0.25))
        self.max_hold_sec = float(s.get("max_hold_sec", 14400))
        self.max_drawdown_pct = float(r.get("max_drawdown_pct", 0.15))
        self.max_daily_loss_frac = float(r.get("max_daily_loss_frac", 0.25))
        self.kill_on_drawdown = bool(r.get("kill_switch_on_drawdown", True))
        self.taker_fee = float((cfg.get("exchange") or {}).get("taker_fee", 0.0005))
        self._spot_hist: Dict[str, List[float]] = {u: [] for u in self.underlyings}
        self._last_signal = 0.0
        self._last_hold_log = 0.0
        self.trade: Optional[OpenTrade] = None

    def set_free_capital(self, free: float):
        if free and free > 0:
            self.free_capital_inr = float(free)

    def budget(self) -> float:
        free = max(self.free_capital_inr, 0.0) or max(self.capital_inr, 0.0)
        lo = max(0.05, min(self.margin_use_frac, self.margin_use_max_frac))
        hi = max(lo, min(0.95, self.margin_use_max_frac))
        use = min(max(self.margin_use_frac, lo), hi)
        raw = free * use
        cap = float(self.max_premium_budget or 0)
        if cap > 0:
            return min(raw, cap)
        return raw

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
        m = re.search(r"-(\d{6})$", symbol or "")
        if not m:
            return None
        try:
            dt = datetime.strptime(m.group(1), "%d%m%y").replace(tzinfo=timezone.utc)
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
                if f"-{u}-" in symbol.upper() or symbol.upper().startswith(("C-" + u, "P-" + u)):
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
        # Delta option premium cost ≈ mark * contract_value (quote units)
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
        for c in cands:
            if c.dte_days < self.min_dte or c.dte_days > self.max_dte:
                continue
            if c.spread_pct > self.max_spread:
                continue
            if c.spot <= 0:
                continue
            moneyness = abs(c.strike - c.spot) / c.spot
            if moneyness > self.atm_band * 2.5:
                continue
            # Signed delta → abs band for puts/calls
            abs_delta = abs(c.delta) if c.delta != 0 else 0.0
            if abs_delta > 0 and (abs_delta < self.min_delta or abs_delta > self.max_delta):
                continue
            # If exchange omits delta, still allow near-ATM (moneyness already gated)
            mom = self.momentum(c.underlying)
            if mom >= self.entry_move_pct and c.option_type != "call":
                continue
            if mom <= -self.entry_move_pct and c.option_type != "put":
                continue
            if abs(mom) < self.entry_move_pct:
                continue
            # Prefer mid-delta ~target, tight spread, DTE ~3d, closer ATM
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
            )
            ranked.append(c)
        ranked.sort(key=lambda x: x.score, reverse=True)
        return ranked

    def size_contracts(self, unit_cost: float) -> int:
        """Integer contracts whose premium notional fits budget."""
        budget = self.budget()
        if unit_cost <= 0 or budget <= 0:
            return 0
        # unit_cost is approx quote/INR-equivalent cost of 1 contract
        n = int(math.floor(budget / unit_cost))
        return max(0, n)

    def pick_entry(self, tickers: List[Dict]) -> Optional[Signal]:
        now = time.time()
        if self.trade:
            return None
        if now - self._last_signal < max(self.min_interval, self.entry_cooldown):
            return None
        cands = []
        spots = {}
        for row in tickers:
            c = self.parse_ticker(row)
            if c:
                spots[c.underlying] = c.spot
                cands.append(c)
        # One spot sample per underlying per scan (avoid drowning momentum history)
        for u, spot in spots.items():
            self.note_spot(u, spot)
        ranked = self.filter_rank(cands)
        if not ranked:
            return None
        best = ranked[0]
        # Pay ask when possible (buy); cost uses contract_value
        px = best.ask if best.ask > 0 else best.mark
        unit_cost = px * best.contract_value
        size = self.size_contracts(unit_cost)
        if size < 1:
            # Prefer next cheaper ranked names that fit ₹1K budgets
            for alt in ranked[1:8]:
                apx = alt.ask if alt.ask > 0 else alt.mark
                uc = apx * alt.contract_value
                sz = self.size_contracts(uc)
                if sz >= 1:
                    best, px, unit_cost, size = alt, apx, uc, sz
                    break
            else:
                print(
                    f"[SCAN] skip size: best={best.symbol} unit_cost≈{unit_cost:.2f} "
                    f"budget=₹{self.budget():.0f}"
                )
                return None
        self._last_signal = now
        mom = self.momentum(best.underlying)
        return Signal(
            action="BUY",
            symbol=best.symbol,
            product_id=best.product_id,
            size=size,
            premium=px,
            underlying=best.underlying,
            option_type=best.option_type,
            strike=best.strike,
            contract_value=best.contract_value,
            reason=(
                f"ENTRY_{best.option_type.upper()}: {best.symbol} mom={mom:+.2%} "
                f"spot={best.spot:.2f} K={best.strike:.2f} dte={best.dte_days:.1f}d "
                f"delta={best.delta:+.2f} mark={px:.4f} unit≈{unit_cost:.2f} size={size} "
                f"budget={self.budget():.2f} TP=+{self.tp_pct:.0%} SL=-{self.sl_pct:.0%}"
            ),
        )

    def arm(self, sig: Signal, fill_premium: Optional[float] = None):
        entry = float(fill_premium or sig.premium)
        cval = float(sig.contract_value or 1.0)
        tp = entry * (1.0 + self.tp_pct)
        sl = entry * (1.0 - self.sl_pct)
        r = abs(entry - sl) * sig.size * cval
        cost = entry * sig.size * cval
        self.trade = OpenTrade(
            symbol=sig.symbol,
            product_id=sig.product_id,
            underlying=sig.underlying,
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
        print(
            f"[PLAN] {sig.symbol} entry={entry:.4f} size={sig.size} cval={cval} "
            f"cost~₹{cost:.0f} | TP=+{self.tp_pct:.0%}→{tp:.4f} "
            f"SL=-{self.sl_pct:.0%}→{sl:.4f} | 1R=₹{r:.0f} trail={self.trail_arm_r}R"
        )

    def clear(self):
        self.trade = None

    def trade_plan_dict(self) -> Dict:
        t = self.trade
        if not t:
            return {}
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

    def restore(self, data: Dict):
        if not data or not data.get("symbol"):
            return
        self.trade = OpenTrade(
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

    def evaluate_exit(self, mark: float) -> Optional[Signal]:
        t = self.trade
        if not t or mark <= 0:
            return None
        cval = float(t.contract_value or 1.0)
        pnl = (mark - t.entry_premium) * t.size * cval
        t.peak_pnl = max(t.peak_pnl, pnl)
        t.peak_premium = max(t.peak_premium, mark)
        giveback = t.peak_pnl - pnl
        arm = t.r_inr * self.trail_arm_r
        fees = t.entry_premium * t.size * cval * self.taker_fee * 2
        giveback_need = max(
            t.r_inr * self.trail_giveback_r,
            t.peak_pnl * self.trail_giveback_of_peak if t.peak_pnl > 0 else 0.0,
        )
        max_loss = t.premium_budget * self.max_loss_frac
        now = time.time()
        if now - self._last_hold_log > 8:
            self._last_hold_log = now
            held = now - t.entry_ts
            print(
                f"[HOLD] {t.symbol} mark={mark:.4f} pnl={pnl:.2f} peak={t.peak_pnl:.2f} "
                f"giveback={giveback:.2f}/{giveback_need:.2f} TP={t.tp:.4f} SL={t.sl:.4f} "
                f"maxloss={max_loss:.2f} held={held:.0f}s/{self.max_hold_sec:.0f}s "
                f"trail={'ON' if t.peak_pnl >= arm else 'off'}"
            )

        def _sell(reason: str) -> Signal:
            self._last_signal = now
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
                "free_capital_inr": f"{free:.2f}",
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
        self._peak_free = 0.0
        self._day_start_free = 0.0
        self._day_key = ""
        self._drawdown_pct = 0.0

    async def _log(self, line: str):
        print(line)
        try:
            await self.store.push_log(line)
        except Exception:
            pass

    async def start(self):
        await self.store.connect()
        self.client = env_delta_client(CFG)
        plan = await self.store.load_trade_plan()
        self.cycle.restore(plan)
        await self._refresh_capital(force=True)
        await self._load_halt_state()
        auth = False
        if self.client.api_key:
            auth = await self.client.ping_auth()
        await self._log("=" * 60)
        await self._log(" RUBAIH GREEKS — Delta options cycle (capital survival)")
        await self._log(f" LIVE_TRADING: {'ON' if self._live else 'OFF (dry-run)'}")
        await self._log(f" Delta auth: {'OK' if auth else 'NO / missing keys'}")
        await self._log(f" AI: {'ENABLED (OpenRouter→NVIDIA)' if self._ai_enabled else 'DISABLED'}")
        await self._log(
            f" Free: {self.cycle.free_capital_inr:.4f} {self._quote_ccy} "
            f"budget={self.cycle.budget():.4f} cap={self.cycle.max_premium_budget:.2f}"
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
            f" Risk: max_dd={self.cycle.max_drawdown_pct:.0%} "
            f"daily_loss={self.cycle.max_daily_loss_frac:.0%} "
            f"halted={self._halted}"
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
        # Prefer live wallet quote (USDT on Delta India options); else ledger; else seed
        free = 0.0
        source = "unknown"
        quote_ccy = "USDT"
        wallet_rows: List[Dict] = []
        if self.client and self.client._auth_ok:
            try:
                bals = await self.client.get_balances()
                wallet_rows = []
                best_usdt = 0.0
                best_inr = 0.0
                for b in bals or []:
                    ccy = str(b.get("asset_symbol") or b.get("currency") or "").upper()
                    avail = _f(
                        b.get("available_balance")
                        or b.get("available_balance_for_margin")
                        or b.get("balance")
                    )
                    total = _f(b.get("balance") or b.get("wallet_balance") or avail)
                    if ccy:
                        wallet_rows.append(
                            {
                                "asset": ccy,
                                "available": avail,
                                "balance": total,
                            }
                        )
                    if ccy in ("USDT", "USD", "USDC") and avail > best_usdt:
                        best_usdt = avail
                    if ccy == "INR" and avail > best_inr:
                        best_inr = avail
                # Delta India options are USDT-quoted — prefer USDT wallet for sizing
                if best_usdt > 0:
                    free = best_usdt
                    quote_ccy = "USDT"
                    source = "wallet:USDT"
                elif best_inr > 0:
                    free = best_inr
                    quote_ccy = "INR"
                    source = "wallet:INR"
            except Exception as e:
                await self._log(f"[CAPITAL] wallet error: {e}")
        if free <= 0:
            ledger = await self.store.load_capital()
            free = _f(ledger.get("free_quote") or ledger.get("free_inr"))
            if free > 0:
                quote_ccy = str(ledger.get("quote_ccy") or "USDT").upper()
                source = "ledger"
        if free <= 0 and FREE_SEED > 0:
            # Env seed is INR intent; convert once to USDT quote for sizing
            free = FREE_SEED / max(self.cycle.usdt_inr, 1.0)
            quote_ccy = "USDT"
            source = "env_seed_inr_to_usdt"
            await self.store.save_capital(free, source, quote_ccy, self.cycle.usdt_inr)
            await self._log(
                f"[CAPITAL] seeded ₹{FREE_SEED:.0f} → {free:.4f} USDT "
                f"(÷ usdt_inr={self.cycle.usdt_inr:.0f}) day-1 posture"
            )
        if free <= 0:
            free = self.cycle.capital_inr / max(self.cycle.usdt_inr, 1.0)
            quote_ccy = "USDT"
            source = "config"
        self.cycle.set_free_capital(free)
        self._capital_source = source
        self._quote_ccy = quote_ccy
        self._last_capital_refresh = time.time()
        await self.store.save_capital(free, source, quote_ccy, self.cycle.usdt_inr)
        await self.store.save_wallet_snapshot(
            wallet_rows, free, source, quote_ccy, self.cycle.usdt_inr
        )
        if force:
            inr = free * self.cycle.usdt_inr if quote_ccy in ("USDT", "USD", "USDC") else free
            await self._log(
                f"[CAPITAL] free={free:.4f} {quote_ccy} (≈₹{inr:.0f}) source={source}"
            )
        await self._update_risk_after_capital(free)

    async def _load_halt_state(self):
        self._halted = await self.store.is_halted()
        self._halt_reason = await self.store.get_halt_reason() if self._halted else ""
        state = await self.store.load_risk_state()
        self._peak_free = _f(state.get("peak_free"))
        self._day_start_free = _f(state.get("day_start_free"))
        self._day_key = str(state.get("day_key") or "")
        self._drawdown_pct = _f(state.get("drawdown_pct"))

    async def _persist_risk_state(self):
        await self.store.save_risk_state(
            {
                "peak_free": self._peak_free,
                "day_start_free": self._day_start_free,
                "day_key": self._day_key,
                "drawdown_pct": self._drawdown_pct,
                "ts": time.time(),
            }
        )

    async def _update_risk_after_capital(self, free: float):
        """Track peak/daily equity; halt on drawdown or daily loss (survives restart)."""
        if free <= 0:
            return
        day_key = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        if day_key != self._day_key or self._day_start_free <= 0:
            self._day_key = day_key
            self._day_start_free = free
            if self._peak_free <= 0:
                self._peak_free = free
            await self._persist_risk_state()
            await self._log(
                f"[RISK] day start free={free:.4f} {self._quote_ccy} peak={self._peak_free:.4f}"
            )

        if free > self._peak_free:
            self._peak_free = free
        self._drawdown_pct = (
            (self._peak_free - free) / self._peak_free if self._peak_free > 0 else 0.0
        )
        daily_loss_frac = (
            (self._day_start_free - free) / self._day_start_free
            if self._day_start_free > 0
            else 0.0
        )
        await self._persist_risk_state()

        if self._halted:
            return
        if not self.cycle.kill_on_drawdown:
            return

        reason = ""
        if self._drawdown_pct >= self.cycle.max_drawdown_pct:
            reason = (
                f"drawdown {self._drawdown_pct:.1%} >= "
                f"{self.cycle.max_drawdown_pct:.1%} (peak={self._peak_free:.4f})"
            )
        elif daily_loss_frac >= self.cycle.max_daily_loss_frac:
            reason = (
                f"daily loss {daily_loss_frac:.1%} >= "
                f"{self.cycle.max_daily_loss_frac:.1%} "
                f"(day_start={self._day_start_free:.4f})"
            )
        if reason:
            await self._trigger_halt(reason)

    async def _trigger_halt(self, reason: str):
        if self._halted:
            return
        self._halted = True
        self._halt_reason = reason
        await self.store.set_halted(True, reason)
        await self._log(f"[HALT] {reason} — flatten open trade; block new entries until resume")
        t = self.cycle.trade
        if t:
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
            await self._execute(sig)
        await self.store.set_status("halted", reason)

    async def _clear_halt(self):
        self._halted = False
        self._halt_reason = ""
        await self.store.set_halted(False)
        # Reset peak/day baseline to current free so we don't immediately re-halt
        free = float(self.cycle.free_capital_inr or 0)
        if free > 0:
            self._peak_free = free
            self._day_start_free = free
            self._day_key = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            self._drawdown_pct = 0.0
            await self._persist_risk_state()
        await self.store.set_status("running")
        await self._log("[HALT] Cleared — entries allowed again (risk baseline reset)")

    async def _publish(self):
        t = self.cycle.trade
        mark = self._mark_cache.get(t.symbol, 0.0) if t else 0.0
        upnl = ((mark - t.entry_premium) * t.size * float(t.contract_value or 1.0)) if t and mark > 0 else 0.0
        free = float(self.cycle.free_capital_inr or 0)
        ccy = getattr(self, "_quote_ccy", "USDT") or "USDT"
        inr_approx = free * float(self.cycle.usdt_inr) if ccy in ("USDT", "USD", "USDC") else free
        snap = {
            "ts": time.time(),
            "mode": "options_cycle",
            "live": self._live,
            "free_capital_inr": free,  # legacy: quote units used for sizing
            "free_quote": free,
            "quote_ccy": ccy,
            "free_inr_approx": inr_approx,
            "usdt_inr": self.cycle.usdt_inr,
            "budget_inr": self.cycle.budget(),
            "budget_quote": self.cycle.budget(),
            "capital_source": self._capital_source,
            "session_pnl": self._session_pnl + upnl,
            "halted": self._halted,
            "halt_reason": self._halt_reason,
            "drawdown_pct": self._drawdown_pct,
            "peak_free": self._peak_free,
            "day_start_free": self._day_start_free,
            "ai_enabled": self._ai_enabled,
            "ai_last_action": (self._ai_last or {}).get("action"),
            "ai_confidence": (self._ai_last or {}).get("confidence"),
            "ai_emergency_conf": 0.95,
            "position": None
            if not t
            else {
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
            },
            "tp_display": f"Premium +{self.cycle.tp_pct*100:.0f}%",
            "sl_display": f"Premium −{self.cycle.sl_pct*100:.0f}%",
            "max_hold_sec": self.cycle.max_hold_sec,
        }
        await self.store.publish_dashboard(snap)

    async def _clear_external_flat(self, reason: str, refresh_wallet: bool = True):
        """Delta already flat (manual close / reduce_only empty) — drop local ghost."""
        t = self.cycle.trade
        sym = t.symbol if t else "?"
        await self._log(f"[SYNC] Clearing local trade {sym} — {reason}")
        self.cycle.clear()
        self._last_flatten_ts = time.time()
        self._last_fill_ts = 0.0
        await self.store.save_trade_plan({})
        if refresh_wallet:
            await self._refresh_capital(force=True)
        else:
            await self.store.save_capital(
                self.cycle.free_capital_inr,
                "ledger",
                getattr(self, "_quote_ccy", "USDT"),
                self.cycle.usdt_inr,
            )
        await self._publish()

    async def _execute(self, sig: Signal):
        await self._log(f"[SIGNAL] {sig.reason}")
        if sig.action == "BUY" and self.cycle.allow_sell_premium is False:
            pass  # buy path only
        live_ok = self._live and self.client and self.client._auth_ok
        fill_px = sig.premium
        if live_ok:
            try:
                if sig.action == "BUY":
                    await self.client.place_order(
                        sig.product_id, sig.size, "buy", order_type="market_order"
                    )
                else:
                    await self.client.place_order(
                        sig.product_id,
                        sig.size,
                        "sell",
                        order_type="market_order",
                        reduce_only=True,
                    )
                self._last_fill_ts = time.time()
                await self._log(f"[FILL] LIVE {sig.action} {sig.symbol} size={sig.size}")
            except Exception as e:
                await self._log(f"[ORDER] failed: {e}")
                code = getattr(e, "code", "") or ""
                text = str(e).lower()
                # Manual close on Delta / already flat — stop EXIT loops
                if sig.action == "SELL" and (
                    code == "no_position_for_reduce_only"
                    or "no_position_for_reduce_only" in text
                ):
                    await self._clear_external_flat(
                        "Delta no_position_for_reduce_only (already flat)"
                    )
                return
        else:
            await self._log(f"[DRY] {sig.action} {sig.symbol} size={sig.size} @ {fill_px:.4f}")
            self._last_fill_ts = time.time()

        await self.store.save_trade(sig)
        if sig.action == "BUY":
            cval = float(sig.contract_value or 1.0)
            cost = fill_px * sig.size * cval
            fee = cost * self.cycle.taker_fee
            self.cycle.arm(sig, fill_px)
            self.cycle.set_free_capital(max(0.0, self.cycle.free_capital_inr - cost - fee))
            await self.store.save_trade_plan(self.cycle.trade_plan_dict())
            await self.store.save_capital(
                self.cycle.free_capital_inr,
                "ledger",
                getattr(self, "_quote_ccy", "USDT"),
                self.cycle.usdt_inr,
            )
            self._capital_source = "ledger"
        else:
            t = self.cycle.trade
            pnl = 0.0
            release = 0.0
            if t:
                cval = float(t.contract_value or 1.0)
                pnl = (fill_px - t.entry_premium) * t.size * cval
                release = t.premium_budget
                fee = fill_px * t.size * cval * self.cycle.taker_fee
                self._session_pnl += pnl - fee
                self.cycle.set_free_capital(
                    max(0.0, self.cycle.free_capital_inr + release + pnl - fee)
                )
            self.cycle.clear()
            self._last_flatten_ts = time.time()
            await self.store.save_trade_plan({})
            await self.store.save_capital(
                self.cycle.free_capital_inr,
                "ledger",
                getattr(self, "_quote_ccy", "USDT"),
                self.cycle.usdt_inr,
            )
            self._capital_source = "ledger"
            await self._log(
                f"[FLAT] pnl≈{pnl:.2f} free={self.cycle.free_capital_inr:.2f}"
            )
            # Authoritative free from Delta after exit
            await self._refresh_capital(force=True)

    async def main_loop(self):
        while self._running:
            try:
                if not self.client:
                    await asyncio.sleep(2)
                    continue
                if time.time() - self._last_capital_refresh > 60:
                    await self._refresh_capital(force=False)
                tickers = await self.client.get_option_tickers(self.cycle.underlyings)
                # refresh marks for open trade
                if self.cycle.trade:
                    sym = self.cycle.trade.symbol
                    try:
                        tk = await self.client.get_ticker(sym)
                        if tk:
                            mark = _f(tk.get("mark_price") or (tk.get("quotes") or {}).get("mark_price"))
                            if mark > 0:
                                self._mark_cache[sym] = mark
                            g = tk.get("greeks") or {}
                            dlt = _f(g.get("delta") or tk.get("delta"))
                            if dlt != 0:
                                self._delta_cache[sym] = dlt
                            exit_sig = self.cycle.evaluate_exit(
                                mark if mark > 0 else self._mark_cache.get(sym, 0)
                            )
                            if exit_sig:
                                await self._execute(exit_sig)
                    except Exception as e:
                        await self._log(f"[MARK] {sym}: {e}")
                elif self._halted:
                    if int(time.time()) % 90 < 3:
                        await self._log(
                            f"[HALT] blocked entries — {self._halt_reason or 'risk'} "
                            f"(POST /api/resume to clear)"
                        )
                else:
                    entry = self.cycle.pick_entry(tickers or [])
                    if entry:
                        await self._execute(entry)
                    elif int(time.time()) % 60 < 3:
                        await self._log(
                            f"[SCAN] candidates from {len(tickers or [])} tickers | "
                            f"free={self.cycle.free_capital_inr:.4f} budget={self.cycle.budget():.4f}"
                        )
                await self._publish()
            except Exception as e:
                await self._log(f"[MAIN] {type(e).__name__}: {e}")
            await asyncio.sleep(3)

    async def sync_loop(self):
        """Keep local open trade aligned with Delta (manual open/close)."""
        while self._running:
            try:
                if not (self.client and self.client._auth_ok):
                    await asyncio.sleep(5)
                    continue

                # Always poll Delta when we think we have a local trade
                if self.cycle.trade:
                    pid = int(self.cycle.trade.product_id or 0)
                    try:
                        positions = await self.client.get_positions(product_id=pid or None)
                    except Exception as e:
                        await self._log(f"[SYNC] positions: {e}")
                        await asyncio.sleep(8)
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

                    # Restored plans after restart: _last_fill_ts may be 0 — still clear if Delta flat
                    age_ok = (
                        self._last_fill_ts <= 0
                        or time.time() - self._last_fill_ts > 8
                    )
                    ghost = age_ok and abs(matched_size) == 0 and (
                        not pid or pid not in open_pids
                    )
                    if ghost:
                        await self._clear_external_flat(
                            "Delta position size=0 (manual/external close)"
                        )
                    elif abs(matched_size) > 0 and abs(matched_size) != abs(
                        int(self.cycle.trade.size or 0)
                    ):
                        # Size changed on exchange — adopt Delta size
                        self.cycle.trade.size = abs(int(matched_size))
                        await self.store.save_trade_plan(self.cycle.trade_plan_dict())
                        await self._log(
                            f"[SYNC] size aligned to Delta → {self.cycle.trade.size} "
                            f"on {self.cycle.trade.symbol}"
                        )

                await asyncio.sleep(5)
            except Exception as e:
                await self._log(f"[SYNC] {e}")
                await asyncio.sleep(8)

    async def command_loop(self):
        while self._running:
            try:
                cmd = await self.store.pop_command()
                if not cmd:
                    await asyncio.sleep(1)
                    continue
                cmd = cmd.strip().lower()
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
                elif cmd in ("sync", "sync_positions", "clear_ghost"):
                    await self._log(f"[CMD] {cmd}")
                    if self.cycle.trade and self.client and self.client._auth_ok:
                        pid = int(self.cycle.trade.product_id or 0)
                        try:
                            positions = await self.client.get_positions(product_id=pid or None)
                            matched = 0
                            for p in positions or []:
                                p_pid = int(
                                    p.get("product_id")
                                    or (p.get("product") or {}).get("id")
                                    or 0
                                )
                                size = int(_f(p.get("size") or p.get("position_size")))
                                if pid and p_pid == pid:
                                    matched = size
                                    break
                            if abs(matched) == 0:
                                await self._clear_external_flat(
                                    "forced sync — Delta flat"
                                )
                            else:
                                await self._log(
                                    f"[SYNC] Delta still open size={matched} "
                                    f"on {self.cycle.trade.symbol}"
                                )
                        except Exception as e:
                            # If sync API fails but user insists clear_ghost
                            if cmd == "clear_ghost":
                                await self._clear_external_flat(
                                    f"forced clear_ghost after sync error: {e}",
                                    refresh_wallet=True,
                                )
                            else:
                                await self._log(f"[SYNC] cmd failed: {e}")
                    elif self.cycle.trade and cmd == "clear_ghost":
                        await self._clear_external_flat("forced clear_ghost")
                    else:
                        await self._log("[SYNC] no local trade to sync")
                    await self._publish()
            except Exception as e:
                await self._log(f"[CMD] {e}")
                await asyncio.sleep(1)

    async def ai_loop(self):
        """Advisory overlay every ~3 minutes. Quant remains authority."""
        while self._running:
            try:
                t = self.cycle.trade
                mark = self._mark_cache.get(t.symbol, 0.0) if t else 0.0
                upnl = 0.0
                pos = None
                if t and mark > 0:
                    upnl = (mark - t.entry_premium) * t.size * float(t.contract_value or 1.0)
                    pos = {
                        "symbol": t.symbol,
                        "side": "long",
                        "option_type": t.option_type,
                        "strike": t.strike,
                        "size": t.size,
                        "entry": t.entry_premium,
                        "mark": mark,
                        "tp": t.tp,
                        "sl": t.sl,
                    }
                context = {
                    "free_capital": self.cycle.free_capital_inr,
                    "quant_signal": "HOLD" if t else "SCAN",
                    "position": pos,
                    "upnl": upnl,
                    "underlyings": self.cycle.underlyings,
                    "notes": "buy-side options cycle; no premium selling",
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
        await self.store.set_status("kill_switch", reason)
        t = self.cycle.trade
        if t:
            sig = Signal(
                action="SELL",
                symbol=t.symbol,
                product_id=t.product_id,
                size=t.size,
                premium=self._mark_cache.get(t.symbol, t.entry_premium),
                underlying=t.underlying,
                option_type=t.option_type,
                strike=t.strike,
                contract_value=t.contract_value,
                reason=f"EXIT_EMERGENCY: {reason}",
            )
            await self._execute(sig)
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
