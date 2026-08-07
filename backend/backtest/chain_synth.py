"""
Synthesize an option-chain snapshot (in Delta's raw ticker row shape) from a
spot price, using Black-Scholes. This lets you smoke-test the backtest
harness — and run rough parameter sweeps — without a real historical
options order-book feed, which Delta does not offer for free.

IMPORTANT — read before trusting any numbers this produces:
This assumes a constant implied vol and a frictionless, always-fillable
book. Real weekly crypto options have skew, IV that jumps around news/
funding events, and bid/ask spreads that blow out exactly when you most
want to exit. A strategy that looks profitable only against this synthetic
chain has NOT been validated — it has only been checked for bugs. Treat
synthetic-chain results as a plumbing test, not a profitability estimate.
For a real answer, feed real_chain data (see logger.py + README.md).
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Dict, List


def _norm_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def bs_price_and_delta(
    spot: float, strike: float, t_years: float, iv: float, option_type: str, r: float = 0.0
) -> tuple[float, float]:
    """European option price + delta. t_years, iv must be > 0."""
    if t_years <= 0 or iv <= 0 or spot <= 0 or strike <= 0:
        intrinsic = max(0.0, (spot - strike) if option_type == "call" else (strike - spot))
        delta = 1.0 if (option_type == "call" and spot > strike) else (
            -1.0 if (option_type == "put" and spot < strike) else 0.0
        )
        return intrinsic, delta
    d1 = (math.log(spot / strike) + (r + 0.5 * iv * iv) * t_years) / (iv * math.sqrt(t_years))
    d2 = d1 - iv * math.sqrt(t_years)
    if option_type == "call":
        price = spot * _norm_cdf(d1) - strike * math.exp(-r * t_years) * _norm_cdf(d2)
        delta = _norm_cdf(d1)
    else:
        price = strike * math.exp(-r * t_years) * _norm_cdf(-d2) - spot * _norm_cdf(-d1)
        delta = _norm_cdf(d1) - 1.0
    return max(price, 1e-6), delta


@dataclass
class ChainConfig:
    underlyings: List[str]
    strike_step_frac: float = 0.01     # strikes spaced 1% of spot apart
    strikes_each_side: int = 6         # how many strikes above/below spot
    dtes_days: List[float] = None      # which expiries to offer each snapshot
    iv: float = 0.55                   # flat annualized IV seed (crypto weeklies run ~40-90%)
    iv_jitter: float = 0.05            # per-snapshot random walk on IV, keeps it non-static
    spread_frac: float = 0.02          # bid/ask spread as a fraction of mid
    # Real Delta contracts are a FRACTION of one coin, not one whole coin —
    # e.g. ~0.001 BTC, ~0.01 ETH per contract. Getting this wrong makes every
    # premium come out in the thousands against a few-dollar budget and the
    # backtest silently produces zero trades. Verify against Delta's live
    # product spec for your actual traded contracts before trusting sizing.
    contract_value: Dict[str, float] = None

    def __post_init__(self):
        if self.dtes_days is None:
            self.dtes_days = [1.5, 3.5, 6.5]
        if self.contract_value is None:
            self.contract_value = {"BTC": 0.001, "ETH": 0.01}

    def cval_for(self, underlying: str) -> float:
        return self.contract_value.get(underlying.upper(), 1.0)


class SyntheticChainGenerator:
    """Stateful generator: call step(ts, spots) once per snapshot in
    chronological order to get a Delta-shaped ticker row list."""

    def __init__(self, cfg: ChainConfig, seed: int = 42):
        import random

        self.cfg = cfg
        self._rng = random.Random(seed)
        self._iv_state: Dict[str, float] = {u: cfg.iv for u in cfg.underlyings}
        self._pid_counter = 100000

    def _next_pid(self) -> int:
        self._pid_counter += 1
        return self._pid_counter

    def current_iv(self, underlying: str) -> float:
        return self._iv_state.get(underlying.upper(), self.cfg.iv)

    def step(self, ts: float, spots: Dict[str, float]) -> List[Dict]:
        rows: List[Dict] = []
        for u in self.cfg.underlyings:
            spot = spots.get(u)
            if not spot or spot <= 0:
                continue
            # gentle IV mean-reversion + noise so the edge gate sees some texture
            iv = self._iv_state[u]
            iv += self._rng.gauss(0, self.cfg.iv_jitter * 0.1) + 0.02 * (self.cfg.iv - iv)
            iv = max(0.15, min(2.0, iv))
            self._iv_state[u] = iv

            step = spot * self.cfg.strike_step_frac
            strikes = [
                round(spot + k * step, 2)
                for k in range(-self.cfg.strikes_each_side, self.cfg.strikes_each_side + 1)
            ]
            cval = self.cfg.cval_for(u)
            for dte in self.cfg.dtes_days:
                t_years = dte / 365.0
                for strike in strikes:
                    for otype, contract_type in (("call", "call_options"), ("put", "put_options")):
                        price, delta = bs_price_and_delta(spot, strike, t_years, iv, otype)
                        if price * cval < 0.05:
                            continue
                        half_spread = max(price * self.cfg.spread_frac / 2.0, 1e-4)
                        bid = max(price - half_spread, 1e-6)
                        ask = price + half_spread
                        expiry_ts_abs = ts + dte * 86400.0
                        # Expiry MUST be part of the symbol: strike alone is not
                        # a unique instrument key. Without this, two different-
                        # dated contracts at the same strike collide in any
                        # dict keyed by symbol, and a held position can get
                        # silently repriced against the wrong contract.
                        symbol = f"{'C' if otype == 'call' else 'P'}-{u}-{int(strike)}-{int(expiry_ts_abs)}"
                        rows.append(
                            {
                                "symbol": symbol,
                                "product_id": self._next_pid(),
                                "contract_type": contract_type,
                                "underlying_asset_symbol": u,
                                "mark_price": price,
                                "close": price,
                                "strike_price": strike,
                                "contract_value": cval,
                                "spot_price": spot,
                                "product_trading_status": "operational",
                                "quotes": {"best_bid": bid, "best_ask": ask},
                                "greeks": {"delta": delta if otype == "call" else -abs(delta), "spot": spot},
                                # synthetic expiry: ts + dte days, matches _parse_expiry_ts's
                                # numeric-epoch branch (values > 1e11 -> ms)
                                "expiry_time": (ts + dte * 86400.0) * 1000.0,
                            }
                        )
        return rows
