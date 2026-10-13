"""Safety admission and executable quote regression tests (no exchange/network)."""
import copy
import sys
import time
import unittest
from pathlib import Path
from datetime import datetime, timedelta, timezone

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import yaml
from greeks_engine import GreeksEngine, OptionsCycle, Signal

CFG = yaml.safe_load((Path(__file__).resolve().parents[1] / "config.yaml").read_text())


def candidate(symbol="C-BTC-65000-201026", underlying="BTC", side="call",
              bid=1.0, ask=1.02, delta=0.4):
    expiry = (datetime.now(timezone.utc) + timedelta(days=3)).isoformat()
    return {
        "symbol": symbol, "product_id": 123, "contract_type": side,
        "underlying_asset_symbol": underlying, "strike_price": 65000,
        "spot_price": 65000, "contract_value": 1,
        "mark_price": 1.01, "quotes": {"best_bid": bid, "best_ask": ask},
        "greeks": {"delta": delta},
        "expiry_time": expiry,
    }


class AdmissionTests(unittest.TestCase):
    def setUp(self):
        self.cfg = copy.deepcopy(CFG)
        self.cycle = OptionsCycle(self.cfg)
        self.cycle.set_free_capital(34)

    def test_missing_or_crossed_quotes_rejected(self):
        for bid, ask in [(0, 1), (1, 0), (2, 1)]:
            with self.subTest(bid=bid, ask=ask):
                self.assertIsNone(self.cycle.parse_ticker(candidate(bid=bid, ask=ask)))

    def test_missing_and_incorrect_signed_delta_rejected(self):
        for delta in [0, -0.4, 1.2]:
            with self.subTest(delta=delta):
                self.assertIsNone(self.cycle.parse_ticker(candidate(delta=delta)))
        self.assertIsNotNone(self.cycle.parse_ticker(candidate()))

    def test_poll_bursts_do_not_synthesize_momentum(self):
        self.cycle.note_spot("BTC", 65000, now=100.0)
        for i in range(10):
            self.cycle.note_spot("BTC", 65000 + i, now=100.0 + i * 0.01)
        self.assertEqual(len(self.cycle._spot_hist["BTC"]), 1)
        self.assertEqual(self.cycle.momentum("BTC"), 0.0)

    def test_reserved_budget_and_fee_are_respected(self):
        budget = self.cycle.budget_for("BTC")
        self.assertGreater(budget, 0)
        self.assertLessEqual(budget, 5.0)
        self.assertLess(self.cycle.budget_for("ETH", reserved_quote=10), budget)
        self.assertLessEqual(self.cycle.budget_for("ETH", reserved_quote=100), 0)

    def test_final_admission_rejects_bad_cost_and_duplicate_underlying(self):
        eng = GreeksEngine()
        eng.cycle.set_free_capital(34.0)
        sig = Signal(action="BUY", symbol="C-BTC-65000-201026",
                     product_id=123, size=1, reason="test",
                     premium=1.0, underlying="BTC", option_type="call",
                     strike=65000.0, contract_value=1.0)
        self.assertEqual(eng._buy_reject_reason(sig), "")
        huge = copy.copy(sig)
        huge.size = 100
        self.assertIn("insufficient", eng._buy_reject_reason(huge))
        eng.cycle.arm(sig)
        self.assertIn("occupied", eng._buy_reject_reason(sig))
        eng._halted = True
        self.assertIn("halted", eng._buy_reject_reason(sig))

    def test_flatten_failed_blocks_admission(self):
        eng = GreeksEngine()
        eng.cycle.set_free_capital(34)
        eng._flatten_failed = True
        sig = Signal(action="BUY", symbol="C-BTC-65000-201026",
                     product_id=123, size=1, reason="test",
                     premium=1, underlying="BTC", option_type="call",
                     strike=65000, contract_value=1)
        self.assertIn("halted", eng._buy_reject_reason(sig))

    def test_live_mode_never_falls_back_to_paper(self):
        import asyncio

        async def execute():
            eng = GreeksEngine()
            eng._live = True
            eng.client = None  # Missing credentials / unavailable exchange
            eng.cycle.set_free_capital(34.0)
            sig = Signal(action="BUY", symbol="C-BTC-65000-201026",
                         product_id=123, size=1, reason="test",
                         premium=1, underlying="BTC", option_type="call",
                         strike=65000, contract_value=1)
            result = await eng._execute_serialized(sig)
            self.assertFalse(result)
            self.assertEqual(eng.cycle.slots_open(), 0)
            self.assertEqual(eng.cycle.free_capital_quote, 34.0)

        asyncio.run(execute())


if __name__ == "__main__":
    unittest.main()
