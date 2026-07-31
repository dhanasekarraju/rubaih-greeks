"""
Rubaih Greeks API — dashboard, settings, kill switch.
Token: RUBAIH_GREEKS_API_TOKEN (header X-API-Token or ?token=)
"""

from __future__ import annotations

import json
import os
from typing import Any, Dict, Optional

import asyncpg
import redis.asyncio as aioredis
import yaml
from dotenv import load_dotenv
from fastapi import Depends, FastAPI, Header, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pathlib import Path

load_dotenv()

ROOT = Path(__file__).resolve().parent
CFG = yaml.safe_load((ROOT / "config.yaml").read_text())
TOKEN = os.getenv("RUBAIH_GREEKS_API_TOKEN", "").strip()
LIVE = os.getenv("LIVE_TRADING", "false").strip().lower() in ("1", "true", "yes")

try:
    from ai_advisor import ai_configured
except Exception:  # pragma: no cover
    def ai_configured() -> bool:  # type: ignore
        return False

app = FastAPI(title="Rubaih Greeks", version="0.1.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

pg_pool: Optional[asyncpg.Pool] = None
rd: Optional[aioredis.Redis] = None


async def require_token(
    x_api_token: Optional[str] = Header(default=None),
    token: Optional[str] = Query(default=None),
):
    got = (x_api_token or token or "").strip()
    if not TOKEN or got != TOKEN:
        raise HTTPException(status_code=401, detail="unauthorized")


@app.on_event("startup")
async def startup():
    global pg_pool, rd
    pg_pool = await asyncpg.create_pool(
        host=os.getenv("DB_HOST", "postgres"),
        port=int(os.getenv("DB_PORT", "5432")),
        user=os.getenv("DB_USER", "greeks"),
        password=os.getenv("DB_PASSWORD", "greeks"),
        database=os.getenv("DB_NAME", "rubaih_greeks"),
        min_size=1,
        max_size=5,
    )
    rd = aioredis.Redis(
        host=os.getenv("REDIS_HOST", "redis"),
        port=int(os.getenv("REDIS_PORT", "6379")),
        decode_responses=True,
    )
    await rd.ping()


@app.on_event("shutdown")
async def shutdown():
    if pg_pool:
        await pg_pool.close()
    if rd:
        await rd.aclose()


@app.get("/api/health")
async def health():
    return {
        "ok": True,
        "service": "rubaih-greeks",
        "live": LIVE,
        "ai_enabled": ai_configured(),
        "ai_order": "openrouter→nvidia",
    }


@app.get("/api/dashboard", dependencies=[Depends(require_token)])
async def dashboard():
    raw = await rd.get("greeks:dashboard") if rd else None
    status = await rd.get("greeks:engine_status") if rd else None
    data = json.loads(raw) if raw else {}
    data["engine_status"] = status or "unknown"
    data["live_trading"] = LIVE
    return data


@app.get("/api/settings", dependencies=[Depends(require_token)])
async def settings():
    s = CFG.get("trading", {}).get("strategy", {})
    t = CFG.get("trading", {})
    stored = await rd.hgetall("greeks:settings") if rd else {}
    free = stored.get("free_capital_inr") or str(t.get("capital_inr", 1000))
    return {
        "mode": "options_cycle",
        "exchange": "delta",
        "live_trading": str(LIVE).lower(),
        "underlyings": ",".join(t.get("underlyings") or ["BTC", "ETH"]),
        "capital_inr": str(t.get("capital_inr", 1000)),
        "free_capital_inr": free,
        "capital_source": stored.get("capital_source", "unknown"),
        "margin_use_frac": str(t.get("margin_use_frac", 0.55)),
        "margin_use_max_frac": str(t.get("margin_use_max_frac", 0.60)),
        "take_profit_premium_pct": str(s.get("take_profit_premium_pct", 0.25)),
        "stop_loss_premium_pct": str(s.get("stop_loss_premium_pct", 0.12)),
        "tp_display": f"Premium +{float(s.get('take_profit_premium_pct', 0.25))*100:.0f}%",
        "sl_display": f"Premium −{float(s.get('stop_loss_premium_pct', 0.12))*100:.0f}%",
        "trail_arm_r": str(s.get("trail_arm_r", 0.5)),
        "min_dte_days": str(s.get("min_dte_days", 1)),
        "max_dte_days": str(s.get("max_dte_days", 7)),
        "allow_sell_premium": str(bool(t.get("allow_sell_premium", False))).lower(),
        "ai_emergency_conf": "0.95",
        "ai_note": "ENTER/EXIT advisory only; EMERGENCY acts only if confidence > 0.95",
        **stored,
    }


@app.get("/api/trades", dependencies=[Depends(require_token)])
async def trades(limit: int = Query(default=30, ge=1, le=200)):
    if not pg_pool:
        return []
    rows = await pg_pool.fetch(
        "SELECT * FROM option_trades ORDER BY timestamp DESC LIMIT $1", limit
    )
    return [dict(r) for r in rows]


@app.get("/api/logs", dependencies=[Depends(require_token)])
async def get_logs(limit: int = Query(default=100, ge=1, le=200)):
    if not rd:
        return []
    rows = await rd.lrange("greeks:logs", 0, limit - 1)
    out = []
    for r in rows:
        try:
            out.append(json.loads(r))
        except Exception:
            out.append({"ts": None, "line": str(r)})
    return out


@app.get("/api/signals", dependencies=[Depends(require_token)])
async def get_signals(limit: int = Query(default=40, ge=1, le=100)):
    """AI decisions (signals) for the mobile Signals tab."""
    if pg_pool:
        rows = await pg_pool.fetch(
            "SELECT * FROM ai_decisions ORDER BY timestamp DESC LIMIT $1", limit
        )
        if rows:
            return [
                {
                    "id": r["id"],
                    "ts": r["timestamp"].isoformat() if r["timestamp"] else None,
                    "model": r["model"],
                    "action": r["action"],
                    "confidence": float(r["confidence"]) if r["confidence"] is not None else None,
                    "reasoning": r["reasoning"],
                    "risk_assessment": r["risk_assessment"],
                }
                for r in rows
            ]
    if not rd:
        return []
    rows = await rd.lrange("greeks:signals", 0, limit - 1)
    out = []
    for r in rows:
        try:
            out.append(json.loads(r))
        except Exception:
            pass
    return out


@app.get("/api/balance", dependencies=[Depends(require_token)])
async def get_balance():
    """Live Delta wallet snapshot + free capital used by the cycle."""
    wallet = {}
    if rd:
        raw = await rd.get("greeks:wallet")
        if raw:
            try:
                wallet = json.loads(raw)
            except Exception:
                wallet = {}
        settings = await rd.hgetall("greeks:settings") or {}
    else:
        settings = {}
    return {
        "free_capital": wallet.get("free_capital")
        or float(settings.get("free_capital_inr") or 0),
        "source": wallet.get("source") or settings.get("capital_source") or "unknown",
        "ts": wallet.get("ts"),
        "balances": wallet.get("balances") or [],
        "live_trading": LIVE,
    }


@app.post("/api/kill", dependencies=[Depends(require_token)])
async def kill():
    if rd:
        await rd.rpush("greeks:commands", "kill")
    return {"ok": True, "queued": "kill"}


@app.post("/api/refresh-capital", dependencies=[Depends(require_token)])
async def refresh_capital():
    if rd:
        await rd.rpush("greeks:commands", "refresh_capital")
    return {"ok": True, "queued": "refresh_capital"}


@app.websocket("/ws")
async def ws(websocket: WebSocket, token: Optional[str] = Query(default=None)):
    if not TOKEN or (token or "").strip() != TOKEN:
        await websocket.close(code=4401)
        return
    await websocket.accept()
    pubsub = rd.pubsub() if rd else None
    try:
        if pubsub:
            await pubsub.subscribe("greeks:dashboard", "greeks:log", "greeks:signal")
        while True:
            if pubsub:
                msg = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
                if msg and msg.get("data"):
                    channel = msg.get("channel")
                    raw = msg["data"]
                    if channel == "greeks:dashboard":
                        await websocket.send_text(raw if isinstance(raw, str) else json.dumps(raw))
                    else:
                        try:
                            data = json.loads(raw) if isinstance(raw, str) else raw
                        except Exception:
                            data = {"line": str(raw)}
                        await websocket.send_text(
                            json.dumps({"channel": channel, "data": data})
                        )
            else:
                await websocket.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        if pubsub:
            await pubsub.unsubscribe("greeks:dashboard", "greeks:log", "greeks:signal")
            await pubsub.aclose()
