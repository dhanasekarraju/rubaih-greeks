"""
Rubaih Greeks API — dashboard, settings, kill switch.
Token: RUBAIH_GREEKS_API_TOKEN (header X-API-Token or ?token=)
"""

from __future__ import annotations

import json
import os
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, Optional

import asyncpg
import redis.asyncio as aioredis
import yaml
from dotenv import load_dotenv
from fastapi import Depends, FastAPI, Header, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pathlib import Path

from command_bus import sign_command

load_dotenv()

ROOT = Path(__file__).resolve().parent
CFG = yaml.safe_load((ROOT / "config.yaml").read_text())
TOKEN = (
    os.getenv("RUBAIH_GREEKS_API_TOKEN")
    or os.getenv("RUBAIH_API_TOKEN")
    or ""
).strip()
LIVE = os.getenv("LIVE_TRADING", "false").strip().lower() in ("1", "true", "yes")

try:
    from ai_advisor import ai_configured
except Exception:  # pragma: no cover
    def ai_configured() -> bool:  # type: ignore
        return False


def _jsonable(v: Any) -> Any:
    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, (datetime, date)):
        return v.isoformat()
    if isinstance(v, dict):
        return {k: _jsonable(x) for k, x in v.items()}
    if isinstance(v, (list, tuple)):
        return [_jsonable(x) for x in v]
    return v


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
    authorization: Optional[str] = Header(default=None),
    token: Optional[str] = Query(default=None),
):
    bearer = ""
    if authorization and authorization.strip().lower().startswith("bearer "):
        bearer = authorization.strip()[7:]
    got = (x_api_token or bearer or token or "").strip()
    if not TOKEN or got != TOKEN:
        raise HTTPException(status_code=401, detail="unauthorized")


@app.on_event("startup")
async def startup():
    global pg_pool, rd
    if len(TOKEN) < 16:
        raise RuntimeError(
            "RUBAIH_GREEKS_API_TOKEN (or RUBAIH_API_TOKEN) must be >=16 chars"
        )
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
    # Fill gaps from redis so mobile Home works even if wallet key is empty
    if rd:
        settings = await rd.hgetall("greeks:settings") or {}
        if data.get("free_capital_inr") is None and settings.get("free_capital_inr"):
            try:
                data["free_capital_inr"] = float(settings["free_capital_inr"])
            except (TypeError, ValueError):
                data["free_capital_inr"] = settings["free_capital_inr"]
        if not data.get("capital_source") and settings.get("capital_source"):
            data["capital_source"] = settings["capital_source"]
        halted = (await rd.get("greeks:halted") or "").strip() in ("1", "true", "yes")
        data["halted"] = bool(data.get("halted")) or halted
        if data["halted"] and not data.get("halt_reason"):
            raw_h = await rd.get("greeks:halt_reason")
            if raw_h:
                try:
                    data["halt_reason"] = json.loads(raw_h).get("reason") or raw_h
                except Exception:
                    data["halt_reason"] = raw_h
        ai_raw = await rd.get("greeks:ai_last")
        if ai_raw and not data.get("ai_last_action"):
            try:
                ai = json.loads(ai_raw)
                data["ai_last_action"] = ai.get("action")
                data["ai_confidence"] = ai.get("confidence")
            except Exception:
                pass
    return _jsonable(data)


@app.get("/api/settings", dependencies=[Depends(require_token)])
async def settings():
    s = CFG.get("trading", {}).get("strategy", {})
    t = CFG.get("trading", {})
    stored = await rd.hgetall("greeks:settings") if rd else {}
    free = stored.get("free_capital_inr") or str(t.get("capital_inr", 1000))
    risk = CFG.get("risk") or {}
    return {
        "mode": "options_cycle",
        "exchange": "delta",
        "live_trading": str(LIVE).lower(),
        "underlyings": ",".join(t.get("underlyings") or ["BTC", "ETH"]),
        "capital_inr": str(t.get("capital_inr", 1000)),
        "free_capital_inr": free,
        "capital_source": stored.get("capital_source", "unknown"),
        "margin_use_frac": str(t.get("margin_use_frac", 0.20)),
        "margin_use_max_frac": str(t.get("margin_use_max_frac", 0.25)),
        "max_premium_budget_usdt": str(t.get("max_premium_budget_usdt", 5.0)),
        "max_open_underlyings": str(t.get("max_open_underlyings", 2)),
        "one_per_underlying": str(bool(t.get("one_per_underlying", True))).lower(),
        "take_profit_premium_pct": str(s.get("take_profit_premium_pct", 0.50)),
        "stop_loss_premium_pct": str(s.get("stop_loss_premium_pct", 0.25)),
        "tp_display": f"Premium +{float(s.get('take_profit_premium_pct', 0.50))*100:.0f}%",
        "sl_display": f"Premium −{float(s.get('stop_loss_premium_pct', 0.25))*100:.0f}%",
        "trail_arm_r": str(s.get("trail_arm_r", 0.7)),
        "max_hold_sec": str(s.get("max_hold_sec", 14400)),
        "min_dte_days": str(s.get("min_dte_days", 1)),
        "max_dte_days": str(s.get("max_dte_days", 7)),
        "min_delta": str(s.get("min_delta", 0.25)),
        "max_delta": str(s.get("max_delta", 0.60)),
        "max_spread_pct": str(s.get("max_spread_pct", 0.03)),
        "max_loss_frac": str(s.get("max_loss_frac", 0.30)),
        "min_edge_multiple": str(s.get("min_edge_multiple", 1.5)),
        "max_drawdown_pct": str(risk.get("max_drawdown_pct", 0.15)),
        "max_daily_loss_frac": str(risk.get("max_daily_loss_frac", 0.25)),
        "max_total_loss_frac": str(risk.get("max_total_loss_frac", 0.35)),
        "bot_allocation_quote": str(risk.get("bot_allocation_quote", 34.0)),
        "min_free_quote": str(risk.get("min_free_quote", 3.0)),
        "auto_resume": str(bool(risk.get("auto_resume", True))).lower(),
        "halt_cooldown_min": str(risk.get("halt_cooldown_min", 60)),
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
    return JSONResponse(content=_jsonable([dict(r) for r in rows]))


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
            return _jsonable([
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
            ])
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
    wallet_free = (
        wallet.get("free_quote")
        if wallet.get("free_quote") is not None
        else wallet.get("free_capital")
    )
    stored_free = settings.get(
        "free_capital_quote",
        settings.get("free_quote", settings.get("free_capital_inr", 0)),
    )
    free_quote = float(wallet_free if wallet_free is not None else stored_free or 0)
    return {
        "free_capital": free_quote,
        "free_quote": free_quote,
        "quote_ccy": wallet.get("quote_ccy") or settings.get("quote_ccy") or "USDT",
        "free_inr_approx": wallet.get("free_inr_approx")
        or float(settings.get("free_inr_approx") or 0),
        "source": wallet.get("source") or settings.get("capital_source") or "unknown",
        "ts": wallet.get("ts"),
        "balances": wallet.get("balances") or [],
        "live_trading": LIVE,
        "halted": (await rd.get("greeks:halted") or "").strip() in ("1", "true", "yes") if rd else False,
    }


async def _queue_command(command: str):
    if not rd:
        raise HTTPException(status_code=503, detail="redis unavailable")
    payload = sign_command(TOKEN, command, source="authenticated_api")
    await rd.rpush("greeks:commands", json.dumps(payload))


@app.post("/api/kill", dependencies=[Depends(require_token)])
async def kill():
    await _queue_command("kill")
    return {"ok": True, "queued": "kill"}


@app.post("/api/refresh-capital", dependencies=[Depends(require_token)])
async def refresh_capital():
    await _queue_command("refresh_capital")
    return {"ok": True, "queued": "refresh_capital"}


@app.post("/api/resume", dependencies=[Depends(require_token)])
async def resume():
    """Clear risk halt so the engine may enter again (resets DD baseline)."""
    await _queue_command("resume")
    return {"ok": True, "queued": "resume"}


@app.post("/api/sync-positions", dependencies=[Depends(require_token)])
async def sync_positions():
    """Force Delta↔local open-trade sync (clears ghost if Delta is flat)."""
    await _queue_command("sync")
    return {"ok": True, "queued": "sync"}


@app.post("/api/clear-history", dependencies=[Depends(require_token)])
async def clear_history():
    """Wipe trade + AI signal + log history in DB/Redis. Does not touch open positions or wallet."""
    deleted = {"option_trades": 0, "ai_decisions": 0, "risk_events": 0, "redis_logs": False, "redis_signals": False}
    if pg_pool:
        for table in ("option_trades", "ai_decisions", "risk_events"):
            row = await pg_pool.fetchrow(f"SELECT COUNT(*)::int AS n FROM {table}")
            n = int(row["n"]) if row else 0
            await pg_pool.execute(f"TRUNCATE TABLE {table} RESTART IDENTITY")
            deleted[table] = n
    if rd:
        deleted["redis_signals"] = bool(await rd.delete("greeks:signals"))
        deleted["redis_logs"] = bool(await rd.delete("greeks:logs"))
    return {"ok": True, "deleted": deleted}


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
