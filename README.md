# Rubaih Greeks — Delta Options Cycle Bot

> Sister of Rubaih (CoinDCX futures). Same feel: **flat → scan → buy → TP/SL/trail → flat**.  
> Exchange: **Delta Exchange (India by default)**. Buy-side options only in v1.  
> **Educational / research software. Live trading can lose money.**

## Day-1 posture (₹1,000)

- Default `LIVE_TRADING=false` (dry-run)
- Seed free capital with `RUBAIH_GREEKS_FREE_INR=1000`
- Treat day 1 as **bug hunt / system check**, not PnL proof
- Scale toward ₹10K only after fills, exits, and manual-close sync look clean

## What it trades

| Rule | v1 choice |
|------|-----------|
| Underlyings | BTC, ETH |
| Direction | Momentum → buy calls (up) or puts (down) |
| Strikes | Near ATM, liquid, DTE band |
| Selling premium | **Off** |
| Sizing | 50–60% of free capital as premium budget |
| Exits | Premium TP +25% / SL −12% / early trail (0.5R) |

## Architecture

```
Mobile/API token → Nginx :8088 → FastAPI → Redis/Postgres
                                      ↑
                         Greeks engine → Delta options API
```

Separate stack from Rubaih futures (different containers, ports, Redis/Postgres, capital).

## Quick start

```bash
cp .env.example .env
# fill DELTA_API_KEY / DELTA_API_SECRET / DB_PASSWORD / RUBAIH_GREEKS_API_TOKEN
# keep LIVE_TRADING=false until dry-run looks right

sudo bash setup-vps.sh
docker compose logs -f greeks_engine
```

Only then:

```bash
# in .env
LIVE_TRADING=true
RUBAIH_GREEKS_FREE_INR=1000
docker compose up -d --force-recreate greeks_engine greeks_api
```

## Safety

| Control | Behavior |
|---------|----------|
| `LIVE_TRADING` | Must be `true` for real Delta orders |
| API token | Required on all routes except `/api/health` |
| Kill switch | Authenticated POST → halt + flatten |
| Manual close | Sync clears local trade when Delta is flat |
| Buy-only | Never opens short options in v1 |

## Ports (coexist with Rubaih futures)

| Service | Host bind |
|---------|-----------|
| API | `127.0.0.1:8018` |
| Nginx | `8088` |
| Postgres | `127.0.0.1:5438` |
| Redis | `127.0.0.1:6388` |
