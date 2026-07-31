#!/usr/bin/env bash
# Rubaih Greeks — VPS bootstrap (separate from Rubaih futures)
set -euo pipefail
cd "$(dirname "$0")"

if [[ ! -f .env ]]; then
  echo "Copy .env.example → .env and fill secrets first."
  exit 1
fi

# shellcheck disable=SC1091
set -a
source .env
set +a

if [[ -z "${DB_PASSWORD:-}" || "${DB_PASSWORD}" == "CHANGE_ME_STRONG_DB_PASSWORD" ]]; then
  echo "Set a strong DB_PASSWORD in .env"
  exit 1
fi

if [[ -z "${RUBAIH_GREEKS_API_TOKEN:-}" ]]; then
  echo "Set RUBAIH_GREEKS_API_TOKEN in .env (openssl rand -hex 32)"
  exit 1
fi

echo "[greeks] Building & starting stack (ports 8088 / 8018)…"
docker compose up -d --build

echo "[greeks] Waiting for API health…"
for i in {1..30}; do
  if curl -fsS "http://127.0.0.1:8018/api/health" >/dev/null 2>&1; then
    echo "[greeks] API healthy."
    docker compose ps
    echo "Public nginx: http://YOUR_IP:8088/api/health"
    echo "Keep LIVE_TRADING=false until dry-run logs look clean."
    exit 0
  fi
  sleep 2
done

echo "[greeks] API not healthy yet — check: docker compose logs -f greeks_api greeks_engine"
exit 1
