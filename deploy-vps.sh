#!/usr/bin/env bash
# Run ON the VPS (after clone). Dry-run deploy for Rubaih Greeks.
set -euo pipefail

REPO_DIR="${HOME}/rubaih-greeks"
REPO_URL="https://github.com/dhanasekarraju/rubaih-greeks.git"

if [[ ! -d "$REPO_DIR/.git" ]]; then
  git clone "$REPO_URL" "$REPO_DIR"
fi
cd "$REPO_DIR"
git pull --ff-only origin main

if [[ ! -f .env ]]; then
  echo "Create $REPO_DIR/.env from .env.example (copy from your laptop)."
  echo "Keep LIVE_TRADING=false for day-1."
  exit 1
fi

# Ensure dry-run on first bring-up
sed -i 's/^LIVE_TRADING=.*/LIVE_TRADING=false/' .env || true

sudo bash setup-vps.sh

echo
echo "Health:  curl -s http://127.0.0.1:8018/api/health"
echo "Public:  curl -s http://103.194.228.130:8088/api/health"
echo "Logs:    docker compose logs -f greeks_engine"
echo "Recreate: docker compose up -d --force-recreate greeks_api nginx greeks_engine"
echo "Keep LIVE_TRADING=false until dry-run looks clean."
