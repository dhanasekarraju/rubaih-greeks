"""Authenticated command envelopes for the Greeks Redis control queue."""

from __future__ import annotations

import hashlib
import hmac
import json
import secrets
import time
from typing import Any, Dict, Optional


ALLOWED_COMMANDS = frozenset(
    {"kill", "flatten", "panic", "refresh_capital", "resume", "sync"}
)


def _canonical(payload: Dict[str, Any]) -> str:
    body = {k: payload[k] for k in sorted(payload) if k != "sig"}
    return json.dumps(body, separators=(",", ":"), sort_keys=True)


def sign_command(
    secret: str,
    command: str,
    source: str = "api",
    data: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    cmd = str(command or "").strip().lower()
    if not secret:
        raise ValueError("command secret is required")
    if cmd not in ALLOWED_COMMANDS:
        raise ValueError(f"unsupported command: {cmd}")
    payload: Dict[str, Any] = {
        "command": cmd,
        "source": source,
        "ts": time.time(),
        "nonce": secrets.token_hex(16),
    }
    if data is not None:
        payload["data"] = data
    payload["sig"] = hmac.new(
        secret.encode("utf-8"),
        _canonical(payload).encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    return payload


def verify_command(
    secret: str,
    payload: Dict[str, Any],
    max_age_sec: float = 120.0,
) -> bool:
    if not secret or not isinstance(payload, dict):
        return False
    command = str(payload.get("command") or "").strip().lower()
    nonce = str(payload.get("nonce") or "")
    signature = str(payload.get("sig") or "")
    if command not in ALLOWED_COMMANDS or len(nonce) < 16 or len(signature) != 64:
        return False
    try:
        ts = float(payload.get("ts") or 0)
    except (TypeError, ValueError):
        return False
    if abs(time.time() - ts) > max_age_sec:
        return False
    expected = hmac.new(
        secret.encode("utf-8"),
        _canonical(payload).encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    return hmac.compare_digest(signature, expected)
