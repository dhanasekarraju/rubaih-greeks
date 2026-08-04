"""
Delta Exchange REST client (India / Global).
HMAC-SHA256 auth per Delta docs: method + timestamp + path + query + body
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import time
from typing import Any, Dict, List, Optional
from urllib.parse import urlencode

import aiohttp


class DeltaAPIError(RuntimeError):
    """Delta HTTP error with parsed code when available."""

    def __init__(
        self,
        message: str,
        status: int = 0,
        payload: Optional[Dict] = None,
        body_text: str = "",
    ):
        super().__init__(message)
        self.status = status
        self.payload = payload or {}
        self.body_text = body_text or ""

    @property
    def code(self) -> str:
        err = self.payload.get("error") if isinstance(self.payload, dict) else None
        if isinstance(err, dict):
            return str(err.get("code") or "")
        return ""


class DeltaClient:
    def __init__(
        self,
        api_key: str,
        api_secret: str,
        base_url: str = "https://api.india.delta.exchange",
        timeout: float = 20.0,
    ):
        self.api_key = (api_key or "").strip()
        self.api_secret = (api_secret or "").strip()
        self.base_url = (base_url or "").rstrip("/")
        self.timeout = timeout
        self.session: Optional[aiohttp.ClientSession] = None
        self._auth_ok = False

    async def _get_session(self) -> aiohttp.ClientSession:
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=self.timeout)
            )
        return self.session

    def _sign(self, method: str, path: str, query: str, body: str, timestamp: str) -> str:
        payload = f"{method}{timestamp}{path}{query}{body}"
        return hmac.new(
            self.api_secret.encode(),
            payload.encode(),
            hashlib.sha256,
        ).hexdigest()

    async def request(
        self,
        method: str,
        path: str,
        params: Optional[Dict] = None,
        body: Optional[Dict] = None,
        auth: bool = False,
    ) -> Any:
        session = await self._get_session()
        query = ""
        if params:
            # Delta signs with raw commas (not %2C). Keep commas unescaped.
            query = "?" + urlencode(
                {k: v for k, v in params.items() if v is not None},
                safe=",",
            )
        body_str = ""
        if body is not None:
            body_str = json.dumps(body, separators=(",", ":"))
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": "rubaih-greeks/1.0",
        }
        if auth:
            if not self.api_key or not self.api_secret:
                raise RuntimeError("DELTA_API_KEY / DELTA_API_SECRET required")
            ts = str(int(time.time()))
            headers.update(
                {
                    "api-key": self.api_key,
                    "timestamp": ts,
                    "signature": self._sign(method.upper(), path, query, body_str, ts),
                }
            )
        url = f"{self.base_url}{path}{query}"
        async with session.request(
            method.upper(),
            url,
            data=body_str if body is not None else None,
            headers=headers,
        ) as resp:
            text = await resp.text()
            try:
                data = json.loads(text) if text else {}
            except json.JSONDecodeError:
                data = {"raw": text[:500]}
            if resp.status >= 400:
                err = DeltaAPIError(
                    f"Delta {method} {path} → {resp.status}: {text[:240]}",
                    status=resp.status,
                    payload=data if isinstance(data, dict) else {},
                    body_text=text,
                )
                raise err
            return data

    async def ping_auth(self) -> bool:
        try:
            data = await self.request("GET", "/v2/wallet/balances", auth=True)
            self._auth_ok = bool(data.get("success", True) or data.get("result") is not None)
            return self._auth_ok
        except Exception as e:
            print(f"[AUTH] Delta failed: {e}")
            self._auth_ok = False
            return False

    async def get_products(
        self,
        contract_types: str = "call_options,put_options",
        underlying: Optional[str] = None,
    ) -> List[Dict]:
        params: Dict[str, Any] = {"contract_types": contract_types, "page_size": 100}
        # Prefer tickers endpoint for live option chain; products for metadata
        data = await self.request("GET", "/v2/products", params=params)
        rows = data.get("result") or []
        if underlying:
            u = underlying.upper()
            rows = [
                r
                for r in rows
                if str((r.get("underlying_asset") or {}).get("symbol") or r.get("underlying_asset_symbol") or "").upper()
                == u
                or str(r.get("symbol", "")).upper().find(u) >= 0
            ]
        return rows if isinstance(rows, list) else []

    async def get_option_tickers(
        self,
        underlyings: Optional[List[str]] = None,
        expiry_date: Optional[str] = None,
    ) -> List[Dict]:
        params: Dict[str, Any] = {
            "contract_types": "call_options,put_options",
        }
        if underlyings:
            params["underlying_asset_symbols"] = ",".join(underlyings)
        if expiry_date:
            params["expiry_date"] = expiry_date
        data = await self.request("GET", "/v2/tickers", params=params)
        rows = data.get("result") or []
        return rows if isinstance(rows, list) else []

    async def get_ticker(self, symbol: str) -> Optional[Dict]:
        data = await self.request("GET", f"/v2/tickers/{symbol}")
        return data.get("result")

    async def get_balances(self) -> List[Dict]:
        data = await self.request("GET", "/v2/wallet/balances", auth=True)
        rows = data.get("result") or []
        return rows if isinstance(rows, list) else []

    async def get_positions(
        self,
        contract_types: str = "call_options,put_options",
        product_id: Optional[int] = None,
    ) -> List[Dict]:
        # Prefer unfiltered margined first (avoids fragile multi-value query signing),
        # then fall back to contract_types filter.
        attempts: List[Optional[Dict[str, Any]]] = [None]
        if product_id:
            attempts.append({"product_id": str(int(product_id))})
        if contract_types:
            attempts.append({"contract_types": contract_types})
        last_err: Optional[Exception] = None
        for params in attempts:
            try:
                data = await self.request(
                    "GET",
                    "/v2/positions/margined",
                    params=params,
                    auth=True,
                )
                rows = data.get("result") or []
                return rows if isinstance(rows, list) else []
            except Exception as e:
                last_err = e
                continue
        if last_err:
            raise last_err
        return []

    async def place_order(
        self,
        product_id: int,
        size: int,
        side: str,
        order_type: str = "market_order",
        limit_price: Optional[str] = None,
        reduce_only: bool = False,
    ) -> Dict:
        body: Dict[str, Any] = {
            "product_id": int(product_id),
            "size": int(size),
            "side": side.lower(),
            "order_type": order_type,
            "reduce_only": bool(reduce_only),
        }
        if limit_price is not None:
            body["limit_price"] = str(limit_price)
        data = await self.request("POST", "/v2/orders", body=body, auth=True)
        return data.get("result") or data

    async def close_position(self, product_id: int, size: Optional[int] = None) -> Dict:
        """Market reduce-only sell to flatten a long option."""
        body: Dict[str, Any] = {
            "product_id": int(product_id),
            "side": "sell",
            "order_type": "market_order",
            "reduce_only": True,
        }
        if size is not None:
            body["size"] = int(size)
        # Prefer close-all for safety when size unknown
        if size is None:
            data = await self.request(
                "POST",
                "/v2/positions/close_all",
                body={"close_all_portfolio": False, "close_all_isolated": True},
                auth=True,
            )
            return data.get("result") or data
        data = await self.request("POST", "/v2/orders", body=body, auth=True)
        return data.get("result") or data

    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()
            self.session = None


def env_delta_client(cfg: Dict) -> DeltaClient:
    base = (
        os.getenv("DELTA_BASE_URL", "").strip()
        or (cfg.get("exchange") or {}).get("base_url")
        or "https://api.india.delta.exchange"
    )
    timeout = float((cfg.get("exchange") or {}).get("rest_timeout_sec", 20))
    return DeltaClient(
        api_key=os.getenv("DELTA_API_KEY", ""),
        api_secret=os.getenv("DELTA_API_SECRET", ""),
        base_url=base,
        timeout=timeout,
    )
