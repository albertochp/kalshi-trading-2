"""
kalshi_client.py

Minimal Kalshi client:
- RSA-PSS request signing
- Websocket auth headers
- REST order creation
- REST metadata fetch for series/events (needed for mapping)
"""

from __future__ import annotations

import base64
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import requests
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

import config
from logging_utils import get_logger

logger = get_logger("TBot")


@dataclass(frozen=True)
class KalshiAuth:
    key_id: str
    private_key_path: str

    def _load_private_key(self):
        with open(self.private_key_path, "rb") as f:
            return serialization.load_pem_private_key(
                f.read(), password=None, backend=default_backend()
            )

    def sign(self, *, method: str, path: str, ts_ms: str, private_key) -> str:
        msg = f"{ts_ms}{method.upper()}{path}"
        sig = private_key.sign(
            msg.encode("utf-8"),
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.DIGEST_LENGTH,
            ),
            hashes.SHA256(),
        )
        return base64.b64encode(sig).decode("utf-8")

    def headers(self, *, method: str, path: str, private_key) -> Dict[str, str]:
        ts_ms = str(int(time.time() * 1000))
        signature = self.sign(method=method, path=path, ts_ms=ts_ms, private_key=private_key)
        return {
            "KALSHI-ACCESS-KEY": self.key_id,
            "KALSHI-ACCESS-SIGNATURE": signature,
            "KALSHI-ACCESS-TIMESTAMP": ts_ms,
            "Content-Type": "application/json",
        }


class KalshiClient:
    def __init__(
        self,
        *,
        api_base: str = config.KALSHI_API_BASE,
        key_id: str = config.KALSHI_API_KEY_ID,
        private_key_path: str = config.KALSHI_PRIVATE_KEY_PATH,
        session: Optional[requests.Session] = None,
    ):
        if not key_id or not private_key_path:
            raise ValueError(
                "Kalshi credentials missing. Set KALSHI_API_KEY_ID and KALSHI_PRIVATE_KEY_PATH."
            )

        self.api_base = api_base.rstrip("/")
        self.auth = KalshiAuth(key_id=key_id, private_key_path=private_key_path)
        self._private_key = self.auth._load_private_key()
        self.session = session or requests.Session()

    def ws_headers(self, ws_path: str = "/trade-api/ws/v2") -> Dict[str, str]:
        return self.auth.headers(method="GET", path=ws_path, private_key=self._private_key)

    def _request(
        self,
        *,
        method: str,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        json_body: Optional[Dict[str, Any]] = None,
        timeout: float = 8.0,
    ) -> Dict[str, Any]:
        url = f"{self.api_base}{path}"
        headers = self.auth.headers(method=method, path=path, private_key=self._private_key)

        resp = self.session.request(
            method=method.upper(),
            url=url,
            headers=headers,
            params=params,
            json=json_body,
            timeout=timeout,
        )

        if resp.status_code >= 400:
            raise requests.HTTPError(f"Kalshi HTTP {resp.status_code}: {resp.text}", response=resp)

        data = resp.json()
        return data if isinstance(data, dict) else {"raw": data}

    # ----------------------------
    # Metadata endpoints (mapping)
    # ----------------------------

    def list_events_for_series(self, *, series_ticker: str, limit: int = 200) -> List[Dict[str, Any]]:
        """
        Returns a list of event summaries for a series.

        Tries multiple endpoint shapes to be robust across Kalshi API versions.
        """
        if not series_ticker:
            raise ValueError("series_ticker required")

        # Helper to run a request and return (events_list or None)
        def _try_path(path: str, params: Optional[Dict[str, Any]] = None) -> Optional[List[Dict[str, Any]]]:
            try:
                data = self._request(method="GET", path=path, params=params, timeout=8.0)
            except Exception as e:
                # Log the failure and return None so we fall back.
                logger.warning(f"Kalshi metadata request failed for path={path} params={params} error={e}")
                # If it's a requests.HTTPError, try to log response text
                try:
                    resp = getattr(e, "response", None)
                    if resp is not None:
                        logger.debug(f"Kalshi response status={resp.status_code} body={resp.text}")
                except Exception:
                    pass
                return None

            # Normal shapes: {"events":[...]} or {"data":{"events":[...]}} or raw list
            if isinstance(data, dict):
                if "events" in data and isinstance(data["events"], list):
                    return data["events"]
                if "data" in data and isinstance(data["data"], dict) and isinstance(data["data"].get("events"), list):
                    return data["data"]["events"]
                # Sometimes API wraps result differently; if raw list under 'raw' (our _request may wrap)
                if "raw" in data and isinstance(data["raw"], list):
                    return data["raw"]
                # If the dict itself looks like an event list, guard-case
                # No list found -> log and return None to attempt fallback
                logger.debug(f"Kalshi metadata unexpected payload keys: {list(data.keys())}")
                return None

            # If _request returned a list directly (should not per current impl, but be defensive)
            if isinstance(data, list):
                return data

            return None

        # Attempt 1: events endpoint with series_ticker param (our original attempt)
        params = {"series_ticker": series_ticker, "limit": limit}
        events = _try_path("/trade-api/v2/events", params=params)
        if events:
            return [e for e in events if isinstance(e, dict)]

        # Attempt 2: alternate series-based endpoint (common shape)
        alt_path = f"/trade-api/v2/series/{series_ticker}/events"
        events = _try_path(alt_path, params={"limit": limit})
        if events:
            return [e for e in events if isinstance(e, dict)]

        # Attempt 3: generic events listing (no params) to inspect server shape
        events = _try_path("/trade-api/v2/events", params=None)
        if events:
            return [e for e in events if isinstance(e, dict)]

        # All attempts failed
        logger.warning(f"No Kalshi events found for series={series_ticker} after trying multiple endpoint shapes.")
        return []

    def get_event_details(self, *, event_ticker: str) -> Dict[str, Any]:
        """
        Returns a detailed event including markets.

        This is a defensive implementation: many Kalshi deployments expose a GET
        at /trade-api/v2/events/{event_ticker}. It returns a dict payload.
        """
        if not event_ticker:
            raise ValueError("event_ticker required")

        # Try expected path
        try:
            return self._request(method="GET", path=f"/trade-api/v2/events/{event_ticker}", timeout=8.0)
        except Exception as e:
            # If the endpoint is not present or returns 400/404 for your account, surface informative message.
            logger.warning(f"get_event_details failed for {event_ticker}: {e}")
            # As a fallback, try fetching generic events list and find matching entry
            try:
                data = self._request(method="GET", path="/trade-api/v2/events", timeout=8.0)
                candidates = data.get("events") or (data.get("data") or {}).get("events") or data.get("raw") or []
                if isinstance(candidates, list):
                    for c in candidates:
                        # event_ticker may be at top level or inside 'event'
                        t = c.get("event_ticker") or (c.get("event") or {}).get("event_ticker")
                        if t and str(t) == str(event_ticker):
                            return c
            except Exception:
                pass

            # Ultimately return an empty dict rather than raising so callers can continue.
            return {}



    # ----------------------------
    # Trading endpoint
    # ----------------------------

    def create_order(
        self,
        *,
        ticker: str,
        action: str,
        side: str,
        count: int,
        yes_price: Optional[int] = None,
        no_price: Optional[int] = None,
        client_order_id: Optional[str] = None,
        order_type: str = config.KALSHI_ORDER_TYPE,
        time_in_force: str = config.KALSHI_TIME_IN_FORCE,
    ) -> Dict[str, Any]:
        if not ticker:
            raise ValueError("ticker required")
        if action not in {"buy", "sell"}:
            raise ValueError("action must be buy or sell")
        if side not in {"yes", "no"}:
            raise ValueError("side must be yes or no")
        if count <= 0:
            raise ValueError("count must be > 0")

        body: Dict[str, Any] = {
            "ticker": ticker,
            "action": action,
            "side": side,
            "type": order_type,
            "count": int(count),
        }

        if client_order_id:
            body["client_order_id"] = str(client_order_id)

        if side == "yes":
            if yes_price is None:
                raise ValueError("yes_price required when side=yes")
            body["yes_price"] = int(yes_price)
        else:
            if no_price is None:
                raise ValueError("no_price required when side=no")
            body["no_price"] = int(no_price)

        if time_in_force:
            body["time_in_force"] = time_in_force

        return self._request(
            method="POST",
            path="/trade-api/v2/orders",
            json_body=body,
            timeout=6.0,
        )
