"""
data_fetch.py

Odds-API data fetching utilities with defensive handling of status filters.

Key fix:
- Odds-API upcoming matches appear as status="pending" (not "upcoming") for this API.
- We therefore query live first; if empty, query pending; if still empty, query without status
  and filter client-side.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any, Dict, List, Optional

import requests

import config
from logging_utils import get_logger


ODDS_EVENTS_URL = "https://api2.odds-api.io/v3/events"


def _raise_for_error_payload(payload: Any) -> None:
    # Odds-API often returns {"error": "..."} rather than a non-200 status
    if isinstance(payload, dict) and "error" in payload:
        raise RuntimeError(f"Odds-API error: {payload.get('error')}")


def fetch_events_sync(
    sport: str,
    league: str,
    status: Optional[str] = None,
    limit: int = 20,
    api_key: Optional[str] = None,
    timeout_sec: float = 10.0,
) -> List[Dict[str, Any]]:
    """
    Synchronous fetch for events list.
    Returns list of event objects. Raises if API returns explicit error payload.
    """
    api_key = (api_key or config.ODDS_API_KEY).strip()
    if not api_key:
        raise RuntimeError("ODDS_API_KEY is empty. Check .env loading order.")

    params = {
        "apiKey": api_key,
        "sport": sport,
        "league": league,
        "limit": str(limit),
    }
    if status:
        params["status"] = status

    r = requests.get(ODDS_EVENTS_URL, params=params, timeout=timeout_sec)
    # Some Odds responses are 200 with {"error": "..."}
    try:
        payload = r.json()
    except Exception as e:
        raise RuntimeError(f"Odds-API non-JSON response: status={r.status_code} body={r.text[:200]}") from e

    _raise_for_error_payload(payload)

    if isinstance(payload, list):
        return payload

    # Unexpected shape
    raise RuntimeError(f"Unexpected Odds-API events payload type: {type(payload)}")


async def fetch_events(
    sport: str,
    league: str,
    status: Optional[str] = None,
    limit: int = 20,
    api_key: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Async wrapper for fetch_events_sync.
    """
    return await asyncio.to_thread(fetch_events_sync, sport, league, status, limit, api_key)


def _filter_relevant_status(events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Keep only events that are actionable for trading:
    - live: in-play
    - pending: upcoming/not started (as observed in your curl output)
    """
    out: List[Dict[str, Any]] = []
    for e in events:
        s = str(e.get("status", "")).lower()
        if s in {"live", "pending"}:
            out.append(e)
    return out


async def fetch_live_then_pending(
    sport: str,
    league: str,
    limit: int,
    logger=None,
) -> List[Dict[str, Any]]:
    """
    Primary bootstrap call: try live first, then pending, then no-status fallback.
    """
    logger = logger or get_logger()

    # 1) live
    live = await fetch_events(sport, league, status="live", limit=limit)
    if live:
        return _filter_relevant_status(live)

    logger.warning("No live events found; trying pending.")
    pending = await fetch_events(sport, league, status="pending", limit=limit)
    if pending:
        return _filter_relevant_status(pending)

    # Fallback: omit status completely and filter client-side.
    logger.warning("No pending events found via status filter; trying without status param.")
    all_ev = await fetch_events(sport, league, status=None, limit=limit)
    filtered = _filter_relevant_status(all_ev)

    return filtered
