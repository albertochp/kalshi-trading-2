"""
Strategy helpers:
- Team name normalization
- Odds->probability conversion helpers (de-vig)
- Mapping Odds-API events -> Kalshi market tickers (per match, avoids Draw collision)
- Sweep sizing using expected ROI per price level

Compatibility improvements:
- Accepts multiple input field names from Odds-API (home/home_team, away/away_team, date/commence_time)
- Preserves original matching heuristics and EventMarketMapping dataclass
"""

from __future__ import annotations

import re
import statistics
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, Tuple

# ============================================================
# Normalization
# ============================================================

SUFFIX_REGEX = re.compile(
    r"\b(fc|ac|cf|sc|afc|calcio|club|ss|as|bc|ud|cd|fk|nk|real|sporting|us|acf|ssd)\b"
)
CLEAN_REGEX = re.compile(r"[^a-z0-9 ]")
SPACE_REGEX = re.compile(r"\s+")

ALIASES = {
    "man utd": "manchester united",
    "man city": "manchester city",
    "inter": "inter milan",
    "internazionale": "inter milan",
    "juve": "juventus",
    "roma": "as roma",
    "tie": "draw",
    "draw 90 mins": "draw",
}


def normalize_team(name: str) -> str:
    if not name:
        return ""
    s = str(name).lower().strip()
    s = SUFFIX_REGEX.sub("", s)
    s = CLEAN_REGEX.sub("", s)
    s = SPACE_REGEX.sub(" ", s).strip()
    return ALIASES.get(s, s)


def get_tokens(s: str) -> Set[str]:
    return set(normalize_team(s).split())


def jaccard_index(set_a: Set[str], set_b: Set[str]) -> float:
    if not set_a or not set_b:
        return 0.0
    return len(set_a.intersection(set_b)) / len(set_a.union(set_b))


def parse_iso_date(date_str: str) -> Optional[datetime]:
    if not date_str:
        return None
    try:
        return datetime.fromisoformat(str(date_str).replace("Z", "+00:00"))
    except Exception:
        return None


# ============================================================
# Odds helpers
# ============================================================


def devig_decimal_odds(
    home_odds: float, away_odds: float, draw_odds: float = 0.0
) -> Optional[Tuple[float, float, float]]:
    """
    Convert decimal odds into de-vigged implied probabilities.

    Returns (p_home, p_away, p_draw) or None if invalid.
    """
    if home_odds <= 1.0 or away_odds <= 1.0:
        return None
    imp_home = 1.0 / home_odds
    imp_away = 1.0 / away_odds
    imp_draw = 1.0 / draw_odds if draw_odds and draw_odds > 1.0 else 0.0
    s = imp_home + imp_away + imp_draw
    if s <= 0:
        return None
    return (imp_home / s, imp_away / s, imp_draw / s)


# ============================================================
# Mapping Odds events -> Kalshi markets
# ============================================================

@dataclass(frozen=True)
class EventMarketMapping:
    odds_event_id: str
    home_team: str
    away_team: str
    commence_time: Optional[str]
    kalshi_event_ticker: str
    ticker_home: str
    ticker_away: str
    ticker_draw: Optional[str]


def _kalshi_event_expiration(event_obj: Dict[str, Any]) -> Optional[datetime]:
    # Try the typical fields; if absent, mapping falls back to name similarity alone.
    exp_str = (
        event_obj.get("expected_expiration_time")
        or event_obj.get("latest_expiration_time")
        or event_obj.get("close_time")
        or event_obj.get("settle_time")
    )
    return parse_iso_date(exp_str) if exp_str else None


def _kalshi_markets_index(event_obj: Dict[str, Any]) -> Dict[str, str]:
    """
    Returns mapping of normalized yes_sub_title -> market ticker.

    Note: draw-like markets are returned under key 'draw'.
    """
    markets = event_obj.get("markets") or []
    out: Dict[str, str] = {}
    for m in markets:
        if not isinstance(m, dict):
            continue
        ticker = m.get("ticker")
        sub = m.get("yes_sub_title") or ""
        if not ticker or not sub:
            continue
        sub_norm = normalize_team(sub)
        if sub_norm in {"draw", "tie"}:
            out["draw"] = str(ticker)
        else:
            out[sub_norm] = str(ticker)
    return out


def _extract_odds_event_fields(odds_ev: Dict[str, Any]) -> Dict[str, Any]:
    """
    Make the odds_event shape tolerant to different APIs / versions.

    Accepts:
      - home_team or home
      - away_team or away
      - commence_time or date

    Returns keys: id, home, away, commence
    """
    ev_id = odds_ev.get("id") or odds_ev.get("event_id") or odds_ev.get("eventId")
    # Many payloads use 'home'/'away'; some use 'home_team'/'away_team'
    home = odds_ev.get("home_team") or odds_ev.get("home") or odds_ev.get("homeTeam")
    away = odds_ev.get("away_team") or odds_ev.get("away") or odds_ev.get("awayTeam")
    # Time fields may be named differently
    commence = odds_ev.get("commence_time") or odds_ev.get("commenceTime") or odds_ev.get("date")
    return {"id": ev_id, "home": home, "away": away, "commence": commence}


def build_event_market_mapping(
    odds_events: List[Dict[str, Any]],
    kalshi_events_detailed: List[Dict[str, Any]],
    *,
    max_time_diff_hours: float = 48.0,
    min_score_threshold: float = 0.65,
    verbose: bool = False,
) -> Dict[str, EventMarketMapping]:
    """
    Build a PER-MATCH mapping, avoiding the "Draw" collision that exists in team->ticker maps.

    Additional optional args:
      - min_score_threshold: minimum matching score to accept a candidate.
      - verbose: if True, log the best score and candidate info for each odds event.
    """
    now = datetime.now(timezone.utc)
    kalshi_prepared: List[Dict[str, Any]] = []

    for ev in kalshi_events_detailed:
        if not isinstance(ev, dict):
            continue
        event_ticker = ev.get("event", {}).get("event_ticker") if "event" in ev else ev.get("event_ticker")
        if not event_ticker:
            event_ticker = ev.get("event_ticker")
        event_ticker = str(event_ticker) if event_ticker else ""
        markets = ev.get("markets") if isinstance(ev.get("markets"), list) else (ev.get("event", {}) or {}).get("markets")
        if markets is None:
            markets = ev.get("markets", [])
        candidate_obj = ev
        if "markets" not in candidate_obj:
            candidate_obj = dict(ev)
            candidate_obj["markets"] = markets

        idx = _kalshi_markets_index(candidate_obj)
        teams = {k for k in idx.keys() if k != "draw"}
        if len(teams) < 2:
            continue

        title = (candidate_obj.get("title") or candidate_obj.get("event", {}).get("title") or "")
        exp_dt = _kalshi_event_expiration(candidate_obj) or _kalshi_event_expiration(candidate_obj.get("event", {}) or {})

        kalshi_prepared.append(
            {
                "event_ticker": event_ticker,
                "title": str(title),
                "title_tokens": get_tokens(title),
                "teams": teams,
                "idx": idx,
                "exp": exp_dt,
            }
        )

    out: Dict[str, EventMarketMapping] = {}

    for odds_ev in odds_events:
        fields = _extract_odds_event_fields(odds_ev)
        odds_id = str(fields.get("id", "")) if fields.get("id") is not None else ""
        home = str(fields.get("home", "")) if fields.get("home") is not None else ""
        away = str(fields.get("away", "")) if fields.get("away") is not None else ""
        commence = fields.get("commence")

        if not odds_id or not home or not away:
            # Skip if core fields missing
            continue

        home_norm = normalize_team(home)
        away_norm = normalize_team(away)
        home_tokens = get_tokens(home)
        away_tokens = get_tokens(away)

        commence_dt = parse_iso_date(str(commence)) if commence else None

        best: Optional[Dict[str, Any]] = None
        best_score = 0.0
        best_title = ""

        for k in kalshi_prepared:
            teams = k["teams"]
            if home_norm not in teams or away_norm not in teams:
                score = (jaccard_index(home_tokens, k["title_tokens"]) + jaccard_index(away_tokens, k["title_tokens"])) * 0.5
            else:
                score = 1.0

            if commence_dt and k["exp"]:
                diff_h = abs((k["exp"] - commence_dt).total_seconds()) / 3600.0
                if diff_h > max_time_diff_hours:
                    continue
                score += max(0.0, 0.25 - (diff_h / max_time_diff_hours) * 0.25)

            if score > best_score:
                best_score = score
                best = k
                best_title = k.get("title", "")

        if verbose:
            # Use logger if available via global get_logger to avoid importing logger here
            try:
                from logging_utils import get_logger as _gl
                _gl().info(f"MATCH DEBUG odds_id={odds_id} home={home} away={away} best_score={best_score:.3f} best_title={best_title}")
            except Exception:
                print(f"MATCH DEBUG odds_id={odds_id} home={home} away={away} best_score={best_score:.3f} best_title={best_title}")

        if not best or best_score < float(min_score_threshold):
            continue

        idx = best["idx"]
        ticker_home = idx.get(home_norm)
        ticker_away = idx.get(away_norm)
        ticker_draw = idx.get("draw")

        if not ticker_home or not ticker_away:
            continue

        out[odds_id] = EventMarketMapping(
            odds_event_id=odds_id,
            home_team=home,
            away_team=away,
            commence_time=str(commence) if commence else None,
            kalshi_event_ticker=best["event_ticker"],
            ticker_home=ticker_home,
            ticker_away=ticker_away,
            ticker_draw=ticker_draw,
        )

    return out


# ============================================================
# Execution sizing: sweep levels by ROI
# ============================================================


def calculate_sweep_orders(
    asks_best_to_worst: List[Tuple[int, int]],  # [(price_cents, qty), ...] sorted ascending price
    *,
    true_prob: float,
    min_roi: float,
    max_cost_cents: int,
    fee_buffer_cents: int = 0,
) -> List[Tuple[int, int]]:
    """
    Choose which ask levels to take while maintaining expected ROI >= min_roi.

    For a binary contract bought at price p (cents):
      EV_cents = 100 * true_prob - p - fee_buffer_cents
      ROI = EV_cents / p

    We sweep from best price upward until ROI drops below threshold or capital is exhausted.
    """
    if not asks_best_to_worst:
        return []
    if true_prob <= 0.0 or true_prob >= 1.0:
        return []

    remaining = int(max_cost_cents)
    orders: List[Tuple[int, int]] = []

    for price, qty in asks_best_to_worst:
        if remaining <= 0:
            break
        if qty <= 0:
            continue
        if price <= 0 or price >= 100:
            continue

        ev_cents = (100.0 * true_prob) - float(price) - float(fee_buffer_cents)
        if ev_cents <= 0:
            # As price worsens, EV only declines further; stop.
            break

        roi = ev_cents / float(price)
        if roi < min_roi:
            break

        max_afford = remaining // price
        take = min(qty, max_afford)
        if take <= 0:
            break
        orders.append((price, int(take)))
        remaining -= int(take) * price

    return orders
