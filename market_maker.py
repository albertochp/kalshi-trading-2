"""
Market maker / execution engine.

Core improvements vs the original version:
- Per-match mapping (no Draw collisions).
- Sportsbook consensus probability (median across fresh bookmakers).
- Signal confirmation with fast-path for goal-like jumps.
- Queue-driven evaluation (no full scan every tick).
- IOC-style behavior (configurable time_in_force) to reduce adverse selection.
- Latency telemetry (rolling percentiles) logged periodically.
- No stdout printing unless debug logging is enabled via logging_utils.setup_logging().
"""

from __future__ import annotations

import asyncio
import statistics
import time
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Deque, Dict, List, Optional, Set, Tuple

import inspect
import websockets

import config
from kalshi_client import KalshiClient
from logging_utils import get_logger
from strategy import EventMarketMapping, calculate_sweep_orders, devig_decimal_odds
from telemetry import LatencyTelemetry
from utils import iter_concatenated_json, json_dumps, json_loads, normalize_name, safe_float

logger = get_logger("TBot")

_CONNECT_PARAMS = set(inspect.signature(websockets.connect).parameters.keys())

def _ws_connect(url: str, *, headers: Optional[dict] = None, **kwargs):
    """Compatibility wrapper for websockets.connect across versions."""
    if headers:
        if "extra_headers" in _CONNECT_PARAMS:
            kwargs["extra_headers"] = headers
        elif "additional_headers" in _CONNECT_PARAMS:
            kwargs["additional_headers"] = headers
        else:
            # Fall back: best-effort
            kwargs["extra_headers"] = headers
    return websockets.connect(url, **kwargs)


class SportsbookState(Enum):
    STABLE = auto()
    PENDING = auto()
    CONFIRMED = auto()


class LocalOrderBook:
    """
    Local replica of a Kalshi order book.

    Kalshi WS snapshot typically gives two lists:
      - yes: bids to buy YES at price p
      - no: bids to buy NO at price p

    We store:
      - bids: YES bids (price -> qty)
      - asks: YES asks inferred from NO bids as (100 - no_price)
    """
    __slots__ = ("bids", "asks")

    def __init__(self) -> None:
        self.bids: Dict[int, int] = {}
        self.asks: Dict[int, int] = {}

    def apply_update(self, msg: dict, *, is_snapshot: bool) -> None:
        if is_snapshot:
            self.bids.clear()
            self.asks.clear()

            yes_levels = (msg.get("msg", {}) or {}).get("yes") or []
            no_levels = (msg.get("msg", {}) or {}).get("no") or []

            for p, q in yes_levels:
                p_i = int(p)
                q_i = int(q)
                if q_i > 0:
                    self.bids[p_i] = q_i

            # NO bids -> YES asks at (100 - no_bid_price)
            for p, q in no_levels:
                no_p = int(p)
                q_i = int(q)
                yes_ask_p = 100 - no_p
                if q_i > 0 and 1 <= yes_ask_p <= 99:
                    self.asks[yes_ask_p] = q_i
            return

        m = msg.get("msg", {}) or {}
        side = m.get("side")
        price = m.get("price")
        delta = m.get("delta")
        if side not in {"yes", "no"}:
            return
        if price is None or delta is None:
            return

        p_i = int(price)
        d_i = int(delta)

        if side == "yes":
            new_q = self.bids.get(p_i, 0) + d_i
            if new_q <= 0:
                self.bids.pop(p_i, None)
            else:
                self.bids[p_i] = new_q
        else:
            # Update NO bids => YES asks at (100 - no_price)
            yes_ask_p = 100 - p_i
            if not (1 <= yes_ask_p <= 99):
                return
            new_q = self.asks.get(yes_ask_p, 0) + d_i
            if new_q <= 0:
                self.asks.pop(yes_ask_p, None)
            else:
                self.asks[yes_ask_p] = new_q

    def best_yes_bid(self) -> Optional[int]:
        return max(self.bids.keys()) if self.bids else None

    def best_yes_ask(self) -> Optional[int]:
        return min(self.asks.keys()) if self.asks else None

    def best_no_ask(self) -> Optional[int]:
        """
        NO asks are derived from YES bids (buying NO means hitting YES bids).
        Best NO ask corresponds to the highest YES bid:
          no_ask = 100 - best_yes_bid
        """
        b = self.best_yes_bid()
        if b is None:
            return None
        no_ask = 100 - b
        return no_ask if 1 <= no_ask <= 99 else None


@dataclass
class MarketTickerState:
    ticker: str
    event_id: str
    label: str  # human-readable (team or draw with teams)
    outcome: str  # "home" | "away" | "draw"

    # Sportsbook-derived
    sb_prob: float = 0.5
    sb_baseline: Optional[float] = None
    sb_state: SportsbookState = SportsbookState.STABLE
    sb_book_count: int = 0

    pending_since: Optional[float] = None
    pending_source_ts: Optional[float] = None  # msg receipt ts that armed pending
    signal_confirmed_ts: Optional[float] = None
    signal_source_ts: Optional[float] = None  # msg receipt ts associated with confirmed signal
    last_sb_update_ts: Optional[float] = None

    # Kalshi order book
    book: LocalOrderBook = field(default_factory=LocalOrderBook)
    last_book_update_ts: Optional[float] = None

    # Risk
    exposure_usd: float = 0.0
    last_trade_ts: Optional[float] = None


class SharedStateManager:
    def __init__(self, telemetry: LatencyTelemetry):
        self._telemetry = telemetry
        self._lock = asyncio.Lock()
        self._markets: Dict[str, MarketTickerState] = {}

        # exposure per event (match)
        self._event_exposure_usd: Dict[str, float] = defaultdict(float)

        # eval queue (tickers to evaluate for execution)
        self._eval_queue: asyncio.Queue[str] = asyncio.Queue()
        self._enqueued: Set[str] = set()

    async def register_market(self, s: MarketTickerState) -> None:
        async with self._lock:
            self._markets[s.ticker] = s

    async def get_market(self, ticker: str) -> Optional[MarketTickerState]:
        async with self._lock:
            return self._markets.get(ticker)

    async def update_sportsbook_prob(
        self,
        *,
        ticker: str,
        new_prob: float,
        book_count: int,
        msg_ts: float,
    ) -> None:
        """
        Update SB consensus probability and manage the hysteresis state machine.

        msg_ts is the monotonic receipt time (perf_counter).
        """
        to_enqueue = False
        sb_confirmed_now = False
        sb_confirm_latency_ms: Optional[float] = None

        async with self._lock:
            s = self._markets.get(ticker)
            if not s:
                return

            now = time.perf_counter()
            s.last_sb_update_ts = now
            s.sb_prob = float(new_prob)
            s.sb_book_count = int(book_count)

            # If baseline not yet set, initialize and do not signal.
            if s.sb_baseline is None:
                s.sb_baseline = s.sb_prob
                s.sb_state = SportsbookState.STABLE
                s.pending_since = None
                s.pending_source_ts = None
                s.signal_confirmed_ts = None
                s.signal_source_ts = None
                return

            # Auto-expire confirmed signals
            if s.sb_state == SportsbookState.CONFIRMED and s.signal_confirmed_ts is not None:
                if now - s.signal_confirmed_ts > config.SIGNAL_TTL_SEC:
                    s.sb_state = SportsbookState.STABLE
                    s.sb_baseline = s.sb_prob
                    s.pending_since = None
                    s.pending_source_ts = None
                    s.signal_confirmed_ts = None
                    s.signal_source_ts = None

            delta = s.sb_prob - float(s.sb_baseline)

            # If not enough books, do not arm/confirm signals (still update sb_prob).
            if book_count < config.SB_MIN_BOOKMAKERS:
                return

            abs_delta = abs(delta)

            # Fast-path for goal-like jumps
            if abs_delta >= config.SB_FAST_SIGNAL_DELTA:
                s.sb_state = SportsbookState.CONFIRMED
                s.signal_confirmed_ts = now
                s.signal_source_ts = msg_ts
                s.pending_since = None
                s.pending_source_ts = None
                to_enqueue = True
                sb_confirmed_now = True
                sb_confirm_latency_ms = (now - msg_ts) * 1000.0

            elif abs_delta >= config.SB_SIGNAL_DELTA:
                if s.sb_state == SportsbookState.STABLE:
                    s.sb_state = SportsbookState.PENDING
                    s.pending_since = now
                    s.pending_source_ts = msg_ts

                elif s.sb_state == SportsbookState.PENDING:
                    if s.pending_since is not None and (now - s.pending_since) >= config.SB_CONFIRMATION_SEC:
                        s.sb_state = SportsbookState.CONFIRMED
                        s.signal_confirmed_ts = now
                        s.signal_source_ts = s.pending_source_ts or msg_ts
                        to_enqueue = True
                        sb_confirmed_now = True
                        if s.signal_source_ts is not None:
                            sb_confirm_latency_ms = (now - s.signal_source_ts) * 1000.0

                elif s.sb_state == SportsbookState.CONFIRMED:
                    # refresh confirmation timestamp if still strongly deviated
                    s.signal_confirmed_ts = now
                    s.signal_source_ts = s.signal_source_ts or msg_ts
                    to_enqueue = True

            else:
                # Revert back to stable; update baseline to new stable value
                if s.sb_state != SportsbookState.STABLE:
                    s.sb_state = SportsbookState.STABLE
                s.pending_since = None
                s.pending_source_ts = None
                s.signal_confirmed_ts = None
                s.signal_source_ts = None
                s.sb_baseline = s.sb_prob

        if sb_confirmed_now and sb_confirm_latency_ms is not None:
            self._telemetry.add("sb_msg_to_signal_confirm_ms", sb_confirm_latency_ms)

        if to_enqueue:
            await self._enqueue_eval(ticker)

    async def update_kalshi_book(self, *, ticker: str, msg: dict, is_snapshot: bool, msg_ts: float) -> None:
        to_enqueue = False
        async with self._lock:
            s = self._markets.get(ticker)
            if not s:
                return
            s.book.apply_update(msg, is_snapshot=is_snapshot)
            s.last_book_update_ts = msg_ts
            if s.sb_state == SportsbookState.CONFIRMED:
                to_enqueue = True
        if to_enqueue:
            await self._enqueue_eval(ticker)

    async def _enqueue_eval(self, ticker: str) -> None:
        async with self._lock:
            if ticker in self._enqueued:
                return
            self._enqueued.add(ticker)
            try:
                self._eval_queue.put_nowait(ticker)
            except asyncio.QueueFull:
                # Extremely unlikely; drop if queue is full.
                self._enqueued.remove(ticker)

    async def next_eval_ticker(self) -> str:
        ticker = await self._eval_queue.get()
        async with self._lock:
            self._enqueued.discard(ticker)
        return ticker

    async def can_add_exposure(self, *, ticker: str, add_usd: float) -> bool:
        async with self._lock:
            s = self._markets.get(ticker)
            if not s:
                return False
            if add_usd <= 0:
                return False

            if add_usd > config.MAX_TRADE_SIZE_USD:
                return False

            if (s.exposure_usd + add_usd) > config.MAX_TICKER_EXPOSURE_USD:
                return False

            ev_exp = self._event_exposure_usd.get(s.event_id, 0.0)
            if (ev_exp + add_usd) > config.MAX_EVENT_EXPOSURE_USD:
                return False

            # Apply
            s.exposure_usd += add_usd
            self._event_exposure_usd[s.event_id] = ev_exp + add_usd
            return True

    async def mark_traded(self, *, ticker: str) -> None:
        async with self._lock:
            s = self._markets.get(ticker)
            if not s:
                return
            now = time.perf_counter()
            s.last_trade_ts = now
            # reset state machine baseline after acting
            s.sb_state = SportsbookState.STABLE
            s.sb_baseline = s.sb_prob
            s.pending_since = None
            s.pending_source_ts = None
            s.signal_confirmed_ts = None
            s.signal_source_ts = None


@dataclass
class _BookmakerQuote:
    ts: float  # monotonic seconds
    p_home: float
    p_away: float
    p_draw: float


class MarketMaker:
    def __init__(self, *, mapping: Dict[str, EventMarketMapping], telemetry: Optional[LatencyTelemetry] = None):
        self.mapping = mapping
        self.telemetry = telemetry or LatencyTelemetry(window=config.TELEMETRY_WINDOW)
        self.state = SharedStateManager(self.telemetry)

        self.running = False

        self.event_ids: Set[str] = set(mapping.keys())
        self.event_to_tickers: Dict[str, Dict[str, str]] = {}  # event_id -> outcome -> ticker
        self.ticker_to_event: Dict[str, str] = {}

        # sportsbook cache: event_id -> bookmaker_norm -> quote
        self.sb_cache: Dict[str, Dict[str, _BookmakerQuote]] = defaultdict(dict)

        # bookmaker allow list
        self.allowed_books_norm: Optional[Set[str]] = None
        if config.TARGET_BOOKMAKERS:
            self.allowed_books_norm = {normalize_name(b) for b in config.TARGET_BOOKMAKERS}

        self.kalshi = KalshiClient()

    async def bootstrap(self) -> None:
        """
        Register all tickers in SharedStateManager.
        """
        for event_id, m in self.mapping.items():
            self.event_to_tickers[event_id] = {
                "home": m.ticker_home,
                "away": m.ticker_away,
            }
            if m.ticker_draw:
                self.event_to_tickers[event_id]["draw"] = m.ticker_draw

            self.ticker_to_event[m.ticker_home] = event_id
            self.ticker_to_event[m.ticker_away] = event_id
            if m.ticker_draw:
                self.ticker_to_event[m.ticker_draw] = event_id

            draw_label = f"Draw ({m.home_team} vs {m.away_team})"
            await self.state.register_market(
                MarketTickerState(ticker=m.ticker_home, event_id=event_id, label=m.home_team, outcome="home")
            )
            await self.state.register_market(
                MarketTickerState(ticker=m.ticker_away, event_id=event_id, label=m.away_team, outcome="away")
            )
            if m.ticker_draw:
                await self.state.register_market(
                    MarketTickerState(ticker=m.ticker_draw, event_id=event_id, label=draw_label, outcome="draw")
                )

    # --------------------------------------------------------
    # LOOP 1: ODDS WS INGESTION
    # --------------------------------------------------------
    async def run_sportsbook_ingestion(self) -> None:
        if not config.ODDS_API_KEY:
            raise ValueError("Missing ODDS_API_KEY env var")

        # Build URL
        url = f"{config.ODDS_WS_URL}?apiKey={config.ODDS_API_KEY}&sport={config.SPORT_KEY}&markets={config.MARKET_KEY}"
        if config.ODDS_WS_USE_EVENT_FILTER and self.event_ids:
            url += "&eventIds=" + ",".join(sorted(self.event_ids))

        backoff = 1.0
        logger.info("Sportsbook WS ingestion started. events=%d", len(self.event_ids))

        while self.running:
            try:
                async with websockets.connect(url, max_size=None) as ws:
                    backoff = 1.0
                    async for frame in ws:
                        if not self.running:
                            break
                        frame_ts = time.perf_counter()
                        # Some frames contain multiple JSON objects concatenated
                        for raw in iter_concatenated_json(frame):
                            t0 = time.perf_counter()
                            try:
                                data = json_loads(raw)
                            except Exception:
                                continue
                            if not isinstance(data, dict):
                                continue
                            if data.get("type") != "updated":
                                continue
                            await self._handle_odds_update(data, msg_ts=frame_ts)
                            self.telemetry.add("sb_update_parse_ms", (time.perf_counter() - t0) * 1000.0)
            except Exception as e:
                logger.warning("Sportsbook WS error: %s. Reconnecting in %.1fs", str(e), backoff)
                await asyncio.sleep(backoff)
                backoff = min(15.0, backoff * 1.6)

    async def _handle_odds_update(self, data: Dict[str, Any], *, msg_ts: float) -> None:
        event_id = data.get("id")
        if not event_id:
            return
        event_id = str(event_id)
        if event_id not in self.event_ids:
            return

        bookie = normalize_name(data.get("bookie", ""))
        if self.allowed_books_norm is not None and bookie not in self.allowed_books_norm:
            return

        markets = data.get("markets") or []
        if not isinstance(markets, list):
            return

        ml = None
        for m in markets:
            if not isinstance(m, dict):
                continue
            key = (m.get("key") or m.get("name") or "").strip()
            if key in {"ML", "h2h"}:
                ml = m
                break
        if not ml:
            return

        odds_list = ml.get("odds") or []
        if not odds_list or not isinstance(odds_list, list):
            return
        o = odds_list[0] if isinstance(odds_list[0], dict) else None
        if not o:
            return

        home_odds = safe_float(o.get("home", o.get("1")))
        away_odds = safe_float(o.get("away", o.get("2")))
        draw_odds = safe_float(o.get("draw", o.get("X")))

        probs = devig_decimal_odds(home_odds, away_odds, draw_odds)
        if probs is None:
            return
        p_home, p_away, p_draw = probs

        now = time.perf_counter()
        self.sb_cache[event_id][bookie] = _BookmakerQuote(ts=now, p_home=p_home, p_away=p_away, p_draw=p_draw)

        # Compute consensus (median across fresh bookmakers)
        cons = self._consensus_for_event(event_id, now=now)
        if not cons:
            return

        tickers = self.event_to_tickers.get(event_id)
        if not tickers:
            return

        # Update tickers. Each update may enqueue the ticker if it arms/confirms a signal.
        if "home" in tickers and "home" in cons:
            p, n = cons["home"]
            await self.state.update_sportsbook_prob(ticker=tickers["home"], new_prob=p, book_count=n, msg_ts=msg_ts)
        if "away" in tickers and "away" in cons:
            p, n = cons["away"]
            await self.state.update_sportsbook_prob(ticker=tickers["away"], new_prob=p, book_count=n, msg_ts=msg_ts)
        if "draw" in tickers and "draw" in cons:
            p, n = cons["draw"]
            await self.state.update_sportsbook_prob(ticker=tickers["draw"], new_prob=p, book_count=n, msg_ts=msg_ts)

    def _consensus_for_event(self, event_id: str, *, now: float) -> Dict[str, Tuple[float, int]]:
        books = self.sb_cache.get(event_id)
        if not books:
            return {}

        # Prune stale and build lists
        fresh_home: List[float] = []
        fresh_away: List[float] = []
        fresh_draw: List[float] = []
        stale_keys: List[str] = []

        for bk, q in books.items():
            if (now - q.ts) > config.SB_BOOKMAKER_STALE_SEC:
                stale_keys.append(bk)
                continue
            fresh_home.append(q.p_home)
            fresh_away.append(q.p_away)
            if q.p_draw > 0:
                fresh_draw.append(q.p_draw)

        for bk in stale_keys:
            books.pop(bk, None)

        out: Dict[str, Tuple[float, int]] = {}
        if len(fresh_home) >= config.SB_MIN_BOOKMAKERS:
            out["home"] = (float(statistics.median(fresh_home)), len(fresh_home))
        if len(fresh_away) >= config.SB_MIN_BOOKMAKERS:
            out["away"] = (float(statistics.median(fresh_away)), len(fresh_away))
        # Draw is optional; require min books too
        if len(fresh_draw) >= config.SB_MIN_BOOKMAKERS:
            out["draw"] = (float(statistics.median(fresh_draw)), len(fresh_draw))

        return out

    # --------------------------------------------------------
    # LOOP 2: KALSHI WS INGESTION
    # --------------------------------------------------------
    async def run_kalshi_ingestion(self) -> None:
        tickers = list(self.ticker_to_event.keys())
        if not tickers:
            logger.warning("No Kalshi tickers to subscribe to.")
            return

        backoff = 1.0
        logger.info("Kalshi WS ingestion started. tickers=%d", len(tickers))

        while self.running:
            try:
                headers = self.kalshi.ws_headers("/trade-api/ws/v2")
                async with _ws_connect(config.KALSHI_WS_URL, headers=headers, max_size=None) as ws:
                    backoff = 1.0
                    sub_msg = {
                        "id": 1,
                        "cmd": "subscribe",
                        "params": {"channels": ["orderbook_delta"], "market_tickers": tickers},
                    }
                    await ws.send(json_dumps(sub_msg))

                    async for frame in ws:
                        if not self.running:
                            break
                        msg_ts = time.perf_counter()
                        try:
                            data = json_loads(frame)
                        except Exception:
                            continue
                        if not isinstance(data, dict):
                            continue
                        t = data.get("type")
                        if t not in {"orderbook_snapshot", "orderbook_delta"}:
                            continue
                        mt = (data.get("msg", {}) or {}).get("market_ticker")
                        if not mt:
                            continue
                        await self.state.update_kalshi_book(
                            ticker=str(mt), msg=data, is_snapshot=(t == "orderbook_snapshot"), msg_ts=msg_ts
                        )
            except Exception as e:
                logger.warning("Kalshi WS error: %s. Reconnecting in %.1fs", str(e), backoff)
                await asyncio.sleep(backoff)
                backoff = min(15.0, backoff * 1.6)

    # --------------------------------------------------------
    # LOOP 3: EXECUTION
    # --------------------------------------------------------
    async def run_execution(self) -> None:
        logger.info("Execution loop started.")
        while self.running:
            ticker = await self.state.next_eval_ticker()
            await self._evaluate_and_trade(ticker)

    async def _evaluate_and_trade(self, ticker: str) -> None:
        s = await self.state.get_market(ticker)
        if not s:
            return

        now = time.perf_counter()

        # Must be a confirmed signal and not in cooldown.
        if s.sb_state != SportsbookState.CONFIRMED:
            return
        if s.signal_confirmed_ts is None or (now - s.signal_confirmed_ts) > config.SIGNAL_TTL_SEC:
            return
        if s.last_trade_ts is not None and (now - s.last_trade_ts) < config.TRADE_COOLDOWN_SEC:
            return
        if s.sb_baseline is None:
            return
        if s.sb_book_count < config.SB_MIN_BOOKMAKERS:
            return

        # Need fresh book
        if s.last_book_update_ts is None or (now - s.last_book_update_ts) > config.KALSHI_BOOK_STALE_SEC:
            return

        delta = s.sb_prob - float(s.sb_baseline)
        intended_side = "yes" if delta > 0 else "no"

        # Derive asks for the intended side
        if intended_side == "yes":
            best_ask = s.book.best_yes_ask()
            if best_ask is None:
                return
            asks = sorted(s.book.asks.items())  # ascending
            # Take only a few levels to reduce order spam/latency.
            asks = asks[:config.MAX_SWEEP_LEVELS]
            orders = calculate_sweep_orders(
                asks,
                true_prob=s.sb_prob,
                min_roi=config.MIN_ROI,
                max_cost_cents=int(config.MAX_TRADE_SIZE_USD * 100),
                fee_buffer_cents=config.KALSHI_FEE_BUFFER_CENTS,
            )
            side = "yes"
        else:
            # buy NO using derived NO asks from YES bids
            best_no_ask = s.book.best_no_ask()
            if best_no_ask is None:
                return
            # highest YES bids first => lowest NO ask
            derived = [(100 - p, q) for p, q in sorted(s.book.bids.items(), reverse=True)]
            derived.sort(key=lambda x: x[0])  # ascending by no_price
            derived = derived[:config.MAX_SWEEP_LEVELS]
            orders = calculate_sweep_orders(
                derived,
                true_prob=(1.0 - s.sb_prob),
                min_roi=config.MIN_ROI,
                max_cost_cents=int(config.MAX_TRADE_SIZE_USD * 100),
                fee_buffer_cents=config.KALSHI_FEE_BUFFER_CENTS,
            )
            side = "no"

        if not orders:
            return

        # Telemetry: signal->submit and sb_msg->submit
        submit_ts = time.perf_counter()
        if s.signal_confirmed_ts is not None:
            self.telemetry.add("signal_confirm_to_order_submit_ms", (submit_ts - s.signal_confirmed_ts) * 1000.0)
        if s.signal_source_ts is not None:
            self.telemetry.add("sb_msg_to_order_submit_ms", (submit_ts - s.signal_source_ts) * 1000.0)

        # Place orders
        placed_any = False
        for price, qty in orders:
            notional_usd = (price * qty) / 100.0
            ok = await self.state.can_add_exposure(ticker=ticker, add_usd=notional_usd)
            if not ok:
                continue
            placed_any = True
            asyncio.create_task(self._send_order(ticker=ticker, side=side, price=price, qty=qty))

        if placed_any:
            await self.state.mark_traded(ticker=ticker)

            # Minimal info in normal logs; detailed book/inputs only in DEBUG logs.
            logger.info(
                "EXEC %s side=%s orders=%s sb=%.3f base=%.3f books=%d",
                s.label, side, orders, s.sb_prob, float(s.sb_baseline), s.sb_book_count
            )

    async def _send_order(self, *, ticker: str, side: str, price: int, qty: int) -> None:
        if config.DRY_RUN:
            logger.info("DRY_RUN order ticker=%s side=%s qty=%d price=%d", ticker, side, qty, price)
            return

        client_order_id = f"{int(time.time() * 1e6)}"
        t0 = time.perf_counter()

        try:
            if side == "yes":
                await asyncio.to_thread(
                    self.kalshi.create_order,
                    ticker=ticker,
                    action="buy",
                    side="yes",
                    count=qty,
                    yes_price=price,
                    client_order_id=client_order_id,
                )
            else:
                await asyncio.to_thread(
                    self.kalshi.create_order,
                    ticker=ticker,
                    action="buy",
                    side="no",
                    count=qty,
                    no_price=price,
                    client_order_id=client_order_id,
                )
        except Exception as e:
            logger.warning("Order failed ticker=%s side=%s qty=%d price=%d err=%s", ticker, side, qty, price, str(e))
        finally:
            self.telemetry.add("order_submit_to_response_ms", (time.perf_counter() - t0) * 1000.0)

    # --------------------------------------------------------
    # LOOP 4: TELEMETRY LOGGER
    # --------------------------------------------------------
    async def run_telemetry_logger(self) -> None:
        # Always log telemetry to file; stdout depends on debug logger config.
        while self.running:
            await asyncio.sleep(config.TELEMETRY_LOG_INTERVAL_SEC)
            lines = self.telemetry.summary_lines()
            if not lines:
                continue
            logger.info("TELEMETRY | %s", " | ".join(lines))

    async def start(self) -> None:
        self.running = True
        await self.bootstrap()

        await asyncio.gather(
            self.run_sportsbook_ingestion(),
            self.run_kalshi_ingestion(),
            self.run_execution(),
            self.run_telemetry_logger(),
        )
