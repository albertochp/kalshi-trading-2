"""
main.py

Entry point for the refactored latency bot.

Behavior summary:
- Load .env first so config variables are populated deterministically.
- Bootstrap: fetch Odds events (live -> pending) and Kalshi series events (then hydrate details).
- Build mapping using existing strategy.build_event_market_mapping(odds_events, kalshi_events_detailed).
- Start MarketMaker with mapping.
- Retry bootstrap on transient failures (sleep between attempts).
"""

from __future__ import annotations

# Load .env before importing config so config values are populated.
from dotenv import load_dotenv
load_dotenv(dotenv_path=".env")

import argparse
import asyncio
import sys
import traceback
from typing import Any, Dict, List

import config
from logging_utils import setup_logging, get_logger
from data_fetch import fetch_live_then_pending
from kalshi_client import KalshiClient
from strategy import build_event_market_mapping
from market_maker import MarketMaker

BOOTSTRAP_RETRY_SEC = 300  # Sleep between bootstrap retries on failure


async def _fetch_kalshi_events_for_series(logger, kalshi_client: KalshiClient, series_ticker: str) -> List[Dict[str, Any]]:
    """
    Fetch summary events for the given Kalshi series, then attempt to hydrate details.
    Fallback: if get_event_details is missing or hydration fails, return the summary entries
    (they often contain enough fields such as 'markets' or 'title' for mapping).
    """
    logger.debug(f"Fetching Kalshi series events for series={series_ticker}")
    summary_list = await asyncio.to_thread(kalshi_client.list_events_for_series, series_ticker=series_ticker, limit=500)
    if not summary_list:
        logger.warning(f"No Kalshi series events returned for series={series_ticker}")
        return []

    # If KalshiClient has get_event_details, attempt to hydrate a *limited* number to avoid rate limits.
    detailed: List[Dict[str, Any]] = []
    hydrate_fn = getattr(kalshi_client, "get_event_details", None)

    if callable(hydrate_fn):
        # Hydrate up to N items (configurable). Keep small to avoid rate limits.
        MAX_HYDRATE = 50
        count = 0
        for s in summary_list:
            if count >= MAX_HYDRATE:
                break
            event_ticker = s.get("event_ticker") or (s.get("event") or {}).get("event_ticker")
            if not event_ticker:
                # If the summary already contains markets, use it directly
                if isinstance(s, dict) and s.get("markets"):
                    detailed.append(s)
                continue
            try:
                det = await asyncio.to_thread(hydrate_fn, event_ticker=event_ticker)
                # If det is empty dict or falsy, fall back to the summary entry
                if det and isinstance(det, dict):
                    detailed.append(det)
                else:
                    # Use summary entry as fallback
                    detailed.append(s)
            except Exception as e:
                logger.warning(f"Failed to fetch details for {event_ticker}: {e}")
                # fallback to summary
                detailed.append(s)
            count += 1

        # If we hydrated fewer than summary_list length, append remaining summaries (unhydrated)
        if len(detailed) < len(summary_list):
            for s in summary_list[len(detailed):]:
                detailed.append(s)
    else:
        # No hydrate support; use the summary list directly
        detailed = list(summary_list)

    logger.info(f"Fetched {len(detailed)} detailed Kalshi events (summary fallback) for series={series_ticker}")
    return detailed



async def bootstrap_mapping(logger) -> Dict[str, Any]:
    """
    Coordinate fetching odds events and kalshi events, then call the strategy mapping.
    Returns the mapping object the MarketMaker expects.
    """
    logger.info(f"BOOTSTRAP starting. league={config.LEAGUE_KEY} series={config.SERIES_TICKER}")

    # 1) Fetch odds events: live -> pending -> fallback
    odds_events = await fetch_live_then_pending(
        sport=config.SPORT_KEY,
        league=config.LEAGUE_KEY,
        limit=config.BOOTSTRAP_EVENT_LIMIT,
        logger=logger,
    )

    if not odds_events:
        raise RuntimeError("No actionable events returned from Odds-API (live/pending).")

    # 2) Fetch Kalshi summary + hydrated details
    kalshi = KalshiClient()
    kalshi_events_detailed = await _fetch_kalshi_events_for_series(logger, kalshi, series_ticker=config.SERIES_TICKER)

    if not kalshi_events_detailed:
        # Not fatal if Kalshi has no events; mapping may still proceed via title similarity,
        # but we surface a warning so the operator can inspect.
        logger.warning("Kalshi returned no detailed events for series; mapping may be incomplete.")

    # 3) Call your existing mapping routine (signature: odds_events, kalshi_events_detailed, ...)
    # Preserve your algorithm by passing the two lists in the expected order.
    mapping = build_event_market_mapping(odds_events, kalshi_events_detailed, verbose=True, min_score_threshold=0.5)


    if not mapping:
        raise RuntimeError("build_event_market_mapping produced no mapped markets. Check SERIES_TICKER or matching heuristics.")

    logger.info(f"BOOTSTRAP complete. mapped_events={len(mapping)}")
    return mapping


async def async_main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--debug", action="store_true", help="Enable console logging (in addition to file).")
    args = parser.parse_args()

    debug_enabled = bool(args.debug) or bool(config.DEBUG)

    setup_logging(log_file=config.LOG_FILE, to_stdout=debug_enabled)
    logger = get_logger()

    logger.info(f"Mode: DRY_RUN={config.DRY_RUN} DEBUG={debug_enabled}")

    # Bootstrap with a retry loop so the bot remains alive even if no events are currently available.
    while True:
        try:
            mapping = await bootstrap_mapping(logger)
            break
        except Exception as e:
            logger.error(f"BOOTSTRAP failed: {e}")
            logger.debug("".join(traceback.format_exc()))
            logger.warning(f"Retrying bootstrap in {BOOTSTRAP_RETRY_SEC} seconds.")
            await asyncio.sleep(BOOTSTRAP_RETRY_SEC)

    # Start the market maker with the mapping.
    maker = MarketMaker(mapping=mapping, logger=logger)
    await maker.run()


def main() -> None:
    try:
        asyncio.run(async_main())
    except KeyboardInterrupt:
        return
    except Exception:
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
