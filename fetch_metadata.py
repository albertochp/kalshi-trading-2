"""
Odds-API metadata fetcher.

Outputs:
- bookmakers list
- active leagues scan (based on live/upcoming events)

This script is optional; it's here to help you populate config env vars.
"""

from __future__ import annotations

import argparse
import json
from typing import Any, Dict, List

import requests

import config
from logging_utils import setup_logging


def fetch_bookmakers(api_key: str) -> List[str]:
    url = "https://api2.odds-api.io/v3/bookmakers"
    r = requests.get(url, params={"apiKey": api_key}, timeout=10)
    r.raise_for_status()
    data = r.json()
    keys: List[str] = []
    if isinstance(data, list):
        for b in data:
            if isinstance(b, dict):
                k = b.get("key") or b.get("name")
                if k:
                    keys.append(str(k))
            else:
                keys.append(str(b))
    return keys


def fetch_active_leagues(api_key: str) -> Dict[str, str]:
    url = "https://api2.odds-api.io/v3/events"
    params = {"apiKey": api_key, "sport": "football", "status": "live", "limit": 50}
    r = requests.get(url, params=params, timeout=10)
    r.raise_for_status()
    events = r.json()
    if not events:
        params["status"] = "upcoming"
        r = requests.get(url, params=params, timeout=10)
        r.raise_for_status()
        events = r.json()

    leagues: Dict[str, str] = {}
    if isinstance(events, list):
        for ev in events:
            if not isinstance(ev, dict):
                continue
            league_obj = ev.get("league")
            if isinstance(league_obj, dict):
                slug = league_obj.get("slug")
                name = league_obj.get("name")
                if slug:
                    leagues[str(slug)] = str(name or slug)
    return leagues


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--debug", action="store_true")
    parser.add_argument("--out", default="odds_api_metadata.txt")
    args = parser.parse_args()

    if args.debug:
        config.DEBUG = True

    logger = setup_logging(debug=config.DEBUG, log_file=config.LOG_FILE, logger_name="TBot")

    if not config.ODDS_API_KEY:
        logger.critical("Missing ODDS_API_KEY environment variable.")
        return 2

    out_path = args.out
    logger.info("Writing metadata to %s", out_path)

    with open(out_path, "w", encoding="utf-8") as f:
        f.write("=== ODDS-API.IO METADATA ===\n")
        f.write("Use these values to update your environment variables / config.py\n\n")

        # 1) Bookmakers
        try:
            books = fetch_bookmakers(config.ODDS_API_KEY)
            f.write("--- VALID BOOKMAKERS (BOOKMAKERS env var) ---\n")
            f.write("ALL_AVAILABLE: " + ",".join(books) + "\n\n")
            f.write("Detailed List:\n")
            for b in books:
                f.write(f" - {b}\n")
            f.write("\n")
            logger.info("Fetched %d bookmakers.", len(books))
        except Exception as e:
            logger.warning("Failed to fetch bookmakers: %s", str(e))
            f.write("Failed to fetch bookmakers.\n\n")

        f.write("=" * 40 + "\n\n")

        # 2) Leagues
        try:
            leagues = fetch_active_leagues(config.ODDS_API_KEY)
            f.write("--- ACTIVE LEAGUES (LEAGUE_KEY env var) ---\n")
            for slug, name in sorted(leagues.items()):
                f.write(f"NAME: {name:<30} | SLUG: {slug}\n")
            f.write("\n")
            logger.info("Found %d active/upcoming leagues.", len(leagues))
        except Exception as e:
            logger.warning("Failed to fetch leagues: %s", str(e))
            f.write("Failed to fetch leagues.\n\n")

    logger.info("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
