#!/usr/bin/env python3
"""
Generate a mapping from normalized team names -> Kalshi event tickers & titles
for a given Kalshi series. Output: team_to_kalshi.json

Usage:
    python scripts/generate_kalshi_team_mapping.py --series KXSERIEAGAME --out team_to_kalshi.json

This uses the same normalize_team() function as strategy.py for consistency.
"""

from __future__ import annotations


import sys
import os
import sys
import os

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

import argparse
import json
from collections import defaultdict
from typing import Dict, List, Set

from dotenv import load_dotenv
load_dotenv(".env")

from kalshi_client import KalshiClient
import config

# copy the normalization code (a minimal version matching strategy.normalize_team)
import re
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


def extract_teams_from_title(title: str) -> List[str]:
    if not title:
        return []
    title = title.strip()
    # common separators
    for sep in [" vs ", " v ", " - ", " vs. ", " v. "]:
        if sep in title.lower():
            parts = title.split(sep)
            if len(parts) >= 2:
                return [parts[0].strip(), parts[1].strip()]
    # fallback heuristics: split by " vs " insensitive to case
    low = title.lower()
    if " vs " in low:
        parts = title.split(" vs ")
        return [parts[0].strip(), parts[1].strip()]
    # last resort: whitespace heuristic (poor)
    tokens = title.split()
    if len(tokens) >= 2:
        return [" ".join(tokens[:-1]), tokens[-1]]
    return []


def build_mapping(series_ticker: str, limit: int = 2000) -> Dict[str, Dict]:
    k = KalshiClient()
    evs = k.list_events_for_series(series_ticker=series_ticker, limit=limit)
    by_team = defaultdict(lambda: {"tickers": set(), "examples": set()})

    for e in evs:
        ticker = e.get("event_ticker") or (e.get("event") or {}).get("event_ticker") or ""
        title = e.get("title") or (e.get("event") or {}).get("title") or ""
        teams = extract_teams_from_title(title)
        if not teams and title:
            # try splitting on uppercase conventions (e.g., "NAPOLI vs FIORENTINA")
            # fallback left blank
            continue
        for team in teams:
            norm = normalize_team(team)
            if not norm:
                continue
            if ticker:
                by_team[norm]["tickers"].add(str(ticker))
            if title:
                by_team[norm]["examples"].add(str(title))

    # convert sets to sorted lists
    out = {}
    for team_norm, v in by_team.items():
        out[team_norm] = {
            "team_norm": team_norm,
            "tickers": sorted(list(v["tickers"])),
            "examples": sorted(list(v["examples"])),
        }
    return out


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--series", required=True, help="Kalshi series ticker (e.g. KXSERIEAGAME)")
    p.add_argument("--limit", type=int, default=2000)
    p.add_argument("--out", default="team_to_kalshi.json")
    args = p.parse_args()

    mapping = build_mapping(args.series, limit=args.limit)
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(mapping, f, indent=2, ensure_ascii=False)

    print(f"Wrote {len(mapping)} normalized teams to {args.out}")


if __name__ == "__main__":
    main()
