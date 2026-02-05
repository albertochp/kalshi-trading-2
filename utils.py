"""
Small utilities optimized for the hot path (WS parsing, normalization).

Keep this file dependency-light: no heavy imports on module load.
"""

from __future__ import annotations

import json
import re
from typing import Any, Iterator

try:
    import orjson  # type: ignore

    def json_loads(s: str) -> Any:
        return orjson.loads(s)

    def json_dumps(obj: Any) -> str:
        return orjson.dumps(obj).decode("utf-8")

except Exception:  # pragma: no cover
    def json_loads(s: str) -> Any:
        return json.loads(s)

    def json_dumps(obj: Any) -> str:
        return json.dumps(obj, separators=(",", ":"))


_WS_SPLIT_RE = re.compile(r"}\s*{")
_SPACE_RE = re.compile(r"\s+")


def safe_float(value: Any) -> float:
    if value in (None, "N/A", "", "NaN", "null"):
        return 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def normalize_name(s: str) -> str:
    """Cheap normalization for bookmaker names and other identifiers."""
    if not s:
        return ""
    return _SPACE_RE.sub(" ", str(s).strip().lower())


def iter_concatenated_json(raw_msg: str) -> Iterator[str]:
    """
    Odds WS sometimes concatenates multiple JSON objects into one frame.
    The original bot split on '}{'. We keep that behavior but tolerate whitespace/newlines.

    This is not a general JSON stream parser, but is fast and matches the observed format.
    """
    if not raw_msg:
        return
    msg = raw_msg.strip()
    if not msg:
        return

    parts = _WS_SPLIT_RE.split(msg)
    if len(parts) == 1:
        yield msg
        return

    for i, part in enumerate(parts):
        p = part.strip()
        if not p:
            continue

        # Re-add braces lost during split.
        if i == 0 and not p.endswith("}"):
            p = p + "}"
        elif i == len(parts) - 1 and not p.startswith("{"):
            p = "{" + p
        else:
            if not p.startswith("{"):
                p = "{" + p
            if not p.endswith("}"):
                p = p + "}"
        yield p
