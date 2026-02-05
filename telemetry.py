"""
Latency telemetry with rolling percentiles.

This is intentionally lightweight:
- O(1) append with a bounded deque per metric.
- Percentiles computed only when logging summaries (not in the hot path).
"""

from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Deque, Dict, Iterable, List, Optional, Tuple


def _percentile(sorted_vals: List[float], p: float) -> float:
    """
    p in [0, 100]. Uses nearest-rank on [0..n-1] index.
    """
    if not sorted_vals:
        return 0.0
    n = len(sorted_vals)
    if n == 1:
        return sorted_vals[0]
    # Clamp and compute index
    p2 = 0.0 if p < 0.0 else 100.0 if p > 100.0 else p
    idx = int(round((p2 / 100.0) * (n - 1)))
    return sorted_vals[idx]


@dataclass(frozen=True)
class MetricSummary:
    count: int
    p50: float
    p90: float
    p99: float
    avg: float
    max_v: float

    def format_ms(self) -> str:
        # values are already ms
        return f"n={self.count} p50={self.p50:.1f} p90={self.p90:.1f} p99={self.p99:.1f} avg={self.avg:.1f} max={self.max_v:.1f}"


class LatencyTelemetry:
    def __init__(self, window: int = 2000):
        self._window = max(50, int(window))
        self._data: Dict[str, Deque[float]] = defaultdict(lambda: deque(maxlen=self._window))

    def add(self, metric: str, value_ms: float) -> None:
        if not metric:
            return
        if value_ms < 0:
            return
        self._data[metric].append(float(value_ms))

    def summarize(self, metric: str) -> Optional[MetricSummary]:
        vals = list(self._data.get(metric, []))
        if not vals:
            return None
        vals.sort()
        cnt = len(vals)
        avg = sum(vals) / cnt
        return MetricSummary(
            count=cnt,
            p50=_percentile(vals, 50.0),
            p90=_percentile(vals, 90.0),
            p99=_percentile(vals, 99.0),
            avg=avg,
            max_v=vals[-1],
        )

    def summary_lines(self) -> List[str]:
        lines: List[str] = []
        for k in sorted(self._data.keys()):
            s = self.summarize(k)
            if s is None:
                continue
            lines.append(f"{k}: {s.format_ms()}")
        return lines
