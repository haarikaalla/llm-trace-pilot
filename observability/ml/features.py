"""
features.py — Turn a stream of LogEvents into per-service, fixed-width time
windows with the features the detectors consume.
"""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Iterable

from ..domain.models import LogEvent


@dataclass
class WindowFeatures:
    service: str
    start: datetime
    end: datetime
    count: int = 0
    errors: int = 0
    warnings: int = 0
    fatal: int = 0
    latencies: list[float] = field(default_factory=list)
    fingerprints: dict[str, int] = field(default_factory=dict)
    error_fingerprints: dict[str, int] = field(default_factory=dict)
    error_types: dict[str, int] = field(default_factory=dict)
    status_5xx: int = 0
    hosts: set[str] = field(default_factory=set)
    trace_ids: set[str] = field(default_factory=set)

    @property
    def error_rate(self) -> float:
        return self.errors / self.count if self.count else 0.0

    @property
    def avg_latency(self) -> float | None:
        return sum(self.latencies) / len(self.latencies) if self.latencies else None

    def p(self, q: float) -> float | None:
        if not self.latencies:
            return None
        xs = sorted(self.latencies)
        idx = min(len(xs) - 1, max(0, int(round(q * (len(xs) - 1)))))
        return xs[idx]

    @property
    def unique_patterns(self) -> int:
        return len(self.fingerprints)

    def vector(self) -> dict[str, float]:
        return {
            "count": float(self.count), "errors": float(self.errors), "error_rate": self.error_rate,
            "avg_latency": self.avg_latency or 0.0, "p95_latency": self.p(0.95) or 0.0,
            "unique_patterns": float(self.unique_patterns), "fatal": float(self.fatal),
            "status_5xx": float(self.status_5xx),
        }


def floor_to_window(ts: datetime, window_sec: int) -> datetime:
    epoch = int(ts.timestamp()) // window_sec * window_sec
    return datetime.fromtimestamp(epoch, tz=timezone.utc)


class FeatureExtractor:
    def __init__(self, window_sec: int = 60):
        self.window_sec = window_sec

    def windows(self, events: Iterable[LogEvent]) -> dict[str, list[WindowFeatures]]:
        """Returns {service: [WindowFeatures sorted by start]} with gaps filled by empty windows."""
        buckets: dict[tuple[str, datetime], WindowFeatures] = {}
        for e in events:
            start = floor_to_window(e.timestamp, self.window_sec)
            key = (e.service, start)
            w = buckets.get(key)
            if w is None:
                w = buckets[key] = WindowFeatures(e.service, start, start + timedelta(seconds=self.window_sec))
            w.count += 1
            if e.level.is_error:
                w.errors += 1
                if e.fingerprint:
                    w.error_fingerprints[e.fingerprint] = w.error_fingerprints.get(e.fingerprint, 0) + 1
                if e.error_type:
                    w.error_types[e.error_type] = w.error_types.get(e.error_type, 0) + 1
            if e.level.value == "FATAL":
                w.fatal += 1
            if e.level.value == "WARN":
                w.warnings += 1
            if e.latency_ms is not None:
                w.latencies.append(float(e.latency_ms))
            if e.fingerprint:
                w.fingerprints[e.fingerprint] = w.fingerprints.get(e.fingerprint, 0) + 1
            if e.status_code and e.status_code >= 500:
                w.status_5xx += 1
            if e.host:
                w.hosts.add(e.host)
            if e.trace_id:
                w.trace_ids.add(e.trace_id)

        by_service: dict[str, list[WindowFeatures]] = defaultdict(list)
        for (svc, _), w in buckets.items():
            by_service[svc].append(w)
        for svc, ws in by_service.items():
            ws.sort(key=lambda w: w.start)
            by_service[svc] = self._fill_gaps(svc, ws)
        return dict(by_service)

    def _fill_gaps(self, svc: str, ws: list[WindowFeatures]) -> list[WindowFeatures]:
        if len(ws) < 2:
            return ws
        out, step = [ws[0]], timedelta(seconds=self.window_sec)
        for w in ws[1:]:
            cur = out[-1].start + step
            while cur < w.start:
                out.append(WindowFeatures(svc, cur, cur + step))
                cur += step
            out.append(w)
        return out
