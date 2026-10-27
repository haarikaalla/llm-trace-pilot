"""
degradation.py — Service degradation detection.

LatencyDegradationDetector : one-sided CUSUM on p95 latency — catches slow,
                             sustained drifts a single-window z-score misses.
ServiceDegradationDetector : composite health score across latency, error rate,
                             5xx ratio and pattern churn, with trend awareness.
"""
from __future__ import annotations

import statistics
from dataclasses import dataclass

from .features import WindowFeatures


@dataclass
class DegradationResult:
    score: float
    baseline_p95: float | None
    current_p95: float | None
    cusum: float
    sustained_windows: int


class LatencyDegradationDetector:
    def __init__(self, k_sigma: float = 0.5, h_sigma: float = 4.0, min_history: int = 5, min_increase_pct: float = 0.3,
                 min_sustained: int = 2):
        self.k = k_sigma
        self.h = h_sigma
        self.min_history = min_history
        self.min_increase_pct = min_increase_pct
        self.min_sustained = min_sustained

    def score(self, history: list[WindowFeatures], current: WindowFeatures) -> DegradationResult:
        series = [w.p(0.95) for w in history if w.p(0.95) is not None]
        cur = current.p(0.95)
        if cur is None or len(series) < self.min_history:
            return DegradationResult(0.0, series[-1] if series else None, cur, 0.0, 0)
        base_n = max(self.min_history, len(series) // 2)
        baseline = series[:base_n]
        mu = statistics.mean(baseline)
        sigma = max(statistics.pstdev(baseline), 0.1 * mu, 1.0)
        # CUSUM over the tail (recent half) + current
        tail = series[base_n:] + [cur]
        s, sustained = 0.0, 0
        for x in tail:
            s = max(0.0, s + (x - mu) / sigma - self.k)
            sustained = sustained + 1 if x > mu * (1 + self.min_increase_pct) else 0
        h = self.h
        score = 0.0
        if s > h and cur > mu * (1 + self.min_increase_pct) and sustained >= self.min_sustained:
            score = min(1.0, 0.5 + (s - h) / (2 * h))
        return DegradationResult(round(score, 4), round(mu, 2), round(cur, 2), round(s, 3), sustained)


@dataclass
class HealthResult:
    score: float               # 0 healthy .. 1 critical
    components: dict[str, float]
    trend: str                 # improving | stable | worsening


class ServiceDegradationDetector:
    """Composite degradation: weighted mix of error rate, 5xx share, latency inflation,
    pattern churn, and the slope of error rate over the recent windows."""

    weights = {"error_rate": 0.4, "latency": 0.25, "churn": 0.15, "trend": 0.2}

    def score(self, history: list[WindowFeatures], current: WindowFeatures) -> HealthResult:
        hist = [w for w in history if w.count > 0]
        base_rate = statistics.mean([w.error_rate for w in hist]) if hist else 0.0
        base_lat = statistics.mean([w.avg_latency for w in hist if w.avg_latency]) if any(w.avg_latency for w in hist) else None
        base_pat = statistics.mean([w.unique_patterns for w in hist]) if hist else 0.0

        err_c = min(1.0, max(0.0, (current.error_rate - base_rate) / max(0.05, 1 - base_rate)) * 2) if current.count else 0.0
        if current.fatal:
            err_c = max(err_c, 0.6)
        lat_c = 0.0
        if base_lat and current.avg_latency:
            lat_c = min(1.0, max(0.0, (current.avg_latency / base_lat - 1.0) / 2.0))
        churn_c = min(1.0, max(0.0, (current.unique_patterns - base_pat) / max(base_pat, 3.0))) if current.errors else 0.0

        recent = [w.error_rate for w in history[-5:]] + [current.error_rate]
        slope = _slope(recent)
        trend_c = min(1.0, max(0.0, slope * 10))
        trend = "worsening" if slope > 0.01 else "improving" if slope < -0.01 else "stable"

        comps = {"error_rate": round(err_c, 3), "latency": round(lat_c, 3), "churn": round(churn_c, 3), "trend": round(trend_c, 3)}
        score = sum(self.weights[k] * v for k, v in comps.items())
        return HealthResult(round(min(1.0, score), 4), comps, trend)


def _slope(ys: list[float]) -> float:
    n = len(ys)
    if n < 2:
        return 0.0
    xs = range(n)
    mx, my = (n - 1) / 2, sum(ys) / n
    num = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    den = sum((x - mx) ** 2 for x in xs)
    return num / den if den else 0.0
