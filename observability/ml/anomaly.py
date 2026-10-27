"""
anomaly.py — Unsupervised univariate detectors. All return a score in [0, 1]
and never require a training phase longer than the sliding history.

RobustZScoreDetector : median / MAD — immune to outliers in the baseline
EWMADetector         : exponentially weighted mean & variance — adapts to drift
VolumeAnomalyDetector: ensemble of the two on event volume (drops *and* floods)
"""
from __future__ import annotations

import math
import statistics
from dataclasses import dataclass


def _sigmoid(x: float) -> float:
    return 1.0 / (1.0 + math.exp(-x))


def score_from_z(z: float, threshold: float = 3.0) -> float:
    """Map |z| to [0,1]: ~0.5 at threshold, ->1 as z grows."""
    return round(_sigmoid((abs(z) - threshold) * 1.5), 4)


@dataclass
class DetectorResult:
    score: float
    z: float
    baseline: float
    spread: float
    current: float
    direction: str  # "up" | "down" | "flat"


class RobustZScoreDetector:
    def __init__(self, threshold: float = 3.0, min_history: int = 5):
        self.threshold = threshold
        self.min_history = min_history

    def score(self, history: list[float], current: float) -> DetectorResult:
        if len(history) < self.min_history:
            return DetectorResult(0.0, 0.0, current, 0.0, current, "flat")
        med = statistics.median(history)
        mad = statistics.median(abs(x - med) for x in history)
        spread = 1.4826 * mad if mad > 0 else max(statistics.pstdev(history), 1e-9)
        # guard: tiny baselines make any value look extreme — enforce a floor
        spread = max(spread, 0.1 * max(abs(med), 1.0))
        z = (current - med) / spread
        direction = "up" if z > 0.5 else "down" if z < -0.5 else "flat"
        return DetectorResult(score_from_z(z, self.threshold), round(z, 3), med, spread, current, direction)


class EWMADetector:
    def __init__(self, alpha: float = 0.3, threshold: float = 3.0, min_history: int = 5):
        self.alpha = alpha
        self.threshold = threshold
        self.min_history = min_history

    def score(self, history: list[float], current: float) -> DetectorResult:
        if len(history) < self.min_history:
            return DetectorResult(0.0, 0.0, current, 0.0, current, "flat")
        mean, var = history[0], 0.0
        for x in history[1:]:
            diff = x - mean
            mean += self.alpha * diff
            var = (1 - self.alpha) * (var + self.alpha * diff * diff)
        std = max(math.sqrt(var), 0.1 * max(abs(mean), 1.0))
        z = (current - mean) / std
        direction = "up" if z > 0.5 else "down" if z < -0.5 else "flat"
        return DetectorResult(score_from_z(z, self.threshold), round(z, 3), mean, std, current, direction)


class VolumeAnomalyDetector:
    """Detects floods (log storms) and silences (service down / shipper broken)."""

    def __init__(self, threshold: float = 3.5):
        self.rz = RobustZScoreDetector(threshold)
        self.ewma = EWMADetector(threshold=threshold)

    def score(self, history: list[float], current: float) -> DetectorResult:
        a, b = self.rz.score(history, current), self.ewma.score(history, current)
        best = a if a.score >= b.score else b
        # a drop to zero from a healthy baseline is always suspicious
        if current == 0 and best.baseline >= 10:
            best.score = max(best.score, 0.8)
            best.direction = "down"
        return best
