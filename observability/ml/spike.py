"""
spike.py — Error spike detection.

Combines two signals so it works for both chatty and quiet services:
  1. Poisson tail probability of the observed error count given the baseline rate
  2. Error-rate (errors/total) jump versus the baseline rate
"""
from __future__ import annotations

import math
from dataclasses import dataclass

from .features import WindowFeatures


def poisson_sf(k: int, lam: float) -> float:
    """P(X >= k) for X ~ Poisson(lam); stable for moderate k."""
    if lam <= 0:
        return 0.0 if k > 0 else 1.0
    if k <= 0:
        return 1.0
    # sum of pmf for 0..k-1 then complement; use logs for stability
    total, log_term = 0.0, -lam
    for i in range(k):
        total += math.exp(log_term)
        log_term += math.log(lam) - math.log(i + 1)
        if total >= 1.0:
            return 0.0
    return max(0.0, 1.0 - total)


@dataclass
class SpikeResult:
    score: float
    baseline_errors: float
    current_errors: int
    baseline_rate: float
    current_rate: float
    p_value: float
    top_error_types: dict[str, int]


class ErrorSpikeDetector:
    def __init__(self, min_errors: int = 3, p_threshold: float = 1e-3, rate_multiplier: float = 3.0):
        self.min_errors = min_errors
        self.p_threshold = p_threshold
        self.rate_multiplier = rate_multiplier

    def score(self, history: list[WindowFeatures], current: WindowFeatures) -> SpikeResult:
        hist = [w for w in history if w.count > 0] or history
        base_err = (sum(w.errors for w in hist) / len(hist)) if hist else 0.0
        base_rate = (sum(w.errors for w in hist) / max(1, sum(w.count for w in hist))) if hist else 0.0
        cur_err, cur_rate = current.errors, current.error_rate

        if cur_err < self.min_errors:
            return SpikeResult(0.0, base_err, cur_err, base_rate, cur_rate, 1.0, dict(current.error_types))

        p = poisson_sf(cur_err, max(base_err, 0.5))
        p_score = min(1.0, max(0.0, (-math.log10(max(p, 1e-12)) - (-math.log10(self.p_threshold))) / 6 + 0.5)) if p < self.p_threshold else 0.0

        if base_rate > 0:
            ratio = cur_rate / base_rate
            rate_score = 1 / (1 + math.exp(-(ratio - self.rate_multiplier))) if ratio >= self.rate_multiplier else 0.0
        else:
            rate_score = min(1.0, cur_rate * 2) if cur_rate > 0.05 else 0.0

        # a cold service with only errors is a spike regardless of history
        if not hist and cur_rate >= 0.5:
            rate_score = max(rate_score, 0.75)

        score = round(max(p_score, rate_score, 0.6 * p_score + 0.6 * rate_score if (p_score and rate_score) else 0), 4)
        return SpikeResult(min(score, 1.0), round(base_err, 2), cur_err, round(base_rate, 4), round(cur_rate, 4),
                           p, dict(sorted(current.error_types.items(), key=lambda kv: -kv[1])[:5]))
