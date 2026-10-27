"""
prediction.py — Incident prediction.

A calibrated logistic model over leading indicators (error-rate velocity,
latency drift, warning build-up, new-pattern ratio, health trend). Weights are
hand-tuned priors; `fit()` lets you refit them from labelled history with a few
epochs of gradient descent — no external ML dependency required.
"""
from __future__ import annotations

import math
from dataclasses import dataclass

from .features import WindowFeatures
from .degradation import _slope


@dataclass
class PredictionResult:
    probability: float
    horizon_min: int
    drivers: dict[str, float]


class IncidentPredictor:
    FEATURES = ("error_velocity", "warn_velocity", "latency_drift", "novel_ratio", "err_level", "fatal_flag")

    def __init__(self, window_sec: int = 60):
        self.window_sec = window_sec
        self.weights = {"bias": -3.0, "error_velocity": 2.5, "warn_velocity": 1.2, "latency_drift": 1.5,
                        "novel_ratio": 1.0, "err_level": 2.0, "fatal_flag": 1.5}

    def features(self, history: list[WindowFeatures], current: WindowFeatures) -> dict[str, float]:
        recent = history[-6:] + [current]
        err_rates = [w.error_rate for w in recent]
        warn_rates = [(w.warnings / w.count) if w.count else 0.0 for w in recent]
        lats = [w.avg_latency for w in recent if w.avg_latency]
        lat_drift = 0.0
        if len(lats) >= 3 and lats[0]:
            lat_drift = max(0.0, min(1.0, (lats[-1] / lats[0] - 1.0)))
        seen = {fp for w in history for fp in w.fingerprints}
        novel = sum(1 for fp in current.fingerprints if fp not in seen)
        novel_ratio = novel / max(1, current.unique_patterns) if history else 0.0
        return {
            "error_velocity": max(0.0, min(1.0, _slope(err_rates) * 20)),
            "warn_velocity": max(0.0, min(1.0, _slope(warn_rates) * 10)),
            "latency_drift": lat_drift,
            "novel_ratio": min(1.0, novel_ratio),
            "err_level": min(1.0, current.error_rate * 3),
            "fatal_flag": 1.0 if current.fatal else 0.0,
        }

    def predict(self, history: list[WindowFeatures], current: WindowFeatures) -> PredictionResult:
        f = self.features(history, current)
        logit = self.weights["bias"] + sum(self.weights[k] * v for k, v in f.items())
        p = 1 / (1 + math.exp(-logit))
        horizon = 5 if p > 0.8 else 15 if p > 0.6 else 30
        drivers = {k: round(self.weights[k] * v, 3) for k, v in f.items() if v > 0}
        return PredictionResult(round(p, 4), horizon, drivers)

    def fit(self, samples: list[tuple[dict[str, float], int]], lr: float = 0.1, epochs: int = 200) -> None:
        """Online logistic regression on (features, label) pairs. Label 1 = incident followed."""
        for _ in range(epochs):
            for f, y in samples:
                logit = self.weights["bias"] + sum(self.weights[k] * f.get(k, 0.0) for k in self.FEATURES)
                p = 1 / (1 + math.exp(-logit))
                g = p - y
                self.weights["bias"] -= lr * g
                for k in self.FEATURES:
                    self.weights[k] -= lr * g * f.get(k, 0.0)
