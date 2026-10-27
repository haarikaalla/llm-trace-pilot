"""
engine.py — DetectionEngine: runs every detector over per-service windows and
emits Anomaly entities. Stateless between runs (history comes from the index),
so any number of detector-worker replicas can run in parallel by service shard.
"""
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Iterable

from ..config import Settings, get_settings
from ..domain.models import Anomaly, AnomalyKind, LogEvent, Severity, utcnow
from .anomaly import VolumeAnomalyDetector
from .degradation import LatencyDegradationDetector, ServiceDegradationDetector
from .features import FeatureExtractor, WindowFeatures
from .patterns import PatternDetector
from .prediction import IncidentPredictor
from .spike import ErrorSpikeDetector


def _sev(score: float) -> Severity:
    if score >= 0.9:
        return Severity.FATAL
    if score >= 0.7:
        return Severity.ERROR
    return Severity.WARN


class DetectionEngine:
    def __init__(self, settings: Settings | None = None):
        self.s = settings or get_settings()
        self.extractor = FeatureExtractor(self.s.detection_window_sec)
        self.volume = VolumeAnomalyDetector()
        self.spike = ErrorSpikeDetector()
        self.latency = LatencyDegradationDetector()
        self.health = ServiceDegradationDetector()
        self.patterns = PatternDetector()
        self.predictor = IncidentPredictor(self.s.detection_window_sec)
        self.threshold = self.s.anomaly_threshold

    # ── public API ──────────────────────────────────────────────────────────
    def run(self, events: Iterable[LogEvent], now: datetime | None = None) -> list[Anomaly]:
        now = now or utcnow()
        anomalies: list[Anomaly] = []
        for service, windows in self.extractor.windows(events).items():
            if not windows:
                continue
            current, history, fraction = self._split(windows, now)
            if current is None:
                continue
            anomalies.extend(self.evaluate_service(service, history, current, fraction))
        anomalies.sort(key=lambda a: -a.score)
        return anomalies

    def service_scores(self, events: Iterable[LogEvent], now: datetime | None = None) -> dict[str, dict]:
        """Lightweight per-service health snapshot for dashboards."""
        now = now or utcnow()
        out: dict[str, dict] = {}
        for service, windows in self.extractor.windows(events).items():
            current, history, _ = self._split(windows, now)
            if current is None:
                continue
            h = self.health.score(history, current)
            pred = self.predictor.predict(history, current)
            out[service] = {
                "health_score": h.score, "trend": h.trend, "components": h.components,
                "incident_probability": pred.probability, "error_rate": round(current.error_rate, 4),
                "event_count": current.count, "avg_latency_ms": round(current.avg_latency, 1) if current.avg_latency else None,
            }
        return out

    # ── internals ───────────────────────────────────────────────────────────
    def _split(self, windows: list[WindowFeatures], now: datetime) -> tuple[WindowFeatures | None, list[WindowFeatures], float]:
        """Current = most recent window; history = the rest; fraction = how much of `current` has elapsed."""
        cutoff = now - timedelta(seconds=self.s.detection_window_sec * (self.s.detection_history_windows + 1))
        windows = [w for w in windows if w.end >= cutoff and w.start <= now]
        if not windows:
            return None, [], 1.0
        current = windows[-1]
        fraction = 1.0
        if current.end > now:
            fraction = max(0.05, (now - current.start).total_seconds() / self.s.detection_window_sec)
            # too little of the window has elapsed: judge the last complete one instead
            if fraction < 0.25 and len(windows) > 1:
                current, fraction = windows[-2], 1.0
                windows = windows[:-1]
        history = windows[:-1]
        if len(history) > self.s.detection_history_windows:
            history = history[-self.s.detection_history_windows:]
        return current, history, fraction

    def evaluate_service(self, service: str, history: list[WindowFeatures], current: WindowFeatures,
                         fraction: float = 1.0) -> list[Anomaly]:
        out: list[Anomaly] = []
        ws, we = current.start, current.end

        sp = self.spike.score(history, current)
        if sp.score >= self.threshold:
            out.append(Anomaly(service, AnomalyKind.ERROR_SPIKE, sp.score, _sev(sp.score),
                               f"Error spike: {sp.current_errors} errors (baseline {sp.baseline_errors}/window, "
                               f"rate {sp.current_rate:.1%} vs {sp.baseline_rate:.1%})", ws, we,
                               evidence={"baseline_errors": sp.baseline_errors, "current_errors": sp.current_errors,
                                         "baseline_rate": sp.baseline_rate, "current_rate": sp.current_rate,
                                         "p_value": sp.p_value, "top_error_types": sp.top_error_types,
                                         "top_fingerprints": sorted(current.error_fingerprints, key=current.error_fingerprints.get, reverse=True)[:5]}))

        lat = self.latency.score(history, current)
        if lat.score >= self.threshold:
            out.append(Anomaly(service, AnomalyKind.LATENCY_DEGRADATION, lat.score, _sev(lat.score),
                               f"Latency degradation: p95 {lat.current_p95}ms vs baseline {lat.baseline_p95}ms "
                               f"(sustained {lat.sustained_windows} windows)", ws, we,
                               evidence={"baseline_p95": lat.baseline_p95, "current_p95": lat.current_p95,
                                         "cusum": lat.cusum, "sustained_windows": lat.sustained_windows}))

        # scale a partial window up to a full-window equivalent; never flag "down" on a partial window
        vol = self.volume.score([float(w.count) for w in history], float(current.count) / fraction)
        if vol.direction == "down" and fraction < 0.9:
            vol.score = 0.0
        if vol.score >= self.threshold:
            out.append(Anomaly(service, AnomalyKind.VOLUME_ANOMALY, vol.score, _sev(vol.score * 0.9),
                               f"Log volume {vol.direction}: {int(vol.current)} events vs baseline {vol.baseline:.0f} (z={vol.z})", ws, we,
                               evidence={"baseline": vol.baseline, "current": vol.current, "z": vol.z, "direction": vol.direction}))

        pat = self.patterns.score(history, current)
        if pat.novel_score >= self.threshold:
            out.append(Anomaly(service, AnomalyKind.NOVEL_PATTERN, pat.novel_score, _sev(pat.novel_score),
                               f"{len(pat.novel_fingerprints)} never-seen error pattern(s) appeared", ws, we,
                               evidence={"fingerprints": pat.novel_fingerprints[:10]}))
        if pat.burst_score >= self.threshold:
            out.append(Anomaly(service, AnomalyKind.PATTERN_BURST, pat.burst_score, _sev(pat.burst_score * 0.9),
                               f"Single pattern dominates {pat.burst_share:.0%} of {current.count} events (possible retry storm)", ws, we,
                               evidence={"fingerprint": pat.burst_fingerprint, "share": pat.burst_share, "count": pat.burst_count}))

        h = self.health.score(history, current)
        if h.score >= self.threshold:
            out.append(Anomaly(service, AnomalyKind.SERVICE_DEGRADATION, h.score, _sev(h.score),
                               f"Service degradation (health {h.score:.2f}, trend {h.trend}): " +
                               ", ".join(f"{k}={v}" for k, v in h.components.items() if v > 0.1), ws, we,
                               evidence={"components": h.components, "trend": h.trend}))

        pred = self.predictor.predict(history, current)
        if (pred.probability >= max(self.threshold, 0.75) and current.count >= 5 and len(history) >= 5
                and not any(a.kind == AnomalyKind.ERROR_SPIKE for a in out)):
            out.append(Anomaly(service, AnomalyKind.INCIDENT_PREDICTION, pred.probability, Severity.WARN,
                               f"Incident likely within ~{pred.horizon_min} min (p={pred.probability:.2f})", ws, we,
                               evidence={"drivers": pred.drivers, "horizon_min": pred.horizon_min}))
        return out
