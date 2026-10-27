"""ML detector tests with synthetic windows — deterministic, no I/O."""
from datetime import datetime, timedelta, timezone

import pytest

from observability.domain.models import AnomalyKind, LogEvent, Severity
from observability.ml import (DetectionEngine, ErrorSpikeDetector, EWMADetector, FeatureExtractor,
                              IncidentPredictor, LatencyDegradationDetector, PatternDetector,
                              RobustZScoreDetector, ServiceDegradationDetector, VolumeAnomalyDetector)
from observability.ml.features import WindowFeatures
from observability.ml.spike import poisson_sf

T0 = datetime(2025, 10, 27, 10, 0, tzinfo=timezone.utc)


def win(i, count=100, errors=2, lat=120.0, fps=None, warnings=0, fatal=0):
    w = WindowFeatures("svc", T0 + timedelta(minutes=i), T0 + timedelta(minutes=i + 1))
    w.count, w.errors, w.warnings, w.fatal = count, errors, warnings, fatal
    w.latencies = [lat * (0.8 + 0.4 * (k % 3) / 2) for k in range(min(count, 30))]
    fps = fps or {"fp-a": count - errors, "fp-err": errors}
    w.fingerprints = dict(fps)
    w.error_fingerprints = {k: v for k, v in fps.items() if "err" in k}
    if errors:
        w.error_types = {"timeout": errors}
    return w


def history(n=15, **kw):
    return [win(i, **kw) for i in range(n)]


# ── univariate detectors ────────────────────────────────────────────────────
def test_robust_z_flags_outlier_and_ignores_normal():
    d = RobustZScoreDetector()
    base = [100, 102, 98, 101, 99, 103, 97, 100, 100, 101]
    assert d.score(base, 101).score < 0.2
    assert d.score(base, 400).score > 0.9
    assert d.score(base, 400).direction == "up"


def test_robust_z_needs_history():
    assert RobustZScoreDetector().score([1, 2], 500).score == 0.0


def test_ewma_adapts_to_drift():
    d = EWMADetector()
    rising = [100 + 10 * i for i in range(15)]
    # next natural step is ~250: should not be an anomaly under EWMA
    assert d.score(rising, 250).score < 0.6
    assert d.score(rising, 900).score > 0.8


def test_volume_detector_catches_silence():
    r = VolumeAnomalyDetector().score([50.0] * 10, 0.0)
    assert r.score >= 0.8 and r.direction == "down"


# ── error spike ─────────────────────────────────────────────────────────────
def test_poisson_sf_basic():
    assert poisson_sf(0, 5) == 1.0
    assert poisson_sf(30, 2) < 1e-10
    assert 0.5 < poisson_sf(2, 2) < 0.7


def test_error_spike_detected():
    r = ErrorSpikeDetector().score(history(), win(20, errors=40))
    assert r.score > 0.9 and r.current_errors == 40 and r.top_error_types == {"timeout": 40}


def test_error_spike_ignores_baseline_noise():
    assert ErrorSpikeDetector().score(history(), win(20, errors=3)).score < 0.5


def test_error_spike_cold_start_all_errors():
    assert ErrorSpikeDetector().score([], win(0, count=20, errors=15)).score >= 0.7


# ── degradation ─────────────────────────────────────────────────────────────
def test_latency_degradation_sustained():
    hist = history(10) + [win(10 + i, lat=400.0) for i in range(4)]
    r = LatencyDegradationDetector().score(hist, win(14, lat=450.0))
    assert r.score >= 0.7 and r.sustained_windows >= 4


def test_latency_single_blip_not_flagged():
    r = LatencyDegradationDetector().score(history(12), win(12, lat=250.0))
    assert r.score < 0.7


def test_service_degradation_composite():
    r = ServiceDegradationDetector().score(history(), win(20, errors=60, lat=600.0))
    assert r.score > 0.6 and r.trend == "worsening"
    assert r.components["error_rate"] > 0.5
    healthy = ServiceDegradationDetector().score(history(), win(20))
    assert healthy.score < 0.2


# ── patterns ────────────────────────────────────────────────────────────────
def test_novel_pattern():
    cur = win(20, errors=10, fps={"fp-a": 90, "fp-err-new": 10})
    r = PatternDetector().score(history(), cur)
    assert r.novel_score >= 0.7 and r.novel_fingerprints == ["fp-err-new"]


def test_pattern_burst():
    cur = win(20, count=1000, errors=0, fps={"fp-a": 980, "fp-b": 20})
    r = PatternDetector().score(history(), cur)
    assert r.burst_score >= 0.7 and r.burst_fingerprint == "fp-a"
    assert PatternDetector().score(history(), win(20)).burst_score == 0.0


# ── prediction ──────────────────────────────────────────────────────────────
def test_predictor_rises_with_leading_indicators():
    pred = IncidentPredictor()
    calm = pred.predict(history(), win(20)).probability
    hist = history(10) + [win(10 + i, errors=5 + 6 * i, warnings=10 + 5 * i, lat=120 + 40 * i) for i in range(5)]
    hot = pred.predict(hist, win(15, errors=40, warnings=40, lat=350, fatal=1)).probability
    assert calm < 0.2 < 0.7 < hot


def test_predictor_fit_moves_weights():
    pred = IncidentPredictor()
    before = dict(pred.weights)
    samples = [({"error_velocity": 1.0, "err_level": 1.0}, 1), ({"error_velocity": 0.0, "err_level": 0.0}, 0)] * 5
    pred.fit(samples, epochs=20)
    assert pred.weights != before


# ── feature extraction + engine ─────────────────────────────────────────────
def _events(minutes=12, per_min=30, spike_last=False):
    out = []
    for m in range(minutes):
        for k in range(per_min):
            err = spike_last and m == minutes - 1 and k < per_min // 2
            out.append(LogEvent(message="x", service="svc", level=Severity.ERROR if err else Severity.INFO,
                                timestamp=T0 + timedelta(minutes=m, seconds=k), fingerprint="e" if err else "i",
                                latency_ms=3000.0 if err else 100.0, error_type="timeout" if err else None))
    return out


def test_feature_extractor_buckets_and_fills_gaps():
    evs = _events(3) + [LogEvent(message="late", service="svc", timestamp=T0 + timedelta(minutes=6))]
    ws = FeatureExtractor(60).windows(evs)["svc"]
    assert len(ws) == 7 and ws[3].count == 0 and ws[0].count == 30


def test_engine_detects_spike_and_nothing_on_calm(obs_env):
    eng = DetectionEngine(obs_env)
    now = T0 + timedelta(minutes=12)
    assert eng.run(_events(12), now) == []
    anomalies = eng.run(_events(12, spike_last=True), now)
    kinds = {a.kind for a in anomalies}
    assert AnomalyKind.ERROR_SPIKE in kinds
    assert all(a.service == "svc" and 0 <= a.score <= 1 for a in anomalies)


def test_engine_partial_window_no_false_volume_drop(obs_env):
    eng = DetectionEngine(obs_env)
    evs = _events(12) + [LogEvent(message="x", service="svc", timestamp=T0 + timedelta(minutes=12, seconds=s)) for s in range(0, 20, 2)]
    anomalies = eng.run(evs, T0 + timedelta(minutes=12, seconds=20))
    assert not any(a.kind == AnomalyKind.VOLUME_ANOMALY for a in anomalies)
