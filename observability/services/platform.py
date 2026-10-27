"""
platform.py — ObservabilityPlatform: composition root.

Wires every component through the port interfaces. In single-process mode
(dashboard, tests, demos) it also runs the processor and detector inline so
the whole platform works with zero infrastructure. In production each worker
runs the same objects in its own container against Kafka + OpenSearch.
"""
from __future__ import annotations

import logging
import threading
import time
from datetime import datetime, timedelta
from typing import Any, Iterable

from ..alerting import AlertManager, build_notifiers
from ..config import Settings, get_settings
from ..domain.models import Incident, IncidentStatus, LogEvent, ServiceHealth, utcnow
from ..ingestion import Collector
from ..intelligence import IncidentSummarizer
from ..ml import DetectionEngine
from ..pipeline import LogProcessor
from ..rca import DependencyMapper, RootCauseAnalyzer
from ..storage import build_incident_repository, build_log_index
from ..streaming import Topics, build_event_bus

log = logging.getLogger(__name__)


class ObservabilityPlatform:
    def __init__(self, settings: Settings | None = None, inline: bool | None = None):
        self.s = settings or get_settings()
        self.bus = build_event_bus(self.s)
        self.index = build_log_index(self.s)
        self.repo = build_incident_repository(self.s)
        self.collector = Collector(self.bus, self.s)
        self.processor = LogProcessor(self.s)
        self.detector = DetectionEngine(self.s)
        self.mapper = DependencyMapper(self.repo)
        self.rca = RootCauseAnalyzer(self.index, self.repo)
        self.summarizer = IncidentSummarizer(self.s)
        self.alerts = AlertManager(self.repo, build_notifiers(self.s), self.bus, settings=self.s,
                                   related_services=self._related)
        self.inline = (self.s.event_bus == "memory") if inline is None else inline
        self._lock = threading.Lock()
        self.last_detection_at: datetime | None = None
        self.listeners: list = []           # real-time fan-out (SSE)
        if self.inline:
            self.bus.subscribe(Topics.ALERTS, lambda r: self._emit("alert", r))
            self.bus.subscribe(Topics.INCIDENTS, lambda r: self._emit("incident", r))

    # ── real-time fan-out ───────────────────────────────────────────────────
    def _emit(self, kind: str, payload: dict[str, Any]) -> None:
        for cb in list(self.listeners):
            try:
                cb({"type": kind, "data": payload, "ts": utcnow().isoformat()})
            except Exception:                           # noqa: BLE001
                pass

    def _related(self, service: str) -> set[str]:
        g = self.mapper.load()
        return g.upstream(service) | g.downstream(service)

    # ── ingestion → processing ──────────────────────────────────────────────
    def ingest(self, records: Iterable[Any], source_type: str | None = None,
               defaults: dict[str, Any] | None = None) -> dict[str, Any]:
        result = self.collector.ingest(records, source_type, defaults)
        if self.inline:
            result["processed"] = self.process_pending()
        return result

    def ingest_events(self, events: Iterable[LogEvent]) -> int:
        """Bypass the parser for already-normalised events (e.g. the tracer bridge)."""
        events = list(events)
        n = self.index.index_bulk(events)
        if self.inline:
            for e in events:
                self._emit("log", e.to_dict())
        else:
            self.bus.publish_many(Topics.LOGS_PROCESSED, (e.to_dict() for e in events))
        return n

    def process_pending(self, max_batches: int = 50) -> int:
        """Drain logs.raw → pipeline → index → logs.processed. Returns events indexed."""
        total = 0
        for _ in range(max_batches):
            raws = self.bus.consume(Topics.LOGS_RAW, max_records=self.s.batch_size)
            if not raws:
                break
            events, dlq = self.processor.process_batch(raws)
            for d in dlq:
                self.bus.publish(Topics.DLQ, d)
            if events:
                total += self.index.index_bulk(events)
                for e in events:
                    self.bus.publish(Topics.LOGS_PROCESSED, e.to_dict(), key=e.service)
                if self.inline:
                    for e in events[-20:]:
                        self._emit("log", e.to_dict())
        return total

    # ── detection → alerting → incidents ────────────────────────────────────
    def run_detection(self, now: datetime | None = None, lookback_sec: int | None = None) -> dict[str, Any]:
        now = now or utcnow()
        lookback = lookback_sec or self.s.detection_window_sec * (self.s.detection_history_windows + 1)
        events = list(self.index.events_since(now - timedelta(seconds=lookback), now))
        with self._lock:
            if events:    # topology first so alert grouping can merge related services
                self.mapper.merge_and_persist(self.mapper.infer_from_events(events))
            anomalies = self.detector.run(events, now)
            alerts, incidents = self.alerts.handle(anomalies)
            for inc in incidents:
                self._enrich_incident(inc, events)
            incidents = self._subsume(incidents)
            anomalous = {a.service for a in anomalies}
            quiet = {s for s in self.index.services(now - timedelta(seconds=lookback))} - anomalous
            resolved = self.alerts.auto_resolve(quiet)
            self.last_detection_at = now
        return {"events_scanned": len(events), "anomalies": [a.to_dict() for a in anomalies],
                "alerts": [a.to_dict() for a in alerts], "incidents": [i.to_dict() for i in incidents],
                "resolved": [i.incident_id for i in resolved]}

    def _enrich_incident(self, inc: Incident, events: list[LogEvent]) -> None:
        try:
            rca = self.rca.analyze(inc, events)
            inc.root_cause = {k: rca[k] for k in ("primary", "primary_confidence", "ranked_services", "hypotheses",
                                                 "impacted_services", "similar_incidents")}
            inc.signature = rca["signature"]
            anomalies = [a.to_dict() for a in self.repo.list_anomalies(limit=50) if a.anomaly_id in inc.anomaly_ids]
            inc.summary = self.summarizer.summarize(inc, rca, anomalies)
            for svc in rca["impacted_services"]:
                if svc not in inc.services:
                    inc.services.append(svc)
            self.repo.save_incident(inc)
        except Exception:                               # noqa: BLE001 — never lose the incident over RCA failure
            log.exception("incident enrichment failed for %s", inc.incident_id)

    def _subsume(self, incidents: list[Incident]) -> list[Incident]:
        """After RCA widened impacted services, fold incidents fully covered by a larger one."""
        incidents = sorted(incidents, key=lambda i: -len(i.services))
        keep: list[Incident] = []
        for inc in incidents:
            parent = next((k for k in keep if set(inc.services) <= set(k.services)), None)
            if parent is None:
                keep.append(inc)
                continue
            parent.anomaly_ids += [a for a in inc.anomaly_ids if a not in parent.anomaly_ids]
            parent.started_at = min(parent.started_at, inc.started_at)
            inc.status, inc.resolved_at = IncidentStatus.RESOLVED, utcnow()
            inc.title = f"[merged into {parent.incident_id}] {inc.title}"
            self.repo.save_incident(inc)
            self.repo.save_incident(parent)
        return keep

    def analyze_incident(self, incident_id: str, force_ai: bool = False) -> dict[str, Any] | None:
        inc = self.repo.get_incident(incident_id)
        if not inc:
            return None
        rca = self.rca.analyze(inc)
        anomalies = [a.to_dict() for a in self.repo.list_anomalies(limit=200) if a.anomaly_id in inc.anomaly_ids]
        inc.root_cause = {k: rca[k] for k in ("primary", "primary_confidence", "ranked_services", "hypotheses",
                                             "impacted_services", "similar_incidents")}
        inc.signature = rca["signature"]
        inc.summary = self.summarizer.summarize(inc, rca, anomalies)
        if inc.status == IncidentStatus.OPEN:
            inc.status = IncidentStatus.INVESTIGATING
        self.repo.save_incident(inc)
        return {"incident": inc.to_dict(), "rca": rca, "anomalies": anomalies}

    # ── read models for dashboards ──────────────────────────────────────────
    def service_health(self, lookback_sec: int = 900) -> list[ServiceHealth]:
        now = utcnow()
        events = list(self.index.events_since(now - timedelta(seconds=lookback_sec), now))
        scores = self.detector.service_scores(events, now)
        open_inc = self.repo.list_incidents(status="open") + self.repo.list_incidents(status="investigating")
        out = []
        for svc in sorted({e.service for e in events}):
            sc = scores.get(svc, {})
            hs = sc.get("health_score", 0.0)
            status = "critical" if hs >= 0.7 else "degraded" if hs >= 0.35 else "healthy"
            n_inc = sum(1 for i in open_inc if svc in i.services)
            if n_inc and status == "healthy":
                status = "degraded"
            out.append(ServiceHealth(svc, status, sc.get("error_rate", 0.0), sc.get("event_count", 0),
                                     sc.get("avg_latency_ms"), hs, n_inc))
        return out

    def overview(self) -> dict[str, Any]:
        now = utcnow()
        health = self.service_health()
        stats = self.repo.stats() if hasattr(self.repo, "stats") else {}
        recent_alerts = self.repo.list_alerts(since=now - timedelta(hours=24), limit=500)
        return {
            "generated_at": now.isoformat(),
            "total_events": self.index.count(),
            "services": len(health),
            "healthy": sum(1 for h in health if h.status == "healthy"),
            "degraded": sum(1 for h in health if h.status == "degraded"),
            "critical": sum(1 for h in health if h.status == "critical"),
            "open_incidents": stats.get("incidents_open", 0),
            "total_incidents": stats.get("incidents_total", 0),
            "anomalies_total": stats.get("anomalies", 0),
            "alerts_24h": len(recent_alerts),
            "collector": self.collector.stats.snapshot(),
            "pipeline": self.processor.metrics(),
            "last_detection_at": self.last_detection_at.isoformat() if self.last_detection_at else None,
            "backends": {"event_bus": self.s.event_bus, "log_index": self.s.log_index},
        }

    def dependency_graph(self) -> dict[str, Any]:
        g = self.mapper.load()
        health = {h.service: h.status for h in self.service_health()}
        d = g.to_dict(significant_only=True)
        d["nodes"] = [{"id": n, "status": health.get(n, "unknown")} for n in sorted(set(d["nodes"]) | set(health))]
        return d

    def error_trends(self, hours: int = 6, bucket_sec: int = 300) -> list[dict[str, Any]]:
        return self.index.timeseries(utcnow() - timedelta(hours=hours), bucket_sec)

    def incident_timeline(self, limit: int = 50) -> list[dict[str, Any]]:
        out = []
        for inc in self.repo.list_incidents(limit=limit):
            d = inc.to_dict()
            d["anomalies"] = [a.to_dict() for a in self.repo.list_anomalies(limit=500) if a.incident_id == inc.incident_id]
            out.append(d)
        return out

    # ── background loop (single-process mode) ───────────────────────────────
    def start_background(self, interval_sec: int | None = None) -> threading.Thread:
        interval = interval_sec or self.s.detector_interval_sec
        stop = threading.Event()
        self._stop = stop

        def loop():
            while not stop.is_set():
                try:
                    self.process_pending()
                    self.run_detection()
                except Exception:                       # noqa: BLE001
                    log.exception("background cycle failed")
                stop.wait(interval)

        t = threading.Thread(target=loop, name="observability-bg", daemon=True)
        t.start()
        return t

    def stop_background(self) -> None:
        if getattr(self, "_stop", None):
            self._stop.set()


_platform: ObservabilityPlatform | None = None
_platform_lock = threading.Lock()


def get_platform(settings: Settings | None = None) -> ObservabilityPlatform:
    global _platform
    with _platform_lock:
        if _platform is None:
            _platform = ObservabilityPlatform(settings)
        return _platform


def reset_platform() -> None:
    global _platform
    with _platform_lock:
        if _platform:
            _platform.stop_background()
        _platform = None
