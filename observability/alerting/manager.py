"""
manager.py — AlertManager: rules -> dedup/suppression -> notify -> incident grouping.

Incident lifecycle: anomalies for the same service inside `incident_group_window_sec`
are attached to one open incident; new services are merged into it when the
dependency graph links them; incidents auto-resolve when the detector reports
no anomalies for the service for a grace period.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import timedelta
from typing import Callable

from ..config import Settings, get_settings
from ..domain.models import Alert, Anomaly, AnomalyKind, Incident, IncidentStatus, Severity, utcnow
from ..domain.ports import EventBus, IncidentRepository, Notifier
from ..streaming.topics import Topics


@dataclass
class AlertRule:
    name: str
    min_score: float = 0.7
    kinds: set[AnomalyKind] = field(default_factory=set)        # empty = all kinds
    min_severity: Severity = Severity.WARN
    services: set[str] = field(default_factory=set)             # empty = all services
    dedup_sec: int | None = None
    labels: dict[str, str] = field(default_factory=dict)

    def matches(self, a: Anomaly) -> bool:
        if a.score < self.min_score or a.severity.rank < self.min_severity.rank:
            return False
        if self.kinds and a.kind not in self.kinds:
            return False
        if self.services and a.service not in self.services:
            return False
        return True


DEFAULT_RULES = [
    AlertRule("critical-anomaly", min_score=0.85, min_severity=Severity.ERROR, labels={"team": "oncall", "page": "true"}),
    AlertRule("error-spike", kinds={AnomalyKind.ERROR_SPIKE}, min_score=0.7, labels={"team": "oncall"}),
    AlertRule("degradation", kinds={AnomalyKind.LATENCY_DEGRADATION, AnomalyKind.SERVICE_DEGRADATION}, min_score=0.7),
    AlertRule("prediction", kinds={AnomalyKind.INCIDENT_PREDICTION}, min_score=0.75, min_severity=Severity.WARN,
              dedup_sec=900, labels={"type": "early-warning"}),
    AlertRule("patterns", kinds={AnomalyKind.NOVEL_PATTERN, AnomalyKind.PATTERN_BURST, AnomalyKind.VOLUME_ANOMALY}, min_score=0.75),
]


class AlertManager:
    def __init__(self, repo: IncidentRepository, notifiers: list[Notifier], bus: EventBus | None = None,
                 rules: list[AlertRule] | None = None, settings: Settings | None = None,
                 related_services: Callable[[str], set[str]] | None = None):
        self.repo = repo
        self.notifiers = notifiers
        self.bus = bus
        self.rules = rules or list(DEFAULT_RULES)
        self.s = settings or get_settings()
        self.related_services = related_services or (lambda svc: set())
        self.suppressed = 0

    # ── anomaly intake ──────────────────────────────────────────────────────
    def handle(self, anomalies: list[Anomaly]) -> tuple[list[Alert], list[Incident]]:
        alerts: list[Alert] = []
        touched: dict[str, Incident] = {}
        for a in anomalies:
            self.repo.save_anomaly(a)
            if self.bus:
                self.bus.publish(Topics.ANOMALIES, a.to_dict(), key=a.service)
            rule = next((r for r in self.rules if r.matches(a)), None)
            if rule is None:
                continue
            incident = self._attach_to_incident(a, touched)
            alert = self._fire(a, rule, incident)
            if alert:
                alerts.append(alert)
        touched = self._merge_related(touched)
        for inc in touched.values():
            self.repo.save_incident(inc)
            if self.bus:
                self.bus.publish(Topics.INCIDENTS, inc.to_dict(), key=inc.incident_id)
        return alerts, [i for i in touched.values() if i.status != IncidentStatus.RESOLVED]

    def _merge_related(self, touched: dict[str, Incident]) -> dict[str, Incident]:
        """Union incidents whose services are neighbours in the dependency graph."""
        incs = sorted(touched.values(), key=lambda i: i.started_at)
        merged_away: set[str] = set()
        for i, a in enumerate(incs):
            if a.incident_id in merged_away:
                continue
            changed = True
            while changed:
                changed = False
                neighbours = set(a.services)
                for svc in a.services:
                    neighbours |= self.related_services(svc)
                for b in incs[i + 1:]:
                    if b.incident_id in merged_away or not (neighbours & set(b.services)):
                        continue
                    for svc in b.services:
                        if svc not in a.services:
                            a.services.append(svc)
                    a.anomaly_ids += [x for x in b.anomaly_ids if x not in a.anomaly_ids]
                    if b.severity.rank > a.severity.rank:
                        a.severity = b.severity
                    a.started_at = min(a.started_at, b.started_at)
                    b.status, b.resolved_at = IncidentStatus.RESOLVED, utcnow()
                    b.title = f"[merged into {a.incident_id}] {b.title}"
                    merged_away.add(b.incident_id)
                    changed = True
            if len(a.services) > 1:
                a.title = f"{', '.join(a.services[:3])}{'…' if len(a.services) > 3 else ''}: multi-service incident"
        return {i.incident_id: i for i in incs}

    def _attach_to_incident(self, a: Anomaly, touched: dict[str, Incident]) -> Incident | None:
        if a.kind == AnomalyKind.INCIDENT_PREDICTION:
            return None        # early warnings alert but never open incidents
        window = self.s.incident_group_window_sec
        inc = next((i for i in touched.values() if a.service in i.services), None)
        if inc is None:
            inc = self.repo.find_open_incident(a.service, window)
        if inc is None:
            for rel in self.related_services(a.service):
                inc = next((i for i in touched.values() if rel in i.services), None) or self.repo.find_open_incident(rel, window)
                if inc:
                    break
        if inc is None:
            inc = Incident(title=f"{a.service}: {a.kind.value.replace('_', ' ')}", severity=a.severity,
                           services=[a.service], started_at=a.window_start)
        if a.service not in inc.services:
            inc.services.append(a.service)
            inc.title = f"{', '.join(inc.services[:3])}{'…' if len(inc.services) > 3 else ''}: multi-service incident"
        if a.anomaly_id not in inc.anomaly_ids:
            inc.anomaly_ids.append(a.anomaly_id)
        if a.severity.rank > inc.severity.rank:
            inc.severity = a.severity
        if inc.status == IncidentStatus.MITIGATED:
            inc.status = IncidentStatus.OPEN
        a.incident_id = inc.incident_id
        self.repo.save_anomaly(a)
        touched[inc.incident_id] = inc
        return inc

    def _fire(self, a: Anomaly, rule: AlertRule, incident: Incident | None) -> Alert | None:
        dedup_key = f"{rule.name}:{a.service}:{a.kind.value}"
        dedup_sec = rule.dedup_sec or self.s.alert_dedup_sec
        last = self.repo.last_alert_time(dedup_key)
        if last and utcnow() - last < timedelta(seconds=dedup_sec):
            self.suppressed += 1
            return None
        alert = Alert(title=f"[{a.kind.value}] {a.service}", severity=a.severity, service=a.service,
                      message=a.description, anomaly_id=a.anomaly_id,
                      incident_id=incident.incident_id if incident else None, dedup_key=dedup_key,
                      labels={"rule": rule.name, **rule.labels})
        self.repo.save_alert(alert)
        if self.bus:
            self.bus.publish(Topics.ALERTS, alert.to_dict(), key=a.service)
        for n in self.notifiers:
            try:
                n.notify(alert)
            except Exception:                           # noqa: BLE001 — one failing sink must not block others
                pass
        return alert

    # ── lifecycle ───────────────────────────────────────────────────────────
    def auto_resolve(self, quiet_services: set[str], grace_sec: int | None = None) -> list[Incident]:
        """Resolve open incidents whose services have all been quiet for `grace_sec`."""
        grace = timedelta(seconds=grace_sec or self.s.incident_group_window_sec)
        resolved = []
        for inc in self.repo.list_incidents(status=IncidentStatus.OPEN.value) + \
                   self.repo.list_incidents(status=IncidentStatus.INVESTIGATING.value):
            if set(inc.services) <= quiet_services and utcnow() - inc.updated_at > grace:
                inc.status = IncidentStatus.RESOLVED
                inc.resolved_at = utcnow()
                self.repo.save_incident(inc)
                resolved.append(inc)
        return resolved
