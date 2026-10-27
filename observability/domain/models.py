"""
models.py — Domain entities. Pure data, no I/O, no framework imports.
"""
from __future__ import annotations

import uuid
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from enum import Enum
from typing import Any


# ── time helpers ────────────────────────────────────────────────────────────
def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def to_iso(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def parse_ts(value: Any) -> datetime | None:
    """Best-effort timestamp parser: epoch (s/ms), ISO-8601, syslog."""
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, (int, float)):
        ts = float(value)
        if ts > 1e12:          # milliseconds
            ts /= 1000.0
        return datetime.fromtimestamp(ts, tz=timezone.utc)
    s = str(value).strip()
    if s.isdigit():
        return parse_ts(int(s))
    try:
        iso = s.replace("Z", "+00:00")
        if " " in iso and "T" not in iso:
            iso = iso.replace(" ", "T", 1)
        dt = datetime.fromisoformat(iso)
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except ValueError:
        pass
    for fmt in ("%Y %b %d %H:%M:%S", "%d/%b/%Y:%H:%M:%S %z", "%Y/%m/%d %H:%M:%S", "%Y-%m-%d %H:%M:%S,%f"):
        try:
            # RFC3164 syslog has no year: assume the current one
            text = f"{utcnow().year} {s}" if fmt.startswith("%Y %b") else s
            dt = datetime.strptime(text, fmt)
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


# ── enums ───────────────────────────────────────────────────────────────────
class SourceType(str, Enum):
    APPLICATION = "application"
    KUBERNETES = "kubernetes"
    INFRASTRUCTURE = "infrastructure"
    CICD = "cicd"
    LLM_TRACE = "llm_trace"
    UNKNOWN = "unknown"


class Severity(str, Enum):
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARN = "WARN"
    ERROR = "ERROR"
    FATAL = "FATAL"

    @property
    def rank(self) -> int:
        return {"DEBUG": 0, "INFO": 1, "WARN": 2, "ERROR": 3, "FATAL": 4}[self.value]

    @property
    def is_error(self) -> bool:
        return self.rank >= 3


class AnomalyKind(str, Enum):
    ERROR_SPIKE = "error_spike"
    LATENCY_DEGRADATION = "latency_degradation"
    VOLUME_ANOMALY = "volume_anomaly"
    NOVEL_PATTERN = "novel_pattern"
    PATTERN_BURST = "pattern_burst"
    INCIDENT_PREDICTION = "incident_prediction"
    SERVICE_DEGRADATION = "service_degradation"


class IncidentStatus(str, Enum):
    OPEN = "open"
    INVESTIGATING = "investigating"
    MITIGATED = "mitigated"
    RESOLVED = "resolved"


# ── entities ────────────────────────────────────────────────────────────────
@dataclass
class LogEvent:
    """A normalized, enriched log record — the canonical unit of the platform."""
    message: str
    service: str = "unknown"
    level: Severity = Severity.INFO
    source_type: SourceType = SourceType.UNKNOWN
    timestamp: datetime = field(default_factory=utcnow)
    event_id: str = field(default_factory=lambda: uuid.uuid4().hex)
    environment: str = "default"
    host: str | None = None
    namespace: str | None = None
    pod: str | None = None
    container: str | None = None
    trace_id: str | None = None
    span_id: str | None = None
    pipeline: str | None = None
    job: str | None = None
    stage: str | None = None
    status_code: int | None = None
    latency_ms: float | None = None
    error_type: str | None = None
    fingerprint: str | None = None
    template: str | None = None
    severity_score: float = 0.0
    attributes: dict[str, Any] = field(default_factory=dict)
    raw: str | None = None

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["timestamp"] = to_iso(self.timestamp)
        d["level"] = self.level.value
        d["source_type"] = self.source_type.value
        return d

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "LogEvent":
        d = dict(d)
        d["timestamp"] = parse_ts(d.get("timestamp")) or utcnow()
        d["level"] = Severity(d.get("level", "INFO"))
        d["source_type"] = SourceType(d.get("source_type", "unknown"))
        d.setdefault("message", "")
        known = {f for f in cls.__dataclass_fields__}
        extra = {k: v for k, v in d.items() if k not in known}
        d = {k: v for k, v in d.items() if k in known}
        if extra:
            d.setdefault("attributes", {}).update(extra)
        return cls(**d)


@dataclass
class Anomaly:
    service: str
    kind: AnomalyKind
    score: float                       # 0..1 normalised anomaly score
    severity: Severity
    description: str
    window_start: datetime
    window_end: datetime
    anomaly_id: str = field(default_factory=lambda: uuid.uuid4().hex[:12])
    detected_at: datetime = field(default_factory=utcnow)
    evidence: dict[str, Any] = field(default_factory=dict)
    incident_id: str | None = None

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d.update({
            "kind": self.kind.value, "severity": self.severity.value,
            "window_start": to_iso(self.window_start), "window_end": to_iso(self.window_end),
            "detected_at": to_iso(self.detected_at),
        })
        return d


@dataclass
class Incident:
    title: str
    severity: Severity
    services: list[str]
    incident_id: str = field(default_factory=lambda: "INC-" + uuid.uuid4().hex[:8].upper())
    status: IncidentStatus = IncidentStatus.OPEN
    started_at: datetime = field(default_factory=utcnow)
    updated_at: datetime = field(default_factory=utcnow)
    resolved_at: datetime | None = None
    anomaly_ids: list[str] = field(default_factory=list)
    root_cause: dict[str, Any] | None = None
    summary: dict[str, Any] | None = None
    signature: dict[str, Any] = field(default_factory=dict)   # for historical similarity

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d.update({
            "severity": self.severity.value, "status": self.status.value,
            "started_at": to_iso(self.started_at), "updated_at": to_iso(self.updated_at),
            "resolved_at": to_iso(self.resolved_at) if self.resolved_at else None,
        })
        return d


@dataclass
class Alert:
    title: str
    severity: Severity
    service: str
    message: str
    alert_id: str = field(default_factory=lambda: uuid.uuid4().hex[:12])
    fired_at: datetime = field(default_factory=utcnow)
    anomaly_id: str | None = None
    incident_id: str | None = None
    dedup_key: str = ""
    labels: dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d.update({"severity": self.severity.value, "fired_at": to_iso(self.fired_at)})
        return d


@dataclass
class ServiceHealth:
    service: str
    status: str                 # healthy | degraded | critical
    error_rate: float
    event_count: int
    avg_latency_ms: float | None
    anomaly_score: float
    open_incidents: int

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class DependencyEdge:
    source: str                 # caller
    target: str                 # callee
    calls: int = 1
    error_calls: int = 0
    avg_latency_ms: float | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
