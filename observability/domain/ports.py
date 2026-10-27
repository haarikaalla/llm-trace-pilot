"""
ports.py — Abstract interfaces the application core depends on.
Adapters (Kafka, OpenSearch, SQLite, webhooks) implement these.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Callable, Iterable, Iterator

from .models import LogEvent, Anomaly, Incident, Alert, DependencyEdge


class EventBus(ABC):
    """Publish/consume JSON-serialisable records on named topics."""

    @abstractmethod
    def publish(self, topic: str, record: dict[str, Any], key: str | None = None) -> None: ...

    def publish_many(self, topic: str, records: Iterable[dict[str, Any]]) -> int:
        n = 0
        for r in records:
            self.publish(topic, r)
            n += 1
        return n

    @abstractmethod
    def consume(self, topic: str, max_records: int = 500, timeout_ms: int = 500) -> list[dict[str, Any]]:
        """Pull up to `max_records` records; returns [] when nothing is available."""

    @abstractmethod
    def subscribe(self, topic: str, handler: Callable[[dict[str, Any]], None]) -> None:
        """Register a push-style handler (used in in-process mode)."""

    def flush(self) -> None: ...

    def close(self) -> None: ...


class LogIndex(ABC):
    """Indexed, searchable log store."""

    @abstractmethod
    def index_bulk(self, events: Iterable[LogEvent]) -> int: ...

    @abstractmethod
    def search(self, query: str | None = None, service: str | None = None,
               level: str | None = None, source_type: str | None = None,
               since: datetime | None = None, until: datetime | None = None,
               trace_id: str | None = None, limit: int = 100) -> list[dict[str, Any]]: ...

    @abstractmethod
    def events_since(self, since: datetime, until: datetime | None = None,
                     limit: int = 50_000) -> Iterator[LogEvent]: ...

    @abstractmethod
    def timeseries(self, since: datetime, bucket_sec: int = 60,
                   service: str | None = None) -> list[dict[str, Any]]:
        """[{bucket, service, count, errors, avg_latency_ms}] ordered by bucket."""

    @abstractmethod
    def services(self, since: datetime | None = None) -> list[str]: ...

    @abstractmethod
    def count(self) -> int: ...

    def ensure_ready(self) -> None: ...


class IncidentRepository(ABC):
    @abstractmethod
    def save_anomaly(self, a: Anomaly) -> None: ...
    @abstractmethod
    def list_anomalies(self, since: datetime | None = None, service: str | None = None,
                       limit: int = 200) -> list[Anomaly]: ...
    @abstractmethod
    def save_incident(self, inc: Incident) -> None: ...
    @abstractmethod
    def get_incident(self, incident_id: str) -> Incident | None: ...
    @abstractmethod
    def list_incidents(self, status: str | None = None, limit: int = 100) -> list[Incident]: ...
    @abstractmethod
    def find_open_incident(self, service: str, within_sec: int) -> Incident | None: ...
    @abstractmethod
    def save_alert(self, alert: Alert) -> None: ...
    @abstractmethod
    def list_alerts(self, since: datetime | None = None, limit: int = 200) -> list[Alert]: ...
    @abstractmethod
    def last_alert_time(self, dedup_key: str) -> datetime | None: ...
    @abstractmethod
    def upsert_dependency(self, edge: DependencyEdge) -> None: ...
    @abstractmethod
    def list_dependencies(self) -> list[DependencyEdge]: ...


class Notifier(ABC):
    @abstractmethod
    def notify(self, alert: Alert) -> bool: ...
