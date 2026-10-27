"""
observability — AI-powered observability & incident intelligence layer for AgentScope.

Layering (clean architecture):
    domain/        entities + port interfaces (no I/O)
    streaming/     EventBus adapters (in-memory, Kafka)
    storage/       LogIndex adapters (SQLite, OpenSearch) + incident repository
    ingestion/     source parsers + HTTP collector
    pipeline/      clean -> parse -> normalize -> enrich
    ml/            anomaly / spike / degradation / prediction detectors
    rca/           correlation, dependency graph, historical comparison
    intelligence/  AI incident summaries (Ollama or deterministic fallback)
    alerting/      rules, dedup, notifiers, incident lifecycle
    services/      microservice entrypoints + in-process platform facade
"""
from .config import Settings, get_settings
from .domain.models import LogEvent, Anomaly, Incident, Alert, SourceType, Severity

__all__ = [
    "Settings", "get_settings",
    "LogEvent", "Anomaly", "Incident", "Alert", "SourceType", "Severity",
]
