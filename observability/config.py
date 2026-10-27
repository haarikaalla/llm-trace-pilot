"""
config.py — 12-factor settings for the observability platform.
Every backend is selectable via environment so the same code runs on a
laptop (memory + SQLite) and in production (Kafka + OpenSearch).
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from functools import lru_cache


def _env_bool(key: str, default: bool) -> bool:
    val = os.environ.get(key)
    if val is None:
        return default
    return val.strip().lower() in ("1", "true", "yes", "on")


def _env_int(key: str, default: int) -> int:
    try:
        return int(os.environ.get(key, default))
    except ValueError:
        return default


def _env_float(key: str, default: float) -> float:
    try:
        return float(os.environ.get(key, default))
    except ValueError:
        return default


@dataclass(frozen=True)
class Settings:
    # ── backends ────────────────────────────────────────────────────────────
    event_bus: str = field(default_factory=lambda: os.environ.get("EVENT_BUS", "memory"))
    log_index: str = field(default_factory=lambda: os.environ.get("LOG_INDEX", "sqlite"))
    db_path: str = field(default_factory=lambda: os.environ.get(
        "OBSERVABILITY_DB", os.environ.get("AGENTSCOPE_DB", "agentscope.db")))

    # ── kafka ───────────────────────────────────────────────────────────────
    kafka_bootstrap: str = field(default_factory=lambda: os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092"))
    kafka_group_id: str = field(default_factory=lambda: os.environ.get("KAFKA_GROUP_ID", "agentscope"))
    kafka_linger_ms: int = field(default_factory=lambda: _env_int("KAFKA_LINGER_MS", 20))
    kafka_batch_size: int = field(default_factory=lambda: _env_int("KAFKA_BATCH_SIZE", 64 * 1024))
    kafka_compression: str = field(default_factory=lambda: os.environ.get("KAFKA_COMPRESSION", "gzip"))

    # ── opensearch ──────────────────────────────────────────────────────────
    opensearch_url: str = field(default_factory=lambda: os.environ.get("OPENSEARCH_URL", "http://localhost:9200"))
    opensearch_user: str = field(default_factory=lambda: os.environ.get("OPENSEARCH_USER", ""))
    opensearch_password: str = field(default_factory=lambda: os.environ.get("OPENSEARCH_PASSWORD", ""))
    opensearch_index_prefix: str = field(default_factory=lambda: os.environ.get("OPENSEARCH_INDEX_PREFIX", "logs"))
    opensearch_verify_certs: bool = field(default_factory=lambda: _env_bool("OPENSEARCH_VERIFY_CERTS", True))

    # ── pipeline / throughput ───────────────────────────────────────────────
    batch_size: int = field(default_factory=lambda: _env_int("PIPELINE_BATCH_SIZE", 500))
    batch_flush_ms: int = field(default_factory=lambda: _env_int("PIPELINE_FLUSH_MS", 250))
    max_message_bytes: int = field(default_factory=lambda: _env_int("MAX_MESSAGE_BYTES", 32 * 1024))
    redact_pii: bool = field(default_factory=lambda: _env_bool("REDACT_PII", True))

    # ── detection ───────────────────────────────────────────────────────────
    detection_window_sec: int = field(default_factory=lambda: _env_int("DETECTION_WINDOW_SEC", 60))
    detection_history_windows: int = field(default_factory=lambda: _env_int("DETECTION_HISTORY_WINDOWS", 30))
    anomaly_threshold: float = field(default_factory=lambda: _env_float("ANOMALY_THRESHOLD", 0.7))
    detector_interval_sec: int = field(default_factory=lambda: _env_int("DETECTOR_INTERVAL_SEC", 30))

    # ── alerting ────────────────────────────────────────────────────────────
    alert_webhook_url: str = field(default_factory=lambda: os.environ.get("ALERT_WEBHOOK_URL", ""))
    alert_dedup_sec: int = field(default_factory=lambda: _env_int("ALERT_DEDUP_SEC", 300))
    incident_group_window_sec: int = field(default_factory=lambda: _env_int("INCIDENT_GROUP_WINDOW_SEC", 600))

    # ── AI ──────────────────────────────────────────────────────────────────
    ollama_url: str = field(default_factory=lambda: os.environ.get("OLLAMA_URL", "http://localhost:11434/api/generate"))
    summary_model: str = field(default_factory=lambda: os.environ.get("SUMMARY_MODEL", "llama3.2"))
    ai_summaries_enabled: bool = field(default_factory=lambda: _env_bool("AI_SUMMARIES", True))

    # ── service ─────────────────────────────────────────────────────────────
    service_name: str = field(default_factory=lambda: os.environ.get("SERVICE_NAME", "agentscope"))
    ingest_api_key: str = field(default_factory=lambda: os.environ.get("INGEST_API_KEY", ""))


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()


def reset_settings_cache() -> None:
    """Used by tests after mutating environment variables."""
    get_settings.cache_clear()
