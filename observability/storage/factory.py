"""factory.py — pick storage adapters from Settings."""
from __future__ import annotations

from ..config import Settings, get_settings
from ..domain.ports import LogIndex, IncidentRepository
from .sqlite_index import SQLiteLogIndex
from .opensearch_index import OpenSearchLogIndex
from .incidents import SQLiteIncidentRepository


def build_log_index(settings: Settings | None = None) -> LogIndex:
    s = settings or get_settings()
    if s.log_index.lower() in ("opensearch", "elasticsearch"):
        return OpenSearchLogIndex(s)
    return SQLiteLogIndex(s.db_path)


def build_incident_repository(settings: Settings | None = None) -> IncidentRepository:
    s = settings or get_settings()
    return SQLiteIncidentRepository(s.db_path)
