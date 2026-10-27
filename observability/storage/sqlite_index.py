"""
sqlite_index.py — LogIndex adapter on SQLite (WAL mode, bulk executemany).
Suitable for dev, tests, edge deployments and as a local buffer. Handles
~10-20k events/sec on a laptop; production uses OpenSearchLogIndex.
"""
from __future__ import annotations

import json
import sqlite3
import threading
from datetime import datetime, timezone
from typing import Any, Iterable, Iterator

from ..domain.models import LogEvent, to_iso, parse_ts
from ..domain.ports import LogIndex

_SCHEMA = """
CREATE TABLE IF NOT EXISTS log_events (
    event_id       TEXT PRIMARY KEY,
    ts             TEXT NOT NULL,
    service        TEXT NOT NULL,
    environment    TEXT,
    level          TEXT NOT NULL,
    source_type    TEXT NOT NULL,
    message        TEXT,
    template       TEXT,
    fingerprint    TEXT,
    host           TEXT, namespace TEXT, pod TEXT, container TEXT,
    trace_id       TEXT, span_id TEXT,
    pipeline       TEXT, job TEXT, stage TEXT,
    status_code    INTEGER,
    latency_ms     REAL,
    error_type     TEXT,
    severity_score REAL DEFAULT 0,
    attributes     TEXT DEFAULT '{}',
    raw            TEXT
);
CREATE INDEX IF NOT EXISTS idx_le_ts        ON log_events(ts);
CREATE INDEX IF NOT EXISTS idx_le_svc_ts    ON log_events(service, ts);
CREATE INDEX IF NOT EXISTS idx_le_level     ON log_events(level);
CREATE INDEX IF NOT EXISTS idx_le_trace     ON log_events(trace_id);
CREATE INDEX IF NOT EXISTS idx_le_fp        ON log_events(fingerprint);
"""

_COLS = ("event_id", "ts", "service", "environment", "level", "source_type", "message", "template",
         "fingerprint", "host", "namespace", "pod", "container", "trace_id", "span_id", "pipeline",
         "job", "stage", "status_code", "latency_ms", "error_type", "severity_score", "attributes", "raw")


class SQLiteLogIndex(LogIndex):
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._lock = threading.Lock()
        self.ensure_ready()

    def _conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=30, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        return conn

    def ensure_ready(self) -> None:
        with self._conn() as c:
            c.executescript(_SCHEMA)

    def index_bulk(self, events: Iterable[LogEvent]) -> int:
        rows = []
        for e in events:
            rows.append((
                e.event_id, to_iso(e.timestamp), e.service, e.environment, e.level.value,
                e.source_type.value, e.message, e.template, e.fingerprint, e.host, e.namespace,
                e.pod, e.container, e.trace_id, e.span_id, e.pipeline, e.job, e.stage,
                e.status_code, e.latency_ms, e.error_type, e.severity_score,
                json.dumps(e.attributes, default=str), e.raw,
            ))
        if not rows:
            return 0
        placeholders = ",".join("?" * len(_COLS))
        with self._lock, self._conn() as c:
            c.executemany(
                f"INSERT OR IGNORE INTO log_events ({','.join(_COLS)}) VALUES ({placeholders})", rows)
        return len(rows)

    @staticmethod
    def _row_to_dict(r: sqlite3.Row) -> dict[str, Any]:
        d = dict(r)
        d["timestamp"] = d.pop("ts")
        try:
            d["attributes"] = json.loads(d.get("attributes") or "{}")
        except json.JSONDecodeError:
            d["attributes"] = {}
        return d

    def search(self, query=None, service=None, level=None, source_type=None, since=None,
               until=None, trace_id=None, limit=100) -> list[dict[str, Any]]:
        sql, params = "SELECT * FROM log_events WHERE 1=1", []
        if query:
            sql += " AND (message LIKE ? OR template LIKE ?)"
            params += [f"%{query}%", f"%{query}%"]
        if service:
            sql += " AND service = ?"; params.append(service)
        if level:
            sql += " AND level = ?"; params.append(level.upper())
        if source_type:
            sql += " AND source_type = ?"; params.append(source_type)
        if trace_id:
            sql += " AND trace_id = ?"; params.append(trace_id)
        if since:
            sql += " AND ts >= ?"; params.append(to_iso(since))
        if until:
            sql += " AND ts <= ?"; params.append(to_iso(until))
        sql += " ORDER BY ts DESC LIMIT ?"
        params.append(int(limit))
        with self._conn() as c:
            return [self._row_to_dict(r) for r in c.execute(sql, params).fetchall()]

    def events_since(self, since: datetime, until: datetime | None = None, limit: int = 50_000) -> Iterator[LogEvent]:
        sql, params = "SELECT * FROM log_events WHERE ts >= ?", [to_iso(since)]
        if until:
            sql += " AND ts <= ?"; params.append(to_iso(until))
        sql += " ORDER BY ts ASC LIMIT ?"; params.append(int(limit))
        with self._conn() as c:
            for r in c.execute(sql, params):
                yield LogEvent.from_dict(self._row_to_dict(r))

    def timeseries(self, since: datetime, bucket_sec: int = 60, service: str | None = None) -> list[dict[str, Any]]:
        sql = """
            SELECT (CAST(strftime('%s', ts) AS INTEGER) / ?) * ? AS bucket_epoch, service,
                   COUNT(*) AS count,
                   SUM(CASE WHEN level IN ('ERROR','FATAL') THEN 1 ELSE 0 END) AS errors,
                   SUM(CASE WHEN level = 'WARN' THEN 1 ELSE 0 END) AS warnings,
                   AVG(latency_ms) AS avg_latency_ms,
                   MAX(latency_ms) AS max_latency_ms,
                   COUNT(DISTINCT fingerprint) AS unique_patterns
            FROM log_events WHERE ts >= ?"""
        params: list[Any] = [bucket_sec, bucket_sec, to_iso(since)]
        if service:
            sql += " AND service = ?"; params.append(service)
        sql += " GROUP BY bucket_epoch, service ORDER BY bucket_epoch ASC"
        with self._conn() as c:
            out = []
            for r in c.execute(sql, params).fetchall():
                d = dict(r)
                d["bucket"] = to_iso(datetime.fromtimestamp(d.pop("bucket_epoch"), tz=timezone.utc))
                out.append(d)
            return out

    def services(self, since: datetime | None = None) -> list[str]:
        sql, params = "SELECT DISTINCT service FROM log_events", []
        if since:
            sql += " WHERE ts >= ?"; params.append(to_iso(since))
        with self._conn() as c:
            return [r[0] for r in c.execute(sql + " ORDER BY service", params).fetchall()]

    def count(self) -> int:
        with self._conn() as c:
            return c.execute("SELECT COUNT(*) FROM log_events").fetchone()[0]

    def top_patterns(self, since: datetime, service: str | None = None, errors_only=True, limit=20) -> list[dict[str, Any]]:
        sql = "SELECT fingerprint, template, service, level, error_type, COUNT(*) AS count, MIN(ts) AS first_seen, MAX(ts) AS last_seen FROM log_events WHERE ts >= ?"
        params: list[Any] = [to_iso(since)]
        if errors_only:
            sql += " AND level IN ('ERROR','FATAL')"
        if service:
            sql += " AND service = ?"; params.append(service)
        sql += " GROUP BY fingerprint ORDER BY count DESC LIMIT ?"; params.append(limit)
        with self._conn() as c:
            return [dict(r) for r in c.execute(sql, params).fetchall()]

    def purge_older_than(self, cutoff: datetime) -> int:
        with self._lock, self._conn() as c:
            cur = c.execute("DELETE FROM log_events WHERE ts < ?", (to_iso(cutoff),))
            return cur.rowcount
