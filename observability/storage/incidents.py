"""
incidents.py — SQLite repository for anomalies, incidents, alerts and the
service dependency graph. Shares the AgentScope database file by default.
"""
from __future__ import annotations

import json
import sqlite3
import threading
from datetime import datetime, timedelta
from typing import Any

from ..domain.models import (Anomaly, Incident, Alert, DependencyEdge, AnomalyKind,
                             IncidentStatus, Severity, parse_ts, to_iso, utcnow)
from ..domain.ports import IncidentRepository

_SCHEMA = """
CREATE TABLE IF NOT EXISTS anomalies (
    anomaly_id   TEXT PRIMARY KEY,
    service      TEXT NOT NULL,
    kind         TEXT NOT NULL,
    score        REAL NOT NULL,
    severity     TEXT NOT NULL,
    description  TEXT,
    window_start TEXT, window_end TEXT, detected_at TEXT,
    evidence     TEXT DEFAULT '{}',
    incident_id  TEXT
);
CREATE INDEX IF NOT EXISTS idx_anom_detected ON anomalies(detected_at);
CREATE INDEX IF NOT EXISTS idx_anom_service  ON anomalies(service, detected_at);

CREATE TABLE IF NOT EXISTS incidents (
    incident_id  TEXT PRIMARY KEY,
    title        TEXT NOT NULL,
    severity     TEXT NOT NULL,
    status       TEXT NOT NULL,
    services     TEXT DEFAULT '[]',
    started_at   TEXT, updated_at TEXT, resolved_at TEXT,
    anomaly_ids  TEXT DEFAULT '[]',
    root_cause   TEXT, summary TEXT, signature TEXT DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_inc_status ON incidents(status, updated_at);

CREATE TABLE IF NOT EXISTS alerts (
    alert_id    TEXT PRIMARY KEY,
    title       TEXT NOT NULL,
    severity    TEXT NOT NULL,
    service     TEXT NOT NULL,
    message     TEXT,
    fired_at    TEXT,
    anomaly_id  TEXT, incident_id TEXT,
    dedup_key   TEXT,
    labels      TEXT DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_alert_dedup ON alerts(dedup_key, fired_at);
CREATE INDEX IF NOT EXISTS idx_alert_fired ON alerts(fired_at);

CREATE TABLE IF NOT EXISTS service_dependencies (
    source TEXT NOT NULL, target TEXT NOT NULL,
    calls INTEGER DEFAULT 0, error_calls INTEGER DEFAULT 0, avg_latency_ms REAL,
    updated_at TEXT,
    PRIMARY KEY (source, target)
);
"""


class SQLiteIncidentRepository(IncidentRepository):
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._lock = threading.Lock()
        with self._conn() as c:
            c.executescript(_SCHEMA)

    def _conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=30, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        return conn

    # ── anomalies ───────────────────────────────────────────────────────────
    def save_anomaly(self, a: Anomaly) -> None:
        with self._lock, self._conn() as c:
            c.execute("""INSERT OR REPLACE INTO anomalies VALUES (?,?,?,?,?,?,?,?,?,?,?)""", (
                a.anomaly_id, a.service, a.kind.value, a.score, a.severity.value, a.description,
                to_iso(a.window_start), to_iso(a.window_end), to_iso(a.detected_at),
                json.dumps(a.evidence, default=str), a.incident_id))

    def list_anomalies(self, since=None, service=None, limit=200) -> list[Anomaly]:
        sql, params = "SELECT * FROM anomalies WHERE 1=1", []
        if since:
            sql += " AND detected_at >= ?"; params.append(to_iso(since))
        if service:
            sql += " AND service = ?"; params.append(service)
        sql += " ORDER BY detected_at DESC LIMIT ?"; params.append(limit)
        with self._conn() as c:
            return [self._row_anomaly(r) for r in c.execute(sql, params).fetchall()]

    @staticmethod
    def _row_anomaly(r) -> Anomaly:
        return Anomaly(
            anomaly_id=r["anomaly_id"], service=r["service"], kind=AnomalyKind(r["kind"]),
            score=r["score"], severity=Severity(r["severity"]), description=r["description"],
            window_start=parse_ts(r["window_start"]), window_end=parse_ts(r["window_end"]),
            detected_at=parse_ts(r["detected_at"]), evidence=json.loads(r["evidence"] or "{}"),
            incident_id=r["incident_id"])

    # ── incidents ───────────────────────────────────────────────────────────
    def save_incident(self, inc: Incident) -> None:
        inc.updated_at = utcnow()
        with self._lock, self._conn() as c:
            c.execute("""INSERT OR REPLACE INTO incidents VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""", (
                inc.incident_id, inc.title, inc.severity.value, inc.status.value, json.dumps(inc.services),
                to_iso(inc.started_at), to_iso(inc.updated_at), to_iso(inc.resolved_at) if inc.resolved_at else None,
                json.dumps(inc.anomaly_ids), json.dumps(inc.root_cause, default=str) if inc.root_cause else None,
                json.dumps(inc.summary, default=str) if inc.summary else None, json.dumps(inc.signature, default=str)))
            if inc.anomaly_ids:
                c.executemany("UPDATE anomalies SET incident_id=? WHERE anomaly_id=?",
                              [(inc.incident_id, a) for a in inc.anomaly_ids])

    @staticmethod
    def _row_incident(r) -> Incident:
        return Incident(
            incident_id=r["incident_id"], title=r["title"], severity=Severity(r["severity"]),
            status=IncidentStatus(r["status"]), services=json.loads(r["services"] or "[]"),
            started_at=parse_ts(r["started_at"]), updated_at=parse_ts(r["updated_at"]),
            resolved_at=parse_ts(r["resolved_at"]), anomaly_ids=json.loads(r["anomaly_ids"] or "[]"),
            root_cause=json.loads(r["root_cause"]) if r["root_cause"] else None,
            summary=json.loads(r["summary"]) if r["summary"] else None,
            signature=json.loads(r["signature"] or "{}"))

    def get_incident(self, incident_id: str) -> Incident | None:
        with self._conn() as c:
            r = c.execute("SELECT * FROM incidents WHERE incident_id=?", (incident_id,)).fetchone()
            return self._row_incident(r) if r else None

    def list_incidents(self, status=None, limit=100) -> list[Incident]:
        sql, params = "SELECT * FROM incidents", []
        if status:
            sql += " WHERE status = ?"; params.append(status)
        sql += " ORDER BY started_at DESC LIMIT ?"; params.append(limit)
        with self._conn() as c:
            return [self._row_incident(r) for r in c.execute(sql, params).fetchall()]

    def find_open_incident(self, service: str, within_sec: int) -> Incident | None:
        cutoff = to_iso(utcnow() - timedelta(seconds=within_sec))
        with self._conn() as c:
            rows = c.execute(
                "SELECT * FROM incidents WHERE status IN ('open','investigating') AND updated_at >= ? ORDER BY updated_at DESC",
                (cutoff,)).fetchall()
        for r in rows:
            if service in json.loads(r["services"] or "[]"):
                return self._row_incident(r)
        return None

    # ── alerts ──────────────────────────────────────────────────────────────
    def save_alert(self, alert: Alert) -> None:
        with self._lock, self._conn() as c:
            c.execute("INSERT OR REPLACE INTO alerts VALUES (?,?,?,?,?,?,?,?,?,?)", (
                alert.alert_id, alert.title, alert.severity.value, alert.service, alert.message,
                to_iso(alert.fired_at), alert.anomaly_id, alert.incident_id, alert.dedup_key,
                json.dumps(alert.labels)))

    def list_alerts(self, since=None, limit=200) -> list[Alert]:
        sql, params = "SELECT * FROM alerts", []
        if since:
            sql += " WHERE fired_at >= ?"; params.append(to_iso(since))
        sql += " ORDER BY fired_at DESC LIMIT ?"; params.append(limit)
        with self._conn() as c:
            return [Alert(alert_id=r["alert_id"], title=r["title"], severity=Severity(r["severity"]),
                          service=r["service"], message=r["message"], fired_at=parse_ts(r["fired_at"]),
                          anomaly_id=r["anomaly_id"], incident_id=r["incident_id"], dedup_key=r["dedup_key"],
                          labels=json.loads(r["labels"] or "{}"))
                    for r in c.execute(sql, params).fetchall()]

    def last_alert_time(self, dedup_key: str) -> datetime | None:
        with self._conn() as c:
            r = c.execute("SELECT MAX(fired_at) FROM alerts WHERE dedup_key=?", (dedup_key,)).fetchone()
            return parse_ts(r[0]) if r and r[0] else None

    # ── dependencies ────────────────────────────────────────────────────────
    def upsert_dependency(self, edge: DependencyEdge) -> None:
        with self._lock, self._conn() as c:
            c.execute("""
                INSERT INTO service_dependencies (source,target,calls,error_calls,avg_latency_ms,updated_at)
                VALUES (?,?,?,?,?,?)
                ON CONFLICT(source,target) DO UPDATE SET
                    calls = calls + excluded.calls,
                    error_calls = error_calls + excluded.error_calls,
                    avg_latency_ms = COALESCE(
                        (COALESCE(service_dependencies.avg_latency_ms, excluded.avg_latency_ms) + COALESCE(excluded.avg_latency_ms, service_dependencies.avg_latency_ms)) / 2.0,
                        excluded.avg_latency_ms),
                    updated_at = excluded.updated_at
            """, (edge.source, edge.target, edge.calls, edge.error_calls, edge.avg_latency_ms, to_iso(utcnow())))

    def list_dependencies(self) -> list[DependencyEdge]:
        with self._conn() as c:
            return [DependencyEdge(source=r["source"], target=r["target"], calls=r["calls"],
                                   error_calls=r["error_calls"], avg_latency_ms=r["avg_latency_ms"])
                    for r in c.execute("SELECT * FROM service_dependencies").fetchall()]

    # ── maintenance ─────────────────────────────────────────────────────────
    def stats(self) -> dict[str, Any]:
        with self._conn() as c:
            return {
                "anomalies": c.execute("SELECT COUNT(*) FROM anomalies").fetchone()[0],
                "incidents_open": c.execute("SELECT COUNT(*) FROM incidents WHERE status IN ('open','investigating')").fetchone()[0],
                "incidents_total": c.execute("SELECT COUNT(*) FROM incidents").fetchone()[0],
                "alerts": c.execute("SELECT COUNT(*) FROM alerts").fetchone()[0],
            }
