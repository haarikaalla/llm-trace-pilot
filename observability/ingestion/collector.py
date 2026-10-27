"""
collector.py — Accepts raw records from any shipper (HTTP, Fluent Bit, Vector,
GitHub webhook, direct SDK) and publishes them to the raw-logs topic.

Throughput design
  * validation is O(1) per record (size + type only; parsing happens downstream)
  * records are keyed by service so one service's burst stays on one partition
  * oversized / malformed records are routed to the DLQ, never dropped silently
"""
from __future__ import annotations

import json
import logging
import threading
import time
from typing import Any, Iterable

from ..config import Settings, get_settings
from ..domain.ports import EventBus
from ..streaming.topics import Topics

log = logging.getLogger(__name__)


class CollectorStats:
    def __init__(self):
        self.accepted = 0
        self.rejected = 0
        self.bytes = 0
        self.batches = 0
        self.started = time.time()
        self._lock = threading.Lock()

    def add(self, accepted: int, rejected: int, nbytes: int) -> None:
        with self._lock:
            self.accepted += accepted
            self.rejected += rejected
            self.bytes += nbytes
            self.batches += 1

    def snapshot(self) -> dict[str, Any]:
        up = max(time.time() - self.started, 1e-6)
        return {
            "accepted": self.accepted, "rejected": self.rejected, "bytes": self.bytes,
            "batches": self.batches, "uptime_sec": round(up, 1),
            "events_per_sec": round(self.accepted / up, 2),
        }


class Collector:
    def __init__(self, bus: EventBus, settings: Settings | None = None):
        self.bus = bus
        self.s = settings or get_settings()
        self.stats = CollectorStats()

    def _validate(self, rec: Any) -> tuple[dict | None, str | None]:
        if isinstance(rec, str):
            rec = {"message": rec}
        if not isinstance(rec, dict):
            return None, "record must be an object or string"
        if not (rec.get("message") or rec.get("log") or rec.get("msg")):
            return None, "record has no message/log field"
        size = len(json.dumps(rec, default=str))
        if size > self.s.max_message_bytes:
            return None, f"record exceeds {self.s.max_message_bytes} bytes"
        return rec, None

    def ingest(self, records: Iterable[Any], source_type: str | None = None,
               defaults: dict[str, Any] | None = None) -> dict[str, Any]:
        accepted = rejected = nbytes = 0
        received_at = time.time()
        for rec in records:
            rec, err = self._validate(rec)
            if err:
                rejected += 1
                self.bus.publish(Topics.DLQ, {"error": err, "record": str(rec)[:1000], "ts": received_at})
                continue
            if source_type and "source_type" not in rec:
                rec["source_type"] = source_type
            if defaults:
                for k, v in defaults.items():
                    rec.setdefault(k, v)
            rec.setdefault("received_at", received_at)
            nbytes += len(rec.get("message") or rec.get("log") or rec.get("msg") or "")
            self.bus.publish(Topics.LOGS_RAW, rec, key=str(rec.get("service") or ""))
            accepted += 1
        self.stats.add(accepted, rejected, nbytes)
        return {"accepted": accepted, "rejected": rejected}

    def ingest_text(self, text: str, source_type: str | None = None,
                    defaults: dict[str, Any] | None = None) -> dict[str, Any]:
        """Newline-delimited plain text or NDJSON."""
        lines = (ln for ln in text.splitlines() if ln.strip())
        return self.ingest(lines, source_type=source_type, defaults=defaults)
