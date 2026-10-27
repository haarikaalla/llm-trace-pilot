"""
processor.py — Chains the ETL stages and produces LogEvent entities.
Batch-oriented for throughput; a single bad record never fails a batch.
"""
from __future__ import annotations

import logging
from typing import Any, Iterable

from ..config import Settings, get_settings
from ..domain.models import LogEvent
from .stages import CleaningStage, ParsingStage, NormalizationStage, EnrichmentStage

log = logging.getLogger(__name__)


def fingerprint_message(service: str, message: str) -> str:
    import hashlib
    return hashlib.sha1(f"{service}|{EnrichmentStage.template_of(message)}".encode()).hexdigest()[:16]


class LogProcessor:
    def __init__(self, settings: Settings | None = None):
        self.s = settings or get_settings()
        self.stages = [CleaningStage(self.s), ParsingStage(), NormalizationStage(), EnrichmentStage(self.s)]
        self.processed = 0
        self.dropped = 0
        self.failed = 0

    def process_one(self, raw: dict[str, Any]) -> LogEvent | None:
        rec: dict[str, Any] | None = dict(raw)
        for stage in self.stages:
            rec = stage(rec)
            if rec is None:
                self.dropped += 1
                return None
        rec.pop("received_at", None)
        known = LogEvent.__dataclass_fields__
        attrs = rec.get("attributes") or {}
        for k in list(rec.keys()):
            if k not in known:
                attrs[k] = rec.pop(k)
        rec["attributes"] = attrs
        self.processed += 1
        return LogEvent(**rec)

    def process_batch(self, raws: Iterable[dict[str, Any]]) -> tuple[list[LogEvent], list[dict[str, Any]]]:
        """Returns (events, dead_letters)."""
        events: list[LogEvent] = []
        dlq: list[dict[str, Any]] = []
        for raw in raws:
            try:
                ev = self.process_one(raw)
                if ev is not None:
                    events.append(ev)
            except Exception as e:                       # noqa: BLE001 — isolate poison records
                self.failed += 1
                dlq.append({"error": f"{type(e).__name__}: {e}", "record": str(raw)[:1000]})
                log.debug("pipeline failure", exc_info=True)
        return events, dlq

    def metrics(self) -> dict[str, int]:
        return {"processed": self.processed, "dropped": self.dropped, "failed": self.failed}
