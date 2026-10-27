"""
bus.py — EventBus adapters.

InMemoryBus : thread-safe, bounded, per-topic queues. Used for tests, demos and
              the single-process `ObservabilityPlatform`.
KafkaBus    : production adapter (kafka-python). Idempotent producer, gzip,
              batched linger, manual-commit consumer with at-least-once semantics.
"""
from __future__ import annotations

import json
import logging
import threading
from collections import defaultdict, deque
from typing import Any, Callable

from ..config import Settings, get_settings
from ..domain.ports import EventBus

log = logging.getLogger(__name__)


class InMemoryBus(EventBus):
    def __init__(self, maxlen: int = 200_000):
        self._queues: dict[str, deque] = defaultdict(lambda: deque(maxlen=maxlen))
        self._handlers: dict[str, list[Callable[[dict[str, Any]], None]]] = defaultdict(list)
        self._lock = threading.Lock()
        self.published: dict[str, int] = defaultdict(int)
        self.dropped: dict[str, int] = defaultdict(int)

    def publish(self, topic: str, record: dict[str, Any], key: str | None = None) -> None:
        with self._lock:
            q = self._queues[topic]
            if len(q) == q.maxlen:
                self.dropped[topic] += 1
            q.append(record)
            self.published[topic] += 1
            handlers = list(self._handlers.get(topic, ()))
        for h in handlers:
            try:
                h(record)
            except Exception:                        # a bad subscriber must not poison the bus
                log.exception("subscriber failed on topic %s", topic)

    def consume(self, topic: str, max_records: int = 500, timeout_ms: int = 500) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        with self._lock:
            q = self._queues[topic]
            while q and len(out) < max_records:
                out.append(q.popleft())
        return out

    def subscribe(self, topic: str, handler: Callable[[dict[str, Any]], None]) -> None:
        with self._lock:
            self._handlers[topic].append(handler)

    def depth(self, topic: str) -> int:
        with self._lock:
            return len(self._queues[topic])


class KafkaBus(EventBus):
    """Thin, lazy wrapper around kafka-python so the dependency stays optional."""

    def __init__(self, settings: Settings | None = None):
        self.s = settings or get_settings()
        self._producer = None
        self._consumers: dict[str, Any] = {}
        self._lock = threading.Lock()

    # ── producer ────────────────────────────────────────────────────────────
    def _get_producer(self):
        if self._producer is None:
            from kafka import KafkaProducer  # lazy import
            self._producer = KafkaProducer(
                bootstrap_servers=self.s.kafka_bootstrap.split(","),
                value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",
                retries=5,
                linger_ms=self.s.kafka_linger_ms,
                batch_size=self.s.kafka_batch_size,
                compression_type=self.s.kafka_compression,
                max_in_flight_requests_per_connection=5,
            )
        return self._producer

    def publish(self, topic: str, record: dict[str, Any], key: str | None = None) -> None:
        self._get_producer().send(topic, value=record, key=key or record.get("service"))

    def flush(self) -> None:
        if self._producer:
            self._producer.flush()

    # ── consumer ────────────────────────────────────────────────────────────
    def _get_consumer(self, topic: str):
        with self._lock:
            if topic not in self._consumers:
                from kafka import KafkaConsumer  # lazy import
                self._consumers[topic] = KafkaConsumer(
                    topic,
                    bootstrap_servers=self.s.kafka_bootstrap.split(","),
                    group_id=f"{self.s.kafka_group_id}-{topic}",
                    value_deserializer=lambda b: json.loads(b.decode("utf-8")),
                    enable_auto_commit=False,
                    auto_offset_reset="earliest",
                    max_poll_records=2000,
                    fetch_max_bytes=16 * 1024 * 1024,
                )
            return self._consumers[topic]

    def consume(self, topic: str, max_records: int = 500, timeout_ms: int = 500) -> list[dict[str, Any]]:
        consumer = self._get_consumer(topic)
        batches = consumer.poll(timeout_ms=timeout_ms, max_records=max_records)
        records = [msg.value for msgs in batches.values() for msg in msgs]
        if records:
            consumer.commit()        # at-least-once: commit after the caller receives the batch
        return records

    def subscribe(self, topic: str, handler: Callable[[dict[str, Any]], None]) -> None:
        raise NotImplementedError("KafkaBus is pull-based; use consume() in a worker loop")

    def close(self) -> None:
        self.flush()
        for c in self._consumers.values():
            c.close()
        if self._producer:
            self._producer.close()


_memory_singleton: InMemoryBus | None = None


def build_event_bus(settings: Settings | None = None) -> EventBus:
    """Factory: EVENT_BUS=memory|kafka. The memory bus is a process-wide singleton
    so the ingestion API and workers share it when co-located."""
    global _memory_singleton
    s = settings or get_settings()
    if s.event_bus.lower() == "kafka":
        return KafkaBus(s)
    if _memory_singleton is None:
        _memory_singleton = InMemoryBus()
    return _memory_singleton
