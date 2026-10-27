"""
processor_worker.py — Stream processor microservice.

Consumes logs.raw from Kafka, runs the ETL pipeline, bulk-indexes into
OpenSearch and republishes to logs.processed. Horizontally scalable: each
replica joins the same consumer group and owns a subset of partitions.

Run:  python -m observability.services.processor_worker
"""
from __future__ import annotations

import logging
import os
import signal
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from observability.config import get_settings
from observability.pipeline import LogProcessor
from observability.storage import build_log_index
from observability.streaming import Topics, build_event_bus

log = logging.getLogger("processor-worker")


def main() -> None:
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(name)s %(message)s")
    s = get_settings()
    bus, index, proc = build_event_bus(s), build_log_index(s), LogProcessor(s)
    index.ensure_ready()
    running = True

    def stop(*_):
        nonlocal running
        running = False
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    log.info("processor started bus=%s index=%s batch=%d", s.event_bus, s.log_index, s.batch_size)
    last_report, indexed = time.time(), 0
    while running:
        raws = bus.consume(Topics.LOGS_RAW, max_records=s.batch_size, timeout_ms=s.batch_flush_ms)
        if not raws:
            time.sleep(0.05)
            continue
        events, dlq = proc.process_batch(raws)
        for d in dlq:
            bus.publish(Topics.DLQ, d)
        if events:
            indexed += index.index_bulk(events)
            for e in events:
                bus.publish(Topics.LOGS_PROCESSED, e.to_dict(), key=e.service)
        if time.time() - last_report > 30:
            log.info("indexed=%d metrics=%s", indexed, proc.metrics())
            last_report = time.time()
    bus.close()
    log.info("processor stopped; total indexed=%d", indexed)


if __name__ == "__main__":
    main()
