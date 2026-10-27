"""Kafka topic names shared by all services."""


class Topics:
    LOGS_RAW = "logs.raw"               # ingestion-api  -> processor-worker
    LOGS_PROCESSED = "logs.processed"   # processor      -> detector-worker / indexers
    ANOMALIES = "observability.anomalies"
    ALERTS = "observability.alerts"
    INCIDENTS = "observability.incidents"
    DLQ = "logs.dlq"                    # unparseable / oversized records

    ALL = (LOGS_RAW, LOGS_PROCESSED, ANOMALIES, ALERTS, INCIDENTS, DLQ)
