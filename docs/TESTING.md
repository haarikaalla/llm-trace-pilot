# Testing Strategy

## Pyramid

| Layer | Location | Infra needed | What it proves |
|---|---|---|---|
| **Unit** | [tests/test_pipeline.py](../tests/test_pipeline.py), [tests/test_ml.py](../tests/test_ml.py) | none | parsers per source, each ETL stage, redaction, fingerprints, every detector on synthetic windows |
| **Component** | [tests/test_platform.py](../tests/test_platform.py) | none (memory bus, temp SQLite) | bus semantics, index/repo round-trips, dependency inference, correlation, historical matching, alert rules/dedup/merge/auto-resolve, tracer bridge |
| **End-to-end (in-process)** | `test_platform_ingest_detect_incident_summary` | none | raw multi-source records → pipeline → index → detectors → alerts → incident → RCA (`primary == db-main`) → summary → health/overview/graph/timeline read models |
| **HTTP contract** | [tests/test_api.py](../tests/test_api.py) | none (FastAPI TestClient) | ingestion endpoints, API-key enforcement, validation (422), incident API, dashboard pages, status transitions |
| **Regression** | [tests/test_tracer.py](../tests/test_tracer.py) | none | original AgentScope behaviour unchanged |
| **Simulation** | [examples/simulate_logs.py](../examples/simulate_logs.py) (run in CI) | none | realistic 8-service topology with injected DB outage; prints anomalies, incident, root cause, summary and single-process throughput |
| **Build/manifests** | CI jobs `docker`, `k8s-manifests` | Docker, kustomize | image builds; Kustomize renders |

```bash
python -m pytest tests/ -v                 # 76 tests, ~3 s
python examples/simulate_logs.py           # e2e scenario + throughput figure
```

## Isolation rules

* `tests/conftest.py::obs_env` gives each test its own SQLite file, resets the
  settings cache, the in-memory bus singleton and the platform singleton.
  No test touches `agentscope.db`.
* `AI_SUMMARIES=false` in tests → deterministic summariser; Ollama is never
  called. The LLM path is covered by its fallback contract (any failure →
  deterministic output with the same schema).
* Detector tests use fixed timestamps (`T0`) and synthetic `WindowFeatures`,
  so scores are reproducible across machines.

## What is intentionally *not* unit-tested here

* `KafkaBus` and `OpenSearchLogIndex` wrap third-party clients with lazy
  imports. They are exercised by the Compose stack (`docker compose up` then
  `SERVICE_ROLE=simulate`) and should be covered by an integration job using
  Testcontainers when a Docker-enabled runner is available:

  ```python
  # sketch — tests/integration/test_kafka_opensearch.py
  with KafkaContainer() as kafka, OpenSearchContainer() as os_:
      monkeypatch.setenv("EVENT_BUS", "kafka"); monkeypatch.setenv("KAFKA_BOOTSTRAP", kafka.get_bootstrap_server())
      monkeypatch.setenv("LOG_INDEX", "opensearch"); monkeypatch.setenv("OPENSEARCH_URL", os_.get_url())
      platform = ObservabilityPlatform(inline=False)
      platform.ingest(records); run processor_worker one batch; assert platform.index.count() == len(records)
  ```

## Load / throughput testing

```bash
python examples/simulate_logs.py --minutes 60 --rate 400      # ~200k events in one process
```

For the distributed path, run Compose and drive `/v1/ingest/bulk` with `hey`
or `k6`; watch Kafka consumer lag and `agentscope_pipeline_processed_total`.
Target SLOs: p99 ingest latency < 50 ms at 5k events/s; processor lag < 10 s.

## Detector quality

* Precision guard rails are encoded as tests: calm traffic yields **zero**
  anomalies (`test_engine_detects_spike_and_nothing_on_calm`), partial windows
  never fire "volume down", single latency blips are not degradation.
* For production tuning, label historical incidents and call
  `IncidentPredictor.fit(samples)`; compare `ANOMALY_THRESHOLD` against alert
  volume per service per day. A `/api/obs/anomalies` export plus incident
  statuses gives the labelled set.

## Security tests

* PII/secret redaction (`test_enrichment_redacts_pii`)
* API-key enforcement (`test_ingest_api_key_enforced`)
* Payload validation and limits (`422` on empty batch, DLQ for oversized/malformed)
* Webhook URL scheme validation (`WebhookNotifier`)
