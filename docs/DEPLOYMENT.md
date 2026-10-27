# Deployment Guide

## 0. Modes

| Mode | Backends | Use for |
|---|---|---|
| **Local** | memory bus + SQLite, everything in the dashboard process | laptops, demos, tests |
| **Compose** | Kafka (KRaft) + OpenSearch + 5 services | staging, load tests, integration |
| **Kubernetes** | Kafka + OpenSearch (or managed), Kustomize manifests, Fluent Bit DaemonSet | production |

All modes run identical code; only environment variables change
(see [.env.example](../.env.example)).

---

## 1. Local (no infrastructure)

```bash
pip install -r requirements.txt
python examples/simulate_logs.py                      # 30 min of multi-source logs + injected incident
uvicorn dashboard.app:app --reload --port 8080
# http://localhost:8080/observability
```

The dashboard process runs the processor and detector in a background thread
(`DASHBOARD_BACKGROUND=1`, default). Push your own logs:

```bash
uvicorn observability.services.ingestion_api:app --port 8081
curl -X POST localhost:8081/v1/ingest -H 'content-type: application/json' \
  -d '{"records":[{"message":"ERROR payment timeout","service":"checkout"}],"source_type":"application"}'
curl -X POST 'localhost:8081/v1/ingest/bulk?source_type=infrastructure' --data-binary @/var/log/syslog
```

Optional AI summaries: install [Ollama](https://ollama.ai), `ollama pull llama3.2`.
Without it the deterministic summariser is used automatically.

---

## 2. Docker Compose (Kafka + OpenSearch)

```bash
cd deploy/docker
docker compose up --build -d          # kafka, opensearch, ingestion-api, processor×2, detector, incident-api, dashboard
docker compose --profile tools up -d  # + OpenSearch Dashboards on :5601
docker compose run --rm -e SERVICE_ROLE=simulate dashboard --minutes 20   # seed traffic
```

| Endpoint | URL |
|---|---|
| Dashboard | http://localhost:8080/observability |
| Ingestion API | http://localhost:8081/v1/ingest (docs at /docs) |
| Incident API | http://localhost:8082/api/obs/overview |
| OpenSearch | http://localhost:9200 |
| Kafka (host) | localhost:29092 |

Lightweight single container: `docker compose --profile lite up dashboard-lite`.

Set `INGEST_API_KEY` and `ALERT_WEBHOOK_URL` in the shell (or a `.env` next to
the compose file) to enable auth and Slack/PagerDuty notifications.

---

## 3. Kubernetes

### 3.1 Prerequisites
* Kubernetes ≥ 1.27, `kubectl`, `kustomize` (or `kubectl -k`)
* An ingress controller (manifests assume nginx) — optional
* KEDA (optional) for lag-based processor autoscaling; delete the
  `ScaledObject` in [processor.yaml](../deploy/k8s/processor.yaml) if not installed
* A container registry

### 3.2 Build & push
```bash
docker build -t ghcr.io/<org>/agentscope:2.0.0 .
docker push ghcr.io/<org>/agentscope:2.0.0
# update images[].newName/newTag in deploy/k8s/kustomization.yaml
```

### 3.3 Configure
* [configmap.yaml](../deploy/k8s/configmap.yaml) — backends, thresholds, Ollama URL
* [secret.yaml](../deploy/k8s/secret.yaml) — `INGEST_API_KEY`, `ALERT_WEBHOOK_URL`,
  OpenSearch credentials. Prefer External Secrets / SealedSecrets in production.
* [ingress.yaml](../deploy/k8s/ingress.yaml) — set your host; TLS via cert-manager annotations.

### 3.4 Deploy
```bash
kubectl apply -k deploy/k8s
kubectl -n agentscope get pods -w
kubectl -n agentscope port-forward svc/dashboard 8080:8080
```

Order of readiness: Kafka/OpenSearch StatefulSets → ingestion-api (readiness
probes `/readyz` check the index) → processor → detector → incident-api → dashboard.
Fluent Bit starts shipping every container's logs immediately via
`/v1/ingest/kubernetes`.

### 3.5 Production hardening checklist
- [ ] Replace the single-node Kafka/OpenSearch with Strimzi / OpenSearch Operator or managed services; set `KAFKA_BOOTSTRAP`, `OPENSEARCH_URL`
- [ ] OpenSearch ISM policy: rollover daily indices, warm after 7d, delete after retention
- [ ] Kafka topics `logs.raw` / `logs.processed`: 12+ partitions, RF 3, 48h retention; `logs.dlq` 7d
- [ ] Enable TLS/SASL on Kafka and basic auth/TLS on OpenSearch (`OPENSEARCH_USER/PASSWORD`, `OPENSEARCH_VERIFY_CERTS`)
- [ ] Put the incident repository on an RWO SSD class PVC with snapshots (or point `OBSERVABILITY_DB` at a mounted Postgres-backed volume in a future adapter)
- [ ] Scrape `/metrics` on ingestion-api with Prometheus; alert on `agentscope_ingest_rejected_total` and consumer lag
- [ ] Set `INGEST_API_KEY` and rotate; terminate TLS at the ingress
- [ ] Review `REDACT_PII=true` patterns for your data classes

### 3.6 Shipping CI/CD logs
Add [ship-ci-logs.yml](../.github/workflows/ship-ci-logs.yml) to any repository
with `AGENTSCOPE_URL` and `AGENTSCOPE_API_KEY` secrets; job logs are POSTed to
`/v1/ingest/bulk?source_type=cicd` after each workflow run. GitLab/Jenkins can
`curl` the same endpoint from a post-build step.

---

## 4. Configuration reference

| Variable | Default | Description |
|---|---|---|
| `EVENT_BUS` | `memory` | `memory` or `kafka` |
| `LOG_INDEX` | `sqlite` | `sqlite` or `opensearch` |
| `OBSERVABILITY_DB` | `AGENTSCOPE_DB` | SQLite path for index (sqlite mode) + incident repo |
| `KAFKA_BOOTSTRAP` / `KAFKA_GROUP_ID` | `localhost:9092` / `agentscope` | broker list, consumer group prefix |
| `OPENSEARCH_URL` / `_USER` / `_PASSWORD` / `_INDEX_PREFIX` | `http://localhost:9200` / – / – / `logs` | cluster + daily index prefix |
| `PIPELINE_BATCH_SIZE` / `PIPELINE_FLUSH_MS` | `500` / `250` | processor batch size and poll timeout |
| `MAX_MESSAGE_BYTES` | `32768` | records above this go to the DLQ |
| `REDACT_PII` | `true` | redact emails, cards, tokens, JWTs, cloud keys |
| `DETECTION_WINDOW_SEC` / `DETECTION_HISTORY_WINDOWS` | `60` / `30` | window size and baseline length |
| `ANOMALY_THRESHOLD` | `0.7` | minimum score to raise an anomaly |
| `DETECTOR_INTERVAL_SEC` | `30` | detector loop period |
| `ALERT_DEDUP_SEC` / `INCIDENT_GROUP_WINDOW_SEC` | `300` / `600` | alert suppression and incident grouping windows |
| `ALERT_WEBHOOK_URL` | – | Slack/PagerDuty/Alertmanager-compatible webhook |
| `AI_SUMMARIES` / `SUMMARY_MODEL` / `OLLAMA_URL` | `true` / `llama3.2` / local | AI summary settings |
| `INGEST_API_KEY` | – | when set, `/v1/ingest*` requires `X-Api-Key` |
| `DASHBOARD_BACKGROUND` | `1` | run processor+detector inside the dashboard (local mode only) |

---

## 5. Operations

* **Health**: `/healthz` (liveness) and `/readyz` (index reachable) on every HTTP service.
* **Metrics**: `GET /metrics` on ingestion-api (Prometheus text format).
* **Manual detection**: `POST /api/obs/detect`; **re-analysis**: `POST /api/obs/incidents/{id}/analyze`.
* **Incident status**: `PATCH /api/obs/incidents/{id}/status` with `open|investigating|mitigated|resolved`.
* **Declared dependencies**: `POST /api/obs/dependencies {"source":"checkout","target":"payment"}`.
* **Dead letters**: consume `logs.dlq` to inspect rejected records.
* **Upgrades**: rolling for stateless services; `Recreate` for the three that share the state PVC.
