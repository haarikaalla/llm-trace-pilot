#!/bin/sh
# Dispatch on SERVICE_ROLE so one image serves every microservice.
set -e
ROLE="${SERVICE_ROLE:-dashboard}"
PORT="${PORT:-8080}"
WORKERS="${WEB_CONCURRENCY:-2}"

case "$1" in
  healthcheck)
    case "$ROLE" in
      dashboard|ingestion-api|incident-api)
        python -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:${PORT}/healthz', timeout=3).status==200 else 1)";;
      *) exit 0;;
    esac
    exit $?;;
esac

case "$ROLE" in
  dashboard)      exec uvicorn dashboard.app:app --host 0.0.0.0 --port "$PORT" --workers 1 ;;
  ingestion-api)  exec uvicorn observability.services.ingestion_api:app --host 0.0.0.0 --port "$PORT" --workers "$WORKERS" ;;
  incident-api)   exec uvicorn observability.services.incident_api:app --host 0.0.0.0 --port "$PORT" --workers 1 ;;
  processor)      exec python -m observability.services.processor_worker ;;
  detector)       exec python -m observability.services.detector_worker ;;
  simulate)       exec python examples/simulate_logs.py "$@" ;;
  *) echo "unknown SERVICE_ROLE=$ROLE" >&2; exit 1 ;;
esac
