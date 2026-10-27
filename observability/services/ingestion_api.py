"""
ingestion_api.py — Ingestion microservice (FastAPI).

  POST /v1/ingest            {"records":[...], "source_type":"application", "defaults":{...}}
  POST /v1/ingest/bulk       NDJSON / plain-text body (one record per line)
  POST /v1/ingest/kubernetes Fluent Bit / Vector k8s envelope arrays
  POST /v1/ingest/cicd       GitHub Actions / GitLab / Jenkins log chunks
  GET  /healthz  /readyz  /metrics

Run:  uvicorn observability.services.ingestion_api:app --port 8081
"""
from __future__ import annotations

import hmac
import os
import sys
from contextlib import asynccontextmanager
from typing import Any

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from fastapi import Depends, FastAPI, Header, HTTPException, Request
from fastapi.responses import JSONResponse, PlainTextResponse
from pydantic import BaseModel, Field

from observability.config import get_settings
from observability.services.platform import get_platform

settings = get_settings()


class IngestBody(BaseModel):
    records: list[Any] = Field(..., min_length=1, max_length=10_000)
    source_type: str | None = None
    defaults: dict[str, Any] | None = None


def require_api_key(x_api_key: str | None = Header(default=None)) -> None:
    if settings.ingest_api_key and not hmac.compare_digest(x_api_key or "", settings.ingest_api_key):
        raise HTTPException(status_code=401, detail="invalid api key")


@asynccontextmanager
async def lifespan(_: FastAPI):
    get_platform()
    yield


app = FastAPI(title="AgentScope Ingestion API", version="2.0.0", lifespan=lifespan)


@app.post("/v1/ingest", dependencies=[Depends(require_api_key)])
async def ingest(body: IngestBody):
    return get_platform().ingest(body.records, body.source_type, body.defaults)


@app.post("/v1/ingest/bulk", dependencies=[Depends(require_api_key)])
async def ingest_bulk(request: Request, source_type: str | None = None, service: str | None = None,
                      environment: str | None = None):
    raw = await request.body()
    if len(raw) > 50 * 1024 * 1024:
        raise HTTPException(status_code=413, detail="body too large")
    text = raw.decode("utf-8", errors="replace")
    defaults = {k: v for k, v in (("service", service), ("environment", environment)) if v}
    p = get_platform()
    result = p.collector.ingest_text(text, source_type, defaults or None)
    if p.inline:
        result["processed"] = p.process_pending()
    return result


@app.post("/v1/ingest/kubernetes", dependencies=[Depends(require_api_key)])
async def ingest_k8s(records: list[dict[str, Any]]):
    return get_platform().ingest(records, source_type="kubernetes")


@app.post("/v1/ingest/cicd", dependencies=[Depends(require_api_key)])
async def ingest_cicd(body: IngestBody):
    return get_platform().ingest(body.records, source_type="cicd", defaults=body.defaults)


@app.get("/healthz")
async def healthz():
    return {"status": "ok"}


@app.get("/readyz")
async def readyz():
    p = get_platform()
    try:
        p.index.ensure_ready()
        return {"status": "ready", "event_bus": p.s.event_bus, "log_index": p.s.log_index}
    except Exception as e:                              # noqa: BLE001
        return JSONResponse({"status": "not-ready", "error": str(e)}, status_code=503)


@app.get("/metrics", response_class=PlainTextResponse)
async def metrics():
    p = get_platform()
    c, pm = p.collector.stats.snapshot(), p.processor.metrics()
    lines = [
        "# TYPE agentscope_ingest_accepted_total counter", f"agentscope_ingest_accepted_total {c['accepted']}",
        "# TYPE agentscope_ingest_rejected_total counter", f"agentscope_ingest_rejected_total {c['rejected']}",
        "# TYPE agentscope_ingest_bytes_total counter", f"agentscope_ingest_bytes_total {c['bytes']}",
        "# TYPE agentscope_pipeline_processed_total counter", f"agentscope_pipeline_processed_total {pm['processed']}",
        "# TYPE agentscope_pipeline_failed_total counter", f"agentscope_pipeline_failed_total {pm['failed']}",
    ]
    return "\n".join(lines) + "\n"
