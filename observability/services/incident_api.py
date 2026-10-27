"""
incident_api.py — Incident intelligence API (FastAPI router + standalone app).

Mounted into the dashboard at /api/obs/* and runnable on its own:
    uvicorn observability.services.incident_api:app --port 8082
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
from contextlib import asynccontextmanager
from datetime import timedelta
from typing import Any

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from fastapi import APIRouter, FastAPI, HTTPException, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from observability.domain.models import IncidentStatus, utcnow
from observability.services.platform import get_platform

router = APIRouter(prefix="/api/obs", tags=["observability"])


class StatusBody(BaseModel):
    status: IncidentStatus


class DependencyBody(BaseModel):
    source: str
    target: str


@router.get("/overview")
async def overview():
    return get_platform().overview()


@router.get("/health/services")
async def service_health(lookback_sec: int = Query(900, ge=60, le=86_400)):
    return [h.to_dict() for h in get_platform().service_health(lookback_sec)]


@router.get("/trends/errors")
async def error_trends(hours: int = Query(6, ge=1, le=168), bucket_sec: int = Query(300, ge=60, le=3600)):
    return get_platform().error_trends(hours, bucket_sec)


@router.get("/anomalies")
async def anomalies(hours: int = Query(24, ge=1, le=720), service: str | None = None, limit: int = Query(200, le=2000)):
    p = get_platform()
    return [a.to_dict() for a in p.repo.list_anomalies(utcnow() - timedelta(hours=hours), service, limit)]


@router.get("/alerts")
async def alerts(hours: int = Query(24, ge=1, le=720), limit: int = Query(200, le=2000)):
    return [a.to_dict() for a in get_platform().repo.list_alerts(utcnow() - timedelta(hours=hours), limit)]


@router.get("/incidents")
async def incidents(status: str | None = None, limit: int = Query(100, le=1000)):
    return [i.to_dict() for i in get_platform().repo.list_incidents(status, limit)]


@router.get("/incidents/timeline")
async def incident_timeline(limit: int = Query(50, le=500)):
    return get_platform().incident_timeline(limit)


@router.get("/incidents/{incident_id}")
async def incident(incident_id: str):
    inc = get_platform().repo.get_incident(incident_id)
    if not inc:
        raise HTTPException(404, "incident not found")
    return inc.to_dict()


@router.post("/incidents/{incident_id}/analyze")
async def analyze(incident_id: str):
    res = await asyncio.to_thread(get_platform().analyze_incident, incident_id)
    if not res:
        raise HTTPException(404, "incident not found")
    return res


@router.patch("/incidents/{incident_id}/status")
async def set_status(incident_id: str, body: StatusBody):
    p = get_platform()
    inc = p.repo.get_incident(incident_id)
    if not inc:
        raise HTTPException(404, "incident not found")
    inc.status = body.status
    if body.status == IncidentStatus.RESOLVED:
        inc.resolved_at = utcnow()
    p.repo.save_incident(inc)
    return inc.to_dict()


@router.get("/dependencies")
async def dependencies():
    return get_platform().dependency_graph()


@router.post("/dependencies")
async def declare_dependency(body: DependencyBody):
    get_platform().mapper.declare(body.source.lower(), body.target.lower())
    return {"ok": True}


@router.get("/logs/search")
async def search_logs(q: str | None = None, service: str | None = None, level: str | None = None,
                      source_type: str | None = None, trace_id: str | None = None,
                      hours: int = Query(24, ge=1, le=720), limit: int = Query(100, le=1000)):
    return get_platform().index.search(q, service, level, source_type, utcnow() - timedelta(hours=hours),
                                       None, trace_id, limit)


@router.post("/detect")
async def run_detection():
    return await asyncio.to_thread(get_platform().run_detection)


@router.get("/stream")
async def stream():
    """Server-Sent Events: live logs, alerts and incidents."""
    p = get_platform()
    queue: asyncio.Queue = asyncio.Queue(maxsize=1000)
    loop = asyncio.get_running_loop()

    def listener(evt: dict[str, Any]) -> None:
        try:
            loop.call_soon_threadsafe(queue.put_nowait, evt)
        except (asyncio.QueueFull, RuntimeError):
            pass

    p.listeners.append(listener)

    async def gen():
        try:
            yield f"event: hello\ndata: {json.dumps({'ts': utcnow().isoformat()})}\n\n"
            while True:
                try:
                    evt = await asyncio.wait_for(queue.get(), timeout=15)
                    yield f"event: {evt['type']}\ndata: {json.dumps(evt['data'], default=str)}\n\n"
                except asyncio.TimeoutError:
                    yield ": keepalive\n\n"
        finally:
            if listener in p.listeners:
                p.listeners.remove(listener)

    return StreamingResponse(gen(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@asynccontextmanager
async def lifespan(_: FastAPI):
    get_platform()
    yield


app = FastAPI(title="AgentScope Incident API", version="2.0.0", lifespan=lifespan)
app.include_router(router)


@app.get("/healthz")
async def healthz():
    return {"status": "ok"}
