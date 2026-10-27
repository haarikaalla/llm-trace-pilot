"""HTTP contract tests for the ingestion API, incident API and dashboard."""
import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def apps(obs_env):
    from observability.services.ingestion_api import app as ingest_app
    from dashboard.app import app as dash_app
    with TestClient(ingest_app) as ic, TestClient(dash_app) as dc:
        yield ic, dc


def test_ingest_endpoints(apps):
    ic, _ = apps
    r = ic.post("/v1/ingest", json={"records": [{"message": "ERROR x", "service": "a"}, "plain WARN line"], "source_type": "application"})
    assert r.status_code == 200 and r.json()["accepted"] == 2
    r = ic.post("/v1/ingest/bulk?service=infra&source_type=infrastructure", content="<34>Oct 27 10:00:03 n1 sshd: fail\n\nline two")
    assert r.json()["accepted"] == 2
    r = ic.post("/v1/ingest/kubernetes", json=[{"log": "2025-10-27T10:00:00Z stdout F hi", "kubernetes": {"pod_name": "p-1"}}])
    assert r.json()["accepted"] == 1
    r = ic.post("/v1/ingest/cicd", json={"records": ["##[error]failed"], "defaults": {"pipeline": "ci"}})
    assert r.json()["accepted"] == 1
    assert ic.post("/v1/ingest", json={"records": []}).status_code == 422
    assert ic.get("/healthz").json()["status"] == "ok"
    assert ic.get("/readyz").json()["status"] == "ready"
    assert "agentscope_ingest_accepted_total 6" in ic.get("/metrics").text


def test_ingest_api_key_enforced(obs_env, monkeypatch):
    import dataclasses
    import observability.services.ingestion_api as mod
    monkeypatch.setattr(mod, "settings", dataclasses.replace(mod.settings, ingest_api_key="secret"))
    with TestClient(mod.app) as c:
        assert c.post("/v1/ingest", json={"records": ["x"]}).status_code == 401
        assert c.post("/v1/ingest", json={"records": ["x"]}, headers={"X-Api-Key": "secret"}).status_code == 200


def test_dashboard_and_incident_api(apps):
    ic, dc = apps
    ic.post("/v1/ingest", json={"records": [{"message": "ERROR boom", "service": "svc"}] * 5})
    assert dc.get("/").status_code == 200
    assert "Incident Intelligence" in dc.get("/observability").text
    ov = dc.get("/api/obs/overview").json()
    assert ov["total_events"] == 5 and ov["backends"] == {"event_bus": "memory", "log_index": "sqlite"}
    assert dc.get("/api/obs/health/services").json()[0]["service"] == "svc"
    assert dc.get("/api/obs/trends/errors").json()[0]["errors"] == 5
    assert dc.get("/api/obs/logs/search?q=boom&level=ERROR").json()
    assert dc.get("/api/obs/dependencies").json()["nodes"][0]["id"] == "svc"
    for p in ("/api/obs/anomalies", "/api/obs/alerts", "/api/obs/incidents", "/api/obs/incidents/timeline"):
        assert dc.get(p).json() == []
    det = dc.post("/api/obs/detect").json()
    assert set(det) == {"events_scanned", "anomalies", "alerts", "incidents", "resolved"}
    assert dc.get("/api/obs/incidents/NOPE").status_code == 404
    assert dc.post("/api/obs/incidents/NOPE/analyze").status_code == 404
    assert dc.post("/api/obs/dependencies", json={"source": "A", "target": "B"}).json() == {"ok": True}
    assert any(e["source"] == "a" for e in dc.get("/api/obs/dependencies").json()["edges"])
    assert dc.get("/api/obs/trends/errors?hours=0").status_code == 422
    assert dc.get("/api/stats").json()["summary"] is not None


def test_incident_status_patch(apps):
    _, dc = apps
    from observability.services.platform import get_platform
    from observability.domain.models import Incident, Severity
    inc = Incident("t", Severity.ERROR, ["svc"])
    get_platform().repo.save_incident(inc)
    r = dc.patch(f"/api/obs/incidents/{inc.incident_id}/status", json={"status": "resolved"})
    assert r.json()["status"] == "resolved" and r.json()["resolved_at"]
    assert dc.patch(f"/api/obs/incidents/{inc.incident_id}/status", json={"status": "bogus"}).status_code == 422
