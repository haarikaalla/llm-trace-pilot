"""RCA, alerting, storage, streaming and end-to-end platform tests."""
import time
from datetime import datetime, timedelta, timezone

import pytest

from observability.alerting import AlertManager, AlertRule, InMemoryNotifier
from observability.domain.models import (Alert, Anomaly, AnomalyKind, Incident, IncidentStatus, LogEvent,
                                         Severity, SourceType, utcnow)
from observability.rca import DependencyMapper, HistoricalMatcher, LogCorrelator, RootCauseAnalyzer, ServiceGraph
from observability.rca.history import build_signature
from observability.storage import SQLiteIncidentRepository, SQLiteLogIndex
from observability.streaming import InMemoryBus, Topics

T0 = datetime(2025, 10, 27, 10, 0, tzinfo=timezone.utc)


def ev(svc, msg="x", level=Severity.INFO, m=0, s=0, trace=None, **kw):
    return LogEvent(message=msg, service=svc, level=level, timestamp=T0 + timedelta(minutes=m, seconds=s),
                    trace_id=trace, fingerprint=kw.pop("fp", msg[:10]), template=msg, **kw)


# ── streaming ───────────────────────────────────────────────────────────────
def test_inmemory_bus_publish_consume_subscribe():
    bus = InMemoryBus(maxlen=3)
    got = []
    bus.subscribe("t", got.append)
    for i in range(5):
        bus.publish("t", {"i": i})
    assert len(got) == 5
    assert [r["i"] for r in bus.consume("t")] == [2, 3, 4]      # bounded: oldest dropped
    assert bus.dropped["t"] == 2 and bus.consume("t") == []


def test_bus_subscriber_exception_is_isolated():
    bus = InMemoryBus()
    bus.subscribe("t", lambda r: 1 / 0)
    bus.publish("t", {"a": 1})
    assert bus.consume("t") == [{"a": 1}]


# ── storage ─────────────────────────────────────────────────────────────────
def test_sqlite_index_roundtrip_and_queries(tmp_path):
    idx = SQLiteLogIndex(str(tmp_path / "i.db"))
    evs = [ev("a", "err one", Severity.ERROR, s=1, trace="t1", latency_ms=50), ev("b", "ok", s=2, trace="t1"),
           ev("a", "ok again", m=1)]
    assert idx.index_bulk(evs) == 3
    assert idx.index_bulk(evs) == 3 and idx.count() == 3            # idempotent on event_id
    assert idx.services() == ["a", "b"]
    assert len(idx.search(query="err")) == 1
    assert len(idx.search(trace_id="t1")) == 2
    assert len(idx.search(level="error", service="a")) == 1
    ts = idx.timeseries(T0 - timedelta(minutes=1), 60)
    assert sum(r["count"] for r in ts) == 3 and any(r["errors"] == 1 for r in ts)
    back = list(idx.events_since(T0, T0 + timedelta(minutes=5)))
    assert len(back) == 3 and back[0].level == Severity.ERROR and back[0].latency_ms == 50
    assert idx.purge_older_than(T0 + timedelta(minutes=1)) == 2


def test_incident_repo_roundtrip(tmp_path):
    repo = SQLiteIncidentRepository(str(tmp_path / "r.db"))
    a = Anomaly("svc", AnomalyKind.ERROR_SPIKE, 0.9, Severity.ERROR, "d", T0, T0, evidence={"k": 1})
    repo.save_anomaly(a)
    inc = Incident("t", Severity.ERROR, ["svc"], anomaly_ids=[a.anomaly_id], signature={"services": ["svc"]})
    repo.save_incident(inc)
    assert repo.get_incident(inc.incident_id).services == ["svc"]
    assert repo.list_anomalies()[0].incident_id == inc.incident_id
    assert repo.find_open_incident("svc", 600).incident_id == inc.incident_id
    assert repo.find_open_incident("other", 600) is None
    al = Alert("t", Severity.ERROR, "svc", "m", dedup_key="k")
    repo.save_alert(al)
    assert repo.last_alert_time("k") is not None and repo.last_alert_time("zz") is None
    from observability.domain.models import DependencyEdge
    repo.upsert_dependency(DependencyEdge("a", "b", 5, 1, 100.0))
    repo.upsert_dependency(DependencyEdge("a", "b", 5, 0, 200.0))
    e = repo.list_dependencies()[0]
    assert e.calls == 10 and e.error_calls == 1 and e.avg_latency_ms == 150.0
    assert repo.stats()["incidents_open"] == 1


# ── RCA ─────────────────────────────────────────────────────────────────────
def test_service_graph_ranking_prefers_deepest_failing():
    g = ServiceGraph()
    for a, b in [("gw", "checkout"), ("checkout", "payment"), ("payment", "db"), ("gw", "catalog")]:
        g.add(a, b, calls=100)
    ranked = g.rank_root_causes({"gw", "checkout", "payment", "db"})
    assert ranked[0]["service"] == "db"
    assert ranked[0]["blast_radius"] == ["checkout", "gw", "payment"]
    assert all(ranked[0]["score"] > r["score"] + 0.4 for r in ranked[1:])


def test_dependency_inference_from_traces_hints_and_attributes():
    evs = []
    for t in range(5):
        evs += [ev("gw", "req", s=t * 10, trace=f"t{t}"), ev("api", "handled", s=t * 10 + 1, trace=f"t{t}")]
    evs.append(ev("api", "TimeoutError calling payments-db after 5s", Severity.ERROR, m=1))
    evs.append(ev("payments-db", "ok", m=1))
    evs.append(ev("api", "x", m=2, attributes={"target_service": "cache"}))
    g = DependencyMapper().infer_from_events(evs)
    assert ("gw", "api") in g.edges and g.edges[("gw", "api")].calls == 5
    assert ("api", "payments-db") in g.edges and g.edges[("api", "payments-db")].error_calls == 1
    assert ("api", "cache") in g.edges


def test_log_correlator_finds_leaders_and_shared_patterns():
    evs = [ev("db", "conn refused", Severity.ERROR, s=0, fp="e1"), ev("api", "conn refused", Severity.ERROR, s=30, fp="e1"),
           ev("api", "conn refused", Severity.ERROR, s=31, fp="e1"), ev("db", "conn refused", Severity.ERROR, s=32, fp="e1"),
           ev("db", "conn refused", Severity.ERROR, s=33, fp="e1"), ev("api", "conn refused", Severity.ERROR, s=34, fp="e1"),
           ev("web", "fine", s=5), ev("web", "fine", s=6)]
    for e in evs:
        e.error_type = "connection" if e.level.is_error else None
    c = LogCorrelator().correlate(evs)
    assert c["leading_services"][0] == "db"
    assert c["significant_services"] == ["api", "db"]
    assert c["shared_patterns"][0]["services"] == ["api", "db"]
    assert "connection" in c["error_type_mix"]


def test_historical_matcher_similarity():
    sig = build_signature(["a", "b"], ["timeout"], ["f1", "f2"], ["error_spike"], {"total_errors": 10})
    past = [Incident("old", Severity.ERROR, ["a", "b"], incident_id="INC-1", signature=sig, root_cause={"primary": "b"},
                     summary={"recommended_actions": ["restart b"]}),
            Incident("other", Severity.ERROR, ["z"], incident_id="INC-2",
                     signature=build_signature(["z"], ["auth"], ["f9"], [], {}))]
    res = HistoricalMatcher().find_similar(sig, past, exclude_id="INC-X")
    assert res[0]["incident_id"] == "INC-1" and res[0]["similarity"] > 0.9 and res[0]["remediation"] == ["restart b"]
    assert all(r["incident_id"] != "INC-2" for r in res)


def test_root_cause_analyzer_end_to_end(tmp_path):
    idx = SQLiteLogIndex(str(tmp_path / "i.db"))
    repo = SQLiteIncidentRepository(str(tmp_path / "i.db"))
    evs = []
    for t in range(10):
        evs += [ev("gw", "req", s=t, trace=f"t{t}", attributes={"target_service": "api"}),
                ev("api", "call db", s=t, trace=f"t{t}", attributes={"target_service": "db"}),
                ev("db", "ok", s=t, trace=f"t{t}")]
    for t in range(6):
        evs += [ev("db", "Out of memory: killed", Severity.FATAL, m=5, s=t, fp="oom", error_type="resource"),
                ev("api", "TimeoutError calling db", Severity.ERROR, m=5, s=t + 1, fp="to", error_type="timeout", trace=f"x{t}", attributes={"target_service": "db"}),
                ev("gw", "upstream api 503", Severity.ERROR, m=5, s=t + 2, fp="up", error_type="dependency", trace=f"x{t}", attributes={"target_service": "api"})]
    idx.index_bulk(evs)
    inc = Incident("api errors", Severity.ERROR, ["api"], started_at=T0 + timedelta(minutes=5))
    rca = RootCauseAnalyzer(idx, repo).analyze(inc, evs)
    assert rca["primary"] == "db"
    assert set(rca["impacted_services"]) >= {"api", "db", "gw"}
    cats = {h["category"] for h in rca["hypotheses"]}
    assert "resource" in cats and "origin" in cats
    assert rca["signature"]["services"] == ["api", "db", "gw"]


# ── alerting ────────────────────────────────────────────────────────────────
def anomaly(svc, kind=AnomalyKind.ERROR_SPIKE, score=0.95, sev=Severity.ERROR):
    return Anomaly(svc, kind, score, sev, f"{kind.value} on {svc}", T0, T0 + timedelta(minutes=1))


def test_alert_rules_match():
    r = AlertRule("r", min_score=0.8, kinds={AnomalyKind.ERROR_SPIKE}, min_severity=Severity.ERROR)
    assert r.matches(anomaly("a"))
    assert not r.matches(anomaly("a", score=0.5))
    assert not r.matches(anomaly("a", kind=AnomalyKind.VOLUME_ANOMALY))
    assert not r.matches(anomaly("a", sev=Severity.WARN))


def test_alert_manager_dedup_grouping_and_merge(tmp_path, obs_env):
    repo = SQLiteIncidentRepository(str(tmp_path / "a.db"))
    sink = InMemoryNotifier()
    graph = {"api": {"db"}, "db": {"api"}}
    mgr = AlertManager(repo, [sink], InMemoryBus(), settings=obs_env, related_services=lambda s: graph.get(s, set()))

    alerts, incidents = mgr.handle([anomaly("api"), anomaly("db", AnomalyKind.LATENCY_DEGRADATION), anomaly("web")])
    assert len(alerts) == 3 and len(sink.sent) == 3
    assert len(incidents) == 2                                   # api+db merged via topology; web separate
    big = next(i for i in incidents if len(i.services) == 2)
    assert set(big.services) == {"api", "db"} and len(big.anomaly_ids) == 2

    alerts2, incidents2 = mgr.handle([anomaly("api")])
    assert alerts2 == [] and mgr.suppressed == 1                 # deduped within window
    assert incidents2[0].incident_id == big.incident_id          # attached to the same open incident

    alerts3, incidents3 = mgr.handle([anomaly("api", AnomalyKind.INCIDENT_PREDICTION, 0.9, Severity.WARN)])
    assert len(alerts3) == 1 and incidents3 == []                # predictions alert but never open incidents


def test_alert_manager_auto_resolve(tmp_path, obs_env):
    repo = SQLiteIncidentRepository(str(tmp_path / "a.db"))
    mgr = AlertManager(repo, [InMemoryNotifier()], settings=obs_env)
    _, incs = mgr.handle([anomaly("api")])
    inc = incs[0]
    inc.updated_at = utcnow() - timedelta(hours=1)
    repo._lock.acquire(); repo._lock.release()
    with repo._conn() as c:
        c.execute("UPDATE incidents SET updated_at=? WHERE incident_id=?", ((utcnow() - timedelta(hours=1)).isoformat(), inc.incident_id))
    assert mgr.auto_resolve({"api"}) and repo.get_incident(inc.incident_id).status == IncidentStatus.RESOLVED
    assert mgr.auto_resolve({"api"}) == []


# ── platform end-to-end ─────────────────────────────────────────────────────
def test_platform_ingest_detect_incident_summary(platform):
    now = utcnow().replace(second=0, microsecond=0)
    recs = []
    for m in range(12, 0, -1):
        ts = now - timedelta(minutes=m)
        for k in range(30):
            level, msg = "INFO", "handled request status=200 latency=90ms"
            if m == 1 and k < 18:
                level, msg = "ERROR", "TimeoutError calling db-main after 5000ms latency=5000ms"
            recs.append({"message": f'{{"timestamp":"{(ts + timedelta(seconds=k)).isoformat()}","level":"{level}","service":"api","msg":"{msg}","target_service":"db-main"}}'})
            if m == 1 and k < 10:
                recs.append({"message": f"<26>{(ts + timedelta(seconds=k)).strftime('%b %d %H:%M:%S')} node-1 postgres: FATAL: too many connections",
                             "service": "db-main"})
            elif k % 3 == 0:
                recs.append({"message": f'{{"timestamp":"{(ts + timedelta(seconds=k)).isoformat()}","level":"INFO","service":"db-main","msg":"query ok"}}'})
    res = platform.ingest(recs)
    assert res["rejected"] == 0 and res["processed"] == res["accepted"] == platform.index.count()

    out = platform.run_detection(now=now + timedelta(seconds=59))
    assert out["anomalies"] and out["alerts"] and out["incidents"]
    inc = platform.repo.get_incident(out["incidents"][0]["incident_id"])
    assert set(inc.services) >= {"api", "db-main"}
    assert inc.root_cause["primary"] == "db-main"
    s = inc.summary
    assert s["generated_by"] == "deterministic"
    assert set(s) >= {"what_happened", "impacted_services", "possible_root_causes", "recommended_actions"}
    assert "db-main" in s["impacted_services"] and s["recommended_actions"]

    health = {h.service: h for h in platform.service_health()}
    assert health["api"].status in ("degraded", "critical") and health["api"].open_incidents == 1
    ov = platform.overview()
    assert ov["open_incidents"] == 1 and ov["total_events"] == platform.index.count()
    g = platform.dependency_graph()
    assert any(e["source"] == "api" and e["target"] == "db-main" for e in g["edges"])
    tl = platform.incident_timeline()
    assert tl[0]["anomalies"]

    # re-analysis via API path and status transitions
    res2 = platform.analyze_incident(inc.incident_id)
    assert res2["incident"]["status"] == "investigating" and res2["rca"]["primary"] == "db-main"
    # second cycle: dedup suppresses duplicate alerts, incident stays attached
    out2 = platform.run_detection(now=now + timedelta(seconds=59))
    assert len(out2["alerts"]) == 0 and platform.repo.stats()["incidents_open"] == 1


def test_platform_listeners_receive_live_events(platform):
    got = []
    platform.listeners.append(got.append)
    platform.ingest([{"message": "ERROR hello", "service": "x"}])
    assert got and got[0]["type"] == "log" and got[0]["data"]["service"] == "x"


def test_platform_dlq_for_bad_records(platform):
    res = platform.ingest([42, {"nomsg": 1}, {"message": "ok"}])
    assert res == {"accepted": 1, "rejected": 2, "processed": 1}
    assert len(platform.bus.consume(Topics.DLQ)) == 2


def test_tracer_bridge_converts_traces(platform):
    from observability.integrations import trace_to_event, enable_tracer_bridge
    from tracer.core import unregister_trace_hook, _trace_hooks
    e = trace_to_event({"name": "answer", "model": "gpt-4o", "latency_ms": 900, "error": "TimeoutError: slow",
                        "session_id": "s1", "trace_id": "t1", "input_tokens": 5, "output_tokens": 0})
    assert e.service == "llm.answer" and e.level == Severity.ERROR and e.error_type == "timeout"
    assert e.source_type == SourceType.LLM_TRACE and e.trace_id == "s1" and e.fingerprint
    enable_tracer_bridge(platform)
    hook = _trace_hooks[-1]
    try:
        from tracer import trace
        @trace(name="step", model="llama3.2")
        def fn(prompt: str): return "ok"
        fn(prompt="hi")
        assert platform.index.search(source_type="llm_trace")[0]["service"] == "llm.step"
    finally:
        unregister_trace_hook(hook)
