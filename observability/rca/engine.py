"""
engine.py — RootCauseAnalyzer: combines correlation, dependency ranking and
historical matching into a ranked list of probable causes with evidence.
"""
from __future__ import annotations

from datetime import timedelta
from typing import Any, Iterable

from ..domain.models import Incident, LogEvent, utcnow
from ..domain.ports import IncidentRepository, LogIndex
from .correlation import LogCorrelator
from .dependency import DependencyMapper, ServiceGraph
from .history import HistoricalMatcher, build_signature

_CAUSE_HINTS = {
    "timeout": "Downstream call timeouts — check dependency latency, connection pools and client timeouts",
    "connection": "Connection failures — check network policy, DNS, service endpoints and pod readiness",
    "dependency": "Upstream dependency returned 5xx / circuit opened — inspect the dependency's own health",
    "database": "Database errors — check connection pool saturation, locks, slow queries, recent migrations",
    "auth": "Authentication/authorization failures — rotated secrets, expired tokens, IAM/RBAC changes",
    "resource": "Resource exhaustion (OOM / disk / fd) — check limits, memory leaks, node pressure",
    "rate_limit": "Rate limiting — upstream quota exhausted or burst traffic; add backoff / raise limits",
    "kubernetes": "Kubernetes scheduling/runtime failure — CrashLoop, image pull, probes, evictions",
    "build": "CI build failure — dependency resolution or compilation error introduced by a recent commit",
    "test": "Test failures in CI — regression introduced by a recent change",
    "deploy": "Deployment/rollout failure — bad config or image; consider rollback",
    "validation": "Input validation failures — a client sending malformed requests or a contract change",
}


class RootCauseAnalyzer:
    def __init__(self, index: LogIndex, repo: IncidentRepository | None = None, lookback_sec: int = 900):
        self.index = index
        self.repo = repo
        self.lookback_sec = lookback_sec
        self.correlator = LogCorrelator()
        self.mapper = DependencyMapper(repo)
        self.matcher = HistoricalMatcher()

    def analyze(self, incident: Incident, events: Iterable[LogEvent] | None = None) -> dict[str, Any]:
        since = incident.started_at - timedelta(seconds=self.lookback_sec)
        until = incident.resolved_at or utcnow()
        evs = list(events) if events is not None else list(self.index.events_since(since, until))

        corr = self.correlator.correlate(evs, incident.services)
        graph = self._graph(evs)
        failing = [s for s in corr["significant_services"] if s not in corr["cicd_services"]] or list(incident.services)
        ranked = graph.rank_root_causes(failing, corr["error_counts"])

        # boost services that errored first and those deepest in failing traces
        leaders = set(corr["leading_services"])
        deepest = corr["deepest_failing_services"]
        for r in ranked:
            if r["service"] in leaders:
                r["score"] = round(min(1.0, r["score"] + 0.15), 3)
                r.setdefault("signals", []).append("errored first")
            if r["service"] in deepest:
                r["score"] = round(min(1.0, r["score"] + 0.15), 3)
                r.setdefault("signals", []).append(f"deepest failing hop in {deepest[r['service']]} traces")
            if r["healthy_dependencies"] and not r["failing_dependencies"]:
                r.setdefault("signals", []).append("its dependencies are healthy")
            if r["failing_dependencies"]:
                r.setdefault("signals", []).append(f"depends on failing {', '.join(r['failing_dependencies'])}")
        ranked.sort(key=lambda d: -d["score"])

        causes = self._hypotheses(corr, ranked)
        signature = build_signature(
            services=failing,
            error_types=list(corr["error_type_mix"].keys()),
            fingerprints=[p["template"][:80] for p in corr["top_error_patterns"]],
            anomaly_kinds=[],
            features={"total_errors": corr["total_errors"], "services": len(failing)},
        )
        past = self.repo.list_incidents(limit=200) if self.repo else []
        similar = self.matcher.find_similar(signature, past, exclude_id=incident.incident_id)

        primary = ranked[0]["service"] if ranked else (incident.services[0] if incident.services else "unknown")
        return {
            "primary": primary,
            "primary_confidence": ranked[0]["score"] if ranked else 0.0,
            "ranked_services": ranked[:8],
            "hypotheses": causes,
            "correlation": corr,
            "dependency_graph": graph.to_dict(),
            "impacted_services": sorted(set(failing) | (set(graph.blast_radius(primary)) & set(failing))),
            "similar_incidents": similar,
            "signature": signature,
            "analyzed_at": utcnow().isoformat(),
        }

    def _graph(self, evs: list[LogEvent]) -> ServiceGraph:
        inferred = self.mapper.infer_from_events(evs)
        return self.mapper.merge_and_persist(inferred) if self.repo else inferred

    @staticmethod
    def _hypotheses(corr: dict[str, Any], ranked: list[dict[str, Any]]) -> list[dict[str, Any]]:
        out = []
        mix = corr["error_type_mix"]
        total = max(1, sum(v["count"] for v in mix.values()))
        for etype, info in sorted(mix.items(), key=lambda kv: -kv[1]["count"]):
            share = info["count"] / total
            out.append({"cause": _CAUSE_HINTS.get(etype, f"{etype} errors"), "category": etype,
                        "confidence": round(min(0.95, 0.3 + share * 0.7), 2),
                        "services": info["services"], "evidence": f"{info['count']} errors ({share:.0%} of all errors)"})
        if corr["kubernetes_signals"]:
            out.insert(0, {"cause": "Kubernetes runtime failure on affected pods", "category": "kubernetes",
                           "confidence": 0.8, "services": list(corr["hot_pods"].keys())[:3] or [],
                           "evidence": corr["kubernetes_signals"][0][:160]})
        if corr["resource_signals"]:
            out.insert(0, {"cause": "Resource exhaustion on host/pod", "category": "resource",
                           "confidence": 0.8, "services": list(corr["hot_hosts"].keys())[:3] or [],
                           "evidence": corr["resource_signals"][0][:160]})
        if corr["cicd_failures"]:
            f = corr["cicd_failures"][0]
            out.append({"cause": f"Recent CI/CD failure in pipeline '{f['pipeline']}' (stage {f['stage']}) — possible bad deploy",
                        "category": "deploy", "confidence": 0.5, "services": [], "evidence": f["message"]})
        if corr["shared_patterns"]:
            sp = corr["shared_patterns"][0]
            out.append({"cause": "Shared failure pattern across services indicates a common dependency",
                        "category": "dependency", "confidence": 0.6, "services": sp["services"],
                        "evidence": sp["template"][:160]})
        if ranked and ranked[0].get("healthy_dependencies") is not None and not ranked[0].get("failing_dependencies"):
            out.insert(0, {"cause": f"Fault originates in '{ranked[0]['service']}' (dependencies healthy, callers failing)",
                           "category": "origin", "confidence": ranked[0]["score"], "services": [ranked[0]["service"]],
                           "evidence": "; ".join(ranked[0].get("signals", [])) or "dependency topology"})
        # dedupe by category keeping highest confidence
        seen: dict[str, dict[str, Any]] = {}
        for h in out:
            if h["category"] not in seen or h["confidence"] > seen[h["category"]]["confidence"]:
                seen[h["category"]] = h
        return sorted(seen.values(), key=lambda h: -h["confidence"])[:6]
