"""
dependency.py — Service dependency graph.

The graph is built from three sources (merged, persisted in IncidentRepository):
  1. declared edges   (config / API)            — authoritative
  2. trace inference  (trace_id hop ordering)   — learned from traffic
  3. message hints    ("calling X", "upstream X", "X returned 503")

Root-cause ranking: a failing service whose *downstream* dependencies are healthy
is a likelier root than one whose dependencies are also failing (fault propagates
upstream, towards callers).
"""
from __future__ import annotations

import re
from collections import defaultdict, deque
from typing import Any, Iterable

from ..domain.models import DependencyEdge, LogEvent
from ..domain.ports import IncidentRepository

HINT_RES = [
    re.compile(r"\b(?:calling|call to|request to|connecting to|upstream|dependency|downstream|from|to)\s+(?:service\s+)?[\"'`]?([a-z][a-z0-9\-_.]{2,40}?)(?:-service|-svc|-api)?[\"'`]?(?:\s+(?:failed|timed out|returned|refused|unavailable|returned)|[:\s,.]|$)", re.I),
    re.compile(r"\bhttps?://([a-z][a-z0-9\-]{2,40})(?:\.[a-z0-9\-]+)*(?::\d+)?/", re.I),
]
STOPWORDS = {"the", "to", "from", "for", "with", "service", "error", "failed", "request", "response",
             "timeout", "connection", "upstream", "downstream", "http", "https", "localhost", "null", "none",
             "user", "client", "server", "database", "db", "cache", "queue", "api", "endpoint", "host", "pod"}


class ServiceGraph:
    def __init__(self):
        self.edges: dict[tuple[str, str], DependencyEdge] = {}

    def add(self, source: str, target: str, calls: int = 1, error_calls: int = 0, latency_ms: float | None = None) -> None:
        if source == target or not source or not target:
            return
        key = (source, target)
        e = self.edges.get(key)
        if e is None:
            self.edges[key] = DependencyEdge(source, target, calls, error_calls, latency_ms)
        else:
            e.calls += calls
            e.error_calls += error_calls
            if latency_ms is not None:
                e.avg_latency_ms = latency_ms if e.avg_latency_ms is None else (e.avg_latency_ms + latency_ms) / 2

    @property
    def services(self) -> set[str]:
        s: set[str] = set()
        for a, b in self.edges:
            s.add(a); s.add(b)
        return s

    def downstream(self, svc: str, significant_only: bool = False) -> set[str]:
        edges = [e for (a, _), e in self.edges.items() if a == svc]
        if not significant_only or not edges:
            return {e.target for e in edges}
        total = sum(e.calls for e in edges)
        # ignore negligible edges and those dominated by the reverse direction (trace-inference noise)
        out = set()
        for e in edges:
            reverse = self.edges.get((e.target, e.source))
            declared = e.calls == 0          # declared via API, no traffic observed yet
            if declared or (e.calls >= max(3, 0.05 * total) and (reverse is None or e.calls > reverse.calls)):
                out.add(e.target)
        return out

    def upstream(self, svc: str, significant_only: bool = False) -> set[str]:
        return {a for a, b in self.edges if b == svc and (not significant_only or svc in self.downstream(a, True))}

    def blast_radius(self, svc: str, significant_only: bool = False) -> list[str]:
        """All callers transitively affected if `svc` fails."""
        seen, q = set(), deque([svc])
        while q:
            cur = q.popleft()
            for up in self.upstream(cur, significant_only):
                if up not in seen:
                    seen.add(up); q.append(up)
        return sorted(seen)

    def rank_root_causes(self, failing: Iterable[str], error_counts: dict[str, int] | None = None) -> list[dict[str, Any]]:
        failing = set(failing)
        error_counts = error_counts or {}
        ranked = []
        connected = self.services
        for svc in failing:
            down = self.downstream(svc, significant_only=True)
            failing_down = down & failing
            # root-likelihood: failing with healthy deps -> high; failing because deps fail -> low.
            # Nodes unknown to the graph get no topology credit when a topology exists.
            if not down:
                dep_score = 1.0 if (svc in connected or not connected) else 0.35
            else:
                dep_score = 1.0 - len(failing_down) / len(down)
            radius = self.blast_radius(svc, significant_only=True)
            radius_score = min(1.0, len(set(radius) & failing) / max(1, len(failing) - 1)) if len(failing) > 1 else 0.5
            volume = error_counts.get(svc, 0)
            score = 0.55 * dep_score + 0.35 * radius_score + 0.10 * min(1.0, volume / 100)
            ranked.append({"service": svc, "score": round(score, 3), "healthy_dependencies": sorted(down - failing_down),
                           "failing_dependencies": sorted(failing_down), "blast_radius": radius,
                           "impacted_failing": sorted(set(radius) & failing)})
        ranked.sort(key=lambda d: -d["score"])
        return ranked

    def to_dict(self, significant_only: bool = False) -> dict[str, Any]:
        edges = list(self.edges.values())
        if significant_only:
            keep = {(a, b) for a in self.services for b in self.downstream(a, significant_only=True)}
            edges = [e for e in edges if (e.source, e.target) in keep]
        return {"nodes": sorted(self.services), "edges": [e.to_dict() for e in edges]}


class DependencyMapper:
    def __init__(self, repo: IncidentRepository | None = None):
        self.repo = repo

    def infer_from_events(self, events: Iterable[LogEvent], known_services: set[str] | None = None) -> ServiceGraph:
        g = ServiceGraph()
        known = set(known_services or set())
        by_trace: dict[str, list[LogEvent]] = defaultdict(list)
        for e in events:
            known.add(e.service)
            if e.trace_id:
                by_trace[e.trace_id].append(e)
            tgt = e.attributes.get("target_service") or e.attributes.get("upstream") or e.attributes.get("peer_service")
            if tgt:
                g.add(e.service, str(tgt).lower(), 1, int(e.level.is_error), e.latency_ms)
        # trace hops: keep only directions that are consistently observed (noise-robust)
        hop_counts: dict[tuple[str, str], list[int | float | None]] = defaultdict(lambda: [0, 0, []])
        for evs in by_trace.values():
            if len(evs) < 2:
                continue
            evs.sort(key=lambda e: e.timestamp)
            hops: list[LogEvent] = []
            for e in evs:
                if not hops or hops[-1].service != e.service:
                    hops.append(e)
            for a, b in zip(hops, hops[1:]):
                hc = hop_counts[(a.service, b.service)]
                hc[0] += 1
                hc[1] += int(b.level.is_error)
                if b.latency_ms is not None:
                    hc[2].append(b.latency_ms)
        for (a, b), (calls, errs, lats) in hop_counts.items():
            reverse = hop_counts.get((b, a), [0, 0, []])[0]
            if calls >= 2 and calls > 1.5 * reverse:
                g.add(a, b, calls, errs, (sum(lats) / len(lats)) if lats else None)
        # message hints restricted to known service names to avoid noise
        for e in events:
            if not e.level.is_error:
                continue
            for rx in HINT_RES:
                for m in rx.finditer(e.message):
                    cand = m.group(1).lower()
                    if cand in STOPWORDS:
                        continue
                    for suffix in ("", "-service", "-svc", "-api"):
                        if cand + suffix in known and cand + suffix != e.service:
                            g.add(e.service, cand + suffix, 1, 1, e.latency_ms)
                            break
        return g

    def load(self) -> ServiceGraph:
        g = ServiceGraph()
        if self.repo:
            for e in self.repo.list_dependencies():
                g.edges[(e.source, e.target)] = e
        return g

    def merge_and_persist(self, inferred: ServiceGraph) -> ServiceGraph:
        if self.repo:
            for e in inferred.edges.values():
                self.repo.upsert_dependency(e)
            return self.load()
        return inferred

    def declare(self, source: str, target: str) -> None:
        if self.repo:
            self.repo.upsert_dependency(DependencyEdge(source, target, calls=0))
