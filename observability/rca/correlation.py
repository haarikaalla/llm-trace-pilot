"""
correlation.py — Log correlation for RCA.

Three correlation axes:
  * trace    : events sharing a trace_id across services (distributed request path)
  * temporal : which services started erroring first within the incident window
  * pattern  : error templates co-occurring across services (shared dependency)
"""
from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timedelta
from typing import Any, Iterable

from ..domain.models import LogEvent


class LogCorrelator:
    def __init__(self, lead_window_sec: int = 120):
        self.lead_window_sec = lead_window_sec

    def correlate(self, events: Iterable[LogEvent], focus_services: list[str] | None = None) -> dict[str, Any]:
        events = list(events)
        errors = [e for e in events if e.level.is_error]
        focus = set(focus_services or [])

        # ── which services are *significantly* failing (not just baseline noise) ──
        error_counts: Counter[str] = Counter(e.service for e in errors)
        total_counts: Counter[str] = Counter(e.service for e in events)
        error_rates = {s: error_counts[s] / total_counts[s] for s in total_counts}
        significant = {s for s, n in error_counts.items() if n >= 3 and error_rates[s] >= 0.15} | focus
        cicd_services = {e.service for e in events if e.source_type.value == "cicd"} - \
                        {e.service for e in events if e.source_type.value != "cicd"}

        # ── temporal ordering: first error per significant service ─────────
        first_error: dict[str, datetime] = {}
        for e in errors:
            if e.service in significant and (e.service not in first_error or e.timestamp < first_error[e.service]):
                first_error[e.service] = e.timestamp
        timeline = sorted(first_error.items(), key=lambda kv: kv[1])
        leaders = [s for s, t in timeline if not timeline or t <= timeline[0][1] + timedelta(seconds=self.lead_window_sec)]

        # ── trace correlation ───────────────────────────────────────────────
        by_trace: dict[str, list[LogEvent]] = defaultdict(list)
        for e in events:
            if e.trace_id:
                by_trace[e.trace_id].append(e)
        trace_paths: list[dict[str, Any]] = []
        upstream_of: Counter[tuple[str, str]] = Counter()
        for tid, evs in by_trace.items():
            if len({e.service for e in evs}) < 2:
                continue
            evs.sort(key=lambda e: e.timestamp)
            svcs = []
            for e in evs:
                if not svcs or svcs[-1] != e.service:
                    svcs.append(e.service)
            err_svcs = [e.service for e in evs if e.level.is_error]
            if err_svcs:
                deepest = err_svcs[-1] if err_svcs else None
                trace_paths.append({"trace_id": tid, "path": svcs, "error_services": sorted(set(err_svcs)),
                                    "deepest_failing": deepest})
                for a, b in zip(svcs, svcs[1:]):
                    upstream_of[(a, b)] += 1
        deepest_counter = Counter(tp["deepest_failing"] for tp in trace_paths if tp["deepest_failing"])

        # ── pattern co-occurrence ───────────────────────────────────────────
        tmpl_services: dict[str, set[str]] = defaultdict(set)
        tmpl_count: Counter[str] = Counter()
        type_services: dict[str, set[str]] = defaultdict(set)
        for e in errors:
            if e.template:
                tmpl_services[e.template].add(e.service)
                tmpl_count[e.template] += 1
            if e.error_type:
                type_services[e.error_type].add(e.service)
        shared_patterns = [{"template": t, "services": sorted(s), "count": tmpl_count[t]}
                           for t, s in tmpl_services.items() if len(s) > 1]
        shared_patterns.sort(key=lambda d: (-len(d["services"]), -d["count"]))
        top_patterns = [{"template": t, "count": c, "services": sorted(tmpl_services[t])}
                        for t, c in tmpl_count.most_common(8)]
        error_type_mix = {t: {"count": sum(1 for e in errors if e.error_type == t), "services": sorted(s)}
                          for t, s in type_services.items()}

        # ── infra signals (k8s / host) ──────────────────────────────────────
        hosts = Counter(e.host for e in errors if e.host)
        pods = Counter(e.pod for e in errors if e.pod)
        k8s_signals = [e.message for e in errors if e.error_type == "kubernetes"][:5]
        resource_signals = [e.message for e in errors if e.error_type == "resource"][:5]
        ci_failures = [{"pipeline": e.pipeline, "stage": e.stage, "message": e.message[:160]}
                       for e in errors if e.source_type.value == "cicd"][:5]

        return {
            "total_events": len(events), "total_errors": len(errors),
            "error_counts": dict(error_counts.most_common()),
            "error_rates": {s: round(r, 4) for s, r in error_rates.items()},
            "significant_services": sorted(significant),
            "cicd_services": sorted(cicd_services),
            "first_error_timeline": [{"service": s, "first_error_at": t.isoformat()} for s, t in timeline],
            "leading_services": leaders,
            "trace_paths": trace_paths[:25],
            "deepest_failing_services": dict(deepest_counter.most_common(5)),
            "inferred_edges": [{"source": a, "target": b, "traces": n} for (a, b), n in upstream_of.most_common(30)],
            "shared_patterns": shared_patterns[:10],
            "top_error_patterns": top_patterns,
            "error_type_mix": error_type_mix,
            "hot_hosts": dict(hosts.most_common(5)), "hot_pods": dict(pods.most_common(5)),
            "kubernetes_signals": k8s_signals, "resource_signals": resource_signals,
            "cicd_failures": ci_failures,
            "focus_services": sorted(focus),
        }
