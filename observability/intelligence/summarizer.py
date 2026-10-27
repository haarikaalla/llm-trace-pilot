"""
summarizer.py — AI-powered incident summaries.

Same pattern as evaluator/judge.py: ask a local Ollama model for structured
JSON; if the model is unavailable or returns garbage, fall back to a
deterministic summary built from the RCA result. The output schema is fixed:

  what_happened, impacted_services, possible_root_causes, recommended_actions,
  severity, confidence, generated_by
"""
from __future__ import annotations

import json
import logging
from typing import Any

import httpx

from ..config import Settings, get_settings
from ..domain.models import Incident

log = logging.getLogger(__name__)

SUMMARY_PROMPT = """You are an SRE incident commander. Using ONLY the structured evidence below, write an incident summary.
Return ONLY valid JSON with exactly these keys and no markdown:
{{"what_happened": "2-3 sentences", "impacted_services": ["..."], "possible_root_causes": ["..."], "recommended_actions": ["..."]}}

INCIDENT: {title} (severity {severity}, started {started})
SERVICES: {services}
ANOMALIES: {anomalies}
RCA PRIMARY SUSPECT: {primary} (confidence {confidence})
HYPOTHESES: {hypotheses}
ERROR TYPES: {error_types}
TOP ERROR PATTERNS: {patterns}
SIMILAR PAST INCIDENTS: {similar}
"""

REMEDIATIONS = {
    "timeout": ["Raise/align client timeouts and add retries with jitter for the failing call",
                "Check the dependency's latency and connection-pool saturation"],
    "connection": ["Verify service endpoints, DNS and NetworkPolicy for the target", "Check pod readiness and restart counts"],
    "dependency": ["Open the dependency's own incident view; enable circuit breaker fallback", "Shed non-critical load"],
    "database": ["Inspect DB connection pool usage and slow-query log", "Check for lock contention or a recent migration; consider failover"],
    "auth": ["Verify secret/token rotation and expiry", "Check recent IAM/RBAC or OIDC configuration changes"],
    "resource": ["Increase memory/CPU limits or scale replicas", "Look for leaks in the latest release; check node pressure/evictions"],
    "rate_limit": ["Apply exponential backoff; request quota increase", "Identify the burst source and throttle at the edge"],
    "kubernetes": ["kubectl describe the crashing pods; check image tag, probes and resource limits", "Roll back the last Deployment revision"],
    "build": ["Pin the broken dependency and re-run the pipeline", "Inspect the last commit touching the build configuration"],
    "test": ["Review the failing test output; revert or fix the regression", "Block the deploy until the pipeline is green"],
    "deploy": ["Roll back to the previous known-good release", "Validate config/secret changes shipped with the release"],
    "validation": ["Identify the client sending malformed requests; check API contract changes", "Add schema validation at the edge"],
    "origin": ["Focus investigation on the primary suspect service first", "Check its recent deploys, config changes and resource usage"],
}


class IncidentSummarizer:
    def __init__(self, settings: Settings | None = None):
        self.s = settings or get_settings()

    def summarize(self, incident: Incident, rca: dict[str, Any], anomalies: list[dict[str, Any]] | None = None) -> dict[str, Any]:
        fallback = self._deterministic(incident, rca, anomalies or [])
        if not self.s.ai_summaries_enabled:
            return fallback
        ai = self._ollama(incident, rca, anomalies or [])
        if not ai:
            return fallback
        merged = dict(fallback)
        for k in ("what_happened", "impacted_services", "possible_root_causes", "recommended_actions"):
            v = ai.get(k)
            if isinstance(v, str) and k == "what_happened" and len(v) > 20:
                merged[k] = v
            elif isinstance(v, list) and v and k != "what_happened":
                merged[k] = [str(x)[:300] for x in v][:8]
        merged["generated_by"] = f"ollama:{self.s.summary_model}"
        return merged

    # ── LLM path ────────────────────────────────────────────────────────────
    def _ollama(self, incident: Incident, rca: dict[str, Any], anomalies: list[dict[str, Any]]) -> dict[str, Any] | None:
        corr = rca.get("correlation", {})
        prompt = SUMMARY_PROMPT.format(
            title=incident.title, severity=incident.severity.value, started=incident.started_at.isoformat(),
            services=", ".join(incident.services),
            anomalies="; ".join(a.get("description", "") for a in anomalies[:6]) or "n/a",
            primary=rca.get("primary"), confidence=rca.get("primary_confidence"),
            hypotheses="; ".join(f"{h['cause']} ({h['confidence']})" for h in rca.get("hypotheses", [])[:5]) or "n/a",
            error_types=", ".join(corr.get("error_type_mix", {}).keys()) or "n/a",
            patterns="; ".join(p["template"][:100] for p in corr.get("top_error_patterns", [])[:4]) or "n/a",
            similar="; ".join(f"{s['incident_id']} ({s['similarity']:.0%})" for s in rca.get("similar_incidents", [])[:3]) or "none",
        )
        try:
            r = httpx.post(self.s.ollama_url, json={"model": self.s.summary_model, "prompt": prompt,
                                                   "stream": False, "format": "json"}, timeout=45)
            raw = r.json().get("response", "").strip()
            raw = raw.removeprefix("```json").removeprefix("```").removesuffix("```").strip()
            parsed = json.loads(raw)
            return parsed if isinstance(parsed, dict) else None
        except Exception as e:                                   # noqa: BLE001 — any failure => fallback
            log.debug("ollama summary unavailable: %s", e)
            return None

    # ── deterministic path ──────────────────────────────────────────────────
    def _deterministic(self, incident: Incident, rca: dict[str, Any], anomalies: list[dict[str, Any]]) -> dict[str, Any]:
        corr = rca.get("correlation", {})
        primary = rca.get("primary", incident.services[0] if incident.services else "unknown")
        impacted = rca.get("impacted_services") or incident.services
        kinds = sorted({a.get("kind", "") for a in anomalies if a.get("kind")})
        err_total = corr.get("total_errors", 0)
        leaders = corr.get("leading_services", [])
        etypes = [t for t, _ in sorted(corr.get("error_type_mix", {}).items(), key=lambda kv: -kv[1]["count"])]

        what = (f"Starting at {incident.started_at.strftime('%H:%M:%S UTC')}, {len(impacted)} service(s) "
                f"({', '.join(impacted[:5])}{'…' if len(impacted) > 5 else ''}) showed "
                f"{', '.join(k.replace('_', ' ') for k in kinds) or 'abnormal behaviour'} with {err_total} error events in the analysis window. ")
        if leaders:
            what += f"Errors appeared first in {', '.join(leaders[:3])}. "
        if etypes:
            what += f"Dominant failure classes: {', '.join(etypes[:4])}."

        causes = [f"{h['cause']} — {h['evidence']}" for h in rca.get("hypotheses", [])[:5]]
        if not causes:
            causes = [f"Fault likely originates in {primary}"]
        similar = rca.get("similar_incidents", [])
        if similar:
            s = similar[0]
            causes.append(f"Resembles past incident {s['incident_id']} ({s['similarity']:.0%} similar)"
                          + (f" whose root cause was {s['root_cause']}" if s.get("root_cause") else ""))

        actions: list[str] = []
        for h in rca.get("hypotheses", [])[:4]:
            for a in REMEDIATIONS.get(h["category"], []):
                if a not in actions:
                    actions.append(a)
        if similar and similar[0].get("remediation"):
            for a in similar[0]["remediation"]:
                if a not in actions:
                    actions.append(f"(worked before) {a}")
        if not actions:
            actions = [f"Investigate recent changes to {primary}", "Check dashboards for the impacted services",
                       "Consider rolling back the most recent deployment"]
        actions.append(f"Notify owners of: {', '.join(impacted[:5])}")

        return {
            "what_happened": what.strip(),
            "impacted_services": impacted,
            "possible_root_causes": causes[:6],
            "recommended_actions": actions[:8],
            "severity": incident.severity.value,
            "primary_suspect": primary,
            "confidence": rca.get("primary_confidence", 0.0),
            "generated_by": "deterministic",
        }
