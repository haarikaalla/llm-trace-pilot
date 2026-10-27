"""
tracer_bridge.py — Feed AgentScope LLM traces into the observability pipeline.

Every `@trace`d call becomes a LogEvent (source_type=llm_trace, service=<step name>),
so LLM agents get the same anomaly detection, incident grouping and RCA as any
other service — e.g. a latency spike in `answer_question` or an error burst from
`gpt-4o` timeouts.
"""
from __future__ import annotations

from datetime import datetime, timezone

from ..domain.models import LogEvent, Severity, SourceType
from ..pipeline.stages import EnrichmentStage


def trace_to_event(rec: dict) -> LogEvent:
    err = rec.get("error")
    msg = (f"LLM call failed: {err}" if err
           else f"LLM call ok model={rec.get('model')} tokens={rec.get('input_tokens', 0) + rec.get('output_tokens', 0)}")
    service = f"llm.{rec.get('name', 'unknown')}"
    ev = LogEvent(
        message=msg,
        service=service,
        level=Severity.ERROR if err else Severity.INFO,
        source_type=SourceType.LLM_TRACE,
        timestamp=datetime.now(timezone.utc),
        trace_id=rec.get("session_id") or rec.get("trace_id"),
        span_id=rec.get("trace_id"),
        latency_ms=float(rec.get("latency_ms") or 0),
        error_type=_classify(err) if err else None,
        attributes={"model": rec.get("model"), "step_num": rec.get("step_num"), "cost_usd": rec.get("cost_usd"),
                    "input_tokens": rec.get("input_tokens"), "output_tokens": rec.get("output_tokens"),
                    "tags": rec.get("tags", [])},
    )
    ev.template = EnrichmentStage.template_of(msg)
    import hashlib
    ev.fingerprint = hashlib.sha1(f"{service}|{ev.template}".encode()).hexdigest()[:16]
    ev.severity_score = 0.7 if err else 0.05
    return ev


def _classify(err: str) -> str:
    e = err.lower()
    if "timeout" in e:
        return "timeout"
    if "rate" in e and "limit" in e or "429" in e:
        return "rate_limit"
    if "auth" in e or "401" in e or "api key" in e:
        return "auth"
    if "connection" in e:
        return "connection"
    return "llm"


def enable_tracer_bridge(platform=None) -> None:
    """Call once at startup: `from observability.integrations import enable_tracer_bridge; enable_tracer_bridge()`."""
    from tracer.core import register_trace_hook
    from ..services.platform import get_platform
    p = platform or get_platform()
    register_trace_hook(lambda rec: p.ingest_events([trace_to_event(rec)]))
