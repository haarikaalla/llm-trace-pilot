"""
stages.py — The four ETL stages. Each is a pure callable: dict -> dict (or None
to drop). The processor chains them; each stage is independently unit-testable.
"""
from __future__ import annotations

import hashlib
import re
from typing import Any

from ..config import Settings, get_settings
from ..domain.models import Severity, SourceType, parse_ts, utcnow
from ..ingestion.parsers import parse_record

ANSI_RE = re.compile(r"\x1b\[[0-9;?]*[ -/]*[@-~]")
CTRL_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")
WS_RE = re.compile(r"[ \t]{2,}")

# ── PII / secret redaction (security boundary) ──────────────────────────────
REDACTIONS = [
    (re.compile(r"[\w.+-]+@[\w-]+\.[\w.-]+"), "<email>"),
    (re.compile(r"\b(?:\d[ -]*?){13,19}\b"), "<card>"),
    (re.compile(r"(?i)\b(bearer|token|api[_-]?key|password|passwd|secret|authorization)\b(\s*[:=]\s*|\s+)[\"']?[\w\-./+=]{6,}"), r"\1\2<redacted>"),
    (re.compile(r"\b(?:AKIA|ASIA)[0-9A-Z]{16}\b"), "<aws-key>"),
    (re.compile(r"\bgh[pousr]_[A-Za-z0-9]{20,}\b"), "<gh-token>"),
    (re.compile(r"\beyJ[\w-]{10,}\.[\w-]{10,}\.[\w-]{10,}\b"), "<jwt>"),
]

# ── template extraction (variables -> placeholders) ─────────────────────────
TEMPLATE_RULES = [
    (re.compile(r"\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b", re.I), "<uuid>"),
    (re.compile(r"\b(?:\d{1,3}\.){3}\d{1,3}(?::\d+)?\b"), "<ip>"),
    (re.compile(r"\b[0-9a-f]{24,64}\b", re.I), "<hash>"),
    (re.compile(r"\b\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}[^\s]*"), "<ts>"),
    (re.compile(r"https?://[^\s\"']+"), "<url>"),
    (re.compile(r"(?<=/)[\w\-]*\d[\w\-]*(?=/|\b)"), "<id>"),
    (re.compile(r"\b\d+(?:\.\d+)?(?:ms|s|MB|GB|KB|%)?\b"), "<num>"),
    (re.compile(r"\b0x[0-9a-f]+\b", re.I), "<hex>"),
]

_LEVEL_MAP = {
    "TRACE": Severity.DEBUG, "DEBUG": Severity.DEBUG, "FINE": Severity.DEBUG,
    "INFO": Severity.INFO, "INFORMATION": Severity.INFO, "NOTICE": Severity.INFO, "I": Severity.INFO,
    "WARN": Severity.WARN, "WARNING": Severity.WARN, "W": Severity.WARN,
    "ERROR": Severity.ERROR, "ERR": Severity.ERROR, "SEVERE": Severity.ERROR, "E": Severity.ERROR,
    "CRITICAL": Severity.FATAL, "CRIT": Severity.FATAL, "FATAL": Severity.FATAL,
    "PANIC": Severity.FATAL, "EMERG": Severity.FATAL, "ALERT": Severity.FATAL, "F": Severity.FATAL,
}

ERROR_CLASSES = [
    ("timeout", re.compile(r"\b(timed? ?out|deadline exceeded|context deadline)\b", re.I)),
    ("connection", re.compile(r"\b(connection (refused|reset|closed)|ECONNREFUSED|ECONNRESET|broken pipe|unreachable|no route to host)\b", re.I)),
    ("dependency", re.compile(r"\b(upstream|downstream|circuit (breaker )?open|service unavailable|503|bad gateway|502)\b", re.I)),
    ("database", re.compile(r"\b(deadlock|too many connections|sql|query failed|duplicate key|constraint|ORA-\d+|pg_|mysql)\b", re.I)),
    ("auth", re.compile(r"\b(unauthori[sz]ed|forbidden|401|403|invalid token|permission denied|access denied)\b", re.I)),
    ("resource", re.compile(r"\b(out of memory|OOMKilled|OOM|disk full|no space left|too many open files|throttl|quota exceeded|CPU throttl)\b", re.I)),
    ("rate_limit", re.compile(r"\b(rate limit|429|too many requests)\b", re.I)),
    ("kubernetes", re.compile(r"\b(CrashLoopBackOff|ImagePullBackOff|ErrImagePull|Evicted|FailedScheduling|Back-off restarting|Liveness probe failed|Readiness probe failed)\b")),
    ("build", re.compile(r"\b(compilation failed|build failed|cannot find module|ModuleNotFoundError|npm ERR!|exit code [1-9])\b", re.I)),
    ("test", re.compile(r"\b(tests? failed|assertion|AssertionError|FAILED tests)\b", re.I)),
    ("deploy", re.compile(r"\b(rollout failed|rollback|helm (upgrade|install) failed|deployment failed)\b", re.I)),
    ("validation", re.compile(r"\b(validation (error|failed)|invalid (input|request|argument)|400 bad request|schema)\b", re.I)),
]


class CleaningStage:
    """Strip ANSI colour codes, control chars, excess whitespace; cap length."""

    def __init__(self, settings: Settings | None = None):
        self.s = settings or get_settings()

    def __call__(self, rec: dict[str, Any]) -> dict[str, Any] | None:
        for key in ("message", "log", "msg"):
            if key in rec and isinstance(rec[key], str):
                text = ANSI_RE.sub("", rec[key])
                text = CTRL_RE.sub("", text)
                text = WS_RE.sub(" ", text).strip()
                if len(text) > self.s.max_message_bytes:
                    text = text[: self.s.max_message_bytes] + "…[truncated]"
                rec[key] = text
        if not (rec.get("message") or rec.get("log") or rec.get("msg")):
            return None
        return rec


class ParsingStage:
    def __call__(self, rec: dict[str, Any]) -> dict[str, Any] | None:
        parsed = parse_record(rec)
        parsed["received_at"] = rec.get("received_at")
        return parsed


class NormalizationStage:
    """Canonical types: ISO UTC timestamps, Severity enum, lowercase service ids."""

    def __call__(self, rec: dict[str, Any]) -> dict[str, Any] | None:
        ts = parse_ts(rec.get("timestamp")) or parse_ts(rec.get("received_at")) or utcnow()
        rec["timestamp"] = ts
        lvl = str(rec.get("level") or "INFO").strip().upper()
        rec["level"] = _LEVEL_MAP.get(lvl, Severity.INFO)
        if rec.get("status_code") is not None and rec["level"].rank < Severity.ERROR.rank:
            try:
                sc = int(rec["status_code"])
                if sc >= 500:
                    rec["level"] = Severity.ERROR
                elif sc >= 400 and rec["level"] == Severity.INFO:
                    rec["level"] = Severity.WARN
            except (TypeError, ValueError):
                rec["status_code"] = None
        svc = str(rec.get("service") or "unknown").strip().lower()
        rec["service"] = re.sub(r"[^a-z0-9._\-/]", "-", svc)[:120] or "unknown"
        rec["environment"] = str(rec.get("environment") or "default").lower()
        rec["message"] = str(rec.get("message") or "").strip()
        try:
            rec["source_type"] = SourceType(rec.get("source_type", "unknown"))
        except ValueError:
            rec["source_type"] = SourceType.UNKNOWN
        if rec.get("latency_ms") is not None:
            try:
                rec["latency_ms"] = float(rec["latency_ms"])
            except (TypeError, ValueError):
                rec["latency_ms"] = None
        return rec


class EnrichmentStage:
    """Add fingerprint/template, error classification, severity score, redaction."""

    def __init__(self, settings: Settings | None = None):
        self.s = settings or get_settings()

    @staticmethod
    def template_of(message: str) -> str:
        t = message
        for rx, repl in TEMPLATE_RULES:
            t = rx.sub(repl, t)
        return t[:500]

    @staticmethod
    def redact(text: str) -> str:
        for rx, repl in REDACTIONS:
            text = rx.sub(repl, text)
        return text

    def __call__(self, rec: dict[str, Any]) -> dict[str, Any] | None:
        msg = rec.get("message", "")
        if self.s.redact_pii:
            msg = self.redact(msg)
            rec["message"] = msg
            if rec.get("raw"):
                rec["raw"] = self.redact(rec["raw"])
        template = self.template_of(msg)
        rec["template"] = template
        rec["fingerprint"] = hashlib.sha1(f"{rec['service']}|{template}".encode()).hexdigest()[:16]

        level: Severity = rec["level"]
        if not rec.get("error_type"):
            for name, rx in ERROR_CLASSES:
                if rx.search(msg):
                    # runtime-failure classes are errors even when the shipper gave no level
                    if level.is_error or name in ("kubernetes", "resource"):
                        rec["error_type"] = name
                    break
        if rec.get("error_type") in ("kubernetes", "resource") and not level.is_error:
            rec["level"] = level = Severity.ERROR

        score = {Severity.DEBUG: 0.0, Severity.INFO: 0.05, Severity.WARN: 0.35,
                 Severity.ERROR: 0.7, Severity.FATAL: 1.0}[level]
        if rec.get("error_type") in ("resource", "kubernetes", "database"):
            score = min(1.0, score + 0.15)
        if rec.get("status_code") and int(rec["status_code"]) >= 500:
            score = max(score, 0.75)
        rec["severity_score"] = round(score, 3)

        attrs = rec.get("attributes") or {}
        if rec.get("source_type") == SourceType.KUBERNETES and rec.get("pod"):
            attrs.setdefault("workload", re.sub(r"(-[a-f0-9]{6,10})?-[a-z0-9]{5}$", "", rec["pod"]))
        if rec.get("source_type") == SourceType.CICD:
            attrs.setdefault("ci_outcome", "failed" if level.is_error else "ok")
        for k in ("logger", "stream", "client_ip", "http_method", "http_path", "exit_code", "version"):
            if k in rec and rec[k] is not None:
                attrs[k] = rec.pop(k)
        rec["attributes"] = attrs
        return rec
