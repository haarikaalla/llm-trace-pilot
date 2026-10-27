"""
parsers.py — Source-specific parsers. Each turns a raw record (dict or line)
into a flat dict of canonical fields consumed by the normalization stage.

Supported inputs
  application     : JSON logs (any logger), logfmt, "<ts> <LEVEL> [<logger>] msg"
  kubernetes      : CRI/containerd ("<ts> stdout F msg"), docker-json, k8s metadata envelopes
                    (as shipped by Fluent Bit / Vector: {"kubernetes": {...}, "log": "..."})
  infrastructure  : syslog RFC3164 / RFC5424, nginx/apache access logs, journald JSON
  cicd            : GitHub Actions, GitLab CI, Jenkins console output, generic "##[error]"
"""
from __future__ import annotations

import json
import re
from typing import Any

from ..domain.models import SourceType

LEVEL_RE = re.compile(r"\b(TRACE|DEBUG|INFO|INFORMATION|NOTICE|WARN|WARNING|ERROR|ERR|SEVERE|CRITICAL|CRIT|FATAL|PANIC|EMERG)\b", re.I)
ISO_TS_RE = re.compile(r"\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:[.,]\d+)?(?:Z|[+-]\d{2}:?\d{2})?")
SYSLOG_3164_RE = re.compile(
    r"^(?:<(?P<pri>\d{1,3})>)?(?P<ts>[A-Z][a-z]{2}\s+\d{1,2} \d{2}:\d{2}:\d{2}) (?P<host>\S+) (?P<app>[\w\-./]+)(?:\[(?P<pid>\d+)\])?:\s*(?P<msg>.*)$")
SYSLOG_5424_RE = re.compile(
    r"^<(?P<pri>\d{1,3})>\d (?P<ts>\S+) (?P<host>\S+) (?P<app>\S+) (?P<pid>\S+) (?P<msgid>\S+) (?P<sd>-|\[.*?\]) ?(?P<msg>.*)$")
ACCESS_LOG_RE = re.compile(
    r'^(?P<ip>\S+) \S+ \S+ \[(?P<ts>[^\]]+)\] "(?P<method>[A-Z]+) (?P<path>\S+) \S+" (?P<status>\d{3}) (?P<bytes>\S+)(?: "(?P<ref>[^"]*)" "(?P<ua>[^"]*)")?(?: (?P<rt>[\d.]+))?')
CRI_RE = re.compile(r"^(?P<ts>\S+) (?P<stream>stdout|stderr) (?P<flag>[PF]) (?P<msg>.*)$", re.S)
LOGFMT_RE = re.compile(r'(\w+)=("(?:[^"\\]|\\.)*"|\S+)')
APP_LINE_RE = re.compile(
    r"^(?P<ts>" + ISO_TS_RE.pattern + r")?\s*\[?(?P<level>[A-Z]{4,9})\]?\s*(?:\[(?P<logger>[^\]]+)\])?\s*[:\-]?\s*(?P<msg>.*)$", re.S)
GHA_RE = re.compile(r"^(?:(?P<ts>\S+Z)\s+)?##\[(?P<cmd>error|warning|notice|group|endgroup|section|debug)\](?P<msg>.*)$")
GITLAB_RE = re.compile(r"^(?:section_(?:start|end):\d+:(?P<section>\w+)\r?\x1b\[0K)?(?P<msg>.*)$")
JENKINS_RE = re.compile(r"^\[Pipeline\]\s*(?P<msg>.*)$|^\[(?P<stage>[^\]]+)\]\s+(?P<msg2>.*)$")
HTTP_STATUS_RE = re.compile(r"\b(?:status(?:_code)?|code|HTTP/\d\.\d\"?)\s*[=: ]\s*(\d{3})\b", re.I)
LATENCY_RE = re.compile(r"\b(?:latency|duration|took|elapsed|response_time|rt)\s*[=:]?\s*(\d+(?:\.\d+)?)\s*(ms|s|µs|us)?\b", re.I)
TRACE_ID_RE = re.compile(r"\b(?:trace[_\-]?id|traceparent|x-b3-traceid)\s*[=:]\s*\"?([0-9a-fA-F\-]{8,64})", re.I)
EXC_RE = re.compile(r"\b([A-Z][A-Za-z0-9]*(?:Exception|Error|Timeout|Fault|Panic|Refused|Denied))\b")


def _level_from_text(text: str) -> str | None:
    m = LEVEL_RE.search(text or "")
    return m.group(1).upper() if m else None


def _try_json(line: str) -> dict | None:
    line = line.strip()
    if not line.startswith("{") or not line.endswith("}"):
        return None
    try:
        obj = json.loads(line)
        return obj if isinstance(obj, dict) else None
    except json.JSONDecodeError:
        return None


def _logfmt(line: str) -> dict | None:
    pairs = LOGFMT_RE.findall(line)
    if len(pairs) < 2 or "=" not in line:
        return None
    out = {}
    for k, v in pairs:
        out[k] = v.strip('"')
    if "msg" not in out and "message" not in out:
        return None
    return out


def _extract_common(fields: dict, text: str) -> None:
    """Pull status code, latency, trace id and exception type out of free text."""
    if "status_code" not in fields:
        m = HTTP_STATUS_RE.search(text)
        if m:
            fields["status_code"] = int(m.group(1))
    if "latency_ms" not in fields:
        m = LATENCY_RE.search(text)
        if m:
            val, unit = float(m.group(1)), (m.group(2) or "ms").lower()
            if unit == "s":
                val *= 1000
            elif unit in ("µs", "us"):
                val /= 1000
            fields["latency_ms"] = val
    if "trace_id" not in fields:
        m = TRACE_ID_RE.search(text)
        if m:
            fields["trace_id"] = m.group(1)
    if "error_type" not in fields:
        m = EXC_RE.search(text)
        if m:
            fields["error_type"] = m.group(1)


# ── parsers ─────────────────────────────────────────────────────────────────
class ApplicationLogParser:
    source_type = SourceType.APPLICATION

    def parse(self, record: dict[str, Any]) -> dict[str, Any]:
        line = str(record.get("message") or record.get("log") or record.get("msg") or "")
        fields: dict[str, Any] = {}
        obj = _try_json(line)
        if obj:
            fields.update(_flatten_structured(obj))
        else:
            lf = _logfmt(line)
            if lf:
                fields.update(_flatten_structured(lf))
            else:
                m = APP_LINE_RE.match(line)
                if m and m.group("level") and _level_from_text(m.group("level")):
                    fields["timestamp"] = m.group("ts") or record.get("timestamp")
                    fields["level"] = m.group("level")
                    fields["message"] = m.group("msg").strip()
                    if m.group("logger"):
                        fields["logger"] = m.group("logger")
                else:
                    fields["message"] = line
                    lvl = _level_from_text(line)
                    if lvl:
                        fields["level"] = lvl
        _extract_common(fields, line)
        return fields


class KubernetesLogParser:
    source_type = SourceType.KUBERNETES
    _app = ApplicationLogParser()

    def parse(self, record: dict[str, Any]) -> dict[str, Any]:
        fields: dict[str, Any] = {}
        k8s = record.get("kubernetes") or {}
        if k8s:
            fields["namespace"] = k8s.get("namespace_name") or k8s.get("namespace")
            fields["pod"] = k8s.get("pod_name") or k8s.get("pod")
            fields["container"] = k8s.get("container_name") or k8s.get("container")
            fields["host"] = k8s.get("host") or k8s.get("node_name")
            labels = k8s.get("labels") or {}
            svc = (labels.get("app.kubernetes.io/name") or labels.get("app")
                   or labels.get("k8s-app") or fields.get("container"))
            if svc:
                fields["service"] = svc
            if labels.get("app.kubernetes.io/version"):
                fields["version"] = labels["app.kubernetes.io/version"]
        line = str(record.get("log") or record.get("message") or "")
        m = CRI_RE.match(line)
        if m:
            fields["timestamp"] = m.group("ts")
            fields["stream"] = m.group("stream")
            line = m.group("msg")
            if m.group("stream") == "stderr":
                fields.setdefault("level", "ERROR")
        inner = self._app.parse({"message": line})
        if "service" in fields:
            inner.pop("service", None)
        fields.update({k: v for k, v in inner.items() if v is not None})
        if "message" not in fields:
            fields["message"] = line.rstrip("\n")
        if "pod" in fields and "service" not in fields and fields["pod"]:
            fields["service"] = re.sub(r"(-[a-f0-9]{6,10})?-[a-z0-9]{5}$", "", fields["pod"])
        return fields


class InfrastructureLogParser:
    source_type = SourceType.INFRASTRUCTURE

    def parse(self, record: dict[str, Any]) -> dict[str, Any]:
        line = str(record.get("message") or record.get("log") or "")
        fields: dict[str, Any] = {}
        obj = _try_json(line)
        if obj:  # journald / cloud-init JSON
            fields.update(_flatten_structured(obj))
            if "PRIORITY" in obj:
                fields["level"] = _syslog_severity(int(obj["PRIORITY"]))
            if "_HOSTNAME" in obj:
                fields["host"] = obj["_HOSTNAME"]
            if "SYSLOG_IDENTIFIER" in obj:
                fields["service"] = obj["SYSLOG_IDENTIFIER"]
            if "MESSAGE" in obj:
                fields["message"] = obj["MESSAGE"]
        elif (m := SYSLOG_5424_RE.match(line)):
            fields.update(timestamp=m.group("ts"), host=m.group("host"), service=m.group("app"),
                          message=m.group("msg"), level=_syslog_severity(int(m.group("pri")) % 8))
        elif (m := SYSLOG_3164_RE.match(line)):
            fields.update(timestamp=m.group("ts"), host=m.group("host"), service=m.group("app"),
                          message=m.group("msg"))
            if m.group("pri"):
                fields["level"] = _syslog_severity(int(m.group("pri")) % 8)
        elif (m := ACCESS_LOG_RE.match(line)):
            status = int(m.group("status"))
            fields.update(timestamp=m.group("ts"), status_code=status, client_ip=m.group("ip"),
                          http_method=m.group("method"), http_path=m.group("path"),
                          message=f'{m.group("method")} {m.group("path")} {status}',
                          level="ERROR" if status >= 500 else "WARN" if status >= 400 else "INFO")
            if m.group("rt"):
                fields["latency_ms"] = float(m.group("rt")) * 1000
        else:
            fields["message"] = line
        if "level" not in fields:
            lvl = _level_from_text(fields.get("message", ""))
            if lvl:
                fields["level"] = lvl
        _extract_common(fields, line)
        return fields


class CICDLogParser:
    source_type = SourceType.CICD

    def parse(self, record: dict[str, Any]) -> dict[str, Any]:
        line = str(record.get("message") or record.get("log") or "")
        fields: dict[str, Any] = {
            "pipeline": record.get("pipeline") or record.get("workflow") or record.get("job_name"),
            "job": record.get("job"),
            "stage": record.get("stage") or record.get("step"),
        }
        if (m := GHA_RE.match(line)):
            cmd = m.group("cmd")
            fields["message"] = m.group("msg").strip()
            fields["level"] = {"error": "ERROR", "warning": "WARN", "debug": "DEBUG"}.get(cmd, "INFO")
            if m.group("ts"):
                fields["timestamp"] = m.group("ts")
            if cmd in ("group", "section"):
                fields["stage"] = fields["stage"] or fields["message"]
        elif line.startswith("section_") and (m := GITLAB_RE.match(line)):
            fields["stage"] = fields["stage"] or m.group("section")
            fields["message"] = m.group("msg")
        elif (m := JENKINS_RE.match(line)):
            fields["message"] = (m.group("msg") or m.group("msg2") or "").strip()
            if m.group("stage"):
                fields["stage"] = fields["stage"] or m.group("stage")
        else:
            fields["message"] = line
        msg = fields.get("message", "")
        if "level" not in fields:
            if re.search(r"\b(FAILED|FAILURE|failed|error|Error|ERROR|exit code [1-9]|Traceback)\b", msg):
                fields["level"] = "ERROR"
            elif re.search(r"\b(warning|WARN|deprecated)\b", msg, re.I):
                fields["level"] = "WARN"
            else:
                fields["level"] = "INFO"
        if (m := re.search(r"\b(?:Step|Stage|Job)\s+[\"']?([\w\-\s/]+?)[\"']?\s+(?:failed|succeeded|passed|completed)", msg, re.I)):
            fields["stage"] = fields["stage"] or m.group(1).strip()
        if (m := re.search(r"\b(?:exit code|exited with)\s*(\d+)", msg, re.I)):
            fields["exit_code"] = int(m.group(1))
        if (m := re.search(r"\b(?:took|in|duration)\s+(\d+)m\s*(\d+)s", msg)):
            fields["latency_ms"] = (int(m.group(1)) * 60 + int(m.group(2))) * 1000
        _extract_common(fields, line)
        fields["service"] = record.get("service") or fields.get("pipeline") or "ci"
        return {k: v for k, v in fields.items() if v is not None}


# ── helpers ─────────────────────────────────────────────────────────────────
_SEVERITY_NAMES = ["FATAL", "FATAL", "FATAL", "ERROR", "WARN", "INFO", "INFO", "DEBUG"]


def _syslog_severity(sev: int) -> str:
    return _SEVERITY_NAMES[max(0, min(7, sev))]


_FIELD_ALIASES = {
    "message": ("message", "msg", "log", "MESSAGE", "event", "text"),
    "level": ("level", "severity", "lvl", "log.level", "loglevel", "levelname", "@level"),
    "timestamp": ("timestamp", "time", "ts", "@timestamp", "asctime", "date", "eventTime"),
    "service": ("service", "service.name", "app", "application", "component", "logger_name", "program"),
    "host": ("host", "hostname", "host.name", "node"),
    "trace_id": ("trace_id", "traceId", "trace.id", "traceID", "dd.trace_id"),
    "span_id": ("span_id", "spanId", "span.id"),
    "status_code": ("status_code", "status", "http.status_code", "response_code", "code"),
    "latency_ms": ("latency_ms", "duration_ms", "response_time_ms", "elapsed_ms"),
    "error_type": ("error_type", "exception", "error.type", "exc_type", "error_class"),
    "environment": ("environment", "env", "deployment.environment", "stage_env"),
    "namespace": ("namespace", "kubernetes.namespace_name"),
    "pod": ("pod", "pod_name", "kubernetes.pod_name"),
    "container": ("container", "container_name"),
}


def _flatten_structured(obj: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for canon, aliases in _FIELD_ALIASES.items():
        for a in aliases:
            if a in obj and obj[a] not in (None, ""):
                out[canon] = obj[a]
                break
    if "status_code" in out:
        try:
            out["status_code"] = int(out["status_code"])
        except (TypeError, ValueError):
            del out["status_code"]
    if "latency_ms" in out:
        try:
            out["latency_ms"] = float(out["latency_ms"])
        except (TypeError, ValueError):
            del out["latency_ms"]
    if "duration" in obj and "latency_ms" not in out:
        try:
            out["latency_ms"] = float(obj["duration"])
        except (TypeError, ValueError):
            pass
    if "message" not in out:
        out["message"] = json.dumps(obj, default=str)[:2000]
    if "exception" in obj and isinstance(obj["exception"], dict):
        out["error_type"] = obj["exception"].get("type") or out.get("error_type")
    used = {a for al in _FIELD_ALIASES.values() for a in al}
    out["attributes"] = {k: v for k, v in obj.items() if k not in used and not k.startswith("_")}
    return out


_PARSERS = {
    SourceType.APPLICATION: ApplicationLogParser(),
    SourceType.KUBERNETES: KubernetesLogParser(),
    SourceType.INFRASTRUCTURE: InfrastructureLogParser(),
    SourceType.CICD: CICDLogParser(),
}


def detect_source(record: dict[str, Any]) -> SourceType:
    declared = record.get("source_type") or record.get("source")
    if declared:
        try:
            return SourceType(str(declared).lower())
        except ValueError:
            pass
    if "kubernetes" in record or record.get("pod") or record.get("pod_name"):
        return SourceType.KUBERNETES
    if record.get("pipeline") or record.get("workflow") or record.get("job"):
        return SourceType.CICD
    line = str(record.get("message") or record.get("log") or "")
    if GHA_RE.match(line) or line.startswith("[Pipeline]") or line.startswith("section_start"):
        return SourceType.CICD
    if CRI_RE.match(line):
        return SourceType.KUBERNETES
    if SYSLOG_5424_RE.match(line) or SYSLOG_3164_RE.match(line) or ACCESS_LOG_RE.match(line):
        return SourceType.INFRASTRUCTURE
    if line.startswith("{") and '"PRIORITY"' in line:
        return SourceType.INFRASTRUCTURE
    return SourceType.APPLICATION


def parse_record(record: dict[str, Any]) -> dict[str, Any]:
    """Detect source, run the matching parser, keep envelope fields as defaults."""
    src = detect_source(record)
    parser = _PARSERS.get(src, _PARSERS[SourceType.APPLICATION])
    parsed = parser.parse(record)
    # shipper-declared service/environment are authoritative; other envelope fields only fill gaps
    for k in ("service", "environment"):
        if record.get(k) not in (None, ""):
            parsed[k] = record[k]
    for k in ("host", "timestamp", "level", "trace_id", "namespace",
              "pod", "container", "pipeline", "job", "stage", "latency_ms", "status_code", "error_type"):
        if k in record and record[k] not in (None, "") and parsed.get(k) in (None, ""):
            parsed[k] = record[k]
    extra_attrs = record.get("attributes") or {}
    if isinstance(extra_attrs, dict):
        parsed.setdefault("attributes", {}).update(extra_attrs)
    parsed["source_type"] = src.value
    parsed["raw"] = str(record.get("message") or record.get("log") or "")[:4000]
    return parsed
