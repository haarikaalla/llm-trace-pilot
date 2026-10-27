"""Pipeline tests: parsers for each source, stages, redaction, fingerprinting."""
import json

import pytest

from observability.domain.models import Severity, SourceType
from observability.ingestion.parsers import detect_source, parse_record
from observability.pipeline import LogProcessor
from observability.pipeline.stages import CleaningStage, EnrichmentStage, NormalizationStage


@pytest.fixture
def proc(obs_env):
    return LogProcessor(obs_env)


# ── source detection ────────────────────────────────────────────────────────
@pytest.mark.parametrize("record,expected", [
    ({"message": "2025-01-01T00:00:00Z ERROR boom"}, SourceType.APPLICATION),
    ({"log": "2025-01-01T00:00:00.1Z stdout F hello", "kubernetes": {"pod_name": "x-1"}}, SourceType.KUBERNETES),
    ({"message": "<34>Oct 11 22:14:15 host su: failed"}, SourceType.INFRASTRUCTURE),
    ({"message": '1.2.3.4 - - [27/Oct/2025:10:00:05 +0000] "GET / HTTP/1.1" 200 5'}, SourceType.INFRASTRUCTURE),
    ({"message": "##[error]Process completed with exit code 1."}, SourceType.CICD),
    ({"message": "[Pipeline] sh", "pipeline": "p"}, SourceType.CICD),
    ({"message": "x", "source_type": "infrastructure"}, SourceType.INFRASTRUCTURE),
])
def test_detect_source(record, expected):
    assert detect_source(record) == expected


# ── application parser ──────────────────────────────────────────────────────
def test_json_application_log(proc):
    rec = {"level": "error", "msg": "db down", "service": "Orders", "duration_ms": 123, "status": 500,
           "trace_id": "abc", "extra": 1}
    ev = proc.process_one({"message": json.dumps(rec)})
    assert ev.service == "orders"
    assert ev.level == Severity.ERROR
    assert ev.latency_ms == 123
    assert ev.status_code == 500
    assert ev.trace_id == "abc"
    assert ev.attributes["extra"] == 1


def test_plain_application_log_level_and_exception(proc):
    ev = proc.process_one({"message": "2025-10-27T10:00:01Z WARN [svc] Retrying after TimeoutError latency=250ms", "service": "a"})
    assert ev.level == Severity.WARN
    assert ev.error_type == "TimeoutError"
    assert ev.latency_ms == 250
    assert ev.timestamp.year == 2025


def test_logfmt(proc):
    ev = proc.process_one({"message": 'level=error msg="payment failed" service=pay status=502'})
    assert ev.level == Severity.ERROR and ev.service == "pay" and ev.status_code == 502


# ── kubernetes parser ───────────────────────────────────────────────────────
def test_kubernetes_cri_with_metadata(proc):
    ev = proc.process_one({
        "log": "2025-10-27T10:00:02.123Z stderr F panic: connection refused to 10.0.0.5:5432",
        "kubernetes": {"namespace_name": "prod", "pod_name": "orders-7d9f8b6c5-x2k9p", "container_name": "orders",
                       "labels": {"app": "orders"}, "host": "node-1"}})
    assert ev.source_type == SourceType.KUBERNETES
    assert ev.service == "orders" and ev.namespace == "prod" and ev.pod.startswith("orders-") and ev.host == "node-1"
    assert ev.level.is_error
    assert ev.error_type == "connection"
    assert "<ip>" in ev.template
    assert ev.attributes["workload"] == "orders"


def test_kubernetes_event_messages_classified(proc):
    ev = proc.process_one({"log": "Back-off restarting failed container", "kubernetes": {"pod_name": "api-abc12"}})
    assert ev.error_type == "kubernetes" and ev.level.is_error


# ── infrastructure parser ───────────────────────────────────────────────────
def test_syslog_3164(proc):
    ev = proc.process_one({"message": "<34>Oct 27 10:00:03 node-1 kernel: Out of memory: Kill process 1234 (java)"})
    assert ev.source_type == SourceType.INFRASTRUCTURE
    assert ev.host == "node-1" and ev.service == "kernel"
    assert ev.level == Severity.FATAL        # pri 34 => severity 2 (critical)
    assert ev.error_type == "resource"


def test_syslog_5424(proc):
    ev = proc.process_one({"message": "<165>1 2025-10-27T10:00:00Z host1 sshd 123 ID47 - Failed password"})
    assert ev.host == "host1" and ev.service == "sshd" and ev.level == Severity.INFO


def test_access_log(proc):
    ev = proc.process_one({"message": '192.168.1.10 - - [27/Oct/2025:10:00:05 +0000] "GET /api/items HTTP/1.1" 503 12 "-" "curl" 0.412'})
    assert ev.status_code == 503 and ev.level == Severity.ERROR and ev.latency_ms == pytest.approx(412)
    assert ev.attributes["http_path"] == "/api/items"


def test_journald_json(proc):
    ev = proc.process_one({"message": json.dumps({"PRIORITY": "3", "_HOSTNAME": "n2", "SYSLOG_IDENTIFIER": "dockerd", "MESSAGE": "oom"})})
    assert ev.level == Severity.ERROR and ev.host == "n2" and ev.service == "dockerd"


# ── cicd parser ─────────────────────────────────────────────────────────────
def test_github_actions_error(proc):
    ev = proc.process_one({"message": "##[error]Process completed with exit code 1.", "pipeline": "build-api", "job": "test"})
    assert ev.source_type == SourceType.CICD
    assert ev.level == Severity.ERROR and ev.pipeline == "build-api" and ev.service == "build-api"
    assert ev.attributes["exit_code"] == 1 and ev.attributes["ci_outcome"] == "failed"


def test_jenkins_stage(proc):
    ev = proc.process_one({"message": "[Pipeline] { (Deploy)", "pipeline": "svc"})
    assert ev.source_type == SourceType.CICD and ev.level == Severity.INFO


# ── stages ──────────────────────────────────────────────────────────────────
def test_cleaning_strips_ansi_and_drops_empty(obs_env):
    c = CleaningStage(obs_env)
    assert c({"message": "\x1b[31mERROR\x1b[0m   x"})["message"] == "ERROR x"
    assert c({"message": "\x1b[0m"}) is None


def test_normalization_maps_levels_and_status():
    n = NormalizationStage()
    r = n({"message": "m", "level": "warning", "service": "My Svc!", "status_code": "503"})
    assert r["level"] == Severity.ERROR and r["service"] == "my-svc-"
    r = n({"message": "m", "level": "bogus", "timestamp": "nonsense"})
    assert r["level"] == Severity.INFO and r["timestamp"] is not None


def test_enrichment_redacts_pii(obs_env):
    ev = LogProcessor(obs_env).process_one({"message": "ERROR login failed for john@example.com token=abcdef123456 card 4111 1111 1111 1111"})
    assert "john@example.com" not in ev.message
    assert "abcdef123456" not in ev.message
    assert "4111" not in ev.message
    assert "<email>" in ev.message


def test_fingerprint_stable_across_variables(obs_env):
    p = LogProcessor(obs_env)
    a = p.process_one({"message": "ERROR user 123 failed from 10.0.0.1 in 250ms", "service": "s"})
    b = p.process_one({"message": "ERROR user 999 failed from 10.9.9.9 in 12ms", "service": "s"})
    c = p.process_one({"message": "ERROR user 999 failed from 10.9.9.9 in 12ms", "service": "other"})
    assert a.fingerprint == b.fingerprint
    assert a.fingerprint != c.fingerprint
    assert a.template == "user <num> failed from <ip> in <num>"


def test_severity_score_ordering(obs_env):
    p = LogProcessor(obs_env)
    lv = {l: p.process_one({"message": f"{l} x"}).severity_score for l in ("DEBUG", "INFO", "WARN", "ERROR", "FATAL")}
    assert lv["DEBUG"] < lv["INFO"] < lv["WARN"] < lv["ERROR"] < lv["FATAL"]


def test_batch_isolates_poison_records(proc):
    events, dlq = proc.process_batch([{"message": "ok"}, {"message": 123}, {"message": ""}])
    assert len(events) == 2 and dlq == [] and proc.metrics()["dropped"] == 1
