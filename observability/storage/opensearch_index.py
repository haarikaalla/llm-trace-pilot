"""
opensearch_index.py — LogIndex adapter on OpenSearch / Elasticsearch 7.x.

* Daily indices  <prefix>-YYYY.MM.DD  behind an index template + ISM-friendly alias
* Bulk API with chunking; refresh is left to the index interval for throughput
* `opensearch-py` is imported lazily so dev installs don't need it
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Iterable, Iterator

from ..config import Settings, get_settings
from ..domain.models import LogEvent, to_iso
from ..domain.ports import LogIndex

log = logging.getLogger(__name__)

INDEX_TEMPLATE = {
    "index_patterns": ["{prefix}-*"],
    "template": {
        "settings": {
            "number_of_shards": 3,
            "number_of_replicas": 1,
            "refresh_interval": "5s",
            "codec": "best_compression",
        },
        "mappings": {
            "dynamic": False,
            "properties": {
                "event_id": {"type": "keyword"},
                "timestamp": {"type": "date"},
                "service": {"type": "keyword"},
                "environment": {"type": "keyword"},
                "level": {"type": "keyword"},
                "source_type": {"type": "keyword"},
                "message": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 1024}}},
                "template": {"type": "keyword", "ignore_above": 1024},
                "fingerprint": {"type": "keyword"},
                "host": {"type": "keyword"}, "namespace": {"type": "keyword"},
                "pod": {"type": "keyword"}, "container": {"type": "keyword"},
                "trace_id": {"type": "keyword"}, "span_id": {"type": "keyword"},
                "pipeline": {"type": "keyword"}, "job": {"type": "keyword"}, "stage": {"type": "keyword"},
                "status_code": {"type": "short"},
                "latency_ms": {"type": "float"},
                "error_type": {"type": "keyword"},
                "severity_score": {"type": "float"},
                "attributes": {"type": "flat_object"},
                "raw": {"type": "text", "index": False},
            },
        },
    },
}


class OpenSearchLogIndex(LogIndex):
    def __init__(self, settings: Settings | None = None, client=None):
        self.s = settings or get_settings()
        self.prefix = self.s.opensearch_index_prefix
        self._client = client
        self._ready = False

    @property
    def client(self):
        if self._client is None:
            from opensearchpy import OpenSearch  # lazy import
            auth = (self.s.opensearch_user, self.s.opensearch_password) if self.s.opensearch_user else None
            self._client = OpenSearch(
                hosts=[self.s.opensearch_url], http_auth=auth,
                verify_certs=self.s.opensearch_verify_certs, timeout=30, max_retries=3, retry_on_timeout=True,
            )
        return self._client

    def ensure_ready(self) -> None:
        if self._ready:
            return
        tpl = dict(INDEX_TEMPLATE)
        tpl["index_patterns"] = [f"{self.prefix}-*"]
        try:
            self.client.indices.put_index_template(name=f"{self.prefix}-template", body=tpl)
        except Exception as e:                       # noqa: BLE001 — ES7 fallback
            log.warning("index template API failed (%s); trying legacy template", e)
            legacy = {"index_patterns": tpl["index_patterns"], **tpl["template"]}
            legacy["mappings"]["properties"]["attributes"] = {"type": "object", "enabled": False}
            self.client.indices.put_template(name=f"{self.prefix}-template", body=legacy)
        self._ready = True

    def _index_for(self, ts: datetime) -> str:
        return f"{self.prefix}-{ts.astimezone(timezone.utc):%Y.%m.%d}"

    def index_bulk(self, events: Iterable[LogEvent]) -> int:
        from opensearchpy.helpers import bulk  # lazy import
        self.ensure_ready()
        actions = ({"_op_type": "index", "_index": self._index_for(e.timestamp), "_id": e.event_id,
                    "_source": e.to_dict()} for e in events)
        ok, errors = bulk(self.client, actions, chunk_size=1000, raise_on_error=False, request_timeout=60)
        if errors:
            log.warning("opensearch bulk: %d failures (first: %s)", len(errors), errors[0])
        return ok

    def _base_query(self, since=None, until=None, service=None, level=None, source_type=None,
                    trace_id=None, query=None) -> dict[str, Any]:
        must: list[dict[str, Any]] = []
        filt: list[dict[str, Any]] = []
        if query:
            must.append({"match": {"message": {"query": query, "operator": "and"}}})
        rng: dict[str, str] = {}
        if since:
            rng["gte"] = to_iso(since)
        if until:
            rng["lte"] = to_iso(until)
        if rng:
            filt.append({"range": {"timestamp": rng}})
        for k, v in (("service", service), ("level", level and level.upper()),
                     ("source_type", source_type), ("trace_id", trace_id)):
            if v:
                filt.append({"term": {k: v}})
        return {"bool": {"must": must or [{"match_all": {}}], "filter": filt}}

    def search(self, query=None, service=None, level=None, source_type=None, since=None,
               until=None, trace_id=None, limit=100) -> list[dict[str, Any]]:
        body = {"query": self._base_query(since, until, service, level, source_type, trace_id, query),
                "sort": [{"timestamp": "desc"}], "size": int(limit)}
        res = self.client.search(index=f"{self.prefix}-*", body=body, ignore_unavailable=True)
        return [h["_source"] for h in res["hits"]["hits"]]

    def events_since(self, since: datetime, until: datetime | None = None, limit: int = 50_000) -> Iterator[LogEvent]:
        from opensearchpy.helpers import scan  # lazy import
        body = {"query": self._base_query(since, until), "sort": ["timestamp"]}
        n = 0
        for hit in scan(self.client, index=f"{self.prefix}-*", query=body, preserve_order=True, size=2000):
            yield LogEvent.from_dict(hit["_source"])
            n += 1
            if n >= limit:
                break

    def timeseries(self, since: datetime, bucket_sec: int = 60, service: str | None = None) -> list[dict[str, Any]]:
        body = {
            "size": 0,
            "query": self._base_query(since=since, service=service),
            "aggs": {"by_service": {"terms": {"field": "service", "size": 500}, "aggs": {
                "by_time": {"date_histogram": {"field": "timestamp", "fixed_interval": f"{bucket_sec}s"}, "aggs": {
                    "errors": {"filter": {"terms": {"level": ["ERROR", "FATAL"]}}},
                    "warnings": {"filter": {"term": {"level": "WARN"}}},
                    "avg_latency_ms": {"avg": {"field": "latency_ms"}},
                    "max_latency_ms": {"max": {"field": "latency_ms"}},
                    "unique_patterns": {"cardinality": {"field": "fingerprint"}},
                }}}}},
        }
        res = self.client.search(index=f"{self.prefix}-*", body=body, ignore_unavailable=True)
        out = []
        for sb in res["aggregations"]["by_service"]["buckets"]:
            for tb in sb["by_time"]["buckets"]:
                out.append({
                    "bucket": tb["key_as_string"], "service": sb["key"], "count": tb["doc_count"],
                    "errors": tb["errors"]["doc_count"], "warnings": tb["warnings"]["doc_count"],
                    "avg_latency_ms": tb["avg_latency_ms"]["value"], "max_latency_ms": tb["max_latency_ms"]["value"],
                    "unique_patterns": tb["unique_patterns"]["value"],
                })
        out.sort(key=lambda d: d["bucket"])
        return out

    def services(self, since: datetime | None = None) -> list[str]:
        body = {"size": 0, "query": self._base_query(since=since),
                "aggs": {"svc": {"terms": {"field": "service", "size": 1000}}}}
        res = self.client.search(index=f"{self.prefix}-*", body=body, ignore_unavailable=True)
        return sorted(b["key"] for b in res["aggregations"]["svc"]["buckets"])

    def count(self) -> int:
        return self.client.count(index=f"{self.prefix}-*", ignore_unavailable=True)["count"]
