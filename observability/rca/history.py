"""
history.py — Historical incident comparison.

Each incident carries a `signature`: services, error types, top fingerprints
and a numeric feature vector. New incidents are compared with weighted Jaccard
(set features) + cosine (numeric features) to surface "we've seen this before"
along with what fixed it last time.
"""
from __future__ import annotations

import math
from typing import Any

from ..domain.models import Incident


def jaccard(a: set, b: set) -> float:
    if not a and not b:
        return 0.0
    return len(a & b) / len(a | b)


def cosine(a: dict[str, float], b: dict[str, float]) -> float:
    keys = set(a) | set(b)
    dot = sum(a.get(k, 0.0) * b.get(k, 0.0) for k in keys)
    na = math.sqrt(sum(v * v for v in a.values()))
    nb = math.sqrt(sum(v * v for v in b.values()))
    return dot / (na * nb) if na and nb else 0.0


def build_signature(services: list[str], error_types: list[str], fingerprints: list[str],
                    anomaly_kinds: list[str], features: dict[str, float] | None = None) -> dict[str, Any]:
    return {
        "services": sorted(set(services)), "error_types": sorted(set(error_types)),
        "fingerprints": sorted(set(fingerprints))[:50], "anomaly_kinds": sorted(set(anomaly_kinds)),
        "features": {k: round(float(v), 4) for k, v in (features or {}).items()},
    }


class HistoricalMatcher:
    weights = {"services": 0.3, "error_types": 0.25, "fingerprints": 0.25, "anomaly_kinds": 0.1, "features": 0.1}

    def similarity(self, a: dict[str, Any], b: dict[str, Any]) -> float:
        s = 0.0
        for k in ("services", "error_types", "fingerprints", "anomaly_kinds"):
            s += self.weights[k] * jaccard(set(a.get(k, [])), set(b.get(k, [])))
        s += self.weights["features"] * cosine(a.get("features", {}), b.get("features", {}))
        return round(s, 4)

    def find_similar(self, signature: dict[str, Any], past: list[Incident], exclude_id: str | None = None,
                     min_similarity: float = 0.25, limit: int = 5) -> list[dict[str, Any]]:
        out = []
        for inc in past:
            if inc.incident_id == exclude_id or not inc.signature:
                continue
            sim = self.similarity(signature, inc.signature)
            if sim >= min_similarity:
                out.append({
                    "incident_id": inc.incident_id, "title": inc.title, "similarity": sim,
                    "status": inc.status.value, "started_at": inc.started_at.isoformat(),
                    "resolved_at": inc.resolved_at.isoformat() if inc.resolved_at else None,
                    "root_cause": (inc.root_cause or {}).get("primary") if inc.root_cause else None,
                    "remediation": ((inc.summary or {}).get("recommended_actions") or [])[:3],
                })
        out.sort(key=lambda d: -d["similarity"])
        return out[:limit]
