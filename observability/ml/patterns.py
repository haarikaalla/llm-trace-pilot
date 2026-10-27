"""
patterns.py — Unusual log-pattern detection on the fingerprint stream.

NOVEL_PATTERN : an error template never seen in the baseline appears
PATTERN_BURST : one template suddenly dominates the window (log storm / retry loop)
"""
from __future__ import annotations

from dataclasses import dataclass, field

from .features import WindowFeatures


@dataclass
class PatternResult:
    novel_score: float
    burst_score: float
    novel_fingerprints: list[str] = field(default_factory=list)
    burst_fingerprint: str | None = None
    burst_share: float = 0.0
    burst_count: int = 0


class PatternDetector:
    def __init__(self, burst_share: float = 0.6, burst_min_count: int = 20, burst_multiplier: float = 5.0):
        self.burst_share = burst_share
        self.burst_min_count = burst_min_count
        self.burst_multiplier = burst_multiplier

    def score(self, history: list[WindowFeatures], current: WindowFeatures) -> PatternResult:
        seen: set[str] = set()
        baseline_counts: dict[str, list[int]] = {}
        for w in history:
            seen.update(w.fingerprints)
            for fp, n in w.fingerprints.items():
                baseline_counts.setdefault(fp, []).append(n)

        novel = [fp for fp in current.error_fingerprints if fp not in seen]
        novel_score = 0.0
        if novel and history:
            novel_errors = sum(current.error_fingerprints[fp] for fp in novel)
            share = novel_errors / max(1, current.errors)
            novel_score = round(min(1.0, 0.4 + 0.6 * share) if novel_errors >= 2 else 0.3, 4)

        burst_fp, burst_score, burst_share, burst_count = None, 0.0, 0.0, 0
        if current.fingerprints and current.count >= self.burst_min_count:
            fp, n = max(current.fingerprints.items(), key=lambda kv: kv[1])
            share = n / current.count
            base = baseline_counts.get(fp)
            base_avg = (sum(base) / len(history)) if base else 0.0
            if share >= self.burst_share and n >= self.burst_min_count and (not base or n >= self.burst_multiplier * max(base_avg, 1)):
                burst_fp, burst_share, burst_count = fp, share, n
                burst_score = round(min(1.0, 0.5 + share / 2), 4)
        return PatternResult(novel_score, burst_score, novel, burst_fp, round(burst_share, 3), burst_count)
