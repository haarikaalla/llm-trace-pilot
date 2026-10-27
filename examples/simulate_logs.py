"""
simulate_logs.py — Generate a realistic multi-source log stream with an injected
incident, push it through the full platform in-process, and print the result.

Works 100% offline (memory bus + SQLite). Run:
    python examples/simulate_logs.py                # 30 min of history, incident in the last 3 min
    python examples/simulate_logs.py --rate 2000    # events/min per service (throughput check)
    python examples/simulate_logs.py --no-incident
"""
from __future__ import annotations

import argparse
import json
import os
import random
import sys
import time
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from observability.services.platform import get_platform

SERVICES = {
    "api-gateway": ["checkout", "catalog", "auth"],
    "checkout": ["payment", "inventory"],
    "payment": ["payment-gateway-db"],
    "inventory": ["inventory-db"],
    "catalog": ["inventory"],
    "auth": [],
    "payment-gateway-db": [],
    "inventory-db": [],
}
PATHS = ["/api/checkout", "/api/items", "/api/items/{id}", "/api/login", "/api/cart", "/health"]
INFO_MSGS = ["Request completed", "Cache hit for key", "Handled request", "Scheduled job finished", "Connection pool stats"]
WARN_MSGS = ["Slow query detected", "Retrying request", "Cache miss ratio high", "Connection pool at 80%"]


def iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def app_log(svc: str, ts: datetime, level: str, msg: str, **kw) -> dict:
    rec = {"timestamp": iso(ts), "level": level, "service": svc, "message": msg,
           "latency_ms": round(random.lognormvariate(4.3, 0.5), 1), "environment": "prod", **kw}
    return {"message": json.dumps(rec), "source_type": "application"}


def k8s_log(svc: str, ts: datetime, line: str, stream="stdout") -> dict:
    return {"log": f"{iso(ts)} {stream} F {line}", "source_type": "kubernetes",
            "kubernetes": {"namespace_name": "prod", "pod_name": f"{svc}-7d9f8b6c5-{random.choice('abcdefghk')}{random.randint(1000, 9999)}",
                           "container_name": svc, "host": f"node-{random.randint(1, 4)}", "labels": {"app": svc}}}


def infra_log(ts: datetime, host: str, msg: str, pri: int = 30) -> dict:
    return {"message": f"<{pri}>{ts.strftime('%b %d %H:%M:%S')} {host} kernel: {msg}", "source_type": "infrastructure"}


def ci_log(ts: datetime, pipeline: str, line: str, stage: str) -> dict:
    return {"message": f"{iso(ts)} {line}", "pipeline": pipeline, "stage": stage, "source_type": "cicd"}


def gen_minute(minute_ts: datetime, rate: int, incident_phase: float, trace_pool: list[str]) -> list[dict]:
    """incident_phase: 0 = healthy, 0..1 = ramping, 1 = full outage in payment-gateway-db -> payment -> checkout -> gateway."""
    out = []
    for svc, deps in SERVICES.items():
        n = max(1, int(random.gauss(rate, rate * 0.1)))
        for _ in range(n):
            ts = minute_ts + timedelta(milliseconds=random.randint(0, 59_999))
            tid = random.choice(trace_pool) if random.random() < 0.3 else None
            affected = svc in ("payment-gateway-db", "payment", "checkout", "api-gateway")
            r = random.random()
            if incident_phase > 0 and affected and r < incident_phase * {"payment-gateway-db": 0.7, "payment": 0.6, "checkout": 0.45, "api-gateway": 0.3}[svc]:
                if svc == "payment-gateway-db":
                    out.append(infra_log(ts, "node-2", f"Out of memory: Kill process {random.randint(1000, 9999)} (postgres) score 900", pri=26))
                    out.append(k8s_log(svc, ts, "FATAL: too many connections for role \"payment\" — connection refused", "stderr"))
                elif svc == "payment":
                    out.append(app_log(svc, ts, "ERROR", f"TimeoutError calling payment-gateway-db after 5000ms trace_id={tid or 'none'}",
                                       latency_ms=5000 + random.randint(0, 300), status_code=503, trace_id=tid, target_service="payment-gateway-db"))
                elif svc == "checkout":
                    out.append(app_log(svc, ts, "ERROR", f"Upstream payment returned 503 Service Unavailable for /api/checkout",
                                       latency_ms=5200 + random.randint(0, 400), status_code=503, trace_id=tid, target_service="payment"))
                else:
                    out.append(app_log(svc, ts, "ERROR", "Request failed: bad gateway from checkout (502)",
                                       latency_ms=5300 + random.randint(0, 400), status_code=502, trace_id=tid, target_service="checkout"))
                continue
            if r < 0.02:
                out.append(app_log(svc, ts, "ERROR", f"{random.choice(['ValidationError', 'KeyError'])}: invalid request field user_id", status_code=400, trace_id=tid))
            elif r < 0.08:
                out.append(app_log(svc, ts, "WARN", random.choice(WARN_MSGS), trace_id=tid))
            elif r < 0.5 and svc.endswith("-db") is False:
                out.append(k8s_log(svc, ts, f"INFO {random.choice(INFO_MSGS)} path={random.choice(PATHS)} status=200 latency={random.randint(20, 300)}ms"))
            else:
                target = random.choice(deps) if deps else None
                out.append(app_log(svc, ts, "INFO", f"{random.choice(INFO_MSGS)} {random.choice(PATHS)} status=200",
                                   status_code=200, trace_id=tid, **({"target_service": target} if target else {})))
    # background infra + CI noise
    out.append(infra_log(minute_ts, f"node-{random.randint(1, 4)}", f"eth0: link up, {random.randint(100, 900)} Mbps"))
    if random.random() < 0.3:
        stage = random.choice(["build", "test", "deploy"])
        ok = random.random() > (0.6 if incident_phase > 0.5 else 0.1)
        line = f"##[{'notice' if ok else 'error'}]Step {stage} {'succeeded' if ok else 'failed with exit code 1'}"
        out.append(ci_log(minute_ts, "checkout-service-deploy", line, stage))
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--minutes", type=int, default=30)
    ap.add_argument("--rate", type=int, default=40, help="events per minute per service")
    ap.add_argument("--no-incident", action="store_true")
    ap.add_argument("--seed", type=int, default=7)
    args = ap.parse_args()
    random.seed(args.seed)

    p = get_platform()
    now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    start = now - timedelta(minutes=args.minutes)
    trace_pool = [os.urandom(8).hex() for _ in range(500)]

    print(f"\n🔭 AgentScope — simulating {args.minutes} min × {len(SERVICES)} services @ {args.rate}/min "
          f"(bus={p.s.event_bus}, index={p.s.log_index})\n")
    t0, total = time.time(), 0
    for m in range(args.minutes + 1):
        ts = start + timedelta(minutes=m)
        remaining = args.minutes - m
        phase = 0.0 if args.no_incident else (1.0 if remaining <= 2 else 0.4 if remaining == 3 else 0.0)
        batch = gen_minute(ts, args.rate, phase, trace_pool)
        res = p.ingest(batch)
        total += res["accepted"]
        if phase and remaining <= 2:
            print(f"  [{ts:%H:%M}] 💥 incident phase={phase:.1f}  +{res['accepted']} events")
    elapsed = time.time() - t0
    print(f"\n  Ingested+processed {total} events in {elapsed:.1f}s → {total / elapsed:,.0f} events/sec (single process)")
    print(f"  Indexed events in store: {p.index.count()}  pipeline={p.processor.metrics()}")

    print("\n🧠 Running detection...")
    res = p.run_detection(now=now + timedelta(seconds=60))
    print(f"  anomalies={len(res['anomalies'])} alerts={len(res['alerts'])} incidents={len(res['incidents'])}")
    for a in res["anomalies"][:8]:
        print(f"   • {a['service']:<22} {a['kind']:<22} score={a['score']:.2f} {a['severity']:<5} {a['description'][:70]}")
    for inc in res["incidents"]:
        full = p.repo.get_incident(inc["incident_id"])
        print(f"\n🚨 {full.incident_id} [{full.severity.value}] {full.title}")
        if full.root_cause:
            print(f"   primary suspect : {full.root_cause['primary']} (confidence {full.root_cause['primary_confidence']})")
            for h in full.root_cause["hypotheses"][:3]:
                print(f"   hypothesis      : {h['cause'][:90]} ({h['confidence']})")
        if full.summary:
            s = full.summary
            print(f"   what happened   : {s['what_happened'][:220]}")
            print(f"   impacted        : {', '.join(s['impacted_services'])}")
            print(f"   actions         : {s['recommended_actions'][0]}")
            print(f"   generated by    : {s['generated_by']}")
    print("\n✅ Done. Start the dashboard:  uvicorn dashboard.app:app --reload --port 8080  →  http://localhost:8080/observability\n")


if __name__ == "__main__":
    main()
