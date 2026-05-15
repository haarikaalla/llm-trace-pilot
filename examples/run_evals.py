"""
run_evals.py — Batch evaluate existing traces.
Pulls un-evaluated traces from DB and scores them with Ollama (or heuristic).
Run: python examples/run_evals.py
"""
import sys, os, sqlite3
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tracer.storage import init_db, get_recent_traces
from evaluator import evaluate

init_db()
traces = get_recent_traces(50)
traces = [t for t in traces if t.get("prompt_text") and t.get("response_text") and not t.get("error")]

print(f"\n📊 Running evals on {len(traces)} traces...\n")
for t in traces:
    scores = evaluate(
        prompt=t["prompt_text"],
        response=t["response_text"],
        trace_id=t["trace_id"],
        judge_model="llama3.2",
    )
    print(f"  {t['trace_id']} → acc:{scores['accuracy']}  help:{scores['helpfulness']}  safe:{scores['safety']}")

print(f"\n✅ Evals complete. Open http://localhost:8080 to see score trends.\n")
