"""
demo_agent.py — Simulate a multi-step research agent.
Works 100% offline with no API keys.
Run: python examples/demo_agent.py
Then open: http://localhost:8080
"""
import sys, os, time, random
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tracer import trace, AgentSession, init_db
from evaluator import evaluate
from prompts import PromptRegistry

init_db()
registry = PromptRegistry()

# Save two versions of each prompt so versioning is demonstrated
registry.save("summarize", "Summarize this text in 3 concise bullet points:\n\n{text}")
registry.save("summarize", "Summarize this in 3 bullets. Be very brief:\n\n{text}")
registry.save("qa",        "Answer this question accurately:\n\nQ: {question}\n\nContext: {context}")

FAKE_RESPONSES = [
    "The analysis reveals three key insights: performance improved by 34%, costs dropped, and reliability held steady.",
    "Based on context provided: (1) the primary mechanism is attention, (2) scaling is crucial, (3) alignment is unsolved.",
    "Key findings suggest that observability tools reduce debugging time by up to 60% in production LLM systems.",
    "The model identified relevant patterns. Summary: data quality > model size for most practical applications.",
    "After processing: transformer-based agents outperform rule-based systems on open-ended tasks by a wide margin.",
]

def fake_llm(prompt="") -> str:
    time.sleep(random.uniform(0.08, 0.9))
    return random.choice(FAKE_RESPONSES)

@trace(name="fetch_context",      model="llama3.2", tags=["retrieval"])
def fetch_context(query: str) -> str:
    time.sleep(random.uniform(0.05, 0.25))
    return f"Context for '{query}': relevant documents retrieved with high similarity scores."

@trace(name="summarize_context",  model="llama3.2", tags=["llm"])
def summarize_context(prompt: str) -> str:
    return fake_llm(prompt)

@trace(name="answer_question",    model="llama3.2", tags=["llm"])
def answer_question(prompt: str) -> str:
    return fake_llm(prompt)

@trace(name="format_output",      model="llama3.2", tags=["post-process"])
def format_output(prompt: str) -> str:
    time.sleep(0.04)
    return "**Result:** " + fake_llm(prompt)

@trace(name="failing_step",       model="gpt-4o",   tags=["test-error"])
def failing_step(prompt: str) -> str:
    raise TimeoutError("Simulated API timeout — demonstrates error tracking")

QUERIES = [
    "What are the latest advances in transformer architecture?",
    "How does LLM observability improve AI systems in production?",
    "What is the role of evaluation metrics in LLM pipelines?",
    "Explain prompt versioning and why teams need it.",
    "How do LLM agents use tools and memory effectively?",
]

print("\n🔭 AgentScope Demo — Generating traces...\n")

for i, query in enumerate(QUERIES):
    with AgentSession("research_agent") as sess:
        print(f"  [{i+1}/5] Session {sess.session_id}: {query[:55]}...")
        ctx     = fetch_context(query=query)
        summary = summarize_context(prompt=registry.render("summarize", text=ctx))
        answer  = answer_question(prompt=registry.render("qa", question=query, context=summary))
        final   = format_output(prompt=answer)

        scores = evaluate(prompt=query, response=final, trace_id=sess.session_id)
        print(f"         Eval → accuracy:{scores['accuracy']}/10  helpfulness:{scores['helpfulness']}/10  safety:{scores['safety']}/10")

# One intentional error trace
print("\n  Simulating error trace...")
try:
    failing_step(prompt="this will time out")
except Exception:
    pass

print("\n✅ Done! 20+ traces written to agentscope.db")
print("   Start dashboard: uvicorn dashboard.app:app --reload --port 8080")
print("   Open browser:    http://localhost:8080\n")
