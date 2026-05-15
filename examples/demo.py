"""
demo.py
Run this to generate sample trace data and see AgentScope in action.
It works 100% offline using Ollama (free local LLMs).

If you don't have Ollama:
  - Download: https://ollama.ai
  - Then run: ollama pull llama3.2

Usage:
    python examples/demo.py
    python examples/demo.py --openai   # uses OpenAI instead (needs API key)
"""
import sys
import os
import time
import argparse

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tracer import trace, AgentSession, init_db
from tracer.core import traced_ollama_call
from tracer.storage import save_trace
from evaluator import evaluate
from prompts import PromptRegistry
from rich.console import Console
from rich.table import Table

console = Console()
init_db()
registry = PromptRegistry()


# ── Save some prompt versions ────────────────────────────────────────────────
registry.save("summarize_v1", "Summarize this in 2 sentences: {text}")
registry.save("summarize_v2", "Summarize this concisely in exactly 2 bullet points: {text}")
registry.save("classify",     "Classify this text as POSITIVE, NEGATIVE, or NEUTRAL: {text}")


# ── Demo 1: Single traced call (Ollama) ──────────────────────────────────────
def demo_single_trace():
    console.rule("[bold cyan]Demo 1: Single traced Ollama call")

    texts = [
        "The new transformer architecture achieves state-of-the-art results on NLP benchmarks.",
        "The model failed to converge during training due to a high learning rate.",
        "Python is a general-purpose programming language known for its readability.",
        "Gradient descent minimizes the loss function by moving in the direction of steepest descent.",
        "The API rate limit was exceeded, causing all requests to fail with a 429 error.",
    ]

    for text in texts:
        prompt = registry.render("summarize_v2", text=text)
        try:
            result = traced_ollama_call(
                name="summarize_text",
                prompt=prompt,
                model="llama3.2",
                tags=["demo", "summarize"],
            )
            console.print(f"[green]✓[/] Response: {result[:80]}...")
        except Exception as e:
            # Simulate a trace even if Ollama isn't running (for demo purposes)
            console.print(f"[yellow]⚠ Ollama not available ({e.__class__.__name__}), saving simulated trace[/]")
            _save_simulated_trace("summarize_text", prompt, text, "llama3.2", tags=["demo","summarize"])
        time.sleep(0.1)


# ── Demo 2: Multi-step agent session ─────────────────────────────────────────
def demo_agent_session():
    console.rule("[bold cyan]Demo 2: Multi-step agent session")

    questions = [
        "What is backpropagation in neural networks?",
        "Explain the attention mechanism in transformers.",
        "What is the difference between supervised and unsupervised learning?",
    ]

    for q in questions:
        with AgentSession("research_agent") as session:
            console.print(f"[dim]Session {session.session_id}:[/] {q[:50]}...")

            # Step 1: Rephrase question
            rephrased = _call_or_simulate(
                name="rephrase_query",
                prompt=f"Rephrase this as a precise technical question: {q}",
                model="llama3.2",
                tags=["agent", "step1"],
                simulated=f"Technical: {q}",
            )

            # Step 2: Generate answer
            answer = _call_or_simulate(
                name="generate_answer",
                prompt=f"Answer this precisely in 3 sentences: {rephrased}",
                model="llama3.2",
                tags=["agent", "step2"],
                simulated=f"Answer to {q}: This is a core concept in ML where {q.lower()} ...",
            )

            # Step 3: Format output
            _call_or_simulate(
                name="format_response",
                prompt=f"Format this as a clean markdown response: {answer}",
                model="llama3.2",
                tags=["agent", "step3"],
                simulated=f"## Answer\n\n{answer[:100]}...",
            )

        time.sleep(0.2)


# ── Demo 3: Simulate some errors ─────────────────────────────────────────────
def demo_errors():
    console.rule("[bold cyan]Demo 3: Simulating error traces")

    error_cases = [
        ("classify_sentiment", "context length exceeded: input too long"),
        ("extract_entities",   "TimeoutError: model response exceeded 30s"),
        ("translate_text",     "model not found: gpt-5-ultra"),
    ]
    for name, err in error_cases:
        save_trace({
            "trace_id":    f"err{name[:4]}",
            "session_id":  None,
            "step_num":    0,
            "name":        name,
            "tags":        ["demo", "error"],
            "model":       "llama3.2",
            "prompt_text": f"Demo error prompt for {name}",
            "response_text": None,
            "input_tokens":  100,
            "output_tokens": 0,
            "cost_usd":      0.0,
            "latency_ms":    abs(hash(name)) % 8000 + 500,
            "error":         err,
        })
        console.print(f"[red]✗[/] Error trace saved: {name}")


# ── Demo 4: LLM-as-Judge eval ─────────────────────────────────────────────────
def demo_eval():
    console.rule("[bold cyan]Demo 4: LLM-as-Judge evaluation")
    console.print("[dim]Running evaluations on recent traces... (needs Ollama)[/]")

    from evaluator.judge import batch_evaluate
    results = batch_evaluate(limit=5, judge_model="llama3.2")

    if results:
        table = Table(show_header=True, header_style="bold cyan")
        table.add_column("Trace ID")
        table.add_column("Accuracy")
        table.add_column("Helpfulness")
        table.add_column("Safety")
        table.add_column("Overall")
        for r in results:
            table.add_row(
                r["trace_id"],
                str(r["accuracy"]),
                str(r["helpfulness"]),
                str(r["safety"]),
                str(r["overall"]),
            )
        console.print(table)
    else:
        console.print("[yellow]No unevaluated traces found (or Ollama not running)[/]")


# ── Helpers ───────────────────────────────────────────────────────────────────
def _save_simulated_trace(name, prompt, response, model, latency_ms=None, tags=None):
    """Save a fake trace when Ollama isn't available — for dashboard demo data."""
    import random, uuid
    from tracer.storage import save_trace
    save_trace({
        "trace_id":      str(uuid.uuid4())[:8],
        "session_id":    None,
        "step_num":      0,
        "name":          name,
        "tags":          tags or [],
        "model":         model,
        "prompt_text":   prompt[:500],
        "response_text": response[:500],
        "input_tokens":  len(prompt) // 4,
        "output_tokens": len(response) // 4,
        "cost_usd":      0.0,
        "latency_ms":    latency_ms or random.randint(200, 4000),
        "error":         None,
    })


def _call_or_simulate(name, prompt, model, tags, simulated):
    try:
        return traced_ollama_call(name=name, prompt=prompt, model=model, tags=tags)
    except Exception:
        _save_simulated_trace(name, prompt, simulated, model, tags=tags)
        return simulated


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--openai", action="store_true", help="Use OpenAI instead of Ollama")
    args = parser.parse_args()

    console.print("\n[bold]🔭 AgentScope Demo[/] — generating sample trace data\n")

    demo_single_trace()
    demo_agent_session()
    demo_errors()
    demo_eval()

    console.print("\n[bold green]✅ Done![/] Open the dashboard:")
    console.print("   [bold cyan]uvicorn dashboard.app:app --reload --port 8080[/]")
    console.print("   Then visit: [link=http://localhost:8080]http://localhost:8080[/link]\n")
