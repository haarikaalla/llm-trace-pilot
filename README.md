# AgentScope — Open-Source LLM Observability Framework

> Trace every LLM call. Score every output. Version every prompt. 

[![CI](https://github.com/haarikaalla/llm-trace-pilot/actions/workflows/ci.yml/badge.svg)](https://github.com/haarikaalla/llm-trace-pilot/actions)
[![Python](https://img.shields.io/badge/python-3.10%2B-blue)](https://python.org)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![SQLite](https://img.shields.io/badge/storage-SQLite-green)](https://sqlite.org)
[![Ollama](https://img.shields.io/badge/LLM-Ollama%20local-purple)](https://ollama.ai)

---

## What is AgentScope?

AgentScope is an open-source observability and evaluation platform for LLM agents. It gives every AI engineering team the ability to:

- **Trace** every LLM call — model, prompt, response, tokens, latency, cost
- **Group** multi-step agent runs under a single session ID
- **Evaluate** outputs automatically using LLM-as-judge scoring (accuracy / helpfulness / safety)
- **Version** prompts so you never lose a working prompt again
- **Visualize** cost trends, eval scores, failure heatmaps in a live dashboard

**Zero cloud. Zero GPU. Zero cost.** Uses SQLite + Ollama locally.

---

## Why does this exist?

| Tool | Cost | Self-hosted? | Laptop-friendly? |
|---|---|---|---|
| LangSmith | $39/month | ❌ | ❌ |
| Langfuse Cloud | Free tier limited | Partial | ❌ |
| **AgentScope** | **$0 forever** | **✅** | **✅** |

---

## Quickstart (3 commands)

git clone https://github.com/haarikaalla/llm-trace-pilot.git
cd llm-trace-pilot

# Mac/Linux
python -m venv venv && source venv/bin/activate

# Windows
python -m venv venv
venv\Scripts\activate

pip install -r requirements.txt

# Generate demo traces
python examples/demo_agent.py

# Start dashboard
uvicorn dashboard.app:app --reload --port 8080
# Open: http://localhost:8080
```

---

## Features

### 1. `@trace` decorator — one line to instrument any function
```python
from tracer import trace

@trace(name="summarize", model="llama3.2", tags=["prod"])
def call_llm(prompt: str) -> str:
    # Your LLM call here — OpenAI, Ollama, anything
    return response
```

### 2. Agent session grouping
```python
from tracer import AgentSession

with AgentSession("research_agent") as s:
    ctx    = fetch_context(query)
    answer = generate_answer(ctx)
    final  = format_output(answer)
# All 3 steps share session_id in the DB
```

### 3. LLM-as-judge evaluation
```python
from evaluator import evaluate

scores = evaluate(
    prompt="Explain gradient descent",
    response="Gradient descent minimizes loss by...",
    judge_model="llama3.2",   # free local model
)
# → {"accuracy": 9, "helpfulness": 8, "safety": 10, "reason": "..."}
```

### 4. Prompt versioning
```python
from prompts import PromptRegistry

reg = PromptRegistry()
v = reg.save("summarize", "Summarize in 3 bullets: {text}")
prompt = reg.render("summarize", text="Some long document...")
history = reg.history("summarize")  # all versions
```

---

## Project structure

```
agentscope/
├── tracer/          # @trace decorator + SQLite storage + session manager
├── evaluator/       # LLM-as-judge engine (Ollama or heuristic fallback)
├── prompts/         # Prompt versioning registry
├── dashboard/       # FastAPI + Jinja2 live dashboard
│   ├── app.py
│   └── templates/index.html
├── examples/        # demo_agent.py, run_evals.py
└── tests/           # Full pytest test suite
```


## Architecture
<img width="1200" height="750" alt="architecture (1)" src="https://github.com/user-attachments/assets/512197a6-57b7-4b2e-b5c9-501ac1925718" />

---

## Dashboard Preview

![Dashboard Overview](dashboard1.png)

![Dashboard Traces](dashboard2.png)


## Running Tests

```bash
pytest tests/ -v
```

---

## Tech Stack 

| Component | Technology |
|---|---|
| Backend | FastAPI + Uvicorn |
| Templates | Jinja2 |
| Charts | Chart.js |
| Storage | SQLite (built into Python) |
| LLM Judge | Ollama (llama3.2 locally) |
| CI/CD | GitHub Actions |



---

## License

MIT © 2025 
