# AgentScope — Complete Setup Guide

## Step 1: Install Python (if not installed)
Download from https://python.org — get Python 3.10 or newer.
Check: `python --version`

## Step 2: Install VS Code
Download from https://code.visualstudio.com
Install the "Python" extension inside VS Code.

## Step 3: Install Ollama (free local LLM)
Download from https://ollama.ai
After installing, open terminal and run:
```
ollama pull llama3.2
```
This downloads a free 2GB local AI model. One-time only.

## Step 4: Clone and set up the project
Open VS Code, open the terminal (Ctrl+` or View > Terminal), then run:
```bash
cd Desktop                          # or wherever you want the project
git clone https://github.com/yourusername/agentscope.git
cd agentscope
python -m venv venv
```

Activate the virtual environment:
- Mac/Linux: `source venv/bin/activate`
- Windows:   `venv\Scripts\activate`

Install dependencies:
```bash
pip install -r requirements.txt
```

## Step 5: Generate demo data
```bash
python examples/demo_agent.py        # LLM traces (5 agent sessions + 1 error)
python examples/simulate_logs.py     # 30 min of app/k8s/infra/CI logs with an injected outage
```
The first writes traces to agentscope.db; the second runs the full ingestion →
detection → RCA → summary pipeline in-process and prints the incident it found.

## Step 6: Start the dashboard
```bash
uvicorn dashboard.app:app --reload --port 8080
```
Open http://localhost:8080 (LLM traces) and http://localhost:8080/observability
(system health, anomalies, incidents, dependency graph, live feed).

## Step 7: Run the test suite
```bash
pytest tests/ -v
```
All 76 tests should pass — no Kafka/OpenSearch needed.

## Step 8 (optional): Run the production-shaped stack
```bash
cd deploy/docker
docker compose up --build -d
```
Brings up Kafka, OpenSearch and the five services. Kubernetes manifests live in
`deploy/k8s` (`kubectl apply -k deploy/k8s`). Full guide: docs/DEPLOYMENT.md.

## Step 9: Push to GitHub
```bash
git init
git add .
git commit -m "feat: initial AgentScope release"
git remote add origin https://github.com/yourusername/agentscope.git
git push -u origin main
```

---

## Common Issues

**"ModuleNotFoundError"** → Make sure venv is activated (you should see `(venv)` in terminal)

**"ollama: command not found"** → Install Ollama from ollama.ai first

**Dashboard shows empty charts** → Run `python examples/demo_agent.py` first to generate data

**/observability shows no services** → Run `python examples/simulate_logs.py`, or POST logs to the ingestion API

**No AI summary / `generated_by: deterministic`** → Ollama isn't reachable; the fallback summary is used. Start Ollama or set `OLLAMA_URL`

**Windows: UnicodeEncodeError when piping output** → `set PYTHONIOENCODING=utf-8`

**Port 8080 already in use** → Change to `--port 8090` or kill the other process
