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
python examples/demo_agent.py
```
This runs a simulated 5-step research agent and writes traces to agentscope.db.

## Step 6: Start the dashboard
```bash
uvicorn dashboard.app:app --reload --port 8080
```
Open your browser: http://localhost:8080

## Step 7: Run the test suite
```bash
pytest tests/ -v
```
All tests should pass.

## Step 8: Push to GitHub
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

**Port 8080 already in use** → Change to `--port 8090` or kill the other process
