"""
AgentScope Dashboard — FastAPI + Jinja2
Run: uvicorn dashboard.app:app --reload --port 8080
Open: http://localhost:8080
"""
import sys, os, json
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from fastapi import FastAPI, Request
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from fastapi.responses import JSONResponse

from tracer.storage import (
    init_db, get_recent_traces, get_stats_summary,
    get_cost_by_day, get_slowest_steps, get_eval_trend, get_failure_heatmap
)

app = FastAPI(title="AgentScope", version="1.0.0")

TMPL_DIR   = os.path.join(os.path.dirname(__file__), "templates")
STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")

templates = Jinja2Templates(directory=TMPL_DIR)
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

@app.on_event("startup")
def startup():
    init_db()

def _build_context():
    return {
        "summary":       get_stats_summary(),
        "cost_by_day":   get_cost_by_day(30),
        "slowest_steps": get_slowest_steps(10),
        "eval_trend":    get_eval_trend(14),
        "recent_traces": get_recent_traces(60),
        "failure_map":   get_failure_heatmap(),
    }

@app.get("/")
async def dashboard(request: Request):
    ctx = _build_context()
    ctx["request"] = request
    return templates.TemplateResponse("index.html", ctx)

@app.get("/api/stats")
async def api_stats():
    return JSONResponse(_build_context())

@app.get("/api/traces")
async def api_traces():
    return JSONResponse(get_recent_traces(200))
