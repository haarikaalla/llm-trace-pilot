import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from fastapi import FastAPI, Request
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, JSONResponse

from tracer.storage import (
    init_db, get_recent_traces, get_stats_summary,
    get_cost_by_day, get_slowest_steps, get_eval_trend, get_failure_heatmap
)

app = FastAPI(title="llm-trace-pilot", version="1.0.0")

BASE = os.path.dirname(os.path.abspath(__file__))
templates = Jinja2Templates(directory=os.path.join(BASE, "templates"))

static_dir = os.path.join(BASE, "static")
if os.path.exists(static_dir):
    app.mount("/static", StaticFiles(directory=static_dir), name="static")

@app.on_event("startup")
def startup():
    init_db()

@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    context = {
        "summary":       get_stats_summary(),
        "cost_by_day":   get_cost_by_day(30),
        "slowest_steps": get_slowest_steps(10),
        "eval_trend":    get_eval_trend(14),
        "recent_traces": get_recent_traces(60),
        "failure_map":   get_failure_heatmap(),
    }
    return templates.TemplateResponse(request=request, name="index.html", context=context)

@app.get("/api/stats")
async def api_stats():
    return JSONResponse({
        "summary":       get_stats_summary(),
        "cost_by_day":   get_cost_by_day(30),
        "slowest_steps": get_slowest_steps(10),
        "eval_trend":    get_eval_trend(14),
        "recent_traces": get_recent_traces(60),
    })
