"""
Tests for AgentScope core components.
Run: pytest tests/ -v
"""
import pytest, os, sys, tempfile
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import tracer.storage as storage
from tracer.core import trace
from tracer.session import AgentSession, get_session_id
from prompts.registry import PromptRegistry

@pytest.fixture
def tmp_db(tmp_path):
    db = str(tmp_path / "test.db")
    os.environ["AGENTSCOPE_DB"] = db
    storage.DB_PATH = db
    storage.init_db(db)
    yield db
    del os.environ["AGENTSCOPE_DB"]

# ── Storage ───────────────────────────────────────────────────────────────────
def test_init_db_creates_tables(tmp_db):
    import sqlite3
    conn = sqlite3.connect(tmp_db)
    tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
    assert "traces" in tables
    assert "evals"  in tables
    assert "prompts" in tables

def test_save_and_get_trace(tmp_db):
    storage.save_trace({
        "trace_id": "abc123", "session_id": "s1", "step_num": 1,
        "name": "test_step", "model": "llama3.2",
        "prompt_text": "hello", "response_text": "world",
        "input_tokens": 5, "output_tokens": 10, "cost_usd": 0.0,
        "latency_ms": 120, "error": None,
    }, db_path=tmp_db)
    rows = storage.get_recent_traces(10, db_path=tmp_db)
    assert len(rows) == 1
    assert rows[0]["trace_id"] == "abc123"
    assert rows[0]["name"]     == "test_step"

def test_stats_summary(tmp_db):
    storage.save_trace({
        "trace_id":"t1","session_id":"s1","step_num":1,"name":"step",
        "model":"llama3.2","prompt_text":"p","response_text":"r",
        "input_tokens":10,"output_tokens":20,"cost_usd":0.0,"latency_ms":200,"error":None
    }, db_path=tmp_db)
    stats = storage.get_stats_summary(db_path=tmp_db)
    assert stats["total_traces"] == 1
    assert stats["total_errors"] == 0

def test_error_trace(tmp_db):
    storage.save_trace({
        "trace_id":"err1","session_id":None,"step_num":0,"name":"bad_step",
        "model":"gpt-4o","prompt_text":"p","response_text":None,
        "input_tokens":0,"output_tokens":0,"cost_usd":0.0,"latency_ms":50,
        "error":"TimeoutError: API timeout"
    }, db_path=tmp_db)
    stats = storage.get_stats_summary(db_path=tmp_db)
    assert stats["total_errors"] == 1

# ── Cost calc ─────────────────────────────────────────────────────────────────
def test_cost_calc_ollama():
    assert storage.calc_cost("llama3.2", 1000, 1000) == 0.0

def test_cost_calc_gpt4o():
    cost = storage.calc_cost("gpt-4o", 1000, 1000)
    assert cost > 0

# ── Session ───────────────────────────────────────────────────────────────────
def test_session_sets_context():
    with AgentSession("test_agent") as s:
        assert get_session_id() == s.session_id
    assert get_session_id() is None

def test_session_id_is_short_string():
    with AgentSession("agent") as s:
        sid = get_session_id()
        assert isinstance(sid, str)
        assert len(sid) == 8

# ── Decorator ────────────────────────────────────────────────────────────────
def test_trace_decorator_saves(tmp_db):
    @trace(name="unit_test_fn", model="llama3.2")
    def my_fn(prompt: str) -> str:
        return "response text"
    my_fn(prompt="test prompt")
    rows = storage.get_recent_traces(5, db_path=tmp_db)
    assert any(r["name"] == "unit_test_fn" for r in rows)

def test_trace_decorator_captures_error(tmp_db):
    @trace(name="error_fn", model="llama3.2")
    def bad_fn(prompt: str) -> str:
        raise ValueError("boom")
    with pytest.raises(ValueError):
        bad_fn(prompt="test")
    rows = storage.get_recent_traces(5, db_path=tmp_db)
    errs = [r for r in rows if r["name"] == "error_fn"]
    assert len(errs) == 1
    assert "boom" in errs[0]["error"]

# ── Prompt Registry ───────────────────────────────────────────────────────────
def test_prompt_save_and_get(tmp_db):
    reg = PromptRegistry(tmp_db)
    v = reg.save("greet", "Hello {name}!")
    assert v == 1
    entry = reg.get("greet")
    assert entry["content"] == "Hello {name}!"
    assert entry["version"] == 1

def test_prompt_versioning(tmp_db):
    reg = PromptRegistry(tmp_db)
    reg.save("p", "version one")
    reg.save("p", "version two")
    assert reg.get("p")["version"] == 2
    assert reg.get("p", version=1)["content"] == "version one"

def test_prompt_render(tmp_db):
    reg = PromptRegistry(tmp_db)
    reg.save("tmpl", "Summarize: {text}")
    result = reg.render("tmpl", text="hello world")
    assert result == "Summarize: hello world"

def test_prompt_history(tmp_db):
    reg = PromptRegistry(tmp_db)
    reg.save("h", "v1"); reg.save("h", "v2"); reg.save("h", "v3")
    hist = reg.history("h")
    assert len(hist) == 3
    assert hist[0]["version"] == 1
