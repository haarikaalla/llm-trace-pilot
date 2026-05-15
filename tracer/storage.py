"""
storage.py — AgentScope SQLite layer
All DB reads/writes for traces, evals, prompts.
"""
import sqlite3, json, os

DB_PATH = os.environ.get("AGENTSCOPE_DB", "agentscope.db")

COST_PER_1K = {
    "gpt-4o":        {"input": 0.005,   "output": 0.015},
    "gpt-4o-mini":   {"input": 0.00015, "output": 0.0006},
    "gpt-3.5-turbo": {"input": 0.0005,  "output": 0.0015},
    "llama3.2":      {"input": 0.0,     "output": 0.0},
    "llama3":        {"input": 0.0,     "output": 0.0},
    "mistral":       {"input": 0.0,     "output": 0.0},
    "gemma2":        {"input": 0.0,     "output": 0.0},
}

def calc_cost(model: str, input_tokens: int, output_tokens: int) -> float:
    rates = COST_PER_1K.get(model, {"input": 0.0, "output": 0.0})
    return round((input_tokens/1000)*rates["input"] + (output_tokens/1000)*rates["output"], 6)

def get_conn(db_path=None):
    conn = sqlite3.connect(db_path or DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db(db_path=None):
    conn = get_conn(db_path or DB_PATH)
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS traces (
        id             INTEGER PRIMARY KEY AUTOINCREMENT,
        trace_id       TEXT NOT NULL,
        session_id     TEXT,
        step_num       INTEGER DEFAULT 0,
        name           TEXT NOT NULL,
        tags           TEXT DEFAULT '[]',
        model          TEXT,
        prompt_text    TEXT,
        response_text  TEXT,
        input_tokens   INTEGER DEFAULT 0,
        output_tokens  INTEGER DEFAULT 0,
        cost_usd       REAL DEFAULT 0.0,
        latency_ms     INTEGER DEFAULT 0,
        error          TEXT,
        prompt_version INTEGER,
        ts             TEXT DEFAULT (datetime('now'))
    );
    CREATE TABLE IF NOT EXISTS evals (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        trace_id    TEXT NOT NULL,
        judge_model TEXT,
        accuracy    REAL,
        helpfulness REAL,
        safety      REAL,
        overall     REAL,
        reason      TEXT,
        ts          TEXT DEFAULT (datetime('now'))
    );
    CREATE TABLE IF NOT EXISTS prompts (
        id      INTEGER PRIMARY KEY AUTOINCREMENT,
        name    TEXT NOT NULL,
        version INTEGER NOT NULL,
        hash    TEXT NOT NULL,
        content TEXT NOT NULL,
        tags    TEXT DEFAULT '[]',
        ts      TEXT DEFAULT (datetime('now')),
        UNIQUE(name, version)
    );
    CREATE INDEX IF NOT EXISTS idx_traces_session ON traces(session_id);
    CREATE INDEX IF NOT EXISTS idx_traces_ts ON traces(ts);
    CREATE INDEX IF NOT EXISTS idx_evals_trace ON evals(trace_id);
    """)
    conn.commit(); conn.close()

def save_trace(data: dict, db_path=None):
    conn = get_conn(db_path or DB_PATH)
    conn.execute("""
        INSERT INTO traces
          (trace_id,session_id,step_num,name,tags,model,
           prompt_text,response_text,input_tokens,output_tokens,
           cost_usd,latency_ms,error,prompt_version)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        data.get("trace_id"), data.get("session_id"), data.get("step_num",0),
        data.get("name"), json.dumps(data.get("tags",[])), data.get("model"),
        data.get("prompt_text"), data.get("response_text"),
        data.get("input_tokens",0), data.get("output_tokens",0),
        data.get("cost_usd",0.0), data.get("latency_ms",0),
        data.get("error"), data.get("prompt_version"),
    ))
    conn.commit(); conn.close()

def save_eval(data: dict, db_path=None):
    conn = get_conn(db_path or DB_PATH)
    overall = round((data.get("accuracy",0)+data.get("helpfulness",0)+data.get("safety",0))/3, 2)
    conn.execute("""
        INSERT INTO evals (trace_id,judge_model,accuracy,helpfulness,safety,overall,reason)
        VALUES (?,?,?,?,?,?,?)
    """, (data.get("trace_id"), data.get("judge_model","llama3.2"),
          data.get("accuracy",0), data.get("helpfulness",0),
          data.get("safety",0), overall, data.get("reason","")))
    conn.commit(); conn.close()

def get_recent_traces(limit=50, db_path=None):
    conn = get_conn(db_path or DB_PATH)
    rows = conn.execute("SELECT * FROM traces ORDER BY ts DESC LIMIT ?", (limit,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]

def get_cost_by_day(days=30, db_path=None):
    conn = get_conn(db_path or DB_PATH)
    rows = conn.execute("""
        SELECT date(ts) as day, ROUND(SUM(cost_usd),6) as total_cost,
               SUM(input_tokens+output_tokens) as total_tokens, COUNT(*) as calls
        FROM traces WHERE ts >= datetime('now', ?)
        GROUP BY day ORDER BY day
    """, (f"-{days} days",)).fetchall()
    conn.close()
    return [dict(r) for r in rows]

def get_slowest_steps(limit=10, db_path=None):
    conn = get_conn(db_path or DB_PATH)
    rows = conn.execute("""
        SELECT name, ROUND(AVG(latency_ms)) as avg_ms, MAX(latency_ms) as max_ms,
               COUNT(*) as calls,
               SUM(CASE WHEN error IS NOT NULL THEN 1 ELSE 0 END) as errors
        FROM traces GROUP BY name ORDER BY avg_ms DESC LIMIT ?
    """, (limit,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]

def get_eval_trend(days=14, db_path=None):
    conn = get_conn(db_path or DB_PATH)
    rows = conn.execute("""
        SELECT date(e.ts) as day,
               ROUND(AVG(e.accuracy),2) as avg_accuracy,
               ROUND(AVG(e.helpfulness),2) as avg_helpfulness,
               ROUND(AVG(e.safety),2) as avg_safety,
               ROUND(AVG(e.overall),2) as avg_overall,
               COUNT(*) as evals_run
        FROM evals e WHERE e.ts >= datetime('now', ?)
        GROUP BY day ORDER BY day
    """, (f"-{days} days",)).fetchall()
    conn.close()
    return [dict(r) for r in rows]

def get_stats_summary(db_path=None):
    conn = get_conn(db_path or DB_PATH)
    r = conn.execute("""
        SELECT COUNT(*) as total_traces,
               ROUND(SUM(cost_usd),4) as total_cost,
               ROUND(AVG(latency_ms)) as avg_latency,
               SUM(CASE WHEN error IS NOT NULL THEN 1 ELSE 0 END) as total_errors,
               COUNT(DISTINCT session_id) as total_sessions,
               COUNT(DISTINCT model) as models_used
        FROM traces
    """).fetchone()
    conn.close()
    return dict(r) if r else {}

def get_failure_heatmap(db_path=None):
    conn = get_conn(db_path or DB_PATH)
    rows = conn.execute("""
        SELECT name, strftime('%H', ts) as hour, COUNT(*) as failures
        FROM traces WHERE error IS NOT NULL
        GROUP BY name, hour ORDER BY failures DESC LIMIT 50
    """).fetchall()
    conn.close()
    return [dict(r) for r in rows]
