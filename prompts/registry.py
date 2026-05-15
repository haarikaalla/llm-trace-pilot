"""
Prompt versioning registry.
Every time you change a prompt, save it here with a version number.
Never lose a working prompt again.
"""
import hashlib
import json
import sqlite3
import os

DB_PATH = os.environ.get("AGENTSCOPE_DB", "agentscope.db")

class PromptRegistry:
    """
    Usage:
        reg = PromptRegistry()
        v = reg.save("summarize", "Summarize this in 3 bullets: {text}")
        prompt = reg.get("summarize")          # latest version
        prompt = reg.get("summarize", version=1)  # specific version
        history = reg.history("summarize")     # all versions
    """
    def __init__(self, db_path: str = DB_PATH):
        self.db = db_path
        self._init()

    def _init(self):
        conn = sqlite3.connect(self.db)
        conn.execute("""CREATE TABLE IF NOT EXISTS prompts (
            id      INTEGER PRIMARY KEY AUTOINCREMENT,
            name    TEXT NOT NULL,
            version INTEGER NOT NULL,
            hash    TEXT NOT NULL,
            content TEXT NOT NULL,
            tags    TEXT,
            ts      TEXT DEFAULT (datetime('now')),
            UNIQUE(name, version)
        )""")
        conn.commit()
        conn.close()

    def save(self, name: str, content: str, tags: list = None) -> int:
        h = hashlib.sha256(content.encode()).hexdigest()[:8]
        conn = sqlite3.connect(self.db)
        row = conn.execute(
            "SELECT MAX(version) FROM prompts WHERE name=?", (name,)
        ).fetchone()
        version = (row[0] or 0) + 1
        conn.execute(
            "INSERT INTO prompts (name,version,hash,content,tags) VALUES (?,?,?,?,?)",
            (name, version, h, content, json.dumps(tags or []))
        )
        conn.commit()
        conn.close()
        return version

    def get(self, name: str, version: int = None) -> dict | None:
        conn = sqlite3.connect(self.db)
        conn.row_factory = sqlite3.Row
        if version:
            row = conn.execute(
                "SELECT * FROM prompts WHERE name=? AND version=?", (name, version)
            ).fetchone()
        else:
            row = conn.execute(
                "SELECT * FROM prompts WHERE name=? ORDER BY version DESC LIMIT 1", (name,)
            ).fetchone()
        conn.close()
        return dict(row) if row else None

    def render(self, name: str, version: int = None, **kwargs) -> str:
        """Get prompt content and fill in template variables."""
        entry = self.get(name, version)
        if not entry:
            raise KeyError(f"Prompt '{name}' not found")
        return entry["content"].format(**kwargs)

    def history(self, name: str) -> list[dict]:
        conn = sqlite3.connect(self.db)
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT * FROM prompts WHERE name=? ORDER BY version", (name,)
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def list_all(self) -> list[dict]:
        conn = sqlite3.connect(self.db)
        conn.row_factory = sqlite3.Row
        rows = conn.execute("""
            SELECT name, MAX(version) as versions, MAX(ts) as last_updated
            FROM prompts GROUP BY name ORDER BY last_updated DESC
        """).fetchall()
        conn.close()
        return [dict(r) for r in rows]
