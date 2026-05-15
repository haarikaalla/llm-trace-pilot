import uuid
import contextvars

_session_ctx  = contextvars.ContextVar("session_id", default=None)
_step_ctx     = contextvars.ContextVar("step_num",   default=0)

class AgentSession:
    """
    Context manager for grouping all steps of one agent run.

    Usage:
        with AgentSession("research_agent") as s:
            result1 = step_one(...)
            result2 = step_two(...)
        # All traces in DB share the same session_id
    """
    def __init__(self, name: str):
        self.session_id = str(uuid.uuid4())[:8]
        self.name = name

    def __enter__(self):
        _session_ctx.set(self.session_id)
        _step_ctx.set(0)
        return self

    def __exit__(self, *_):
        _session_ctx.set(None)
        _step_ctx.set(0)

    def __repr__(self):
        return f"AgentSession(name={self.name!r}, id={self.session_id})"

def get_session_id() -> str | None:
    return _session_ctx.get()

def next_step() -> int:
    n = _step_ctx.get() + 1
    _step_ctx.set(n)
    return n
