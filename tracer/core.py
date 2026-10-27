"""
core.py — The @trace decorator.
Wrap any function to automatically log every LLM call to SQLite.
"""
import time
import uuid
import functools
from typing import Callable
from tracer.storage import save_trace, calc_cost
from tracer.session import get_session_id, next_step

_trace_hooks: list[Callable[[dict], None]] = []


def register_trace_hook(fn: Callable[[dict], None]) -> None:
    """Subscribe to every saved trace (used by observability.integrations.tracer_bridge)."""
    if fn not in _trace_hooks:
        _trace_hooks.append(fn)


def unregister_trace_hook(fn: Callable[[dict], None]) -> None:
    if fn in _trace_hooks:
        _trace_hooks.remove(fn)


def _notify_hooks(record: dict) -> None:
    for hook in list(_trace_hooks):
        try:
            hook(record)
        except Exception:          # a failing hook must never break the traced call
            pass

def trace(name: str = None, model: str = None, tags: list = None):
    """
    Decorator: instruments any LLM-calling function.

    Usage:
        @trace(name="summarize", model="llama3.2", tags=["prod"])
        def call_llm(prompt: str) -> str:
            ...

    Automatically captures: latency, token counts, cost, errors, session grouping.
    """
    def decorator(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            trace_id   = str(uuid.uuid4())[:8]
            session_id = get_session_id()
            step_num   = next_step() if session_id else 0

            # Best-effort prompt extraction
            prompt_val = (
                kwargs.get("prompt")
                or (str(args[0]) if args else "")
            )

            start  = time.time()
            result = None
            error  = None

            try:
                result = fn(*args, **kwargs)
                return result
            except Exception as e:
                error = str(e)
                raise
            finally:
                latency_ms = int((time.time() - start) * 1000)

                in_tok, out_tok, response_text = 0, 0, ""
                used_model = model or "unknown"

                if isinstance(result, dict):
                    # OpenAI-style response object
                    usage      = result.get("usage", {})
                    in_tok     = usage.get("prompt_tokens", 0)
                    out_tok    = usage.get("completion_tokens", 0)
                    used_model = result.get("model", used_model)
                    choices    = result.get("choices", [])
                    if choices:
                        response_text = choices[0].get("message", {}).get("content", "")
                elif isinstance(result, str):
                    response_text = result[:2000]
                    out_tok = len(response_text) // 4   # ~4 chars per token
                    in_tok  = len(str(prompt_val)) // 4

                cost_usd = calc_cost(used_model, in_tok, out_tok)

                record = {
                    "trace_id":      trace_id,
                    "session_id":    session_id,
                    "step_num":      step_num,
                    "name":          name or fn.__name__,
                    "tags":          tags or [],
                    "model":         used_model,
                    "prompt_text":   str(prompt_val)[:2000],
                    "response_text": response_text,
                    "input_tokens":  in_tok,
                    "output_tokens": out_tok,
                    "cost_usd":      cost_usd,
                    "latency_ms":    latency_ms,
                    "error":         error,
                }
                save_trace(record)
                _notify_hooks(record)

        return wrapper
    return decorator
