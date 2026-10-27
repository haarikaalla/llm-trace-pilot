"""Shared fixtures: isolated DB per test, fresh in-memory bus and platform."""
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


@pytest.fixture
def obs_env(tmp_path, monkeypatch):
    db = str(tmp_path / "obs.db")
    monkeypatch.setenv("OBSERVABILITY_DB", db)
    monkeypatch.setenv("AGENTSCOPE_DB", db)
    monkeypatch.setenv("EVENT_BUS", "memory")
    monkeypatch.setenv("LOG_INDEX", "sqlite")
    monkeypatch.setenv("AI_SUMMARIES", "false")
    monkeypatch.setenv("DASHBOARD_BACKGROUND", "0")
    from observability.config import reset_settings_cache, get_settings
    from observability.services import platform as pl
    import observability.streaming.bus as bus_mod
    reset_settings_cache()
    bus_mod._memory_singleton = None
    pl.reset_platform()
    yield get_settings()
    pl.reset_platform()
    bus_mod._memory_singleton = None
    reset_settings_cache()


@pytest.fixture
def platform(obs_env):
    from observability.services.platform import ObservabilityPlatform
    return ObservabilityPlatform(obs_env)
