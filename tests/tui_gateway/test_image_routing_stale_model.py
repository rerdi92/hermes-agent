"""Regression coverage for live TUI image-routing identity."""

import json
import threading

from types import SimpleNamespace
from unittest.mock import patch

from tui_gateway.server import (
    _active_image_routing_identity,
    _active_image_routing_runtime,
    _enrich_attached_images_for_agent,
)


def test_live_agent_identity_wins_after_model_switch():
    agent = SimpleNamespace(provider="alibaba", model="qwen3.7-plus")

    with patch(
        "agent.auxiliary_client._read_main_provider",
        side_effect=AssertionError("stale provider fallback used"),
    ), patch(
        "agent.auxiliary_client._read_main_model",
        side_effect=AssertionError("stale model fallback used"),
    ):
        identity = _active_image_routing_identity(agent)

    assert identity == ("alibaba", "qwen3.7-plus")


def test_missing_agent_identity_uses_runtime_fallback():
    agent = SimpleNamespace(provider="", model=None)

    with patch(
        "agent.auxiliary_client._read_main_provider", return_value="openai-codex"
    ), patch(
        "agent.auxiliary_client._read_main_model", return_value="gpt-5.5-codex"
    ):
        identity = _active_image_routing_identity(agent)

    assert identity == ("openai-codex", "gpt-5.5-codex")


def test_text_enrichment_worker_binds_live_switched_runtime(tmp_path):
    """Pre-turn vision must use the live TUI agent, not persisted config."""
    image = tmp_path / "sample.png"
    image.write_bytes(b"not-a-real-image")
    agent = SimpleNamespace(
        provider="custom",
        model="switched-vision-model",
        base_url="http://127.0.0.1:9876/v1",
        api_key="live-key",
        api_mode="openai_chat",
        auth_mode="bearer",
    )
    captured = {}

    async def fake_vision_analyze_tool(**_kwargs):
        from agent.auxiliary_client import _RUNTIME_MAIN_CONTEXT

        captured["runtime"] = dict(_RUNTIME_MAIN_CONTEXT.get() or {})
        return json.dumps({"success": True, "analysis": "live runtime used"})

    def worker():
        from agent.auxiliary_client import _RUNTIME_MAIN_CONTEXT

        captured["before"] = _RUNTIME_MAIN_CONTEXT.get()
        captured["result"] = _enrich_attached_images_for_agent(
            agent,
            "describe it",
            [str(image)],
        )
        captured["after"] = _RUNTIME_MAIN_CONTEXT.get()

    with patch(
        "tools.vision_tools.vision_analyze_tool",
        side_effect=fake_vision_analyze_tool,
    ):
        thread = threading.Thread(target=worker)
        thread.start()
        thread.join(timeout=3)

    assert not thread.is_alive()
    assert captured["before"] is None
    assert captured["after"] is None
    assert captured["runtime"] == {
        "provider": "custom",
        "model": "switched-vision-model",
        "base_url": "http://127.0.0.1:9876/v1",
        "api_key": "live-key",
        "api_mode": "openai_chat",
        "auth_mode": "bearer",
    }
    assert "live runtime used" in captured["result"]


def test_active_image_runtime_uses_identity_fallback_and_live_endpoint():
    agent = SimpleNamespace(
        provider="",
        model=None,
        base_url="http://live.example/v1",
        api_key="live-key",
        api_mode="responses",
        auth_mode="oauth",
    )
    with patch(
        "agent.auxiliary_client._read_main_provider", return_value="openai-codex"
    ), patch(
        "agent.auxiliary_client._read_main_model", return_value="gpt-live"
    ):
        runtime = _active_image_routing_runtime(agent)

    assert runtime == {
        "provider": "openai-codex",
        "model": "gpt-live",
        "base_url": "http://live.example/v1",
        "api_key": "live-key",
        "api_mode": "responses",
        "auth_mode": "oauth",
    }
