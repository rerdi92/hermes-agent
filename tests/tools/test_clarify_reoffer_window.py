"""Regression coverage for clarify's overall re-offer deadline."""

import json

from tools.clarify_tool import cap_clarify_attempt_timeout, clarify_tool


def test_attempt_timeout_is_capped_by_remaining_overall_window(monkeypatch):
    monkeypatch.setattr("tools.clarify_tool._load_reoffer_policy", lambda: (3, 10))
    ticks = iter([100.0, 106.0])
    monkeypatch.setattr("tools.clarify_tool.time.monotonic", lambda: next(ticks))
    observed_timeouts = []

    def callback(question, choices):
        observed_timeouts.append(
            cap_clarify_attempt_timeout(
                3600,
                {
                    "clarify_reoffer_attempts": 3,
                    "clarify_reoffer_window_seconds": 10,
                },
            )
        )
        return "[clarify prompt cancelled]"

    result = json.loads(
        clarify_tool(
            "Proceed?",
            choices=["yes", "no"],
            callback=callback,
        )
    )

    assert observed_timeouts == [4.0]
    assert result["selection_status"] == "cancelled"
