from tools.delegate_tool import (
    _DelegationProgressTracker,
    _register_subagent,
    _unregister_subagent,
    list_active_subagents,
)


class _FakeAgent:
    _subagent_id = "sa-1"
    session_id = "child-session"

    def __init__(self):
        self.activity = {
            "last_activity_ts": 100.0,
            "last_activity_desc": "waiting for stream response (30s, no chunks yet)",
            "current_tool": None,
            "api_call_count": 2,
            "budget_used": 3,
            "budget_max": 12,
        }

    def get_activity_summary(self):
        return dict(self.activity)


def test_tracker_reports_heartbeat_phase_without_raw_activity_text():
    child = _FakeAgent()
    tracker = _DelegationProgressTracker([(0, {"goal": "inspect"}, child)], now_fn=lambda: 110.0)

    tracker.mark_running(0)
    snapshot = tracker.snapshot()

    item = snapshot["children"][0]
    assert item == {
        "task_index": 0,
        "subagent_id": "sa-1",
        "goal": "inspect",
        "status": "running",
        "phase": "waiting_model",
        "heartbeat_at": 100.0,
        "heartbeat_age_seconds": 10.0,
        "api_calls": 2,
        "budget_used": 3,
        "budget_max": 12,
    }
    assert "last_activity_desc" not in item
    assert "session_id" not in item


def test_tracker_reports_tool_and_terminal_phases():
    child = _FakeAgent()
    child.activity.update({"last_activity_ts": 108.0, "current_tool": "terminal"})
    tracker = _DelegationProgressTracker([(0, {"goal": "build"}, child)], now_fn=lambda: 110.0)

    tracker.mark_running(0)
    running = tracker.snapshot()["children"][0]
    assert running["phase"] == "tool"
    assert running["current_tool"] == "terminal"

    tracker.mark_terminal(0, {"status": "completed", "duration_seconds": 4.2})
    terminal = tracker.snapshot()["children"][0]
    assert terminal["status"] == "completed"
    assert terminal["phase"] == "completed"
    assert terminal["duration_seconds"] == 4.2


def test_tracker_normalizes_timeout_cancelled_and_unknown_as_non_success():
    child = _FakeAgent()
    tracker = _DelegationProgressTracker([(0, {"goal": "build"}, child)], now_fn=lambda: 110.0)

    tracker.mark_terminal(0, {"status": "timeout"})
    assert tracker.snapshot()["children"][0]["status"] == "failed"

    tracker = _DelegationProgressTracker([(0, {"goal": "build"}, child)], now_fn=lambda: 110.0)
    tracker.mark_terminal(0, {"status": "cancelled"})
    assert tracker.snapshot()["children"][0]["status"] == "interrupted"

    tracker = _DelegationProgressTracker([(0, {"goal": "build"}, child)], now_fn=lambda: 110.0)
    tracker.mark_terminal(0, {"status": "unexpected-status"})
    assert tracker.snapshot()["children"][0]["status"] == "unknown"


def test_active_subagent_snapshot_includes_sanitized_heartbeat():
    child = _FakeAgent()
    record = {
        "subagent_id": "sa-1",
        "parent_id": None,
        "depth": 1,
        "goal": "inspect",
        "model": "m",
        "started_at": 90.0,
        "tool_count": 0,
        "status": "running",
        "agent": child,
    }
    _register_subagent(record)
    try:
        item = list_active_subagents(now=110.0)[0]
    finally:
        _unregister_subagent("sa-1")

    assert item["heartbeat_at"] == 100.0
    assert item["heartbeat_age_seconds"] == 10.0
    assert item["phase"] == "waiting_model"
    assert item["api_calls"] == 2
    assert "agent" not in item
    assert "last_activity_desc" not in item


def test_malformed_activity_numbers_fail_closed_for_tracker_and_legacy_snapshot():
    child = _FakeAgent()
    child.activity.update(
        {
            "last_activity_ts": 10**10_000,
            "api_call_count": "not-an-int",
            "last_activity_desc": object(),
            "budget_used": float("nan"),
            "budget_max": float("inf"),
            "current_tool": {"not": "serializable metadata"},
        }
    )
    tracker = _DelegationProgressTracker([(0, {"goal": "inspect"}, child)], now_fn=lambda: 110.0)
    tracker.mark_running(0)
    tracked = tracker.snapshot()["children"][0]
    assert tracked["phase"] == "starting"
    assert "api_calls" not in tracked
    assert "budget_used" not in tracked
    assert "budget_max" not in tracked

    record = {
        "subagent_id": "sa-malformed",
        "started_at": 90.0,
        "status": "running",
        "agent": child,
    }
    _register_subagent(record)
    try:
        active = list_active_subagents(now=110.0)[0]
    finally:
        _unregister_subagent("sa-malformed")

    assert active["heartbeat_at"] == 90.0
    assert active["phase"] == "starting"
    assert "api_calls" not in active
    assert "budget_used" not in active
    assert "budget_max" not in active
