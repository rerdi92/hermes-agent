"""Tests for async (background) delegation — tools/async_delegation.py.

Covers the dispatch handle, non-blocking behavior, completion-event delivery
onto the shared process_registry.completion_queue, the rich re-injection block
formatting, capacity rejection, and crash handling.
"""

import json
import queue
import sys
import threading
import time

import pytest

from tools import async_delegation as ad
from tools.process_registry import process_registry, format_process_notification


@pytest.fixture(autouse=True)
def _clean_state():
    gateway_server = sys.modules.get("tui_gateway.server")
    if gateway_server is not None:
        gateway_server._stop_all_notification_pollers()
        for sid in list(gateway_server._sessions):
            gateway_server._close_session_by_id(sid, end_reason="test_cleanup")
    ad._reset_for_tests()
    while not process_registry.completion_queue.empty():
        process_registry.completion_queue.get_nowait()
    yield
    if gateway_server is not None:
        gateway_server._stop_all_notification_pollers()
        for sid in list(gateway_server._sessions):
            gateway_server._close_session_by_id(sid, end_reason="test_cleanup")
    ad._reset_for_tests()
    while not process_registry.completion_queue.empty():
        process_registry.completion_queue.get_nowait()


def _drain_one(timeout=5.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if not process_registry.completion_queue.empty():
            return process_registry.completion_queue.get_nowait()
        time.sleep(0.02)
    return None


def test_dispatch_returns_immediately_without_blocking():
    gate = threading.Event()

    def runner():
        gate.wait(timeout=5)
        return {"status": "completed", "summary": "done", "api_calls": 1,
                "duration_seconds": 0.1, "model": "m"}

    t0 = time.monotonic()
    res = ad.dispatch_async_delegation(
        goal="g", context=None, toolsets=None, role="leaf", model="m",
        session_key="", runner=runner, max_async_children=3,
    )
    elapsed = time.monotonic() - t0

    assert res["status"] == "dispatched"
    assert res["delegation_id"].startswith("deleg_")
    # Non-blocking invariant: dispatch returned while the runner is still
    # gated (active), so it cannot have waited on the gate. The active_count
    # check is the environment-independent proof; the generous wall-clock
    # bound is a loose sanity backstop, not the primary assertion (a loaded
    # CI runner can be slow but never anywhere near the runner's 5s gate).
    assert ad.active_count() == 1
    assert elapsed < 4.0, f"dispatch blocked {elapsed:.2f}s (gate is 5s)"
    gate.set()


def test_async_executor_workers_are_daemon_threads():
    gate = threading.Event()

    def runner():
        gate.wait(timeout=5)
        return {"status": "completed", "summary": "done"}

    res = ad.dispatch_async_delegation(
        goal="daemon check", context=None, toolsets=None, role="leaf", model="m",
        session_key="", runner=runner, max_async_children=1,
    )
    assert res["status"] == "dispatched"

    deadline = time.monotonic() + 2
    worker = None
    while time.monotonic() < deadline:
        worker = next(
            (t for t in threading.enumerate() if t.name.startswith("async-delegate")),
            None,
        )
        if worker is not None:
            break
        time.sleep(0.02)
    assert worker is not None
    assert worker.daemon is True
    gate.set()
    assert _drain_one() is not None


def test_completion_event_lands_on_shared_queue_with_session_key():
    def runner():
        return {"status": "completed", "summary": "the result",
                "api_calls": 3, "duration_seconds": 2.0, "model": "test-model"}

    res = ad.dispatch_async_delegation(
        goal="compute X", context="some context", toolsets=["web", "file"],
        role="leaf", model="test-model", session_key="agent:main:cli:dm:local",
        parent_session_id="20260703_parent_sid",
        runner=runner, max_async_children=3,
    )
    assert res["status"] == "dispatched"

    evt = _drain_one()
    assert evt is not None
    assert evt["type"] == "async_delegation"
    assert evt["summary"] == "the result"
    assert evt["session_key"] == "agent:main:cli:dm:local"
    assert evt["parent_session_id"] == "20260703_parent_sid"
    assert evt["delegation_id"] == res["delegation_id"]


def test_rich_reinjection_block_is_self_contained():
    def runner():
        return {"status": "completed", "summary": "The answer is 42.",
                "api_calls": 7, "duration_seconds": 3.5, "model": "test-model"}

    ad.dispatch_async_delegation(
        goal="Compute the meaning of life",
        context="User is a philosopher. Respond tersely.",
        toolsets=["web"], role="leaf", model="test-model",
        session_key="", runner=runner, max_async_children=3,
    )
    evt = _drain_one()
    assert evt is not None
    text = format_process_notification(evt)
    assert text is not None
    for needle in [
        "ASYNC DELEGATION COMPLETE",
        "Compute the meaning of life",
        "User is a philosopher",
        "Toolsets: web",
        "The answer is 42.",
        "Status: completed",
        "API calls: 7",
    ]:
        assert needle in text, f"missing {needle!r}"


def test_dispatch_rejected_at_capacity():
    ev = threading.Event()

    def blocker():
        ev.wait(timeout=5)
        return {"status": "completed", "summary": "x"}

    for i in range(2):
        r = ad.dispatch_async_delegation(
            goal=f"task{i}", context=None, toolsets=None, role="leaf",
            model="m", session_key="", runner=blocker, max_async_children=2,
        )
        assert r["status"] == "dispatched"

    r3 = ad.dispatch_async_delegation(
        goal="task3", context=None, toolsets=None, role="leaf", model="m",
        session_key="", runner=blocker, max_async_children=2,
    )
    assert r3["status"] == "rejected"
    assert "capacity reached" in r3["error"]
    ev.set()


def test_crashed_runner_produces_error_completion():
    def boom():
        raise RuntimeError("subagent exploded")

    r = ad.dispatch_async_delegation(
        goal="risky", context=None, toolsets=None, role="leaf", model="m",
        session_key="", runner=boom, max_async_children=3,
    )
    assert r["status"] == "dispatched"
    evt = _drain_one()
    assert evt is not None
    assert evt["status"] == "error"
    text = format_process_notification(evt)
    assert text is not None
    assert "did not complete successfully" in text
    assert "subagent exploded" in text


def test_interrupt_all_signals_running_children():
    ev = threading.Event()
    interrupted = {"count": 0}

    def blocker():
        ev.wait(timeout=5)
        return {"status": "interrupted", "summary": None,
                "error": "cancelled"}

    def interrupt_fn():
        interrupted["count"] += 1
        ev.set()

    ad.dispatch_async_delegation(
        goal="long task", context=None, toolsets=None, role="leaf",
        model="m", session_key="", runner=blocker,
        interrupt_fn=interrupt_fn, max_async_children=3,
    )
    n = ad.interrupt_all(reason="test")
    assert n == 1
    assert interrupted["count"] == 1
    # child still emits a completion event after interrupt
    evt = _drain_one()
    assert evt is not None
    assert evt["status"] == "interrupted"


def test_completed_records_pruned_to_cap():
    # Run more than the retention cap quickly; ensure list doesn't grow forever.
    for i in range(ad._MAX_RETAINED_COMPLETED + 10):
        ad.dispatch_async_delegation(
            goal=f"t{i}", context=None, toolsets=None, role="leaf", model="m",
            session_key="", runner=lambda: {"status": "completed", "summary": "ok"},
            max_async_children=ad._MAX_RETAINED_COMPLETED + 20,
        )
    # let workers finish
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline and ad.active_count() > 0:
        time.sleep(0.05)
    assert len(ad.list_async_delegations()) <= ad._MAX_RETAINED_COMPLETED


def test_progress_snapshot_is_redacted_and_uses_completed_children_only():
    gate = threading.Event()

    def runner():
        gate.wait(timeout=5)
        return {"results": [], "total_duration_seconds": 0.1}

    progress = {
        "children": [
            {
                "task_index": 0,
                "goal": "CHILD_SECRET_A",
                "status": "completed",
                "heartbeat_at": 100.0,
                "phase": "completed",
            },
            {
                "task_index": 1,
                "goal": "CHILD_SECRET_B",
                "status": "running",
                "heartbeat_at": 105.0,
                "phase": "tool",
                "current_tool": "read_file",
            },
        ]
    }
    dispatched = ad.dispatch_async_delegation_batch(
        goals=["CHILD_SECRET_A", "CHILD_SECRET_B"],
        context="must never leave the registry",
        toolsets=["file"],
        role="leaf",
        model="m",
        session_key="private-session-key",
        origin_ui_session_id="ui-owner",
        runner=runner,
        progress_fn=lambda: progress,
        max_async_children=1,
    )

    snapshot = ad.list_async_delegation_progress(now=110.0, owner_session_ids=["ui-owner"])
    assert len(snapshot) == 1
    item = snapshot[0]
    assert item["delegation_id"] == dispatched["delegation_id"]
    assert item["total_count"] == 2
    assert item["completed_count"] == 1
    assert item["progress_percent"] == 50
    assert item["heartbeat_at"] == 105.0
    assert item["heartbeat_age_seconds"] == 5.0
    active_child = next(child for child in item["children"] if child["task_index"] == 1)
    assert active_child["current_tool"] == "read_file"
    assert "context" not in item
    assert "toolsets" not in item
    assert "session_key" not in item
    assert "origin_ui_session_id" not in item
    assert "parent_session_id" not in item
    assert "goals" not in item
    assert "goal" not in item
    assert all("goal" not in child for child in item["children"])
    assert "CHILD_SECRET_A" not in json.dumps(item)
    assert "CHILD_SECRET_B" not in json.dumps(item)
    assert "progress_fn" not in item
    assert all("session_id" not in child for child in item["children"])
    assert ad.list_async_delegation_progress(now=110.0, owner_session_ids=["other-session"]) == []
    gate.set()


# ---------------------------------------------------------------------------
# Integration: delegate_task(background=True) routing
# ---------------------------------------------------------------------------

def test_delegate_task_background_routes_async_and_does_not_block(monkeypatch):
    """delegate_task(background=True) returns a handle without running the
    child synchronously, and the child completes on the background thread.
    A single task is dispatched as a one-item background batch unit."""
    from unittest.mock import MagicMock, patch
    import tools.delegate_tool as dt

    parent = MagicMock()
    parent._delegate_depth = 0
    parent.session_id = "sess"
    parent._interrupt_requested = False
    parent._active_children = []
    parent._active_children_lock = None
    fake_child = MagicMock()
    fake_child._delegate_role = "leaf"
    fake_child._subagent_id = "s1"
    fake_child.session_id = "child-sess"
    fake_child.get_activity_summary.return_value = {
        "last_activity_ts": time.time(),
        "last_activity_desc": "running child",
        "current_tool": None,
        "api_call_count": 1,
        "budget_used": 1,
        "budget_max": 10,
    }

    gate = threading.Event()

    def slow_child(task_index, goal, child=None, parent_agent=None, **kw):
        gate.wait(timeout=5)  # a sync impl would hang delegate_task here
        return {
            "task_index": 0, "status": "completed", "summary": f"done: {goal}",
            "api_calls": 1, "duration_seconds": 0.1, "model": "m",
            "exit_reason": "completed",
        }

    creds = {
        "model": "m", "provider": None, "base_url": None, "api_key": None,
        "api_mode": None, "command": None, "args": None,
    }
    # monkeypatch (not `with`) so patches outlive delegate_task's return and
    # remain active while the background worker runs.
    monkeypatch.setattr(dt, "_build_child_agent", lambda **kw: fake_child)
    monkeypatch.setattr(dt, "_run_single_child", slow_child)
    monkeypatch.setattr(dt, "_resolve_delegation_credentials", lambda *a, **k: creds)
    out = dt.delegate_task(
        goal="the real task", context="ctx",
        background=True, parent_agent=parent,
    )

    import json
    parsed = json.loads(out)
    assert parsed["status"] == "dispatched"
    assert parsed["mode"] == "background"
    assert parsed["delegation_id"].startswith("deleg_")
    # Non-blocking invariant: delegate_task returned while the child is STILL
    # blocked on the closed gate, so no completion event exists yet.
    assert process_registry.completion_queue.empty()
    assert ad.active_count() == 1  # one background batch unit, not finished
    progress = ad.list_async_delegation_progress(owner_session_ids=["sess"])
    assert len(progress) == 1
    assert progress[0]["delegation_id"] == parsed["delegation_id"]
    assert progress[0]["total_count"] == 1
    assert progress[0]["finished_count"] == 0
    assert progress[0]["progress_percent"] == 0
    assert progress[0]["children"][0]["status"] == "running"
    assert "session_id" not in progress[0]["children"][0]

    gate.set()
    evt = _drain_one()
    assert evt is not None
    assert evt["type"] == "async_delegation"
    # Single task rides the batch path → carries a 1-item results list.
    assert evt.get("is_batch") is True
    assert len(evt["results"]) == 1
    assert evt["results"][0]["summary"] == "done: the real task"
    text = format_process_notification(evt)
    assert text is not None
    assert "the real task" in text


def test_delegate_task_background_uses_live_tui_agent_session_id(monkeypatch):
    """TUI async delegation must route to the live/compressed agent id.

    Regression: delegate_task captured the stale approval/session context key
    after compression rotated parent_agent.session_id. The resulting completion
    was orphaned and could be consumed by an unrelated desktop session poller.
    """
    import json
    from unittest.mock import MagicMock
    import tools.delegate_tool as dt
    from gateway.session_context import clear_session_vars, set_session_vars
    from tools.approval import reset_current_session_key, set_current_session_key

    parent = MagicMock()
    parent._delegate_depth = 0
    parent.session_id = "post-compress-tip"
    parent._interrupt_requested = False
    parent._active_children = []
    parent._active_children_lock = None
    fake_child = MagicMock()
    fake_child._delegate_role = "leaf"

    creds = {
        "model": "m", "provider": None, "base_url": None, "api_key": None,
        "api_mode": None, "command": None, "args": None,
    }
    monkeypatch.setattr(dt, "_build_child_agent", lambda **kw: fake_child)
    monkeypatch.setattr(dt, "_resolve_delegation_credentials", lambda *a, **k: creds)
    monkeypatch.setattr(
        dt,
        "_run_single_child",
        lambda *a, **k: {
            "task_index": 0,
            "status": "completed",
            "summary": "done",
            "api_calls": 1,
            "duration_seconds": 0.1,
            "model": "m",
            "exit_reason": "completed",
        },
    )

    approval_token = set_current_session_key("pre-compress-parent")
    session_tokens = set_session_vars(
        source="tui",
        session_key="pre-compress-parent",
        ui_session_id="origin-tab",
    )
    try:
        out = dt.delegate_task(goal="bg task", background=True, parent_agent=parent)
        assert json.loads(out)["status"] == "dispatched"
        evt = _drain_one()
    finally:
        reset_current_session_key(approval_token)
        clear_session_vars(session_tokens)

    assert evt is not None
    assert evt["type"] == "async_delegation"
    assert evt["session_key"] == "post-compress-tip"
    assert evt["origin_ui_session_id"] == "origin-tab"


def test_delegate_task_background_batch_runs_as_one_unit(monkeypatch):
    """A multi-item batch with background=True dispatches the WHOLE fan-out as
    ONE background unit (one handle, one async slot). The children run in
    parallel and join; the consolidated results come back as a single
    completion event when ALL of them finish."""
    import json
    from unittest.mock import MagicMock, patch
    import tools.delegate_tool as dt

    parent = MagicMock()
    parent._delegate_depth = 0
    parent.session_id = "sess"
    parent._interrupt_requested = False
    parent._active_children = []
    parent._active_children_lock = None

    fake_child = MagicMock()
    fake_child._delegate_role = "leaf"

    gate = threading.Event()

    def _blocking_child(task_index, goal, child=None, parent_agent=None, **kw):
        gate.wait(timeout=5)
        return {
            "task_index": task_index, "status": "completed",
            "summary": f"done: {goal}", "api_calls": 1,
            "duration_seconds": 0.1, "model": "m", "exit_reason": "completed",
        }

    creds = {
        "model": "m", "provider": None, "base_url": None, "api_key": None,
        "api_mode": None, "command": None, "args": None,
    }

    # Use monkeypatch (not a `with` block) so the patches stay active while the
    # background worker thread runs _execute_and_aggregate AFTER delegate_task
    # has already returned.
    monkeypatch.setattr(dt, "_build_child_agent", lambda **kw: fake_child)
    monkeypatch.setattr(dt, "_run_single_child", _blocking_child)
    monkeypatch.setattr(dt, "_resolve_delegation_credentials", lambda *a, **k: creds)
    out = dt.delegate_task(
        tasks=[{"goal": "a"}, {"goal": "b"}, {"goal": "c"}],
        background=True,
        parent_agent=parent,
    )

    parsed = json.loads(out)
    assert parsed["status"] == "dispatched"
    assert parsed["mode"] == "background"
    assert parsed["count"] == 3
    assert parsed["delegation_id"].startswith("deleg_")
    assert parsed["goals"] == ["a", "b", "c"]
    # ONE background unit for the whole fan-out (not three), and the call
    # returned while all children are still blocked → chat not blocked.
    assert process_registry.completion_queue.empty()
    assert ad.active_count() == 1

    # Release the children; the whole batch joins and emits ONE event.
    gate.set()
    evt = _drain_one()
    assert evt is not None
    assert evt["type"] == "async_delegation"
    assert evt.get("is_batch") is True
    assert len(evt["results"]) == 3
    summaries = sorted(r["summary"] for r in evt["results"])
    assert summaries == ["done: a", "done: b", "done: c"]
    # The consolidated notification names all three tasks in one block.
    text = format_process_notification(evt)
    assert text is not None
    assert "TASK 1/3" in text and "TASK 2/3" in text and "TASK 3/3" in text
    assert "done: a" in text and "done: b" in text and "done: c" in text
    # No more events — it's a single combined completion, not N of them.
    assert _drain_one() is None


def test_model_dispatch_forces_background():
    """The MODEL-facing dispatch path forces background=True for any top-level
    delegation (single task OR batch), and keeps it off for an orchestrator
    subagent (depth > 0). Direct delegate_task() callers are unaffected (they
    keep the synchronous default)."""
    import tools.delegate_tool as dt
    from unittest.mock import MagicMock

    top = MagicMock()
    top._delegate_depth = 0
    sub = MagicMock()
    sub._delegate_depth = 1

    # Registry-fallback helper: top-level always background, regardless of
    # single vs batch; subagent never.
    assert dt._model_background_value({"goal": "x"}, top) is True
    assert dt._model_background_value(
        {"tasks": [{"goal": "a"}, {"goal": "b"}]}, top
    ) is True
    assert dt._model_background_value({"tasks": [{"goal": "a"}]}, top) is True
    assert dt._model_background_value({"goal": "x"}, sub) is False
    assert dt._model_background_value(
        {"tasks": [{"goal": "a"}, {"goal": "b"}]}, sub
    ) is False


def test_run_agent_dispatch_forces_background():
    """run_agent._dispatch_delegate_task — the live model path — forces
    background on for any top-level delegation (single OR batch) and off for a
    subagent."""
    from unittest.mock import patch
    import run_agent

    class _FakeAgent:
        _delegate_depth = 0

    captured = {}

    def _fake_delegate(**kwargs):
        captured.update(kwargs)
        return "{}"

    with patch("tools.delegate_tool.delegate_task", _fake_delegate):
        agent = _FakeAgent()
        run_agent.AIAgent._dispatch_delegate_task(agent, {"goal": "x"})
        assert captured["background"] is True

        run_agent.AIAgent._dispatch_delegate_task(
            agent, {"tasks": [{"goal": "a"}, {"goal": "b"}]}
        )
        assert captured["background"] is True

        sub = _FakeAgent()
        sub._delegate_depth = 1
        run_agent.AIAgent._dispatch_delegate_task(sub, {"goal": "x"})
        assert captured["background"] is False


def test_dispatch_never_forwards_model_toolsets():
    """The model has no toolsets argument — subagents always inherit the
    parent's toolsets. Even if a model smuggles a `toolsets` key into the
    tool-call args, the live dispatch path must NOT forward it to
    delegate_task (which no longer accepts it) and must not crash."""
    from unittest.mock import patch
    import run_agent

    class _FakeAgent:
        _delegate_depth = 0

    captured = {}

    def _fake_delegate(**kwargs):
        captured.update(kwargs)
        return "{}"

    with patch("tools.delegate_tool.delegate_task", _fake_delegate):
        run_agent.AIAgent._dispatch_delegate_task(
            _FakeAgent(), {"goal": "x", "toolsets": ["web", "terminal"]}
        )
    assert "toolsets" not in captured


def test_delegate_task_background_detaches_child_from_parent(monkeypatch):
    """A background child must NOT remain in parent._active_children —
    otherwise parent-turn interrupts / cache evicts / session close would
    kill the detached subagent mid-run."""
    from unittest.mock import MagicMock, patch
    import tools.delegate_tool as dt

    parent = MagicMock()
    parent._delegate_depth = 0
    parent.session_id = "sess"
    parent._active_children = []
    parent._active_children_lock = threading.Lock()
    fake_child = MagicMock()
    fake_child._delegate_role = "leaf"
    fake_child._subagent_id = "s1"

    gate = threading.Event()

    def slow_child(task_index, goal, child=None, parent_agent=None, **kw):
        gate.wait(timeout=5)
        return {"task_index": 0, "status": "completed", "summary": "ok"}

    def build_and_register(**kw):
        # Mirror what the real _build_child_agent does: register the child
        # for interrupt propagation.
        parent._active_children.append(fake_child)
        return fake_child

    creds = {
        "model": "m", "provider": None, "base_url": None, "api_key": None,
        "api_mode": None, "command": None, "args": None,
    }
    with patch.object(dt, "_build_child_agent", side_effect=build_and_register), \
         patch.object(dt, "_run_single_child", side_effect=slow_child), \
         patch.object(dt, "_resolve_delegation_credentials", return_value=creds):
        out = dt.delegate_task(goal="bg task", background=True, parent_agent=parent)

    import json
    assert json.loads(out)["status"] == "dispatched"
    # Child detached immediately at dispatch, while it is still running.
    assert fake_child not in parent._active_children
    gate.set()
    assert _drain_one() is not None


def test_concurrent_dispatch_respects_capacity():
    """Two threads racing dispatch with cap=1 must yield exactly one accept
    (capacity check and record insert are atomic under the records lock)."""
    gate = threading.Event()

    def blocker():
        gate.wait(timeout=5)
        return {"status": "completed", "summary": "x"}

    results = []
    barrier = threading.Barrier(2)

    def racer():
        barrier.wait(timeout=5)
        results.append(
            ad.dispatch_async_delegation(
                goal="race", context=None, toolsets=None, role="leaf",
                model="m", session_key="", runner=blocker,
                max_async_children=1,
            )
        )

    threads = [threading.Thread(target=racer) for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10)
    statuses = sorted(r["status"] for r in results)
    assert statuses == ["dispatched", "rejected"]
    gate.set()


# ---------------------------------------------------------------------------
# Gateway routing: session_key -> platform/chat_id, rich formatting, injection
# ---------------------------------------------------------------------------

def _make_async_evt(**over):
    evt = {
        "type": "async_delegation",
        "delegation_id": "deleg_x1",
        "session_key": "agent:main:telegram:dm:12345:678",
        "goal": "Investigate flaky test",
        "context": "repo /tmp/p",
        "toolsets": ["terminal"],
        "role": "leaf",
        "model": "m",
        "status": "completed",
        "summary": "Found the bug in test_foo",
        "api_calls": 4,
        "duration_seconds": 12.0,
        "dispatched_at": 1000.0,
        "completed_at": 1012.0,
    }
    evt.update(over)
    return evt


def test_gateway_enriches_routing_from_session_key():
    from gateway.run import GatewayRunner

    runner = object.__new__(GatewayRunner)
    evt = _make_async_evt()
    runner._enrich_async_delegation_routing(evt)
    assert evt["platform"] == "telegram"
    assert evt["chat_id"] == "12345"
    assert evt["thread_id"] == "678"


def test_gateway_formatter_renders_async_block():
    from gateway.run import _format_gateway_process_notification

    txt = _format_gateway_process_notification(_make_async_evt())
    assert txt is not None
    assert "ASYNC DELEGATION COMPLETE" in txt
    assert "Found the bug in test_foo" in txt
    assert "Investigate flaky test" in txt


def test_gateway_watch_drain_requeues_async_without_looping():
    from gateway.run import _drain_gateway_watch_events

    q = queue.Queue()
    async_evt = _make_async_evt()
    watch_evt = {
        "type": "watch_match",
        "session_id": "proc_1",
        "command": "pytest",
        "pattern": "READY",
        "output": "READY",
    }
    q.put(async_evt)
    q.put(watch_evt)

    watch_events = _drain_gateway_watch_events(q)

    assert watch_events == [watch_evt]
    assert q.qsize() == 1
    assert q.get_nowait() == async_evt


def test_gateway_builds_routable_source_from_enriched_event():
    from gateway.run import GatewayRunner

    runner = object.__new__(GatewayRunner)
    evt = _make_async_evt()
    runner._enrich_async_delegation_routing(evt)
    src = runner._build_process_event_source(evt)
    assert src is not None
    assert src.platform.value == "telegram"
    assert src.chat_id == "12345"


def test_gateway_cli_origin_event_left_unrouted():
    """An empty session_key (CLI origin) is left without routing fields."""
    from gateway.run import GatewayRunner

    runner = object.__new__(GatewayRunner)
    evt = _make_async_evt(session_key="")
    runner._enrich_async_delegation_routing(evt)
    assert "platform" not in evt


def test_empty_batch_results_become_unknown_evidence_not_completed_success():
    ad.dispatch_async_delegation_batch(
        goals=["first", "second"],
        context=None,
        toolsets=None,
        role="leaf",
        model="m",
        session_key="owner",
        parent_session_id="owner",
        runner=lambda: {"results": []},
        progress_fn=lambda: [],
        max_async_children=1,
    )

    assert _drain_one() is not None
    item = ad.list_async_delegation_progress(owner_session_ids=["owner"])[0]
    assert item["status"] == "failed"
    assert item["completed_count"] == 0
    assert item["failed_count"] == 2
    assert [child["status"] for child in item["children"]] == ["unknown", "unknown"]


def test_batch_progress_callback_runs_outside_registry_lock():
    callback_entered = threading.Event()
    callback_release = threading.Event()
    read_finished = threading.Event()

    def slow_progress():
        callback_entered.set()
        callback_release.wait(timeout=5)
        return {"children": []}

    with ad._records_lock:
        ad._records["deleg-lock"] = {
            "delegation_id": "deleg-lock",
            "goals": ["one"],
            "goal": "one",
            "status": "running",
            "dispatched_at": time.time(),
            "progress_fn": slow_progress,
        }

    finalizer = threading.Thread(
        target=ad._finalize_batch,
        args=("deleg-lock", {"results": [{"task_index": 0, "status": "completed"}]}, "completed"),
    )
    finalizer.start()
    assert callback_entered.wait(timeout=2)

    reader = threading.Thread(target=lambda: (ad.active_count(), read_finished.set()))
    reader.start()
    assert read_finished.wait(timeout=0.5), "registry read blocked behind external progress callback"

    callback_release.set()
    finalizer.join(timeout=2)
    reader.join(timeout=2)


def test_completed_progress_records_expire_by_monotonic_age(monkeypatch):
    monkeypatch.setattr(ad, "_COMPLETED_RECORD_TTL_SECONDS", 10.0)
    with ad._records_lock:
        ad._records["deleg-old"] = {
            "delegation_id": "deleg-old",
            "goal": "old",
            "goals": ["old"],
            "session_key": "owner",
            "status": "completed",
            "dispatched_at": 1.0,
            "completed_at": 2.0,
            "completed_monotonic": 5.0,
            "final_progress": {"children": [{"task_index": 0, "status": "completed"}]},
        }

    assert ad.list_async_delegation_progress(
        owner_session_ids=["owner"], now=20.0, monotonic_now=20.0
    ) == []
    assert ad.list_async_delegations() == []


def test_progress_payload_has_hard_string_child_and_byte_bounds():
    children = [
        {
            "task_index": index,
            "subagent_id": "s" * 1_000,
            "goal": "g" * 10_000,
            "status": "running",
            "phase": "tool",
            "current_tool": "t" * 10_000,
        }
        for index in range(200)
    ]
    children[0].update(
        {
            "heartbeat_at": object(),
            "api_calls": 10**10_000,
            "budget_used": float("nan"),
            "budget_max": float("inf"),
        }
    )
    with ad._records_lock:
        for record_index in range(20):
            delegation_id = f"deleg-big-{record_index}"
            ad._records[delegation_id] = {
                "delegation_id": delegation_id,
                "goal": "r" * 10_000,
                "goals": ["x"] * 200,
                "session_key": "owner",
                "status": "running",
                "dispatched_at": time.time(),
                "progress_fn": lambda: {"children": children},
            }

    payload = ad.list_async_delegation_progress(owner_session_ids=["owner"])
    encoded = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    envelope = {
        "id": "delegation-progress",
        "result": {
            "delegations": payload,
            "process_instance_id": "f" * 32,
            "process_local": True,
            "schema_version": 1,
            "snapshot_at": time.time(),
        },
    }
    envelope_bytes = json.dumps(envelope, separators=(",", ":")).encode("utf-8")
    assert len(encoded) <= ad._MAX_PROGRESS_DELEGATIONS_BYTES
    assert len(envelope_bytes) <= ad._MAX_PROGRESS_RESPONSE_BYTES
    assert len(payload[0]["children"]) <= ad._MAX_PROGRESS_CHILDREN
    assert "goal" not in payload[0]
    assert all("goal" not in child for child in payload[0]["children"])
    assert "r" * 161 not in encoded.decode("utf-8")
    assert all("subagent_id" not in child and "duration_seconds" not in child for child in payload[0]["children"])
    assert all(key not in payload[0] for key in ("role", "model", "is_batch", "dispatched_at", "completed_at"))


def test_progress_counts_all_children_even_when_export_list_is_capped():
    children = [
        {"task_index": index, "goal": f"g{index}", "status": "completed", "phase": "completed"}
        for index in range(50)
    ]
    with ad._records_lock:
        ad._records["deleg-fifty"] = {
            "delegation_id": "deleg-fifty",
            "goal": "fifty",
            "goals": [f"g{index}" for index in range(50)],
            "session_key": "owner",
            "status": "completed",
            "dispatched_at": time.time(),
            "completed_at": time.time(),
            "completed_monotonic": time.monotonic(),
            "final_progress": {"children": children},
        }

    item = ad.list_async_delegation_progress(owner_session_ids=["owner"])[0]
    assert len(item["children"]) == ad._MAX_PROGRESS_CHILDREN
    assert item["total_count"] == 50
    assert item["finished_count"] == 50
    assert item["completed_count"] == 50
    assert item["progress_percent"] == 100


def test_single_completion_publishes_one_child_of_evidence():
    dispatched = ad.dispatch_async_delegation(
        goal="single",
        context=None,
        toolsets=None,
        role="leaf",
        model="m",
        session_key="owner",
        parent_session_id="owner",
        runner=lambda: {"status": "completed", "summary": "done"},
        max_async_children=1,
    )
    assert dispatched["status"] == "dispatched"
    assert _drain_one() is not None

    item = ad.list_async_delegation_progress(owner_session_ids=["owner"])[0]
    assert item["status"] == "completed"
    assert item["finished_count"] == 1
    assert item["completed_count"] == 1
    assert item["progress_percent"] == 100


def test_running_progress_records_sort_before_completed_tail():
    now = time.time()
    with ad._records_lock:
        ad._records["deleg-old"] = {
            "delegation_id": "deleg-old",
            "goal": "old",
            "session_key": "owner",
            "status": "completed",
            "dispatched_at": now - 10,
            "completed_at": now - 5,
            "completed_monotonic": time.monotonic(),
            "final_progress": {"children": [{"task_index": 0, "status": "completed"}]},
        }
        ad._records["deleg-live"] = {
            "delegation_id": "deleg-live",
            "goal": "live",
            "session_key": "owner",
            "status": "running",
            "dispatched_at": now,
        }

    payload = ad.list_async_delegation_progress(owner_session_ids=["owner"])
    assert [item["delegation_id"] for item in payload[:2]] == ["deleg-live", "deleg-old"]


def test_export_cap_prioritizes_running_children():
    children = [
        {
            "task_index": index,
            "status": "completed" if index < 32 else "running",
            "phase": "completed" if index < 32 else "tool",
        }
        for index in range(40)
    ]
    with ad._records_lock:
        ad._records["deleg-cap"] = {
            "delegation_id": "deleg-cap",
            "goals": [f"g{index}" for index in range(40)],
            "session_key": "owner",
            "status": "running",
            "dispatched_at": time.time(),
            "progress_fn": lambda: {"children": children},
        }

    item = ad.list_async_delegation_progress(owner_session_ids=["owner"])[0]
    assert item["running_count"] == 8
    assert sum(child["status"] == "running" for child in item["children"]) == 8


def test_finalize_batch_reconciles_running_callback_with_terminal_result():
    with ad._records_lock:
        ad._records["deleg-reconcile"] = {
            "delegation_id": "deleg-reconcile",
            "goals": ["secret"],
            "session_key": "owner",
            "status": "running",
            "dispatched_at": time.time(),
            "progress_fn": lambda: {
                "children": [{"task_index": 0, "status": "running", "phase": "tool"}]
            },
        }

    ad._finalize_batch(
        "deleg-reconcile",
        {"results": [{"task_index": 0, "status": "completed"}]},
        "completed",
    )
    item = ad.list_async_delegation_progress(owner_session_ids=["owner"])[0]
    assert item["status"] == "completed"
    assert item["finished_count"] == 1
    assert item["completed_count"] == 1
    assert item["running_count"] == 0
    assert item["progress_percent"] == 100
    assert item["children"][0]["status"] == "completed"


def test_running_root_does_not_count_malformed_unknown_child_as_finished():
    with ad._records_lock:
        ad._records["deleg-malformed-running"] = {
            "delegation_id": "deleg-malformed-running",
            "goals": ["secret"],
            "session_key": "owner",
            "status": "running",
            "dispatched_at": time.time(),
            "progress_fn": lambda: {"children": [{"task_index": 0, "phase": "running"}]},
        }

    item = ad.list_async_delegation_progress(owner_session_ids=["owner"])[0]
    assert item["status"] == "running"
    assert item["finished_count"] == 0
    assert item["failed_count"] == 0
    assert item["running_count"] == 1
    assert item["progress_percent"] == 0


def test_progress_read_revalidates_record_after_callback_race():
    entered = threading.Event()
    release = threading.Event()
    holder = {}

    def progress_fn():
        entered.set()
        assert release.wait(timeout=2)
        return {"children": [{"task_index": 0, "status": "running"}]}

    with ad._records_lock:
        record = {
            "delegation_id": "deleg-race",
            "goals": ["secret"],
            "session_key": "owner",
            "status": "running",
            "dispatched_at": time.time(),
            "progress_fn": progress_fn,
        }
        ad._records["deleg-race"] = record

    reader = threading.Thread(
        target=lambda: holder.setdefault(
            "payload", ad.list_async_delegation_progress(owner_session_ids=["owner"])
        )
    )
    reader.start()
    assert entered.wait(timeout=2)
    with ad._records_lock:
        record["status"] = "completed"
        record["completed_at"] = time.time()
        record["completed_monotonic"] = time.monotonic()
        record["progress_fn"] = None
        record["final_progress"] = {
            "children": [{"task_index": 0, "status": "completed", "phase": "completed"}]
        }
    release.set()
    reader.join(timeout=2)

    assert not reader.is_alive()
    item = holder["payload"][0]
    assert item["status"] == "completed"
    assert item["finished_count"] == 1
    assert item["completed_count"] == 1
    assert item["progress_percent"] == 100


