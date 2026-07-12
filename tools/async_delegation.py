#!/usr/bin/env python3
"""
Async (background) delegation registry.

Backs ``delegate_task(background=true)``: the parent agent dispatches a
subagent that runs on a module-level daemon executor and returns a handle
immediately, so the user and the model can keep working while the child runs.

When the child finishes, a completion event is pushed onto the SHARED
``process_registry.completion_queue`` with ``type="async_delegation"``. The
CLI (``cli.py`` process_loop) and gateway (``_run_process_watcher`` /
``completion_queue`` drain) already poll that queue while the agent is idle
and forge a fresh user/internal turn from each event. We deliberately reuse
that rail rather than reaching into a running agent loop:

  - completions surface as a NEW turn when the agent is idle, never spliced
    between a tool result and an assistant message. That keeps strict
    message-role alternation legal and the prompt cache intact (hard
    invariant: never mutate past context).
  - we inherit the queue's de-dup, crash-recovery checkpoint, and the
    existing CLI + gateway drain wiring for free — no new drain loops in the
    two largest files in the repo.

The completion payload carries a RICH, self-contained task-source block (the
original goal, the context the parent supplied, toolsets, model, dispatch
time, status, and the full result summary). When the result re-enters the
conversation the parent may be deep in unrelated context and won't remember
why the subagent existed; the block lets it either use the result or
re-dispatch if the world has moved on.

This module owns ONLY the async lifecycle. The actual child build + run is
delegated back to ``delegate_tool._run_single_child`` via an injected
runner, so all the credential leasing, heartbeat, timeout, and result-shaping
logic stays in one place.
"""

from __future__ import annotations

import json
import logging
import math
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable, Dict, List, Optional

from tools.daemon_pool import DaemonThreadPoolExecutor
from tools.thread_context import propagate_context_to_thread

logger = logging.getLogger(__name__)

# Back-compat alias — the daemon executor now lives in tools.daemon_pool so
# other subsystems (tool_executor, memory_manager, delegate_tool, skills_hub)
# can share it. Existing imports of ``_DaemonThreadPoolExecutor`` keep working.
_DaemonThreadPoolExecutor = DaemonThreadPoolExecutor


# ---------------------------------------------------------------------------
# Module-level state
# ---------------------------------------------------------------------------
# A persistent daemon executor (NOT a `with ThreadPoolExecutor()` block, which
# would join on exit and defeat the whole point of async). Workers are daemon
# threads so a hard process exit doesn't hang on an in-flight child.
_executor: Optional[ThreadPoolExecutor] = None
_executor_lock = threading.Lock()
_executor_max_workers: int = 0

_records_lock = threading.Lock()
# delegation_id -> record dict. Kept for the lifetime of the run plus a short
# tail after completion so `list_async_delegations()` can show recent results.
_records: Dict[str, Dict[str, Any]] = {}

_DEFAULT_MAX_ASYNC_CHILDREN = 3
# How many completed records to retain for status queries before pruning.
_MAX_RETAINED_COMPLETED = 50
_COMPLETED_RECORD_TTL_SECONDS = 900.0
_PROGRESS_HEARTBEAT_STALE_SECONDS = 75.0
_MAX_PROGRESS_CHILDREN = 32
_MAX_PROGRESS_TEXT_CHARS = 160
_MAX_PROGRESS_RESPONSE_BYTES = 60_000
_MAX_PROGRESS_DELEGATIONS_BYTES = 58_000
_PROGRESS_TERMINAL_STATUSES = frozenset(
    {"completed", "failed", "error", "interrupted", "unknown"}
)
_PROGRESS_CHILD_KEYS = frozenset(
    {
        "task_index",
        "status",
        "phase",
        "heartbeat_at",
        "heartbeat_age_seconds",
        "current_tool",
        "api_calls",
        "budget_used",
        "budget_max",
    }
)


def _normalize_progress_status(value: Any) -> str:
    status = str(value or "").strip().lower()
    if status in {"completed", "success"}:
        return "completed"
    if status in {"failed", "error", "timeout", "timed_out"}:
        return "failed"
    if status in {"interrupted", "cancelled", "canceled"}:
        return "interrupted"
    if status in {"queued", "running"}:
        return status
    return "unknown"


def _progress_text(value: Any, limit: int = _MAX_PROGRESS_TEXT_CHARS) -> str:
    compact = " ".join(str(value or "").split())
    if len(compact) <= limit:
        return compact
    return compact[: max(0, limit - 1)] + "…"


def _progress_index(value: Any, fallback: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError, OverflowError):
        return fallback
    return parsed if parsed >= 0 else fallback


def _progress_number(value: Any) -> Optional[float]:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    try:
        parsed = float(value)
    except (OverflowError, TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def _get_executor(max_workers: int) -> ThreadPoolExecutor:
    """Lazily create (or grow) the shared daemon executor.

    We never shrink — ThreadPoolExecutor can't resize — but if the configured
    cap grows between calls we rebuild a larger pool. Existing in-flight
    futures keep running on the old pool until it's garbage collected.
    """
    global _executor, _executor_max_workers
    with _executor_lock:
        if _executor is None or max_workers > _executor_max_workers:
            # Daemon threads: thread_name_prefix aids debugging in stack dumps.
            _executor = _DaemonThreadPoolExecutor(
                max_workers=max_workers,
                thread_name_prefix="async-delegate",
            )
            _executor_max_workers = max_workers
        return _executor


def active_count() -> int:
    """Number of async delegations currently running."""
    with _records_lock:
        return sum(1 for r in _records.values() if r.get("status") == "running")


def _new_delegation_id() -> str:
    return f"deleg_{uuid.uuid4().hex[:8]}"


def _prune_completed_locked(monotonic_now: Optional[float] = None) -> None:
    """Drop expired completed records, then enforce the retention count cap.

    Caller must hold ``_records_lock``. A monotonic completion stamp prevents
    wall-clock jumps from extending or prematurely ending the retention tail.
    """
    observed = time.monotonic() if monotonic_now is None else float(monotonic_now)
    expired = [
        rid
        for rid, record in _records.items()
        if record.get("status") != "running"
        and isinstance(record.get("completed_monotonic"), (int, float))
        and observed - float(record["completed_monotonic"]) > _COMPLETED_RECORD_TTL_SECONDS
    ]
    for rid in expired:
        _records.pop(rid, None)

    completed = [
        (rid, r)
        for rid, r in _records.items()
        if r.get("status") != "running"
    ]
    if len(completed) <= _MAX_RETAINED_COMPLETED:
        return
    # Oldest-first by completion time (fall back to dispatch time).
    completed.sort(key=lambda kv: kv[1].get("completed_at") or kv[1].get("dispatched_at") or 0)
    for rid, _ in completed[: len(completed) - _MAX_RETAINED_COMPLETED]:
        _records.pop(rid, None)


def dispatch_async_delegation(
    *,
    goal: str,
    context: Optional[str],
    toolsets: Optional[List[str]],
    role: str,
    model: Optional[str],
    session_key: str,
    parent_session_id: Optional[str] = None,
    runner: Callable[[], Dict[str, Any]],
    origin_ui_session_id: str = "",
    interrupt_fn: Optional[Callable[[], None]] = None,
    max_async_children: int = _DEFAULT_MAX_ASYNC_CHILDREN,
) -> Dict[str, Any]:
    """Spawn ``runner`` on the daemon executor and return a handle immediately.

    Parameters
    ----------
    goal, context, toolsets, role, model
        The dispatch-time task spec, captured verbatim for the rich
        completion block.
    session_key
        The gateway session_key (from ``tools.approval.get_current_session_key``)
        captured on the parent thread BEFORE dispatch, because the daemon
        worker thread won't carry the contextvar. Used to route the
        completion back to the originating session.
    parent_session_id
        The durable ``state.db`` session id of the parent agent that spawned
        the delegation. Carried on the completion event so the gateway can
        pin routing to the spawning session instead of recovering the latest
        ``ended_at IS NULL`` row for the peer tuple (#57498).
    runner
        Zero-arg callable that builds + runs the child and returns the same
        result dict ``_run_single_child`` produces. Runs on the worker thread.
    interrupt_fn
        Optional callable to signal the child to stop (used on shutdown /
        explicit cancel).
    max_async_children
        Concurrency cap. When at capacity the dispatch is REJECTED (the caller
        should fall back to sync or tell the user) rather than queued, so a
        runaway model can't pile up unbounded background work.

    Returns
    -------
    dict
        ``{"status": "dispatched", "delegation_id": ...}`` on success, or
        ``{"status": "rejected", "error": ...}`` when at capacity.
    """
    delegation_id = _new_delegation_id()
    dispatched_at = time.time()
    record: Dict[str, Any] = {
        "delegation_id": delegation_id,
        "goal": goal,
        "context": context,
        "toolsets": list(toolsets) if toolsets else None,
        "role": role,
        "model": model,
        "session_key": session_key,
        "origin_ui_session_id": origin_ui_session_id,
        "parent_session_id": parent_session_id,
        "status": "running",
        "dispatched_at": dispatched_at,
        "completed_at": None,
        "interrupt_fn": interrupt_fn,
    }
    # Capacity check and record insert under ONE lock hold — checking
    # active_count() separately would let two concurrent dispatches (e.g.
    # from different gateway sessions) both pass the check and exceed the cap.
    with _records_lock:
        running = sum(
            1 for r in _records.values() if r.get("status") == "running"
        )
        if running >= max_async_children:
            return {
                "status": "rejected",
                "error": (
                    f"Async delegation capacity reached ({max_async_children} "
                    f"running). Wait for one to finish (its result will re-enter "
                    f"the chat), or run this task synchronously "
                    f"(background=false). Raise delegation.max_concurrent_children in "
                    f"config.yaml to allow more concurrent background subagents."
                ),
            }
        _records[delegation_id] = record

    executor = _get_executor(max_async_children)

    def _worker() -> None:
        result: Dict[str, Any] = {}
        status = "error"
        try:
            result = runner() or {}
            status = result.get("status") or "completed"
        except Exception as exc:  # noqa: BLE001 — must never crash the worker
            logger.exception("Async delegation %s crashed", delegation_id)
            result = {
                "status": "error",
                "summary": None,
                "error": f"{type(exc).__name__}: {exc}",
                "api_calls": 0,
                "duration_seconds": round(time.time() - dispatched_at, 2),
            }
            status = "error"
        finally:
            _finalize(delegation_id, result, status)

    try:
        # Propagate the dispatching profile so the detached child resolves
        # get_hermes_home() under the right profile.
        executor.submit(propagate_context_to_thread(_worker))
    except Exception as exc:  # pragma: no cover — pool submit failure is rare
        with _records_lock:
            _records.pop(delegation_id, None)
        return {
            "status": "rejected",
            "error": f"Failed to schedule async delegation: {exc}",
        }

    logger.info(
        "Dispatched async delegation %s (session_key=%s): %s",
        delegation_id, session_key or "<cli>", (goal or "")[:80],
    )
    return {"status": "dispatched", "delegation_id": delegation_id}


def _finalize(delegation_id: str, result: Dict[str, Any], status: str) -> None:
    """Mark a record complete and push the completion event onto the queue."""
    with _records_lock:
        record = _records.get(delegation_id)
        if record is None:
            return
        record["status"] = status
        record["completed_at"] = time.time()
        record["completed_monotonic"] = time.monotonic()
        child_status = _normalize_progress_status(result.get("status") or status)
        record["final_progress"] = {
            "children": [
                {
                    "task_index": 0,
                    "goal": record.get("goal") or "",
                    "status": child_status,
                    "phase": child_status,
                }
            ]
        }
        record["interrupt_fn"] = None  # drop the closure; child is done
        # Snapshot fields needed for the event while holding the lock.
        event_record = dict(record)
        _prune_completed_locked()

    _push_completion_event(event_record, result, status)


def _push_completion_event(
    record: Dict[str, Any], result: Dict[str, Any], status: str
) -> None:
    """Push a type='async_delegation' event onto the shared completion queue.

    Best-effort: a failure here must not crash the worker, but it WOULD mean a
    silently-lost result, so we log loudly.
    """
    try:
        from tools.process_registry import process_registry
    except Exception as exc:  # pragma: no cover
        logger.error(
            "Async delegation %s finished but process_registry import failed; "
            "result lost: %s",
            record.get("delegation_id"), exc,
        )
        return

    summary = result.get("summary")
    error = result.get("error")
    dispatched_at = record.get("dispatched_at") or time.time()
    completed_at = record.get("completed_at") or time.time()

    evt = {
        "type": "async_delegation",
        "delegation_id": record.get("delegation_id"),
        # session_key routes the completion back to the originating gateway
        # session; empty string => CLI (single-session) path.
        "session_key": record.get("session_key", ""),
        "origin_ui_session_id": record.get("origin_ui_session_id", ""),
        "parent_session_id": record.get("parent_session_id"),
        "goal": record.get("goal", ""),
        "context": record.get("context"),
        "toolsets": record.get("toolsets"),
        "role": record.get("role"),
        "model": result.get("model") or record.get("model"),
        "status": status,
        "summary": summary,
        "error": error,
        "api_calls": result.get("api_calls", 0),
        "duration_seconds": result.get(
            "duration_seconds", round(completed_at - dispatched_at, 2)
        ),
        "dispatched_at": dispatched_at,
        "completed_at": completed_at,
        "exit_reason": result.get("exit_reason"),
    }
    try:
        process_registry.completion_queue.put(evt)
    except Exception as exc:  # pragma: no cover
        logger.error(
            "Async delegation %s: failed to enqueue completion event; "
            "result lost: %s",
            record.get("delegation_id"), exc,
        )


def dispatch_async_delegation_batch(
    *,
    goals: List[str],
    context: Optional[str],
    toolsets: Optional[List[str]],
    role: str,
    model: Optional[str],
    session_key: str,
    parent_session_id: Optional[str] = None,
    runner: Callable[[], Dict[str, Any]],
    progress_fn: Optional[Callable[[], Dict[str, Any]]] = None,
    origin_ui_session_id: str = "",
    interrupt_fn: Optional[Callable[[], None]] = None,
    max_async_children: int = _DEFAULT_MAX_ASYNC_CHILDREN,
) -> Dict[str, Any]:
    """Dispatch a WHOLE fan-out batch as ONE background unit.

    Unlike ``dispatch_async_delegation`` (which backs a single subagent),
    ``runner`` here runs the entire batch — it builds and joins on every child
    in parallel and returns the combined ``{"results": [...],
    "total_duration_seconds": N}`` dict that the synchronous path would have
    returned. We occupy ONE async slot for the whole batch (the in-batch
    parallelism is bounded separately by ``max_concurrent_children``), so a
    single ``delegate_task`` fan-out never exhausts the async pool by itself.

    When the batch finishes, a SINGLE completion event is pushed onto the
    shared ``process_registry.completion_queue`` carrying the full per-task
    ``results`` list, so the consolidated summaries re-enter the conversation
    as one message once every child is done — the chat is never blocked while
    they run.

    Returns ``{"status": "dispatched", "delegation_id": ...}`` on success or
    ``{"status": "rejected", "error": ...}`` when the async pool is at
    capacity.
    """
    delegation_id = _new_delegation_id()
    dispatched_at = time.time()
    n = len(goals)
    # A combined goal label for status listings / the completion header.
    combined_goal = (
        goals[0] if n == 1 else f"{n} parallel subagents: " + "; ".join(g[:40] for g in goals)
    )
    record: Dict[str, Any] = {
        "delegation_id": delegation_id,
        "goal": combined_goal,
        "goals": list(goals),
        "context": context,
        "toolsets": list(toolsets) if toolsets else None,
        "role": role,
        "model": model,
        "session_key": session_key,
        "origin_ui_session_id": origin_ui_session_id,
        "parent_session_id": parent_session_id,
        "status": "running",
        "dispatched_at": dispatched_at,
        "completed_at": None,
        "interrupt_fn": interrupt_fn,
        "progress_fn": progress_fn,
        "is_batch": True,
    }
    with _records_lock:
        running = sum(
            1 for r in _records.values() if r.get("status") == "running"
        )
        if running >= max_async_children:
            return {
                "status": "rejected",
                "error": (
                    f"Async delegation capacity reached ({max_async_children} "
                    f"running). Wait for one to finish (its result will re-enter "
                    f"the chat), or raise delegation.max_concurrent_children in "
                    f"config.yaml to allow more concurrent background units."
                ),
            }
        _records[delegation_id] = record

    executor = _get_executor(max_async_children)

    def _worker() -> None:
        combined: Dict[str, Any] = {}
        status = "error"
        try:
            combined = runner() or {}
            child_results = combined.get("results") or []
            status = (
                "completed"
                if len(child_results) == n
                and all(
                    isinstance(item, dict)
                    and _normalize_progress_status(item.get("status")) == "completed"
                    for item in child_results
                )
                else "error"
            )
        except Exception as exc:  # noqa: BLE001 — must never crash the worker
            logger.exception("Async delegation batch %s crashed", delegation_id)
            combined = {
                "results": [],
                "error": f"{type(exc).__name__}: {exc}",
                "total_duration_seconds": round(time.time() - dispatched_at, 2),
            }
            status = "error"
        finally:
            _finalize_batch(delegation_id, combined, status)

    try:
        # Propagate the dispatching profile to the detached batch children.
        executor.submit(propagate_context_to_thread(_worker))
    except Exception as exc:  # pragma: no cover
        with _records_lock:
            _records.pop(delegation_id, None)
        return {
            "status": "rejected",
            "error": f"Failed to schedule async delegation batch: {exc}",
        }

    logger.info(
        "Dispatched async delegation batch %s (%d task(s), session_key=%s)",
        delegation_id, n, session_key or "<cli>",
    )
    return {"status": "dispatched", "delegation_id": delegation_id}


def _finalize_batch(
    delegation_id: str, combined: Dict[str, Any], status: str
) -> None:
    """Mark a batch record complete and push ONE combined completion event.

    The external progress callback is snapshotted under the registry lock but
    invoked outside it. Final state is committed only after reacquiring the
    lock and confirming the same still-running record remains registered.
    """
    with _records_lock:
        record = _records.get(delegation_id)
        if record is None:
            return
        record_ref = record
        progress_fn = record.get("progress_fn")
        goals = list(record.get("goals") or [])

    if callable(progress_fn):
        try:
            final_progress = progress_fn() or {}
        except Exception:
            logger.debug("Async delegation final progress callback failed", exc_info=True)
            final_progress = {}
    else:
        final_progress = {}

    if not isinstance(final_progress, dict):
        final_progress = {}
    raw_children = final_progress.get("children")
    by_index = {
        _progress_index(item.get("task_index"), index): dict(item)
        for index, item in enumerate(raw_children if isinstance(raw_children, list) else [])
        if isinstance(item, dict)
    }
    result_by_index = {
        _progress_index(item.get("task_index"), index): item
        for index, item in enumerate(combined.get("results") or [])
        if isinstance(item, dict)
    }
    children = []
    for index, goal in enumerate(goals):
        child = by_index.get(index) or {"task_index": index, "goal": goal}
        result = result_by_index.get(index)
        if result is not None:
            child_status = _normalize_progress_status(result.get("status"))
        elif str(child.get("status") or "") in {"queued", "running", ""}:
            child_status = "unknown"
        else:
            child_status = _normalize_progress_status(child.get("status"))
        child["status"] = child_status
        child["phase"] = child_status
        children.append(child)
    final_progress = {"children": children}

    with _records_lock:
        record = _records.get(delegation_id)
        if record is not record_ref or record.get("status") != "running":
            return
        record["status"] = status
        record["completed_at"] = time.time()
        record["completed_monotonic"] = time.monotonic()
        record["final_progress"] = final_progress
        record["interrupt_fn"] = None
        record["progress_fn"] = None
        event_record = dict(record)
        _prune_completed_locked()

    try:
        from tools.process_registry import process_registry
    except Exception as exc:  # pragma: no cover
        logger.error(
            "Async delegation batch %s finished but process_registry import "
            "failed; result lost: %s",
            delegation_id, exc,
        )
        return

    dispatched_at = event_record.get("dispatched_at") or time.time()
    completed_at = event_record.get("completed_at") or time.time()
    evt = {
        "type": "async_delegation",
        "delegation_id": delegation_id,
        "session_key": event_record.get("session_key", ""),
        "origin_ui_session_id": event_record.get("origin_ui_session_id", ""),
        "parent_session_id": event_record.get("parent_session_id"),
        "goal": event_record.get("goal", ""),
        "goals": event_record.get("goals"),
        "context": event_record.get("context"),
        "toolsets": event_record.get("toolsets"),
        "role": event_record.get("role"),
        "model": event_record.get("model"),
        "status": status,
        "is_batch": True,
        # The full per-task results list — the formatter renders a
        # consolidated multi-task block from this.
        "results": combined.get("results") or [],
        "error": combined.get("error"),
        "total_duration_seconds": combined.get("total_duration_seconds"),
        "dispatched_at": dispatched_at,
        "completed_at": completed_at,
    }
    try:
        process_registry.completion_queue.put(evt)
    except Exception as exc:  # pragma: no cover
        logger.error(
            "Async delegation batch %s: failed to enqueue completion event; "
            "result lost: %s",
            delegation_id, exc,
        )


def list_async_delegations() -> List[Dict[str, Any]]:
    """Snapshot of async delegations (running + recently completed).

    Safe to call from any thread. Excludes non-serialisable callbacks.
    """
    with _records_lock:
        _prune_completed_locked()
        return [
            {k: v for k, v in r.items() if k not in {"interrupt_fn", "progress_fn"}}
            for r in _records.values()
        ]


def list_async_delegation_progress(
    *,
    owner_session_ids: List[str],
    now: Optional[float] = None,
    monotonic_now: Optional[float] = None,
) -> List[Dict[str, Any]]:
    """Return a bounded, serialisable, redacted progress snapshot for UI use.

    Dispatch context, toolset metadata, routing keys, callbacks, and interrupt
    closures never leave this process-local registry. Progress is based only on
    terminal child counts; iteration budgets remain separate telemetry.
    """
    owners = {
        str(value or "").strip()
        for value in owner_session_ids
        if str(value or "").strip()
    }
    if not owners:
        return []
    observed_at = time.time() if now is None else float(now)
    with _records_lock:
        _prune_completed_locked(monotonic_now)
        records = [
            (record, dict(record))
            for record in _records.values()
            if owners.intersection(
                {
                    str(record.get("origin_ui_session_id") or ""),
                    str(record.get("parent_session_id") or ""),
                    str(record.get("session_key") or ""),
                }
            )
        ]
    records.sort(
        key=lambda item: (
            item[1].get("status") != "running",
            -float(item[1].get("completed_at") or item[1].get("dispatched_at") or 0.0),
        )
    )

    snapshots: List[Dict[str, Any]] = []
    for record_ref, record in records:
        progress_fn = record.get("progress_fn")
        if callable(progress_fn):
            try:
                progress = progress_fn() or {}
            except Exception:
                logger.debug("Async delegation progress callback failed", exc_info=True)
                progress = {}
            with _records_lock:
                current = _records.get(record.get("delegation_id"))
                if current is not record_ref:
                    continue
                if (
                    current.get("status") != record.get("status")
                    or current.get("progress_fn") is not progress_fn
                ):
                    record = dict(current)
                    progress = current.get("final_progress") or {}
        else:
            progress = record.get("final_progress") or {}

        status = _normalize_progress_status(record.get("status"))
        raw_children = progress.get("children") if isinstance(progress, dict) else []
        all_children = [
            raw for raw in (raw_children if isinstance(raw_children, list) else []) if isinstance(raw, dict)
        ]
        all_statuses = [_normalize_progress_status(raw.get("status")) for raw in all_children]
        all_heartbeats = [
            heartbeat
            for raw in all_children
            if (heartbeat := _progress_number(raw.get("heartbeat_at"))) is not None
        ]
        visible_children = sorted(
            all_children,
            key=lambda raw: (
                _normalize_progress_status(raw.get("status")) in _PROGRESS_TERMINAL_STATUSES,
                _progress_index(raw.get("task_index"), 0),
            ),
        )
        children: List[Dict[str, Any]] = []
        for raw in visible_children[:_MAX_PROGRESS_CHILDREN]:
            child = {key: raw[key] for key in _PROGRESS_CHILD_KEYS if key in raw}
            child["task_index"] = _progress_index(child.get("task_index"), 0)
            child["status"] = _normalize_progress_status(child.get("status"))
            child["phase"] = _progress_text(child.get("phase") or child["status"], 32)
            if "current_tool" in child:
                child["current_tool"] = _progress_text(child.get("current_tool"), 80)
            for numeric_key in ("heartbeat_at", "api_calls", "budget_used", "budget_max"):
                numeric_value = _progress_number(child.get(numeric_key))
                if numeric_value is None:
                    child.pop(numeric_key, None)
                else:
                    child[numeric_key] = numeric_value
            heartbeat = child.get("heartbeat_at")
            if isinstance(heartbeat, (int, float)):
                child["heartbeat_age_seconds"] = round(
                    max(0.0, observed_at - float(heartbeat)), 1
                )
            children.append(child)

        goals = list(record.get("goals") or [])
        total_count = len(goals) or (1 if record.get("goal") else len(all_children))
        finished_statuses = {"completed", "failed", "interrupted"}
        if status in _PROGRESS_TERMINAL_STATUSES:
            finished_statuses.add("unknown")
        finished_count = min(
            total_count,
            sum(1 for status_value in all_statuses if status_value in finished_statuses),
        )
        completed_count = min(
            finished_count,
            sum(1 for status_value in all_statuses if status_value == "completed"),
        )
        failed_count = min(
            max(0, finished_count - completed_count),
            sum(
                1
                for status_value in all_statuses
                if status_value in {"failed", "interrupted", "unknown"}
            ),
        )

        heartbeat_at = max(
            all_heartbeats
            or [float(record.get("completed_at") or record.get("dispatched_at") or observed_at)]
        )
        heartbeat_age = round(max(0.0, observed_at - heartbeat_at), 1)
        phase = (
            status
            if status in _PROGRESS_TERMINAL_STATUSES
            else "waiting_peer"
            if 0 < finished_count < total_count
            else "running"
        )
        snapshots.append(
            {
                "delegation_id": _progress_text(record.get("delegation_id"), 64),
                "status": status,
                "phase": phase,
                "total_count": total_count,
                "finished_count": finished_count,
                "completed_count": completed_count,
                "failed_count": failed_count,
                "running_count": max(0, total_count - finished_count),
                "progress_percent": round((finished_count / total_count) * 100)
                if total_count
                else 0,
                "heartbeat_at": heartbeat_at,
                "heartbeat_age_seconds": heartbeat_age,
                "stale": status == "running"
                and heartbeat_age > _PROGRESS_HEARTBEAT_STALE_SECONDS,
                "children": children,
            }
        )
    bounded: List[Dict[str, Any]] = []
    for snapshot in snapshots:
        candidate = [*bounded, snapshot]
        encoded = json.dumps(
            candidate,
            ensure_ascii=False,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
        if len(encoded) > _MAX_PROGRESS_DELEGATIONS_BYTES:
            continue
        bounded.append(snapshot)
    return bounded


def interrupt_all(reason: str = "shutdown") -> int:
    """Signal every running async delegation to stop. Returns how many.

    Used on ``/stop`` and gateway shutdown so a dangling background subagent
    can't keep burning tokens with no one listening. The child still emits a
    completion event (status='interrupted') via the normal finalize path.
    """
    count = 0
    with _records_lock:
        targets = [
            r for r in _records.values() if r.get("status") == "running"
        ]
    for r in targets:
        fn = r.get("interrupt_fn")
        if callable(fn):
            try:
                fn()
                count += 1
            except Exception as exc:
                logger.debug(
                    "interrupt_all: %s interrupt failed: %s",
                    r.get("delegation_id"), exc,
                )
    if count:
        logger.info("Interrupted %d async delegation(s) (%s)", count, reason)
    return count


def interrupt_for_session(
    session_key: str = "",
    origin_ui_session_id: str = "",
    parent_session_id: str = "",
    reason: str = "session_end",
) -> int:
    """Signal running async delegations owned by ONE session to stop.

    A delegation's lifecycle is bound to the session that spawned it: when
    that session ends, its in-flight background subagents must end with it —
    a completed orphan would otherwise sit on the shared completion queue
    with no live owner, either leaking into another chat or burning tokens
    with no one listening (#55578).

    Selectors (any matching field claims the record):
    - ``origin_ui_session_id``: the live TUI tab/window that commissioned it.
    - ``session_key``: the durable routing key captured at dispatch.
    - ``parent_session_id``: the spawning agent's durable session-db id —
      the right selector for gateway chats, whose ``session_key`` (the
      platform conversation key) SURVIVES a ``/new`` reset while the
      session id rotates.

    Returns how many were interrupted.
    """
    if not session_key and not origin_ui_session_id and not parent_session_id:
        return 0
    count = 0
    with _records_lock:
        targets = [
            r for r in _records.values()
            if r.get("status") == "running"
            and (
                (origin_ui_session_id and str(r.get("origin_ui_session_id") or "") == origin_ui_session_id)
                or (session_key and str(r.get("session_key") or "") == session_key)
                or (parent_session_id and str(r.get("parent_session_id") or "") == parent_session_id)
            )
        ]
    for r in targets:
        fn = r.get("interrupt_fn")
        if callable(fn):
            try:
                fn()
                count += 1
            except Exception as exc:
                logger.debug(
                    "interrupt_for_session: %s interrupt failed: %s",
                    r.get("delegation_id"), exc,
                )
    if count:
        logger.info(
            "Interrupted %d async delegation(s) for ending session (%s)",
            count, reason,
        )
    return count


def _reset_for_tests() -> None:
    """Test-only: clear all state and tear down the executor."""
    global _executor, _executor_max_workers
    with _executor_lock:
        if _executor is not None:
            _executor.shutdown(wait=False)
        _executor = None
        _executor_max_workers = 0
    with _records_lock:
        _records.clear()
