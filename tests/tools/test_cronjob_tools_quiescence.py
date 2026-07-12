from __future__ import annotations

import json
from unittest.mock import patch


def _result(status, *, run_success=None):
    from cron.quiescence import DispatchResult

    return DispatchResult(
        status=status,
        job_id="j1",
        mode="immediate",
        request_id="req",
        attempt_token="a" if status in {"ACCEPTED", "COMPLETED"} else None,
        run_token="r" if status in {"ACCEPTED", "COMPLETED"} else None,
        run_success=run_success,
    )


def test_immediate_tool_brokers_without_local_claim_run_or_mark(monkeypatch):
    import cron.quiescence as q
    from tools.cronjob_tools import _execute_job_now

    monkeypatch.setattr(q, "request_broker_dispatch", lambda job_id, **kw: _result("ACCEPTED"))
    with patch("tools.cronjob_tools.claim_job_for_fire") as claim, \
         patch("cron.scheduler.run_one_job") as run, \
         patch("tools.cronjob_tools.mark_job_run") as mark:
        out = _execute_job_now({"id": "j1"})
    assert out["dispatch_status"] == "ACCEPTED"
    assert out["claimed"] is True
    assert out["completed"] is False
    assert out["pending"] is True
    assert out["success"] is None
    claim.assert_not_called()
    run.assert_not_called()
    mark.assert_not_called()


def test_immediate_quiescent_busy_is_explicit_non_success(monkeypatch):
    import cron.quiescence as q
    from tools.cronjob_tools import _execute_job_now

    monkeypatch.setattr(q, "request_broker_dispatch", lambda job_id, **kw: _result("QUIESCENT_BUSY"))
    out = _execute_job_now({"id": "j1"})
    assert out == {
        "claimed": False,
        "success": False,
        "error": "Cron broker is quiescent/busy; retry the immediate run explicitly.",
        "dispatch_status": "QUIESCENT_BUSY",
        "retryable": True,
    }


def test_cronjob_run_reports_broker_failure_as_top_level_failure(monkeypatch):
    from tools.cronjob_tools import cronjob

    monkeypatch.setattr("tools.cronjob_tools.resolve_job_ref", lambda ref: {"id": "j1", "name": "job"})
    monkeypatch.setattr("tools.cronjob_tools._execute_job_now", lambda job: {
        "claimed": False,
        "success": False,
        "error": "Canonical cron broker unavailable.",
        "dispatch_status": "BROKER_UNAVAILABLE",
        "retryable": True,
    })
    out = json.loads(cronjob(action="run", job_id="j1"))
    assert out["success"] is False
    assert out["dispatch_status"] == "BROKER_UNAVAILABLE"
    assert out["retryable"] is True
