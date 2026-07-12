"""Tests for broker-only cronjob action='run' immediate dispatch (#41037)."""
import json
from unittest.mock import patch

from tools.cronjob_tools import cronjob, _execute_job_now


_JOB = {"id": "job-run-1", "name": "manual run", "prompt": "hi",
        "schedule": {"kind": "cron", "expr": "0 9 * * *"}}


def _dispatch(status, *, run_success=None):
    from cron.quiescence import DispatchResult

    return DispatchResult(
        status=status,
        job_id="job-run-1",
        mode="immediate",
        request_id="req",
        attempt_token="a" if status in {"ACCEPTED", "COMPLETED"} else None,
        run_token="r" if status in {"ACCEPTED", "COMPLETED"} else None,
        run_success=run_success,
    )


class TestCronjobRunExecutesImmediately:
    def test_run_action_claims_and_fires_via_run_one_job(self, monkeypatch):
        """action='run' returns structured broker acceptance without local effects."""
        ran = {"job": "after-run", "last_status": "ok", "last_error": None}
        requested = []
        monkeypatch.setattr(
            "cron.quiescence.request_broker_dispatch",
            lambda job_id, **kw: requested.append((job_id, kw)) or _dispatch("ACCEPTED"),
        )
        with patch("tools.cronjob_tools.resolve_job_ref", return_value=dict(_JOB)), \
             patch("tools.cronjob_tools.claim_job_for_fire") as m_claim, \
             patch("cron.scheduler.run_one_job") as m_run, \
             patch("tools.cronjob_tools.mark_job_run") as m_mark, \
             patch("tools.cronjob_tools.get_job", return_value=ran):
            out = json.loads(cronjob(action="run", job_id="job-run-1"))

        assert out["success"] is True
        assert out["dispatch_status"] == "ACCEPTED"
        assert out["retryable"] is False
        assert out["job"]["executed"] is False
        assert out["job"]["execution_pending"] is True
        assert out["job"]["execution_success"] is None
        from hermes_constants import get_hermes_home
        assert requested == [
            (
                "job-run-1",
                {"mode": "immediate", "profile_home": get_hermes_home()},
            )
        ]
        m_claim.assert_not_called()
        m_run.assert_not_called()
        m_mark.assert_not_called()

    def test_run_skips_when_claim_lost(self, monkeypatch):
        """Broker dedup is explicit and never triggers a local fallback."""
        monkeypatch.setattr(
            "cron.quiescence.request_broker_dispatch",
            lambda *_a, **_k: _dispatch("ALREADY_RUNNING"),
        )
        with patch("tools.cronjob_tools.resolve_job_ref", return_value=dict(_JOB)), \
             patch("tools.cronjob_tools.claim_job_for_fire") as m_claim, \
             patch("cron.scheduler.run_one_job") as m_run, \
             patch("tools.cronjob_tools.mark_job_run") as m_mark, \
             patch("tools.cronjob_tools.get_job", return_value=dict(_JOB)):
            out = json.loads(cronjob(action="run", job_id="job-run-1"))

        assert out["success"] is False
        assert out["dispatch_status"] == "ALREADY_RUNNING"
        assert out["retryable"] is False
        assert out["job"]["executed"] is False
        assert out["job"]["execution_success"] is False
        assert "already being fired" in out["job"]["execution_skipped"].lower()
        m_claim.assert_not_called()
        m_run.assert_not_called()
        m_mark.assert_not_called()

    def test_run_reports_failure_from_last_status(self, monkeypatch):
        """A completed broker run carries structured unsuccessful completion."""
        failed = {"id": "job-run-1", "last_status": "error", "last_error": "provider 500"}
        monkeypatch.setattr(
            "cron.quiescence.request_broker_dispatch",
            lambda *_a, **_k: _dispatch("COMPLETED", run_success=False),
        )
        with patch("tools.cronjob_tools.resolve_job_ref", return_value=dict(_JOB)), \
             patch("tools.cronjob_tools.claim_job_for_fire") as m_claim, \
             patch("cron.scheduler.run_one_job") as m_run, \
             patch("tools.cronjob_tools.mark_job_run") as m_mark, \
             patch("tools.cronjob_tools.get_job", return_value=failed):
            out = json.loads(cronjob(action="run", job_id="job-run-1"))

        assert out["success"] is False
        assert out["dispatch_status"] == "COMPLETED"
        assert out["retryable"] is False
        assert out["job"]["executed"] is True
        assert out["job"]["execution_success"] is False
        assert out["job"]["execution_error"] == "Cron job completed unsuccessfully."
        m_claim.assert_not_called()
        m_run.assert_not_called()
        m_mark.assert_not_called()

    def test_execute_job_now_bails_without_claim(self, monkeypatch):
        """_execute_job_now surfaces broker dedup without local claim/run/mark."""
        monkeypatch.setattr(
            "cron.quiescence.request_broker_dispatch",
            lambda *_a, **_k: _dispatch("ALREADY_RUNNING"),
        )
        with patch("tools.cronjob_tools.claim_job_for_fire") as m_claim, \
             patch("cron.scheduler.run_one_job") as m_run, \
             patch("tools.cronjob_tools.mark_job_run") as m_mark:
            res = _execute_job_now(dict(_JOB))

        assert res == {
            "claimed": False,
            "success": False,
            "error": "Job is already being fired by the scheduler; not run again.",
            "dispatch_status": "ALREADY_RUNNING",
            "retryable": False,
        }
        m_claim.assert_not_called()
        m_run.assert_not_called()
        m_mark.assert_not_called()

    def test_execute_job_now_marks_failure_on_exception(self, monkeypatch):
        """Broker unavailability is retryable and never marks locally."""
        monkeypatch.setattr(
            "cron.quiescence.request_broker_dispatch",
            lambda *_a, **_k: _dispatch("BROKER_UNAVAILABLE"),
        )
        with patch("tools.cronjob_tools.claim_job_for_fire") as m_claim, \
             patch("cron.scheduler.run_one_job") as m_run, \
             patch("tools.cronjob_tools.mark_job_run") as m_mark:
            res = _execute_job_now(dict(_JOB))

        assert res == {
            "claimed": False,
            "success": False,
            "error": "Canonical cron broker unavailable.",
            "dispatch_status": "BROKER_UNAVAILABLE",
            "retryable": True,
        }
        m_claim.assert_not_called()
        m_run.assert_not_called()
        m_mark.assert_not_called()
