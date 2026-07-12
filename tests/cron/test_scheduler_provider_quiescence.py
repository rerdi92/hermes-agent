from __future__ import annotations

from unittest.mock import patch


def _dispatch(status="ACCEPTED", mode="provider", job_id="j1"):
    from cron.quiescence import DispatchResult

    return DispatchResult(
        status=status,
        job_id=job_id,
        mode=mode,
        request_id="req",
        attempt_token="a" if status in {"ACCEPTED", "COMPLETED"} else None,
        run_token="r" if status in {"ACCEPTED", "COMPLETED"} else None,
        run_success=True if status == "COMPLETED" else None,
    )


def test_direct_run_one_job_without_broker_capability_hard_stops_before_effects(monkeypatch):
    from cron.scheduler import run_one_job

    with patch("cron.scheduler.run_job") as effects, patch("cron.scheduler.mark_job_run") as mark:
        result = run_one_job({"id": "j1"})
    assert result.status.value == "BROKER_REQUIRED"
    effects.assert_not_called()
    mark.assert_not_called()


def test_provider_fire_due_brokers_and_has_no_claim_or_run_fallback(monkeypatch):
    import cron.quiescence as q
    from cron.scheduler_provider import InProcessCronScheduler

    calls = []
    monkeypatch.setattr(q, "request_broker_dispatch", lambda job_id, **kw: calls.append((job_id, kw)) or _dispatch())
    with patch("cron.jobs.claim_job_for_fire") as claim, patch("cron.scheduler.run_one_job") as run:
        result = InProcessCronScheduler().fire_due("j1")
    assert result.status.value == "ACCEPTED"
    assert calls == [("j1", {"mode": "provider"})]
    claim.assert_not_called()
    run.assert_not_called()


def test_provider_unavailable_is_structured_and_falsy(monkeypatch):
    import cron.quiescence as q
    from cron.scheduler_provider import InProcessCronScheduler

    monkeypatch.setattr(q, "request_broker_dispatch", lambda *a, **k: _dispatch("BROKER_UNAVAILABLE"))
    result = InProcessCronScheduler().fire_due("j1")
    assert result.status.value == "BROKER_UNAVAILABLE"
    assert result.retryable is True
    assert bool(result) is False


def test_inprocess_ticker_passes_broker_through_to_shared_tick(monkeypatch):
    import threading
    from cron.scheduler_provider import InProcessCronScheduler

    stop = threading.Event()
    calls = []

    def fake_tick(**kwargs):
        calls.append(kwargs)
        stop.set()
        return 0

    monkeypatch.setattr("cron.scheduler.tick", fake_tick)
    monkeypatch.setattr("cron.jobs.record_ticker_heartbeat", lambda **kw: None)
    broker = object()
    InProcessCronScheduler().start(stop, interval=0, broker=broker)
    assert calls[0]["broker"] is broker
    assert calls[0]["sync"] is False
