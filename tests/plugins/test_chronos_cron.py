"""Unit tests for the Chronos NAS-mediated cron provider (Phase 4D).

All NAS calls are mocked — ZERO live network. These prove:
  - is_available is config-only (no network), false without config.
  - one-shot arming sends the right provision payload (incl. sub-minute fires —
    the agent owns the time, so there's no 1-minute floor).
  - reconcile arms missing, cancels orphaned, skips paused.
  - fire_due re-arms the next one-shot after a successful run, and repeat-N
    (job gone) stops re-arming.
"""

import pytest


@pytest.fixture
def temp_home(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    yield tmp_path


@pytest.fixture
def chronos(monkeypatch):
    """A ChronosCronScheduler with a fake NAS client capturing calls."""
    from plugins.cron_providers.chronos import ChronosCronScheduler

    class FakeClient:
        def __init__(self):
            self.provisions = []
            self.cancels = []
            self._armed = []

        def provision(self, *, job_id, fire_at, agent_callback_url, dedup_key):
            self.provisions.append({
                "job_id": job_id, "fire_at": fire_at,
                "agent_callback_url": agent_callback_url, "dedup_key": dedup_key,
            })
            return {"schedule_id": f"sched-{job_id}"}

        def cancel(self, *, job_id):
            self.cancels.append(job_id)
            return {}

        def list_armed(self):
            return list(self._armed)

    prov = ChronosCronScheduler()
    fake = FakeClient()
    prov._client = fake
    # callback_url is read via _cfg; patch the module helper to avoid config.
    monkeypatch.setattr("plugins.cron_providers.chronos._cfg",
                        lambda *k, default="": "https://agent.example/" if k[-1] == "callback_url" else "https://portal.test")
    return prov, fake


# -- is_available -------------------------------------------------------------

def test_is_available_false_without_config(temp_home, monkeypatch):
    from plugins.cron_providers.chronos import ChronosCronScheduler

    monkeypatch.setattr("plugins.cron_providers.chronos._cfg", lambda *k, default="": "")
    assert ChronosCronScheduler().is_available() is False


def test_is_available_true_with_config_and_token(temp_home, monkeypatch):
    import plugins.cron_providers.chronos as mod
    from plugins.cron_providers.chronos import ChronosCronScheduler

    monkeypatch.setattr(mod, "_cfg", lambda *k, default="": "https://x" )
    monkeypatch.setattr("hermes_cli.auth.get_provider_auth_state",
                        lambda pid: {"access_token": "tok"})
    assert ChronosCronScheduler().is_available() is True


def test_is_available_makes_no_network(temp_home, monkeypatch):
    """is_available must not construct the NAS client / hit network."""
    import plugins.cron_providers.chronos as mod
    from plugins.cron_providers.chronos import ChronosCronScheduler

    monkeypatch.setattr(mod, "_cfg", lambda *k, default="": "https://x")
    monkeypatch.setattr("hermes_cli.auth.get_provider_auth_state",
                        lambda pid: {"access_token": "tok"})
    p = ChronosCronScheduler()

    def explode():
        raise AssertionError("is_available must not build the NAS client")

    monkeypatch.setattr(p, "_get_client", explode)
    assert p.is_available() is True  # did not call _get_client


# -- arming -------------------------------------------------------------------

def test_arm_one_shot_sends_provision(chronos):
    prov, fake = chronos
    prov._arm_one_shot({"id": "j1", "next_run_at": "2026-06-18T12:00:00+00:00"})

    assert len(fake.provisions) == 1
    p = fake.provisions[0]
    assert p["job_id"] == "j1"
    assert p["fire_at"] == "2026-06-18T12:00:00+00:00"
    assert p["dedup_key"] == "j1:2026-06-18T12:00:00+00:00"
    assert p["agent_callback_url"] == "https://agent.example/"


def test_arm_one_shot_preserves_sub_minute_fire(chronos):
    """Sub-minute fire times survive — the agent owns the time, so there's no
    1-minute scheduler floor."""
    prov, fake = chronos
    prov._arm_one_shot({"id": "j2", "next_run_at": "2026-06-18T12:00:30+00:00"})
    assert fake.provisions[0]["fire_at"] == "2026-06-18T12:00:30+00:00"


def test_arm_one_shot_noop_without_next_run(chronos):
    prov, fake = chronos
    prov._arm_one_shot({"id": "j3", "next_run_at": None})
    assert fake.provisions == []


# -- reconcile ----------------------------------------------------------------

def test_reconcile_arms_all_enabled(temp_home, chronos, monkeypatch):
    prov, fake = chronos
    jobs = [
        {"id": "a", "enabled": True, "next_run_at": "2026-06-18T12:00:00+00:00", "state": "scheduled"},
        {"id": "b", "enabled": True, "next_run_at": "2026-06-18T12:05:00+00:00", "state": "scheduled"},
    ]
    monkeypatch.setattr("cron.jobs.load_jobs", lambda: jobs)
    monkeypatch.setattr("cron.jobs.get_job", lambda jid: next(j for j in jobs if j["id"] == jid))

    prov.reconcile()
    assert {p["job_id"] for p in fake.provisions} == {"a", "b"}
    assert fake.cancels == []


def test_reconcile_cancels_orphan_arms_desired(temp_home, chronos, monkeypatch):
    prov, fake = chronos
    # NAS already has a stale arm for deleted job "gone".
    prov._armed = {"gone": "2026-06-18T11:00:00+00:00"}
    jobs = [{"id": "a", "enabled": True, "next_run_at": "2026-06-18T12:00:00+00:00", "state": "scheduled"}]
    monkeypatch.setattr("cron.jobs.load_jobs", lambda: jobs)
    monkeypatch.setattr("cron.jobs.get_job", lambda jid: next((j for j in jobs if j["id"] == jid), None))

    prov.reconcile()
    assert [p["job_id"] for p in fake.provisions] == ["a"]
    assert fake.cancels == ["gone"]


def test_reconcile_skips_paused(temp_home, chronos, monkeypatch):
    prov, fake = chronos
    jobs = [{"id": "p", "enabled": True, "next_run_at": "2026-06-18T12:00:00+00:00", "state": "paused"}]
    monkeypatch.setattr("cron.jobs.load_jobs", lambda: jobs)
    monkeypatch.setattr("cron.jobs.get_job", lambda jid: next((j for j in jobs if j["id"] == jid), None))

    prov.reconcile()
    assert fake.provisions == []


def test_reconcile_skips_already_armed_same_time(temp_home, chronos, monkeypatch):
    prov, fake = chronos
    prov._armed = {"a": "2026-06-18T12:00:00+00:00"}
    jobs = [{"id": "a", "enabled": True, "next_run_at": "2026-06-18T12:00:00+00:00", "state": "scheduled"}]
    monkeypatch.setattr("cron.jobs.load_jobs", lambda: jobs)
    monkeypatch.setattr("cron.jobs.get_job", lambda jid: jobs[0])

    prov.reconcile()
    assert fake.provisions == []  # already armed at the same time → no re-arm


# -- broker fire / completion re-arm -----------------------------------------

def _dispatch(status, *, run_success=None, job_id="j1", attempt_token="a"):
    from cron.quiescence import DispatchResult

    return DispatchResult(
        status=status,
        job_id=job_id,
        mode="provider",
        request_id="req",
        attempt_token=attempt_token,
        run_token="r",
        run_success=run_success,
        completion_next_run_at="later" if run_success is not None else None,
    )


def test_fire_due_submits_without_rearming(chronos, monkeypatch):
    import cron.quiescence as q

    prov, fake = chronos
    broker = object()
    prov._broker = broker
    monkeypatch.setattr(
        q,
        "request_broker_dispatch",
        lambda job_id, **kwargs: _dispatch("ACCEPTED"),
    )

    result = prov.fire_due("j1")

    assert result.status.value == "ACCEPTED"
    assert fake.provisions == []


def test_completed_fire_no_rearm_when_job_gone(chronos, monkeypatch):
    """repeat-N exhausted / one-shot completed means the schedule stops."""
    prov, fake = chronos
    monkeypatch.setattr("cron.jobs.get_job", lambda jid: None)

    assert prov.on_dispatch_completed(_dispatch("COMPLETED", run_success=True)) is True
    assert fake.provisions == []


def test_failed_completion_still_rearms_next_schedule(chronos, monkeypatch):
    prov, fake = chronos
    monkeypatch.setattr(
        "cron.jobs.get_job",
        lambda jid: {"id": jid, "enabled": True, "next_run_at": "later"},
    )

    completed = _dispatch("COMPLETED", run_success=False)
    assert prov.on_dispatch_completed(completed) is True
    assert prov.on_dispatch_completed(completed) is False
    assert [item["job_id"] for item in fake.provisions] == ["j1"]


def test_completion_dedup_key_includes_job_id(chronos, monkeypatch):
    prov, fake = chronos
    monkeypatch.setattr(
        "cron.jobs.get_job",
        lambda jid: {"id": jid, "enabled": True, "next_run_at": "later"},
    )

    assert prov.on_dispatch_completed(
        _dispatch("COMPLETED", run_success=True, job_id="j1", attempt_token="same")
    ) is True
    assert prov.on_dispatch_completed(
        _dispatch("COMPLETED", run_success=True, job_id="j2", attempt_token="same")
    ) is True
    assert [item["job_id"] for item in fake.provisions] == ["j1", "j2"]


def test_rearm_exception_is_retryable_then_deduplicated(chronos, monkeypatch):
    prov, _fake = chronos
    monkeypatch.setattr(
        "cron.jobs.get_job",
        lambda jid: {"id": jid, "enabled": True, "next_run_at": "later"},
    )
    calls = []

    def transient_failure(job, **kwargs):
        calls.append((job["id"], kwargs))
        if len(calls) == 1:
            raise RuntimeError("SDK failed after external arm")

    monkeypatch.setattr(prov, "_arm_one_shot", transient_failure)
    completed = _dispatch("COMPLETED", run_success=True)
    with pytest.raises(RuntimeError, match="Chronos re-arm retry required"):
        prov.on_dispatch_completed(completed)
    assert prov.on_dispatch_completed(completed) is True
    assert prov.on_dispatch_completed(completed) is False
    assert [item[0] for item in calls] == ["j1", "j1"]
    assert calls[0][1] == calls[1][1]
    import hashlib

    assert calls[0][1]["dedup_key"].endswith(hashlib.sha256(b"a").hexdigest())


def test_rearm_retry_skips_stale_fire_time_after_job_changes(chronos, monkeypatch):
    prov, _fake = chronos
    current = {"id": "j1", "enabled": True, "next_run_at": "later"}
    monkeypatch.setattr("cron.jobs.get_job", lambda jid: dict(current))
    calls = []

    def ambiguous_failure(job, **kwargs):
        calls.append(kwargs)
        raise RuntimeError("remote accepted, local response lost")

    monkeypatch.setattr(prov, "_arm_one_shot", ambiguous_failure)
    completed = _dispatch("COMPLETED", run_success=True)
    with pytest.raises(RuntimeError, match="Chronos re-arm retry required"):
        prov.on_dispatch_completed(completed)
    current["next_run_at"] = "later-2"

    assert prov.on_dispatch_completed(completed) is True
    assert len(calls) == 1


def test_real_completion_rearms_post_cas_fire_time(temp_home, chronos):
    from datetime import timedelta
    import hashlib

    from cron import jobs
    from cron.quiescence import DispatchResult

    prov, fake = chronos
    job = jobs.create_job(prompt="rearm", schedule="every 1h")
    stored = jobs.load_jobs()
    stored[0]["next_run_at"] = (jobs._hermes_now() - timedelta(seconds=1)).isoformat()
    jobs.save_jobs(stored)
    due = jobs.scan_due_jobs_read_only(jobs._hermes_now()).jobs[0]
    reservation = jobs.reserve_job_attempt(
        job["id"],
        "attempt-real",
        "run-real",
        "provider",
        due.observed_job_sha256,
        {"pid": 1},
        jobs._hermes_now(),
    )
    outcome = jobs.complete_reserved_attempt(
        job["id"],
        "attempt-real",
        success=True,
    )
    assert reservation.status == "RESERVED"
    persisted_fire = jobs.get_job(job["id"])["next_run_at"]

    assert outcome.next_run_at == persisted_fire
    result = DispatchResult(
        status="COMPLETED",
        job_id=job["id"],
        mode="provider",
        request_id="req-real",
        attempt_token_sha256=hashlib.sha256(b"attempt-real").hexdigest(),
        run_success=True,
        completion_next_run_at=outcome.next_run_at,
    )
    assert prov.on_dispatch_completed(result) is True
    assert fake.provisions[-1]["fire_at"] == persisted_fire
    assert fake.provisions[-1]["dedup_key"].endswith(
        hashlib.sha256(b"attempt-real").hexdigest()
    )
