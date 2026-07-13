from __future__ import annotations

from datetime import timedelta
from unittest.mock import patch

import pytest


@pytest.fixture
def store(tmp_path, monkeypatch):
    import cron.jobs as jobs

    cron_dir = tmp_path / "cron"
    monkeypatch.setattr(jobs, "CRON_DIR", cron_dir)
    monkeypatch.setattr(jobs, "JOBS_FILE", cron_dir / "jobs.json")
    monkeypatch.setattr(jobs, "OUTPUT_DIR", cron_dir / "output")
    now = jobs._hermes_now()
    jobs.save_jobs([
        {
            "id": "j1",
            "name": "job one",
            "enabled": True,
            "state": "scheduled",
            "schedule": {"kind": "interval", "minutes": 5},
            "next_run_at": (now - timedelta(seconds=1)).isoformat(),
            "repeat": {"times": 5, "completed": 0},
        }
    ])
    return jobs, tmp_path, now


class CapturingSubmitter:
    def __init__(self, fail=False):
        self.calls = []
        self.fail = fail

    def __call__(self, callback):
        if self.fail:
            raise RuntimeError("submit exploded")
        self.calls.append(callback)
        return object()


def _broker(store, submitter, runner=lambda job: True):
    jobs, home, _now = store
    from cron.quiescence import CronBroker, OwnerIdentity

    return CronBroker(
        profile_home=home,
        owner_identity=OwnerIdentity(pid=123, create_time=456.0, profile_home_hash="homehash"),
        submit=submitter,
        runner=runner,
    )


def test_missing_process_local_capability_hard_stops_before_execution(store):
    jobs, _home, now = store
    from cron.quiescence import execute_reserved_job

    due = jobs.scan_due_jobs_read_only(now).jobs[0]
    before = jobs._current_cron_store().jobs_file.read_bytes()
    called = []
    result = execute_reserved_job(due.normalized_job_copy, None, None, lambda job: called.append(job))
    assert result.status.value == "BROKER_REQUIRED"
    assert called == []
    assert jobs._current_cron_store().jobs_file.read_bytes() == before


def test_admission_contention_precedes_claim_and_is_path_specific(store):
    jobs, home, now = store
    from cron.quiescence import AdmissionLock

    submitter = CapturingSubmitter()
    broker = _broker(store, submitter)
    due = jobs.scan_due_jobs_read_only(now).jobs[0]
    before = jobs._current_cron_store().jobs_file.read_bytes()

    held = AdmissionLock(home)
    assert held.acquire(timeout=0.0)
    try:
        ticker = broker.admit(due, mode="ticker", lock_timeout=0.0)
        provider = broker.admit(due, mode="provider", lock_timeout=0.01)
        immediate = broker.admit(due, mode="immediate", lock_timeout=0.01)
    finally:
        held.release()

    assert ticker.status.value == "DEFERRED_QUIESCENCE"
    assert provider.status.value == "DEFERRED_QUIESCENCE"
    assert immediate.status.value == "QUIESCENT_BUSY"
    assert jobs._current_cron_store().jobs_file.read_bytes() == before
    assert submitter.calls == []


def test_admission_reserves_publishes_and_submits_reserved_snapshot(store):
    jobs, _home, now = store
    submitter = CapturingSubmitter()
    observed_runner_jobs = []
    broker = _broker(store, submitter, lambda job: observed_runner_jobs.append(job) or True)
    due = jobs.scan_due_jobs_read_only(now).jobs[0]

    accepted = broker.admit(due, mode="ticker")

    assert accepted.status.value == "ACCEPTED"
    assert accepted.attempt_token and accepted.run_token
    assert len(submitter.calls) == 1
    snapshot = broker.ledger_snapshot()
    assert snapshot["generation"] >= 1
    assert snapshot["active_by_job_id"] == {"j1": accepted.run_token}
    persisted = jobs.get_job("j1")
    assert persisted["run_claim"]["attempt_token"] == accepted.attempt_token

    completed = submitter.calls[0]()
    assert completed.status.value == "COMPLETED"
    assert observed_runner_jobs[0]["run_claim"]["attempt_token"] == accepted.attempt_token
    assert broker.ledger_snapshot()["active_by_job_id"] == {}
    assert jobs.get_job("j1")["last_status"] == "ok"


def test_same_job_dedup_creates_no_second_tokens_or_claim(store):
    jobs, _home, now = store
    submitter = CapturingSubmitter()
    broker = _broker(store, submitter)
    due = jobs.scan_due_jobs_read_only(now).jobs[0]
    first = broker.admit(due, mode="ticker")
    before = jobs._current_cron_store().jobs_file.read_bytes()

    second = broker.admit(due, mode="provider")

    assert first.run_token
    assert second.status.value == "ALREADY_RUNNING"
    assert second.attempt_token is None and second.run_token is None
    assert len(submitter.calls) == 1
    assert jobs._current_cron_store().jobs_file.read_bytes() == before


def test_every_real_attempt_has_unique_attempt_and_run_tokens(store):
    jobs, _home, now = store
    submitter = CapturingSubmitter()
    broker = _broker(store, submitter)
    due = jobs.scan_due_jobs_read_only(now).jobs[0]
    first = broker.admit(due, mode="ticker")
    submitter.calls.pop(0)()

    jobs.trigger_job("j1")
    due2 = jobs.scan_due_jobs_read_only(jobs._hermes_now()).jobs[0]
    second = broker.admit(due2, mode="immediate")
    assert first.attempt_token != second.attempt_token
    assert first.run_token != second.run_token


def test_submit_failure_rolls_back_before_removing_busy_token(store):
    jobs, _home, now = store
    submitter = CapturingSubmitter(fail=True)
    broker = _broker(store, submitter)
    due = jobs.scan_due_jobs_read_only(now).jobs[0]
    before = jobs.load_jobs()[0]

    result = broker.admit(due, mode="ticker")

    assert result.status.value == "SUBMIT_FAILED"
    assert jobs.load_jobs()[0] == before
    assert broker.ledger_snapshot()["active_by_job_id"] == {}


def test_standalone_tick_brokers_due_jobs_without_local_execution(monkeypatch, tmp_path):
    import cron.scheduler as scheduler
    from cron.jobs import DueJob, DueScan
    from cron.quiescence import DispatchResult

    due = DueJob("j1", "a" * 64, "slot", "slot", {"id": "j1"}, {})
    monkeypatch.setattr("cron.jobs.scan_due_jobs_read_only", lambda now=None: DueScan("now", (due,)))
    monkeypatch.setattr(scheduler, "_get_hermes_home", lambda: tmp_path)
    requested = []
    monkeypatch.setattr(
        "cron.quiescence.request_broker_dispatch",
        lambda job_id, **kw: requested.append((job_id, kw)) or DispatchResult(
            status="ACCEPTED", job_id=job_id, mode="ticker", request_id="req",
            attempt_token="a", run_token="r",
        ),
    )
    with patch("cron.scheduler.run_one_job") as local_run:
        assert scheduler.tick(verbose=False, sync=False) == 1
    assert requested == [("j1", {"mode": "ticker", "profile_home": tmp_path})]
    local_run.assert_not_called()


def test_completion_never_needs_admission_lock(store):
    jobs, home, now = store
    from cron.quiescence import AdmissionLock

    submitter = CapturingSubmitter()
    broker = _broker(store, submitter)
    due = jobs.scan_due_jobs_read_only(now).jobs[0]
    broker.admit(due, mode="ticker")

    held = AdmissionLock(home)
    assert held.acquire(timeout=0.0)
    try:
        completed = submitter.calls[0]()
    finally:
        held.release()
    assert completed.status.value == "COMPLETED"
    assert broker.ledger_snapshot()["active_by_job_id"] == {}


def test_completion_hook_outcome_is_recorded_separately(store):
    import json

    jobs, home, now = store
    submitter = CapturingSubmitter()
    from cron.quiescence import CronBroker, OwnerIdentity

    broker = CronBroker(
        profile_home=home,
        owner_identity=OwnerIdentity(
            pid=123, create_time=456.0, profile_home_hash="homehash"
        ),
        submit=submitter,
        runner=lambda job: True,
        completion_hook=lambda result: False,
    )
    broker.register_owner(
        command_kind="CANONICAL_GATEWAY_RUN",
        argv=["python", "-m", "hermes_cli.main", "gateway", "run"],
    )
    due = jobs.scan_due_jobs_read_only(now).jobs[0]
    accepted = broker.admit(due, mode="ticker")
    completed = submitter.calls[0]()

    assert completed.status.value == "COMPLETED"
    records = list(
        (home / "cron" / "quiescence" / "completion-hooks").glob("*.json")
    )
    assert len(records) == 1
    payload = json.loads(records[0].read_text(encoding="utf-8"))
    assert payload["status"] == "NO_ACTION"
    assert payload["job_id"] == "j1"
    assert payload["attempt_token_sha256"]
    assert accepted.attempt_token not in records[0].read_text(encoding="utf-8")
    assert broker.close_owner() is True


def test_completion_hook_is_not_lost_when_legacy_global_hook_lock_is_held(store):
    jobs, home, now = store
    submitter = CapturingSubmitter()
    from cron.quiescence import AdmissionLock, CronBroker, OwnerIdentity

    calls = []
    broker = CronBroker(
        profile_home=home,
        owner_identity=OwnerIdentity(123, 456.0, "homehash"),
        submit=submitter,
        runner=lambda job: True,
        completion_hook=lambda result: calls.append(result.job_id) or True,
    )
    broker.register_owner(
        command_kind="CANONICAL_GATEWAY_RUN",
        argv=["python", "-m", "hermes_cli.main", "gateway", "run"],
    )
    due = jobs.scan_due_jobs_read_only(now).jobs[0]
    broker.admit(due, mode="ticker")
    legacy_lock = AdmissionLock(home, lock_name="completion-hook.lock")
    assert legacy_lock.acquire(timeout=0.0)
    try:
        completed = submitter.calls[0]()
    finally:
        legacy_lock.release()

    assert completed.status.value == "COMPLETED"
    assert calls == ["j1"]
    assert broker.close_owner() is True


def test_stale_started_completion_hook_record_is_recovered(store):
    import json

    jobs, home, now = store
    submitter = CapturingSubmitter()
    import cron.quiescence as q

    broker = q.CronBroker(
        profile_home=home,
        owner_identity=q.OwnerIdentity(123, 456.0, "homehash"),
        submit=submitter,
        runner=lambda job: True,
        completion_hook=lambda result: True,
    )
    broker.register_owner(
        command_kind="CANONICAL_GATEWAY_RUN",
        argv=["python", "-m", "hermes_cli.main", "gateway", "run"],
    )
    due = jobs.scan_due_jobs_read_only(now).jobs[0]
    broker.admit(due, mode="provider")
    submitter.calls[0]()
    record_path = next(
        (home / "cron" / "quiescence" / "completion-hooks").glob("*.json")
    )
    key = q._read_transport_key(home)
    record = json.loads(record_path.read_text(encoding="utf-8"))
    record.pop("auth_tag", None)
    record["status"] = "STARTED"
    record = q._signed_transport_payload(record, key, domain="completion-hook")
    q._atomic_json_write(record_path, record)
    calls = []

    recovered = q.recover_completion_hooks(
        home,
        lambda result: calls.append(
            (result.job_id, result.attempt_token_sha256)
        )
        or True,
        owner_epoch=broker.owner_epoch,
    )

    assert recovered == 1
    assert calls == [("j1", record["attempt_token_sha256"])]
    assert json.loads(record_path.read_text(encoding="utf-8"))["status"] == "SUCCEEDED"
    assert broker.close_owner() is True


def test_malformed_existing_hook_journal_hard_stops_before_runner(store):
    import hashlib

    jobs, home, now = store
    import cron.quiescence as q

    submitter = CapturingSubmitter()
    runner_calls = []
    broker = q.CronBroker(
        profile_home=home,
        owner_identity=q.OwnerIdentity(123, 456.0, "homehash"),
        submit=submitter,
        runner=lambda job: runner_calls.append(job) or True,
        completion_hook=lambda result: True,
    )
    broker.register_owner(
        command_kind="CANONICAL_GATEWAY_RUN",
        argv=["python", "-m", "hermes_cli.main", "gateway", "run"],
    )
    due = jobs.scan_due_jobs_read_only(now).jobs[0]
    accepted = broker.admit(due, mode="provider")
    attempt_hash = hashlib.sha256(accepted.attempt_token.encode()).hexdigest()
    record_path = q._completion_hook_record_path(home, "j1", attempt_hash)
    q._atomic_json_write(record_path, {"malformed": True})

    result = submitter.calls[0]()

    assert result.status.value == "UNKNOWN"
    assert runner_calls == []
    assert jobs.get_job("j1")["run_claim"]["attempt_token"] == accepted.attempt_token
    assert broker.close_owner() is True


def test_pending_hook_with_newer_claim_is_not_recovered(store):
    jobs, home, now = store
    import cron.quiescence as q

    submitter = CapturingSubmitter()
    hook_calls = []
    broker = q.CronBroker(
        profile_home=home,
        owner_identity=q.OwnerIdentity(123, 456.0, "homehash"),
        submit=submitter,
        runner=lambda job: True,
        completion_hook=lambda result: hook_calls.append(result) or True,
    )
    broker.register_owner(
        command_kind="CANONICAL_GATEWAY_RUN",
        argv=["python", "-m", "hermes_cli.main", "gateway", "run"],
    )
    due = jobs.scan_due_jobs_read_only(now).jobs[0]
    accepted = broker.admit(due, mode="provider")
    mutated = jobs.load_jobs()
    for field in ("run_claim", "fire_claim"):
        mutated[0][field] = {
            **mutated[0][field],
            "attempt_token": "attempt-B",
            "run_token": "run-B",
        }
    jobs.save_jobs(mutated)

    result = submitter.calls[0]()
    recovered = q.recover_completion_hooks(
        home, lambda value: hook_calls.append(value) or True
    )

    assert result.status.value == "UNKNOWN"
    assert recovered == 0
    assert hook_calls == []
    assert jobs.get_job("j1")["run_claim"]["attempt_token"] == "attempt-B"
    assert broker.close_owner() is True


def test_recovery_rejects_signed_wrong_schema_and_noncanonical_path(store):
    import json

    jobs, home, now = store
    import cron.quiescence as q

    submitter = CapturingSubmitter()
    broker = q.CronBroker(
        profile_home=home,
        owner_identity=q.OwnerIdentity(123, 456.0, "homehash"),
        submit=submitter,
        runner=lambda job: True,
        completion_hook=lambda result: True,
    )
    broker.register_owner(
        command_kind="CANONICAL_GATEWAY_RUN",
        argv=["python", "-m", "hermes_cli.main", "gateway", "run"],
    )
    broker.admit(jobs.scan_due_jobs_read_only(now).jobs[0], mode="provider")
    submitter.calls[0]()
    directory = home / "cron" / "quiescence" / "completion-hooks"
    canonical = next(directory.glob("*.json"))
    key = q._read_transport_key(home)
    original = json.loads(canonical.read_text(encoding="utf-8"))

    wrong_schema = dict(original)
    wrong_schema.pop("auth_tag", None)
    wrong_schema["schema"] = "wrong.schema"
    wrong_schema["status"] = "PENDING"
    q._atomic_json_write(
        directory / "wrong-schema.json",
        q._signed_transport_payload(
            wrong_schema, key, domain="completion-hook"
        ),
    )
    copied = dict(original)
    copied.pop("auth_tag", None)
    copied["status"] = "PENDING"
    q._atomic_json_write(
        directory / "copied.json",
        q._signed_transport_payload(copied, key, domain="completion-hook"),
    )

    malformed_variants = []
    uppercase = dict(original)
    uppercase.pop("auth_tag", None)
    uppercase["attempt_token_sha256"] = str(
        original["attempt_token_sha256"]
    ).upper()
    uppercase["status"] = "COMMITTED"
    malformed_variants.append(uppercase)

    extra_field = dict(original)
    extra_field.pop("auth_tag", None)
    extra_field["attempt_token_sha256"] = "1" * 64
    extra_field["unexpected"] = "value"
    extra_field["status"] = "COMMITTED"
    malformed_variants.append(extra_field)

    integer_success = dict(original)
    integer_success.pop("auth_tag", None)
    integer_success["attempt_token_sha256"] = "2" * 64
    integer_success["run_success"] = 1
    integer_success["status"] = "COMMITTED"
    malformed_variants.append(integer_success)

    numeric_identity = dict(original)
    numeric_identity.pop("auth_tag", None)
    numeric_identity["attempt_token_sha256"] = "3" * 64
    numeric_identity["job_id"] = 123
    numeric_identity["request_id"] = 456
    numeric_identity["status"] = "COMMITTED"
    malformed_variants.append(numeric_identity)

    fabricated_without_proof = dict(original)
    fabricated_without_proof.pop("auth_tag", None)
    fabricated_without_proof["attempt_token_sha256"] = "4" * 64
    fabricated_without_proof["status"] = "COMMITTED"
    malformed_variants.append(fabricated_without_proof)

    started_without_outcome = dict(original)
    started_without_outcome.pop("auth_tag", None)
    started_without_outcome["attempt_token_sha256"] = "5" * 64
    started_without_outcome["run_success"] = None
    started_without_outcome["status"] = "STARTED"
    malformed_variants.append(started_without_outcome)

    for malformed in malformed_variants:
        malformed_path = q._completion_hook_record_path(
            home,
            malformed["job_id"],
            malformed["attempt_token_sha256"],
        )
        q._atomic_json_write(
            malformed_path,
            q._signed_transport_payload(
                malformed, key, domain="completion-hook"
            ),
        )

    calls = []
    assert q.recover_completion_hooks(home, lambda value: calls.append(value) or True) == 0
    assert calls == []
    assert broker.close_owner() is True


def test_stale_pending_is_not_recovered_after_newer_attempt_removes_job(store):
    jobs, home, now = store
    import cron.quiescence as q

    job = jobs.load_jobs()[0]
    job["schedule"] = {"kind": "once", "run_at": job["next_run_at"]}
    job["repeat"] = {"times": 1, "completed": 0}
    jobs.save_jobs([job])
    submitter = CapturingSubmitter()
    hooks = []
    broker = q.CronBroker(
        profile_home=home,
        owner_identity=q.OwnerIdentity(123, 456.0, "homehash"),
        submit=submitter,
        runner=lambda value: True,
        completion_hook=lambda value: hooks.append(value) or True,
    )
    broker.register_owner(
        command_kind="CANONICAL_GATEWAY_RUN",
        argv=["python", "-m", "hermes_cli.main", "gateway", "run"],
    )
    accepted = broker.admit(
        jobs.scan_due_jobs_read_only(now).jobs[0], mode="provider"
    )
    mutated = jobs.load_jobs()
    for field in ("run_claim", "fire_claim"):
        mutated[0][field] = {
            **mutated[0][field],
            "attempt_token": "attempt-B",
            "run_token": "run-B",
        }
    jobs.save_jobs(mutated)
    assert submitter.calls[0]().status.value == "UNKNOWN"
    removed = jobs.complete_reserved_attempt(
        "j1", "attempt-B", success=True, error=None, delivery_error=None
    )
    assert removed.removed is True
    assert jobs.get_job("j1") is None

    assert q.recover_completion_hooks(home, lambda value: hooks.append(value) or True) == 0
    assert hooks == []
    assert broker.close_owner() is True


def test_pending_completed_attempt_is_not_recovered_with_newer_live_claims(store):
    import json

    jobs, home, now = store
    import cron.quiescence as q

    submitter = CapturingSubmitter()
    broker = q.CronBroker(
        profile_home=home,
        owner_identity=q.OwnerIdentity(123, 456.0, "homehash"),
        submit=submitter,
        runner=lambda value: True,
        completion_hook=lambda value: True,
    )
    broker.register_owner(
        command_kind="CANONICAL_GATEWAY_RUN",
        argv=["python", "-m", "hermes_cli.main", "gateway", "run"],
    )
    broker.admit(jobs.scan_due_jobs_read_only(now).jobs[0], mode="provider")
    result = submitter.calls[0]()
    attempt_hash = result.attempt_token_sha256
    record = next(
        (home / "cron" / "quiescence" / "completion-hooks").glob("*.json")
    )
    key = q._read_transport_key(home)
    pending = json.loads(record.read_text(encoding="utf-8"))
    pending.pop("auth_tag", None)
    pending["status"] = "PENDING"
    pending["run_success"] = None
    q._atomic_json_write(
        record,
        q._signed_transport_payload(pending, key, domain="completion-hook"),
    )
    mutated = jobs.load_jobs()
    for field in ("run_claim", "fire_claim"):
        mutated[0][field] = {
            "attempt_token": "attempt-B",
            "run_token": "run-B",
            "claimed_at": now.isoformat(),
            "owner": {"pid": 999},
        }
    jobs.save_jobs(mutated)
    assert jobs.get_job("j1")["last_completed_attempt_sha256"] == attempt_hash

    calls = []
    assert q.recover_completion_hooks(home, lambda value: calls.append(value) or True) == 0
    assert calls == []
    assert jobs.get_job("j1")["run_claim"]["attempt_token"] == "attempt-B"
    assert broker.close_owner() is True


def test_pending_recovery_uses_post_cas_fire_time_from_completion_proof(
    store, monkeypatch
):
    import json

    jobs, home, now = store
    import cron.quiescence as q

    original_invoke = q._invoke_durable_completion_hook
    monkeypatch.setattr(q, "_invoke_durable_completion_hook", lambda *args, **kwargs: False)
    submitter = CapturingSubmitter()
    broker = q.CronBroker(
        profile_home=home,
        owner_identity=q.OwnerIdentity(123, 456.0, "homehash"),
        submit=submitter,
        runner=lambda value: True,
        completion_hook=lambda value: True,
    )
    broker.register_owner(
        command_kind="CANONICAL_GATEWAY_RUN",
        argv=["python", "-m", "hermes_cli.main", "gateway", "run"],
    )
    broker.admit(jobs.scan_due_jobs_read_only(now).jobs[0], mode="provider")
    result = submitter.calls[0]()
    persisted_fire = jobs.get_job("j1")["next_run_at"]
    assert result.completion_next_run_at == persisted_fire

    record = next(
        (home / "cron" / "quiescence" / "completion-hooks").glob("*.json")
    )
    key = q._read_transport_key(home)
    pending = json.loads(record.read_text(encoding="utf-8"))
    pending.pop("auth_tag", None)
    pending["status"] = "PENDING"
    pending["run_success"] = None
    pending["completion_next_run_at"] = "stale-precommit-fire"
    q._atomic_json_write(
        record,
        q._signed_transport_payload(pending, key, domain="completion-hook"),
    )
    monkeypatch.setattr(q, "_invoke_durable_completion_hook", original_invoke)

    recovered = []
    assert q.recover_completion_hooks(
        home, lambda value: recovered.append(value) or True
    ) == 1
    assert recovered[0].completion_next_run_at == persisted_fire
    terminal = json.loads(record.read_text(encoding="utf-8"))
    assert terminal["status"] == "SUCCEEDED"
    assert terminal["completion_next_run_at"] == persisted_fire
    assert broker.close_owner() is True


def test_retryable_completion_hook_remains_committed_until_recovery(store):
    import json

    jobs, home, now = store
    import cron.quiescence as q

    submitter = CapturingSubmitter()
    calls = []
    import threading

    entered = threading.Event()
    release = threading.Event()

    def hook(result):
        calls.append(result.job_id)
        assert result.completion_next_run_at == jobs.get_job(result.job_id)["next_run_at"]
        if len(calls) == 1:
            raise q.RetryableCompletionHookError("retry")
        entered.set()
        assert release.wait(timeout=2)
        return True

    broker = q.CronBroker(
        profile_home=home,
        owner_identity=q.OwnerIdentity(123, 456.0, "homehash"),
        submit=submitter,
        runner=lambda value: True,
        completion_hook=hook,
    )
    broker.register_owner(
        command_kind="CANONICAL_GATEWAY_RUN",
        argv=["python", "-m", "hermes_cli.main", "gateway", "run"],
    )
    broker.admit(jobs.scan_due_jobs_read_only(now).jobs[0], mode="provider")
    assert submitter.calls[0]().status.value == "COMPLETED"
    record = next(
        (home / "cron" / "quiescence" / "completion-hooks").glob("*.json")
    )
    assert json.loads(record.read_text(encoding="utf-8"))["status"] == "COMMITTED"

    stop = threading.Event()
    retry_thread = threading.Thread(
        target=q.serve_completion_hook_recovery_loop,
        args=(stop, home, hook),
        kwargs={"interval": 0.01},
        daemon=True,
    )
    retry_thread.start()
    assert entered.wait(timeout=2)
    stop.set()
    retry_thread.join(timeout=0.05)
    assert retry_thread.is_alive() is True
    assert json.loads(record.read_text(encoding="utf-8"))["status"] == "STARTED"
    release.set()
    retry_thread.join(timeout=1)
    assert retry_thread.is_alive() is False

    assert calls == ["j1", "j1"]
    assert json.loads(record.read_text(encoding="utf-8"))["status"] == "SUCCEEDED"
    assert broker.close_owner() is True


def test_gateway_reserved_runner_does_not_repeat_claim_or_terminal_mutation(store):
    jobs, _home, now = store
    import cron.scheduler as scheduler

    submitter = CapturingSubmitter()
    observed_running_counts = []

    def run_job(*args, **kwargs):
        observed_running_counts.append(len(scheduler.get_running_job_ids()))
        return True, "output", "final response", None

    broker = _broker(
        store,
        submitter,
        lambda job: scheduler._run_reserved_job_effects(job),
    )
    due = jobs.scan_due_jobs_read_only(now).jobs[0]

    with patch("cron.scheduler.claim_dispatch") as claim, patch(
        "cron.scheduler.mark_job_run"
    ) as mark, patch(
        "cron.scheduler.run_job", side_effect=run_job,
    ), patch(
        "cron.scheduler.save_job_output", return_value="/tmp/output"
    ), patch(
        "cron.scheduler._deliver_result", return_value=None
    ), patch(
        "agent.secret_scope.build_profile_secret_scope", return_value=None
    ), patch(
        "agent.secret_scope.set_secret_scope", return_value=object()
    ), patch(
        "agent.secret_scope.reset_secret_scope"
    ):
        accepted = broker.admit(due, mode="ticker")
        completed = submitter.calls[0]()

    assert accepted.status.value == "ACCEPTED"
    assert completed.status.value == "COMPLETED"
    assert completed.run_success is True
    assert observed_running_counts == [1]
    assert scheduler.get_running_job_ids() == frozenset()
    claim.assert_not_called()
    mark.assert_not_called()
    persisted = jobs.get_job("j1")
    assert persisted["repeat"]["completed"] == 1
    assert persisted["run_claim"] is None
    assert persisted["fire_claim"] is None
    assert broker.ledger_snapshot()["active_by_job_id"] == {}


def test_gateway_reserved_interrupt_commits_through_attempt_cas(store):
    jobs, _home, now = store
    import cron.scheduler as scheduler

    submitter = CapturingSubmitter()

    def interrupted_run(*args, **kwargs):
        assert scheduler.mark_running_jobs_interrupted("gateway shutdown") == ["j1"]
        return True, "truncated output", "plausible final response", None

    broker = _broker(
        store,
        submitter,
        lambda job: scheduler._run_reserved_job_effects(job),
    )
    due = jobs.scan_due_jobs_read_only(now).jobs[0]

    with patch("cron.scheduler.mark_job_run") as legacy_mark, patch(
        "cron.scheduler.run_job", side_effect=interrupted_run,
    ), patch(
        "cron.scheduler.save_job_output", return_value="/tmp/output"
    ), patch(
        "cron.scheduler._deliver_result", return_value=None
    ), patch(
        "agent.secret_scope.build_profile_secret_scope", return_value=None
    ), patch(
        "agent.secret_scope.set_secret_scope", return_value=object()
    ), patch(
        "agent.secret_scope.reset_secret_scope"
    ):
        broker.admit(due, mode="ticker")
        completed = submitter.calls[0]()

    assert completed.status.value == "COMPLETED"
    assert completed.run_success is False
    legacy_mark.assert_not_called()
    persisted = jobs.get_job("j1")
    assert persisted["last_status"] == "error"
    assert "gateway shutdown" in persisted["last_error"]
    assert persisted["run_claim"] is None
    assert persisted["fire_claim"] is None


def test_broker_running_registry_requires_all_exact_owners_to_release():
    import cron.scheduler as scheduler

    first = scheduler._register_broker_running_job("shared-id")
    second = scheduler._register_broker_running_job("shared-id")
    try:
        assert first is not second
        assert scheduler.get_running_job_ids() == frozenset({"shared-id"})

        scheduler._release_broker_running_job("shared-id", first)
        assert scheduler.get_running_job_ids() == frozenset({"shared-id"})

        scheduler._release_broker_running_job("shared-id", second)
        assert scheduler.get_running_job_ids() == frozenset()
    finally:
        scheduler._broker_running_job_tokens.clear()


def test_broker_interrupt_flag_is_consumable_once_per_active_owner():
    import cron.scheduler as scheduler

    first = scheduler._register_broker_running_job("shared-id")
    second = scheduler._register_broker_running_job("shared-id")
    try:
        assert scheduler.mark_running_jobs_interrupted("shutdown") == ["shared-id"]
        assert scheduler._consume_interrupted_flag("shared-id") is True
        assert scheduler._consume_interrupted_flag("shared-id") is True
        assert scheduler._consume_interrupted_flag("shared-id") is False
    finally:
        scheduler._release_broker_running_job("shared-id", first)
        scheduler._release_broker_running_job("shared-id", second)
        scheduler._interrupted_job_ids.clear()
        scheduler._interrupted_job_remaining.clear()
