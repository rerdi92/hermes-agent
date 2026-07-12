from __future__ import annotations

import copy
import json
from datetime import timedelta

import pytest


@pytest.fixture
def store(tmp_path, monkeypatch):
    import cron.jobs as jobs

    cron_dir = tmp_path / "cron"
    monkeypatch.setattr(jobs, "CRON_DIR", cron_dir)
    monkeypatch.setattr(jobs, "JOBS_FILE", cron_dir / "jobs.json")
    monkeypatch.setattr(jobs, "OUTPUT_DIR", cron_dir / "output")
    return jobs


def _due_job(jobs, *, job_id="j1", kind="interval", repeat=None):
    now = jobs._hermes_now()
    job = {
        "id": job_id,
        "name": job_id,
        "enabled": True,
        "state": "scheduled",
        "schedule": {"kind": kind, "minutes": 5} if kind == "interval" else {"kind": "once", "run_at": (now - timedelta(seconds=1)).isoformat()},
        "next_run_at": (now - timedelta(seconds=1)).isoformat(),
        "repeat": repeat,
    }
    jobs.save_jobs([job])
    return job


def test_due_scan_is_byte_read_only_and_returns_hash_slot_and_copy(store):
    original = _due_job(store)
    before = store._current_cron_store().jobs_file.read_bytes()

    scan = store.scan_due_jobs_read_only(store._hermes_now())

    assert store._current_cron_store().jobs_file.read_bytes() == before
    assert len(scan.jobs) == 1
    due = scan.jobs[0]
    assert due.job_id == "j1"
    assert len(due.observed_job_sha256) == 64
    assert due.observed_next_run_at == original["next_run_at"]
    assert due.due_slot == original["next_run_at"]
    assert due.normalized_job_copy == original
    due.normalized_job_copy["name"] = "caller mutation"
    assert store.load_jobs()[0]["name"] == "j1"


def test_due_scan_does_not_repair_or_rewrite_legacy_bare_list(store):
    now = store._hermes_now()
    job = {
        "id": "legacy",
        "enabled": True,
        "state": "scheduled",
        "schedule": {
            "kind": "once",
            "run_at": (now - timedelta(seconds=1)).isoformat(),
        },
        "next_run_at": (now - timedelta(seconds=1)).isoformat(),
    }
    store._current_cron_store().jobs_file.parent.mkdir(parents=True)
    store._current_cron_store().jobs_file.write_text(json.dumps([job]), encoding="utf-8")
    before = store._current_cron_store().jobs_file.read_bytes()

    scan = store.scan_due_jobs_read_only(now)

    assert [item.job_id for item in scan.jobs] == ["legacy"]
    assert store._current_cron_store().jobs_file.read_bytes() == before
    assert sorted(path.name for path in store._current_cron_store().jobs_file.parent.iterdir()) == [
        "jobs.json"
    ]


def test_read_only_scans_do_not_create_missing_profile_tree(store):
    cron_dir = store._current_cron_store().jobs_file.parent
    assert cron_dir.exists() is False

    assert store.scan_due_jobs_read_only().jobs == ()
    assert store.scan_job_for_dispatch_read_only("missing") is None

    assert cron_dir.exists() is False


def test_attempt_lifecycle_follows_context_scoped_profile_store(
    tmp_path, monkeypatch
):
    import hashlib
    import cron.jobs as jobs

    default_home = tmp_path / "default"
    profile_home = tmp_path / "profiles" / "coder"
    default_cron = default_home / "cron"
    monkeypatch.setattr(jobs, "CRON_DIR", default_cron)
    monkeypatch.setattr(jobs, "JOBS_FILE", default_cron / "jobs.json")
    monkeypatch.setattr(jobs, "OUTPUT_DIR", default_cron / "output")

    with jobs.use_cron_store(profile_home):
        _due_job(jobs, job_id="profile-job")
        due = jobs.scan_due_jobs_read_only(jobs._hermes_now()).jobs[0]
        reserved = jobs.reserve_job_attempt(
            "profile-job",
            "profile-attempt",
            "profile-run",
            "provider",
            due.observed_job_sha256,
            {"pid": 1},
            jobs._hermes_now(),
        )
        completed = jobs.complete_reserved_attempt(
            "profile-job", "profile-attempt", success=True
        )
        attempt_hash = hashlib.sha256(b"profile-attempt").hexdigest()
        proof = jobs.load_completion_proof_read_only("profile-job", attempt_hash)

        assert reserved.status == "RESERVED"
        assert completed.status == "COMPLETED"
        assert proof is not None
        assert jobs._current_cron_store().jobs_file == profile_home / "cron" / "jobs.json"

    assert default_cron.exists() is False
    assert (profile_home / "cron" / "jobs.json").exists()
    assert (profile_home / "cron" / ".completion-proof-key.json").exists()


def test_attempt_reservation_fails_closed_when_os_lock_fails(store, monkeypatch):
    _due_job(store)
    due = store.scan_due_jobs_read_only(store._hermes_now()).jobs[0]
    before = store._current_cron_store().jobs_file.read_bytes()

    class DeniedLock:
        LK_LOCK = 1
        LK_UNLCK = 2

        @staticmethod
        def locking(*_args):
            raise PermissionError("lock denied")

    monkeypatch.setattr(store, "fcntl", None)
    monkeypatch.setattr(store, "msvcrt", DeniedLock)

    with pytest.raises(RuntimeError, match="cross-process jobs lock"):
        store.reserve_job_attempt(
            "j1",
            "attempt-1",
            "run-1",
            "ticker",
            due.observed_job_sha256,
            {"pid": 1},
            store._hermes_now(),
        )

    assert store._current_cron_store().jobs_file.read_bytes() == before


def test_attempt_rollback_and_completion_fail_closed_when_os_lock_fails(
    store, monkeypatch
):
    _due_job(store)
    due = store.scan_due_jobs_read_only(store._hermes_now()).jobs[0]
    reserved = store.reserve_job_attempt(
        "j1", "attempt-1", "run-1", "ticker", due.observed_job_sha256,
        {"pid": 1}, store._hermes_now(),
    )
    before = store._current_cron_store().jobs_file.read_bytes()

    class DeniedLock:
        LK_LOCK = 1
        LK_UNLCK = 2

        @staticmethod
        def locking(*_args):
            raise PermissionError("lock denied")

    monkeypatch.setattr(store, "fcntl", None)
    monkeypatch.setattr(store, "msvcrt", DeniedLock)

    with pytest.raises(RuntimeError, match="cross-process jobs lock"):
        store.rollback_reserved_attempt(reserved.receipt)
    assert store._current_cron_store().jobs_file.read_bytes() == before

    with pytest.raises(RuntimeError, match="cross-process jobs lock"):
        store.complete_reserved_attempt(
            "j1", "attempt-1", success=True, error=None, delivery_error=None
        )
    assert store._current_cron_store().jobs_file.read_bytes() == before


def test_reservation_is_single_transaction_and_worker_gets_reserved_snapshot(store):
    _due_job(store, repeat={"times": 3, "completed": 0})
    due = store.scan_due_jobs_read_only(store._hermes_now()).jobs[0]

    reservation = store.reserve_job_attempt(
        "j1", "attempt-1", "run-1", "ticker", due.observed_job_sha256,
        {"pid": 10, "create_time": 20.0}, store._hermes_now(),
    )

    assert reservation.status == "RESERVED"
    assert reservation.receipt.attempt_token == "attempt-1"
    assert reservation.receipt.run_token == "run-1"
    assert reservation.receipt.before["repeat"]["completed"] == 0
    assert reservation.receipt.after["repeat"]["completed"] == 1
    assert reservation.job["run_claim"]["attempt_token"] == "attempt-1"
    assert reservation.job["fire_claim"]["run_token"] == "run-1"
    assert reservation.job["next_run_at"] != due.observed_next_run_at
    assert store.load_jobs()[0] == reservation.job
    assert reservation.receipt.post_job_sha256 != due.observed_job_sha256


def test_finite_oneshot_is_counted_once_at_reservation_not_completion(store):
    _due_job(store, kind="once", repeat={"times": 2, "completed": 0})
    due = store.scan_due_jobs_read_only(store._hermes_now()).jobs[0]
    reservation = store.reserve_job_attempt(
        "j1", "attempt-1", "run-1", "provider", due.observed_job_sha256,
        {"pid": 10}, store._hermes_now(),
    )
    assert reservation.job["repeat"]["completed"] == 1

    outcome = store.complete_reserved_attempt(
        "j1", "attempt-1", success=False, error="boom", delivery_error=None
    )
    assert outcome.status == "COMPLETED"
    completed = store.get_job("j1")
    assert completed["repeat"]["completed"] == 1
    assert completed["last_status"] == "error"
    assert completed["run_claim"] is None
    assert completed["fire_claim"] is None
    import hashlib

    attempt_hash = hashlib.sha256(b"attempt-1").hexdigest()
    proof = store.load_completion_proof_read_only("j1", attempt_hash)
    assert proof is not None
    assert proof["run_success"] is False
    proof_path = store._completion_proof_path("j1", attempt_hash)
    tampered = json.loads(proof_path.read_text(encoding="utf-8"))
    tampered["run_success"] = True
    proof_path.write_text(json.dumps(tampered), encoding="utf-8")
    assert store.load_completion_proof_read_only("j1", attempt_hash) is None


def test_reservation_rejects_stale_hash_and_active_attempt_without_mutation(store):
    original = _due_job(store)
    before = store._current_cron_store().jobs_file.read_bytes()
    stale = store.reserve_job_attempt(
        "j1", "a", "r", "ticker", "0" * 64, {"pid": 1}, store._hermes_now()
    )
    assert stale.status == "STALE_SCAN"
    assert store._current_cron_store().jobs_file.read_bytes() == before

    due = store.scan_due_jobs_read_only(store._hermes_now()).jobs[0]
    first = store.reserve_job_attempt(
        "j1", "a1", "r1", "ticker", due.observed_job_sha256, {"pid": 1}, store._hermes_now()
    )
    second = store.reserve_job_attempt(
        "j1", "a2", "r2", "provider", first.receipt.post_job_sha256, {"pid": 2}, store._hermes_now()
    )
    assert second.status == "ALREADY_RUNNING"
    assert store.get_job("j1")["run_claim"]["attempt_token"] == "a1"


def test_submit_failure_rollback_uses_full_after_and_post_hash_cas(store):
    original = _due_job(store)
    due = store.scan_due_jobs_read_only(store._hermes_now()).jobs[0]
    reserved = store.reserve_job_attempt(
        "j1", "a1", "r1", "immediate", due.observed_job_sha256, {"pid": 1}, store._hermes_now()
    )

    rolled = store.rollback_reserved_attempt(reserved.receipt)
    assert rolled.status == "ROLLED_BACK"
    assert store.load_jobs()[0] == original

    due = store.scan_due_jobs_read_only(store._hermes_now()).jobs[0]
    reserved = store.reserve_job_attempt(
        "j1", "a2", "r2", "immediate", due.observed_job_sha256, {"pid": 1}, store._hermes_now()
    )
    mutated = store.load_jobs()
    mutated[0]["next_run_at"] = store._hermes_now().isoformat()
    store.save_jobs(mutated)
    mismatch = store.rollback_reserved_attempt(reserved.receipt)
    assert mismatch.status == "CAS_MISMATCH"
    assert store.get_job("j1")["run_claim"]["attempt_token"] == "a2"


def test_stale_completion_token_cannot_clear_newer_attempt(store):
    _due_job(store)
    due = store.scan_due_jobs_read_only(store._hermes_now()).jobs[0]
    store.reserve_job_attempt(
        "j1", "new-attempt", "new-run", "ticker", due.observed_job_sha256,
        {"pid": 1}, store._hermes_now(),
    )
    before = copy.deepcopy(store.get_job("j1"))

    outcome = store.complete_reserved_attempt(
        "j1", "old-attempt", success=True, error=None, delivery_error=None
    )

    assert outcome.status == "TOKEN_MISMATCH"
    assert store.get_job("j1") == before


def test_completion_cas_does_not_clear_newer_split_fire_claim(store):
    _due_job(store)
    due = store.scan_due_jobs_read_only(store._hermes_now()).jobs[0]
    store.reserve_job_attempt(
        "j1",
        "attempt-1",
        "run-1",
        "ticker",
        due.observed_job_sha256,
        {"pid": 1},
        store._hermes_now(),
    )
    mutated = store.load_jobs()
    mutated[0]["fire_claim"] = {
        **mutated[0]["fire_claim"],
        "attempt_token": "attempt-2",
        "run_token": "run-2",
    }
    store.save_jobs(mutated)
    before = copy.deepcopy(store.get_job("j1"))

    outcome = store.complete_reserved_attempt(
        "j1", "attempt-1", success=True, error=None, delivery_error=None
    )

    assert outcome.status == "TOKEN_MISMATCH"
    assert store.get_job("j1") == before


def test_final_finite_oneshot_completion_removes_job(store):
    _due_job(store, kind="once", repeat={"times": 1, "completed": 0})
    due = store.scan_due_jobs_read_only(store._hermes_now()).jobs[0]
    store.reserve_job_attempt(
        "j1", "a", "r", "immediate", due.observed_job_sha256, {"pid": 1}, store._hermes_now()
    )
    outcome = store.complete_reserved_attempt(
        "j1", "a", success=True, error=None, delivery_error="delivery failed"
    )
    assert outcome.status == "COMPLETED"
    assert outcome.removed is True
    assert store.get_job("j1") is None
