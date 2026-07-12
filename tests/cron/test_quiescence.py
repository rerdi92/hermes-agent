from __future__ import annotations

import dataclasses
import sys

import pytest


def _result(status, **overrides):
    from cron.quiescence import DispatchResult

    values = {
        "status": status,
        "job_id": "job-1",
        "mode": "provider",
        "request_id": "request-1",
    }
    values.update(overrides)
    return DispatchResult(**values)


def test_dispatch_result_is_frozen_and_derives_all_invariants():
    from cron.quiescence import DispatchStatus

    accepted = _result(DispatchStatus.ACCEPTED, attempt_token="a", run_token="r")
    assert accepted.schema == "hermes.cron.dispatch-result.v1"
    assert accepted.accepted is True
    assert accepted.completed is False
    assert accepted.run_success is None
    assert accepted.retryable is False
    assert bool(accepted) is True
    with pytest.raises(dataclasses.FrozenInstanceError):
        accepted.status = DispatchStatus.UNKNOWN

    completed_failure = _result(
        DispatchStatus.COMPLETED,
        attempt_token="a",
        run_token="r",
        run_success=False,
    )
    assert completed_failure.accepted is True
    assert completed_failure.completed is True
    assert completed_failure.run_success is False
    assert bool(completed_failure) is True


@pytest.mark.parametrize(
    ("status", "retryable"),
    [
        ("DEFERRED_QUIESCENCE", True),
        ("QUIESCENT_BUSY", True),
        ("STALE_SCAN", True),
        ("BROKER_UNAVAILABLE", True),
        ("SUBMIT_FAILED", True),
        ("ACCEPTED", False),
        ("COMPLETED", False),
        ("ALREADY_RUNNING", False),
        ("JOB_NOT_FOUND", False),
        ("JOB_NOT_RUNNABLE", False),
        ("BROKER_PROTOCOL_MISMATCH", False),
        ("BROKER_REQUIRED", False),
        ("RESERVATION_FAILED", False),
        ("UNKNOWN", False),
    ],
)
def test_dispatch_result_retryability_is_exact(status, retryable):
    kwargs = {"run_success": True} if status == "COMPLETED" else {}
    result = _result(status, **kwargs)
    assert result.retryable is retryable
    assert bool(result) is (status in {"ACCEPTED", "COMPLETED"})


def test_dispatch_result_rejects_invalid_mode_and_run_success_shape():
    with pytest.raises(ValueError, match="mode"):
        _result("ACCEPTED", mode="local")
    with pytest.raises(ValueError, match="run_success"):
        _result("ACCEPTED", run_success=True)
    with pytest.raises(ValueError, match="run_success"):
        _result("COMPLETED")


def test_internal_canonical_broker_exception_is_unknown_hard_stop():
    from cron.quiescence import (
        DispatchStatus,
        OwnerIdentity,
        request_broker_dispatch,
    )

    class BrokenBroker:
        owner_identity = OwnerIdentity(1, 1.0, "a" * 64)

        def admit(self, due_job, *, mode):
            raise RuntimeError("reservation invariant failed")

    result = request_broker_dispatch(
        "job-1", mode="provider", broker=BrokenBroker(), due_job=object()
    )

    assert result.status is DispatchStatus.UNKNOWN
    assert result.retryable is False


@pytest.mark.parametrize(
    ("status", "code", "retry", "hard_stop"),
    [
        ("ACCEPTED", 202, False, False),
        ("COMPLETED", 202, False, False),
        ("ALREADY_RUNNING", 200, False, False),
        ("JOB_NOT_FOUND", 200, False, False),
        ("JOB_NOT_RUNNABLE", 200, False, False),
        ("DEFERRED_QUIESCENCE", 503, True, False),
        ("QUIESCENT_BUSY", 503, True, False),
        ("BROKER_UNAVAILABLE", 503, True, False),
        ("SUBMIT_FAILED", 503, True, False),
        ("STALE_SCAN", 409, True, False),
        ("BROKER_PROTOCOL_MISMATCH", 503, False, True),
        ("BROKER_REQUIRED", 503, False, True),
        ("RESERVATION_FAILED", 503, False, True),
        ("UNKNOWN", 503, False, True),
    ],
)
def test_shared_http_mapper_is_exhaustive(status, code, retry, hard_stop):
    from cron.quiescence import dispatch_result_to_http

    kwargs = {"run_success": True} if status == "COMPLETED" else {}
    response = dispatch_result_to_http(_result(status, **kwargs))
    assert response.status_code == code
    assert response.body["status"] == status
    assert response.body["retryable"] is retry
    assert response.body["hard_stop"] is hard_stop
    assert response.body["job_id"] == "job-1"
    assert "retry_guidance" in response.body if retry else "retry_guidance" not in response.body


def test_windows_command_classifier_keeps_bare_gateway_and_restart_dispatch_capable():
    from cron.quiescence import classify_hermes_command

    bare = classify_hermes_command(
        ["pythonw.exe", "-m", "hermes_cli.main", "gateway"], platform="win32"
    )
    restart = classify_hermes_command(
        ["pythonw.exe", "-m", "hermes_cli.main", "gateway", "restart"],
        platform="win32",
    )
    status = classify_hermes_command(
        ["python.exe", "-m", "hermes_cli.main", "gateway", "status"],
        platform="win32",
    )
    inspect = classify_hermes_command(
        [
            "python.exe",
            "-m",
            "hermes_cli.main",
            "cron",
            "quiescence",
            "inspect",
        ],
        platform="win32",
    )

    assert bare.command_kind == "BARE_GATEWAY_DEFAULT_RUN"
    assert bare.dispatch_capable is True
    assert bare.known_non_dispatch is False
    assert restart.command_kind == "GATEWAY_RESTART_RUNTIME_CAPABLE"
    assert restart.dispatch_capable is True
    assert restart.known_non_dispatch is False
    assert status.command_kind == "GATEWAY_STATUS_QUERY_CLIENT"
    assert status.dispatch_capable is False
    assert status.known_non_dispatch is True
    assert inspect.command_kind == "CRON_MANAGEMENT_NON_EXECUTION"
    assert inspect.dispatch_capable is False
    assert inspect.known_non_dispatch is True


def test_live_census_keeps_access_denied_hermes_and_exposes_current_sid(
    tmp_path, monkeypatch
):
    import psutil

    import cron.quiescence as q

    owner = q.OwnerIdentity(101, 10.5, q.profile_home_sha256(tmp_path))

    class FakeProcess:
        def __init__(self, info, *, denied=False):
            self.info = info
            self.denied = denied

        def username(self):
            return "HQ\\user"

        def cmdline(self):
            if self.denied:
                raise psutil.AccessDenied(self.info["pid"])
            return ["pythonw.exe", "-m", "hermes_cli.main", "gateway", "run"]

        def exe(self):
            return str(tmp_path / self.info["name"])

        def cwd(self):
            return str(tmp_path)

        def environ(self):
            return {"HERMES_HOME": str(tmp_path)}

    owner_process = FakeProcess(
        {
            "pid": 101,
            "ppid": 1,
            "create_time": 10.5,
            "username": "HQ\\user",
            "name": "pythonw.exe",
        }
    )
    denied_process = FakeProcess(
        {
            "pid": 202,
            "ppid": 1,
            "create_time": 20.5,
            "username": None,
            "name": "hermes.exe",
        },
        denied=True,
    )
    monkeypatch.setattr(
        psutil, "process_iter", lambda attrs: [owner_process, denied_process]
    )
    monkeypatch.setattr(psutil, "Process", lambda pid: owner_process)

    def fake_sid(pid):
        if pid == 202:
            raise psutil.AccessDenied(pid)
        return "S-1-5-21-test"

    monkeypatch.setattr(q, "_windows_process_sid", fake_sid, raising=False)

    provider, username = q.build_live_process_snapshot_provider(tmp_path, owner)
    rows = list(provider())

    assert username == "HQ\\user"
    assert getattr(provider, "current_sid") == "S-1-5-21-test"
    denied = next(row for row in rows if row["pid"] == 202)
    assert denied["error"] == "AccessDenied"


def test_windows_default_profile_keeps_dispatch_peer_without_environment(
    tmp_path, monkeypatch
):
    import psutil

    import cron.quiescence as q

    monkeypatch.setenv("LOCALAPPDATA", str(tmp_path / "LocalAppData"))
    profile = tmp_path / "LocalAppData" / "hermes"
    owner = q.OwnerIdentity(101, 10.5, q.profile_home_sha256(profile))

    class FakeProcess:
        def __init__(self, pid, argv):
            self.info = {
                "pid": pid,
                "ppid": 1,
                "create_time": 10.5 if pid == 101 else 20.5,
                "username": "HQ\\user",
                "name": "python.exe",
            }
            self._argv = argv

        def username(self):
            return "HQ\\user"

        def cmdline(self):
            return self._argv

        def exe(self):
            return str(tmp_path / "outside" / "python.exe")

        def cwd(self):
            return str(tmp_path / "outside")

        def environ(self):
            raise psutil.AccessDenied(self.info["pid"])

    owner_process = FakeProcess(
        101, ["python", "-m", "hermes_cli.main", "gateway", "run"]
    )
    peer_process = FakeProcess(
        202, ["python", "-m", "hermes_cli.main", "cron", "tick"]
    )
    monkeypatch.setattr(
        psutil, "process_iter", lambda attrs: [owner_process, peer_process]
    )
    monkeypatch.setattr(psutil, "Process", lambda pid: owner_process)
    monkeypatch.setattr(q, "_windows_process_sid", lambda pid: "S-1-5-21-test")

    provider, _username = q.build_live_process_snapshot_provider(profile, owner)

    assert [row["pid"] for row in provider()] == [101, 202]


@pytest.mark.skipif(sys.platform != "win32", reason="Windows SID census contract")
def test_actual_same_sid_default_profile_dispatch_peer_is_visible(
    tmp_path, monkeypatch
):
    import os
    import subprocess
    import sys
    import time

    import cron.quiescence as q

    monkeypatch.setenv("LOCALAPPDATA", str(tmp_path / "LocalAppData"))
    monkeypatch.delenv("HERMES_HOME", raising=False)
    profile = tmp_path / "LocalAppData" / "hermes"
    pid, create_time = q.current_process_identity()
    owner = q.OwnerIdentity(pid, create_time, q.profile_home_sha256(profile))
    child = subprocess.Popen(
        [
            sys.executable,
            "-c",
            "import time; time.sleep(10)",
            "-m",
            "hermes_cli.main",
            "cron",
            "tick",
        ],
        cwd=str(tmp_path),
        env={key: value for key, value in os.environ.items() if key != "HERMES_HOME"},
    )
    try:
        provider, _username = q.build_live_process_snapshot_provider(profile, owner)
        deadline = time.monotonic() + 3
        visible = set()
        while time.monotonic() < deadline:
            visible = {int(row["pid"]) for row in provider()}
            if child.pid in visible:
                break
            time.sleep(0.05)
        assert child.pid in visible
    finally:
        child.terminate()
        child.wait(timeout=3)


def test_two_pass_process_census_is_activation_safe_only_for_one_owner_and_registered_peers(tmp_path):
    import json

    from cron.quiescence import (
        OwnerIdentity,
        collect_process_census,
        profile_home_sha256,
    )

    home_hash = profile_home_sha256(tmp_path)
    owner = OwnerIdentity(101, 10.5, home_hash)
    owner_argv = ["pythonw.exe", "-m", "hermes_cli.main", "gateway", "run"]
    peer_argv = ["python.exe", "-m", "hermes_cli.main", "cron", "tick"]
    first = [
        {
            "pid": 101,
            "ppid": 1,
            "create_time": 10.5,
            "username": "HQ\\user",
            "sid": "S-1-5-21-test",
            "name": "pythonw.exe",
            "exe": str(tmp_path / "venv" / "pythonw.exe"),
            "argv": owner_argv,
            "cwd": str(tmp_path),
        },
        {
            "pid": 202,
            "ppid": 101,
            "create_time": 20.5,
            "username": "HQ\\user",
            "sid": "S-1-5-21-test",
            "name": "python.exe",
            "exe": str(tmp_path / "venv" / "python.exe"),
            "argv": peer_argv,
            "cwd": str(tmp_path),
        },
    ]
    snapshots = iter([first, [dict(row) for row in first]])
    registry = [
        {
            "pid": 101,
            "create_time": 10.5,
            "profile_home_hash": home_hash,
            "protocol_version": 1,
            "broker_only": True,
            "command_kind": "CANONICAL_GATEWAY_RUN",
        },
        {
            "pid": 202,
            "create_time": 20.5,
            "profile_home_hash": home_hash,
            "protocol_version": 1,
            "broker_only": True,
            "command_kind": "CRON_TICK",
        },
    ]

    census = collect_process_census(
        profile_home=tmp_path,
        owner_identity=owner,
        registry_records=registry,
        snapshot_provider=lambda: next(snapshots),
        current_sid="S-1-5-21-test",
        platform="win32",
    )

    by_pid = {entry.pid: entry for entry in census.entries}
    assert by_pid[101].classification == "BROKER_OWNER"
    assert by_pid[202].classification == "REGISTERED_BROKER_ONLY"
    assert census.activation_safe is True
    assert census.hard_stop_reasons == ()

    evidence = census.to_evidence()
    rendered = json.dumps(evidence, sort_keys=True)
    assert evidence["schema"] == "hermes.cron.process-census.v1"
    assert "argv_sha256" in rendered
    assert "hermes_cli.main" not in rendered
    assert "raw_env" not in rendered


@pytest.mark.parametrize(
    "second,registry,expected_reason",
    [
        ([{"pid": 101, "create_time": 99.0, "error": None}], [], "PID_REUSE"),
        (
            [{"pid": 101, "create_time": 10.5, "error": "AccessDenied"}],
            [],
            "ACCESS_DENIED",
        ),
        (
            [],
            [
                {
                    "pid": 303,
                    "create_time": 30.5,
                    "profile_home_hash": "target",
                    "protocol_version": 1,
                    "broker_only": True,
                    "command_kind": "CRON_RUN",
                }
            ],
            "REGISTRY_ONLY_DEAD",
        ),
    ],
)
def test_process_census_fail_closes_pid_reuse_access_denied_and_dead_registry(
    tmp_path, second, registry, expected_reason
):
    from cron.quiescence import OwnerIdentity, collect_process_census, profile_home_sha256

    home_hash = profile_home_sha256(tmp_path)
    owner = OwnerIdentity(999, 1.0, home_hash)
    first = [
        {
            "pid": 101,
            "ppid": 1,
            "create_time": 10.5,
            "username": "HQ\\user",
            "sid": "S-1-5-21-test",
            "name": "python.exe",
            "exe": str(tmp_path / "venv" / "python.exe"),
            "argv": ["python.exe", "-m", "hermes_cli.main", "cron", "run", "j1"],
            "cwd": str(tmp_path),
        }
    ]
    normalized_registry = [
        {
            **record,
            "profile_home_hash": (
                home_hash
                if record["profile_home_hash"] == "target"
                else record["profile_home_hash"]
            ),
        }
        for record in registry
    ]
    snapshots = iter([first, second])

    census = collect_process_census(
        profile_home=tmp_path,
        owner_identity=owner,
        registry_records=normalized_registry,
        snapshot_provider=lambda: next(snapshots),
        current_sid="S-1-5-21-test",
        platform="win32",
    )

    assert census.activation_safe is False
    assert any(expected_reason in reason for reason in census.hard_stop_reasons)


def test_local_file_transport_round_trips_through_registered_broker(tmp_path, monkeypatch):
    import os
    import psutil
    import threading
    from types import SimpleNamespace

    from cron.quiescence import (
        CronBroker,
        DispatchResult,
        OwnerIdentity,
        profile_home_sha256,
        request_broker_dispatch,
    )

    owner = OwnerIdentity(
        os.getpid(), psutil.Process().create_time(), profile_home_sha256(tmp_path)
    )
    broker = CronBroker(
        profile_home=tmp_path,
        owner_identity=owner,
        submit=lambda fn: None,
        runner=lambda job: True,
    )
    broker.register_owner(
        command_kind="CANONICAL_GATEWAY_RUN",
        argv=["pythonw.exe", "-m", "hermes_cli.main", "gateway", "run"],
    )
    due = SimpleNamespace(job_id="j1")
    monkeypatch.setattr("cron.jobs.scan_job_for_dispatch_read_only", lambda job_id: due)
    monkeypatch.setattr(
        broker,
        "admit",
        lambda due_job, mode: DispatchResult(
            status="ACCEPTED",
            job_id=due_job.job_id,
            mode=mode,
            request_id="broker-result",
            attempt_token="a",
            run_token="r",
        ),
    )
    stop = threading.Event()
    server = threading.Thread(
        target=broker.serve_request_queue,
        args=(stop,),
        kwargs={"poll_interval": 0.005},
        daemon=True,
    )
    server.start()
    assert broker.wait_request_server_started(timeout=1.0)
    broker.mark_owner_ready()
    try:
        result = request_broker_dispatch(
            "j1", mode="immediate", profile_home=tmp_path, timeout=1.0
        )
    finally:
        stop.set()
        server.join(timeout=1)
        broker.close_owner()

    assert result.status.value == "ACCEPTED"
    assert result.job_id == "j1"
    assert result.mode == "immediate"
    assert result.attempt_token is None
    assert result.run_token is None
    assert server.is_alive() is False


def test_owner_registration_is_unready_until_server_started_and_marked(tmp_path):
    import json
    import os
    import psutil

    from cron.quiescence import CronBroker, OwnerIdentity, profile_home_sha256

    owner = OwnerIdentity(
        os.getpid(), psutil.Process().create_time(), profile_home_sha256(tmp_path)
    )
    broker = CronBroker(
        profile_home=tmp_path,
        owner_identity=owner,
        submit=lambda fn: None,
        runner=lambda job: True,
    )
    broker.register_owner(
        command_kind="CANONICAL_GATEWAY_RUN", argv=["hermes", "gateway", "run"]
    )
    payload = json.loads(broker.owner_registry_path.read_text(encoding="utf-8"))
    assert broker.ready is False
    assert payload["ready"] is False


def test_second_owner_registration_for_same_profile_hard_stops(tmp_path):
    import os
    import psutil
    import pytest

    from cron.quiescence import CronBroker, OwnerIdentity, profile_home_sha256

    owner = OwnerIdentity(
        os.getpid(), psutil.Process().create_time(), profile_home_sha256(tmp_path)
    )
    first = CronBroker(
        profile_home=tmp_path,
        owner_identity=owner,
        submit=lambda fn: None,
        runner=lambda job: True,
    )
    second = CronBroker(
        profile_home=tmp_path,
        owner_identity=owner,
        submit=lambda fn: None,
        runner=lambda job: True,
    )
    first.register_owner(
        command_kind="CANONICAL_GATEWAY_RUN", argv=["hermes", "gateway", "run"]
    )
    with pytest.raises(RuntimeError, match="owner lock|owner registration"):
        second.register_owner(
            command_kind="CANONICAL_GATEWAY_RUN",
            argv=["hermes", "gateway", "run"],
        )
    first.close_owner()


def test_dead_ready_owner_cannot_authenticate_dispatch(tmp_path):
    import json

    from cron.quiescence import profile_home_sha256, request_broker_dispatch

    owners = tmp_path / "cron" / "quiescence" / "owners"
    owners.mkdir(parents=True)
    (owners / "dead.json").write_text(
        json.dumps(
            {
                "schema": "hermes.cron.owner-registration.v1",
                "owner": {
                    "pid": 99999999,
                    "create_time": 1.0,
                    "profile_home_hash": profile_home_sha256(tmp_path),
                    "protocol_version": 1,
                },
                "profile_home_hash": profile_home_sha256(tmp_path),
                "protocol_version": 1,
                "broker_only": True,
                "ready": True,
            }
        ),
        encoding="utf-8",
    )
    result = request_broker_dispatch(
        "j1", mode="immediate", profile_home=tmp_path, timeout=0.01
    )
    assert result.status.value in {"UNKNOWN", "BROKER_UNAVAILABLE"}


def test_dispatch_request_persists_only_minimal_authenticated_envelope(
    tmp_path, monkeypatch
):
    import os
    import psutil

    import cron.quiescence as q

    owner = q.OwnerIdentity(
        os.getpid(), psutil.Process().create_time(), q.profile_home_sha256(tmp_path)
    )
    broker = q.CronBroker(
        profile_home=tmp_path,
        owner_identity=owner,
        submit=lambda fn: None,
        runner=lambda job: True,
    )
    broker.register_owner(
        command_kind="CANONICAL_GATEWAY_RUN",
        argv=["hermes", "gateway", "run"],
    )
    broker._request_server_started.set()
    broker.mark_owner_ready()
    captured = {}
    real_write = q._atomic_json_write

    def capture(path, payload):
        if path.parent.name == "requests":
            captured.update(payload)
        return real_write(path, payload)

    monkeypatch.setattr(q, "_atomic_json_write", capture)
    try:
        q.request_broker_dispatch(
            "j1", mode="immediate", profile_home=tmp_path, timeout=0.01
        )
    finally:
        broker.close_owner()
    assert set(captured) == {
        "schema",
        "protocol_version",
        "request_id",
        "nonce",
        "caller_pid",
        "caller_create_time",
        "owner",
        "owner_epoch",
        "profile_home_sha256",
        "job_id",
        "mode",
        "issued_at",
        "expires_at",
        "status",
        "request_sha256",
        "auth_tag",
    }
    key = q._read_transport_key(tmp_path)
    assert captured["request_sha256"] == q._request_payload_sha256(captured)
    assert q._verify_transport_payload(
        captured, key, domain="dispatch-request"
    )
    assert "attempt_token" not in captured
    assert "run_token" not in captured
    assert broker.close_owner() is True


def test_broker_drops_tampered_dispatch_request_without_admission(
    tmp_path, monkeypatch
):
    import threading
    import time

    import cron.quiescence as q

    pid, create_time = q.current_process_identity()
    owner = q.OwnerIdentity(pid, create_time, q.profile_home_sha256(tmp_path))
    broker = q.CronBroker(
        profile_home=tmp_path,
        owner_identity=owner,
        submit=lambda fn: None,
        runner=lambda job: True,
    )
    broker.register_owner(
        command_kind="CANONICAL_GATEWAY_RUN",
        argv=["pythonw.exe", "-m", "hermes_cli.main", "gateway", "run"],
    )
    stop = threading.Event()
    server = threading.Thread(
        target=broker.serve_request_queue, args=(stop,), daemon=True
    )
    server.start()
    assert broker.wait_request_server_started(1.0)
    broker.mark_owner_ready()

    calls = []
    monkeypatch.setattr(
        q,
        "request_broker_dispatch",
        lambda *args, **kwargs: calls.append((args, kwargs)),
    )
    key = q._read_transport_key(tmp_path)
    request_id = "tampered-request"
    issued_at = time.time()
    payload = {
        "schema": q.REQUEST_SCHEMA,
        "protocol_version": q.PROTOCOL_VERSION,
        "request_id": request_id,
        "nonce": "n" * 32,
        "caller_pid": pid,
        "caller_create_time": create_time,
        "owner": owner.to_dict(),
        "owner_epoch": broker.owner_epoch,
        "profile_home_sha256": q.profile_home_sha256(tmp_path),
        "job_id": "original-job",
        "mode": "immediate",
        "issued_at": issued_at,
        "expires_at": issued_at + 2.0,
        "status": "REQUESTED",
    }
    payload["request_sha256"] = q._request_payload_sha256(payload)
    payload = q._signed_transport_payload(payload, key, domain="dispatch-request")
    payload["job_id"] = "tampered-job"
    request_path = (
        tmp_path / "cron" / "quiescence" / "requests" / f"{request_id}.json"
    )
    q._atomic_json_write(request_path, payload)
    deadline = time.monotonic() + 1.0
    while request_path.exists() and time.monotonic() < deadline:
        time.sleep(0.01)

    assert calls == []
    assert not (
        tmp_path / "cron" / "quiescence" / "responses" / f"{request_id}.json"
    ).exists()
    stop.set()
    server.join(timeout=1.0)
    assert broker.close_owner() is True


def test_local_file_transport_multiple_ready_owners_is_unknown_hard_stop(tmp_path):
    import json

    from cron.quiescence import request_broker_dispatch

    owners = tmp_path / "cron" / "quiescence" / "owners"
    owners.mkdir(parents=True)
    for pid in (101, 202):
        (owners / f"{pid}.json").write_text(
            json.dumps(
                {
                    "schema": "hermes.cron.owner-registration.v1",
                    "owner": {
                        "pid": pid,
                        "create_time": float(pid),
                        "profile_home_hash": "x",
                        "protocol_version": 1,
                    },
                    "protocol_version": 1,
                    "broker_only": True,
                    "command_kind": "CANONICAL_GATEWAY_RUN",
                    "argv_sha256": "a" * 64,
                    "ready": True,
                }
            ),
            encoding="utf-8",
        )

    result = request_broker_dispatch(
        "j1", mode="immediate", profile_home=tmp_path, timeout=0.01
    )

    assert result.status.value == "UNKNOWN"
    assert result.retryable is False
