from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sys
import threading
import time
from types import SimpleNamespace

import pytest


def _start_broker(home: Path):
    import psutil
    from cron.quiescence import CronBroker, OwnerIdentity, profile_home_sha256

    owner = OwnerIdentity(
        os.getpid(), psutil.Process().create_time(), profile_home_sha256(home), 1
    )
    broker = CronBroker(
        profile_home=home,
        owner_identity=owner,
        submit=lambda fn: None,
        runner=lambda job: True,
    )
    broker.register_owner(command_kind="CANONICAL_GATEWAY_RUN", argv=["hermes", "gateway", "run"])
    stop = threading.Event()
    thread = threading.Thread(
        target=broker.serve_request_queue,
        args=(stop,),
        kwargs={"poll_interval": 0.005},
        daemon=True,
    )
    thread.start()
    assert broker.wait_request_server_started(1.0)
    broker.mark_owner_ready()
    return broker, stop, thread


def _stop_broker(broker, stop, thread):
    stop.set()
    thread.join(timeout=2)
    broker.close_owner()


def _script(tmp_path: Path, text: str) -> Path:
    path = (tmp_path / "child.py").resolve()
    path.write_text(text, encoding="utf-8")
    return path


def test_parser_accepts_exact_argv_after_double_dash():
    from hermes_cli.subcommands.cron import build_cron_parser

    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command")
    build_cron_parser(subparsers, cmd_cron=lambda args: None)
    args = parser.parse_args(
        [
            "cron",
            "quiescent-exec",
            "--wait-timeout",
            "3",
            "--child-timeout",
            "4",
            "--expected-argv-sha256",
            "a" * 64,
            "--",
            sys.executable,
            "--version",
        ]
    )
    assert args.cron_command == "quiescent-exec"
    assert args.wait_timeout == 3
    assert args.child_timeout == 4
    assert args.expected_argv_sha256 == "a" * 64
    assert args.argv[-2:] == [sys.executable, "--version"]


@pytest.mark.parametrize(
    "argv",
    [
        [],
        ["relative.exe"],
        [sys.executable, "$(whoami)"],
        [sys.executable, "ok;bad"],
        [sys.executable, "--api-key=secret"],
    ],
)
def test_argv_validation_rejects_unsafe_shapes(argv):
    from cron.quiescence import validate_quiescent_argv

    with pytest.raises(ValueError):
        validate_quiescent_argv(argv)


def test_hash_mismatch_rejects_before_spawn(tmp_path):
    from cron.quiescence import execute_quiescent_child

    script = _script(tmp_path, "print('must-not-run')\n")
    with pytest.raises(ValueError, match="SHA-256 mismatch"):
        execute_quiescent_child(
            [sys.executable, str(script)],
            expected_argv_sha256="0" * 64,
            profile_home=tmp_path / "home",
            wait_timeout=0.1,
            child_timeout=1,
        )


def test_live_owner_ack_runs_exact_child_with_minimal_env_and_redacted_evidence(tmp_path, monkeypatch):
    from cron.quiescence import canonical_argv_sha256, execute_quiescent_child

    home = tmp_path / "home"
    broker, stop, thread = _start_broker(home)
    monkeypatch.setenv("G2A_SECRET_SENTINEL", "must-not-inherit")
    secret = "sk-proj-abcdefghijklmnopqrstuvwxyz0123456789"
    script = _script(
        tmp_path,
        "import os\n"
        "print('minimal=' + str(os.getenv('G2A_SECRET_SENTINEL') is None))\n"
        f"print('{secret}')\n",
    )
    argv = [sys.executable, str(script)]
    try:
        result = execute_quiescent_child(
            argv,
            expected_argv_sha256=canonical_argv_sha256(argv),
            profile_home=home,
            wait_timeout=2,
            child_timeout=3,
        )
    finally:
        _stop_broker(broker, stop, thread)

    assert result.exit_code == 0
    assert result.status == "COMPLETED"
    assert "minimal=True" in result.stdout
    assert secret not in result.stdout
    evidence_path = Path(result.evidence_path)
    evidence = json.loads(evidence_path.read_text(encoding="utf-8"))
    serialized = evidence_path.read_text(encoding="utf-8")
    assert evidence["schema"] == "hermes.cron.quiescent-exec-evidence.v1"
    assert evidence["argv_sha256"] == canonical_argv_sha256(argv)
    assert str(script) not in serialized
    assert sys.executable not in serialized
    assert not list((home / "cron" / "quiescence" / "requests").glob("*.json"))
    assert not list((home / "cron" / "quiescence" / "responses").glob("*.json"))


def test_admission_lock_is_held_for_child_lifetime(tmp_path):
    from cron.quiescence import AdmissionLock, canonical_argv_sha256, execute_quiescent_child

    home = tmp_path / "home"
    marker = tmp_path / "started"
    broker, stop, server = _start_broker(home)
    script = _script(
        tmp_path,
        "from pathlib import Path\n"
        "import time\n"
        f"Path({str(marker)!r}).write_text('started')\n"
        "time.sleep(0.4)\n",
    )
    argv = [sys.executable, str(script)]
    holder = {}

    def run():
        holder["result"] = execute_quiescent_child(
            argv,
            expected_argv_sha256=canonical_argv_sha256(argv),
            profile_home=home,
            wait_timeout=2,
            child_timeout=2,
        )

    worker = threading.Thread(target=run)
    worker.start()
    deadline = time.monotonic() + 2
    while not marker.exists() and time.monotonic() < deadline:
        time.sleep(0.01)
    competing = AdmissionLock(home)
    try:
        assert marker.exists()
        assert competing.acquire(0.0) is False
        worker.join(timeout=3)
        assert holder["result"].exit_code == 0
        assert competing.acquire(0.1) is True
    finally:
        competing.release()
        _stop_broker(broker, stop, server)


def test_child_timeout_returns_124_and_cleans_transport(tmp_path):
    from cron.quiescence import canonical_argv_sha256, execute_quiescent_child

    home = tmp_path / "home"
    broker, stop, thread = _start_broker(home)
    script = _script(tmp_path, "import time\ntime.sleep(5)\n")
    argv = [sys.executable, str(script)]
    try:
        result = execute_quiescent_child(
            argv,
            expected_argv_sha256=canonical_argv_sha256(argv),
            profile_home=home,
            wait_timeout=2,
            child_timeout=0.05,
        )
    finally:
        _stop_broker(broker, stop, thread)

    assert result.exit_code == 124
    assert result.status == "TIMEOUT"
    assert result.timed_out is True
    assert Path(result.evidence_path).exists()
    assert not list((home / "cron" / "quiescence" / "responses").glob("*.json"))


@pytest.mark.skipif(os.name != "nt", reason="Windows process-tree contract")
def test_child_timeout_terminates_grandchild_tree(tmp_path):
    import psutil

    from cron.quiescence import canonical_argv_sha256, execute_quiescent_child

    home = tmp_path / "home"
    pid_file = tmp_path / "grandchild.pid"
    broker, stop, thread = _start_broker(home)
    script = _script(
        tmp_path,
        "from pathlib import Path\n"
        "import subprocess, sys, time\n"
        "child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)'])\n"
        f"Path({str(pid_file)!r}).write_text(str(child.pid))\n"
        "time.sleep(60)\n",
    )
    argv = [sys.executable, str(script)]
    started = time.monotonic()
    try:
        result = execute_quiescent_child(
            argv,
            expected_argv_sha256=canonical_argv_sha256(argv),
            profile_home=home,
            wait_timeout=2,
            child_timeout=0.25,
        )
    finally:
        _stop_broker(broker, stop, thread)

    assert time.monotonic() - started < 5
    assert result.exit_code == 124
    grandchild_pid = int(pid_file.read_text(encoding="utf-8"))
    deadline = time.monotonic() + 3
    while psutil.pid_exists(grandchild_pid) and time.monotonic() < deadline:
        time.sleep(0.02)
    assert not psutil.pid_exists(grandchild_pid)


@pytest.mark.skipif(os.name != "nt", reason="Windows process-tree contract")
def test_unconfirmed_windows_termination_retains_admission_until_retry(
    tmp_path, monkeypatch
):
    import io

    import cron.quiescence as q

    home = tmp_path / "home"
    allow_termination = {"value": False}

    class FakeProcess:
        pid = 424242
        returncode = None
        _handle = 1
        stdout = io.BytesIO(b"")
        stderr = io.BytesIO(b"")

        def wait(self, timeout=None):
            if self.returncode is None:
                raise q.subprocess.TimeoutExpired(["fake"], timeout)
            return self.returncode

        def poll(self):
            return self.returncode

    fake_process = FakeProcess()

    class FakeJob:
        def __init__(self):
            self.closed = False

        def assign(self, process):
            self.process = process

        def terminate(self):
            if not allow_termination["value"]:
                return False
            fake_process.returncode = 1
            return True

        def wait_empty(self, timeout=5.0):
            return fake_process.returncode is not None

        def close(self):
            self.closed = True

    monkeypatch.setattr(q, "_WindowsKillOnCloseJob", FakeJob)
    monkeypatch.setattr(q.subprocess, "Popen", lambda *args, **kwargs: fake_process)
    monkeypatch.setattr(q, "_resume_suspended_windows_process", lambda pid: None)
    monkeypatch.setattr(
        q,
        "_await_quiescence_ack",
        lambda *args, **kwargs: {
            "request_id": "r1",
            "request_path": home / "request.json",
            "response_path": home / "response.json",
        },
    )
    argv = [sys.executable, str(tmp_path / "fake.py")]

    result = q.execute_quiescent_child(
        argv,
        expected_argv_sha256=q.canonical_argv_sha256(argv),
        profile_home=home,
        wait_timeout=1,
        child_timeout=0.01,
    )

    assert result.status == "UNKNOWN_HARD_STOP"
    competing = q.AdmissionLock(home)
    assert competing.acquire(0.0) is False
    containment_files = list(
        (home / "cron" / "quiescence" / "containments").glob("*.json")
    )
    assert len(containment_files) == 1
    allow_termination["value"] = True
    assert q.retry_unconfirmed_containments_once() == 1
    assert containment_files[0].exists() is False
    assert competing.acquire(0.1) is True
    competing.release()


@pytest.mark.skipif(os.name != "nt", reason="Windows process-tree contract")
def test_assignment_failure_retains_admission_until_child_exit(
    tmp_path, monkeypatch
):
    import io

    import cron.quiescence as q

    home = tmp_path / "home"
    allow_termination = {"value": False}

    class FakeProcess:
        pid = 434343
        returncode = None
        _handle = 1
        stdout = io.BytesIO(b"")
        stderr = io.BytesIO(b"")

        def wait(self, timeout=None):
            if self.returncode is None:
                raise q.subprocess.TimeoutExpired(["fake"], timeout)
            return self.returncode

        def poll(self):
            return self.returncode

        def terminate(self):
            if allow_termination["value"]:
                self.returncode = 1

        kill = terminate

    fake_process = FakeProcess()

    class FailingAssignJob:
        def assign(self, process):
            raise OSError("assign denied")

        def close(self):
            pass

    monkeypatch.setattr(q, "_WindowsKillOnCloseJob", FailingAssignJob)
    monkeypatch.setattr(q.subprocess, "Popen", lambda *args, **kwargs: fake_process)
    monkeypatch.setattr(
        q,
        "_terminate_owned_process",
        lambda process, windows_job=None: (
            setattr(process, "returncode", 1) or True
            if allow_termination["value"]
            else False
        ),
    )
    monkeypatch.setattr(
        q,
        "_await_quiescence_ack",
        lambda *args, **kwargs: {
            "request_id": "r2",
            "request_path": home / "request.json",
            "response_path": home / "response.json",
        },
    )
    argv = [sys.executable, str(tmp_path / "fake.py")]

    result = q.execute_quiescent_child(
        argv,
        expected_argv_sha256=q.canonical_argv_sha256(argv),
        profile_home=home,
        wait_timeout=1,
        child_timeout=0.01,
    )

    assert result.status == "UNKNOWN_HARD_STOP"
    competing = q.AdmissionLock(home)
    assert competing.acquire(0.0) is False
    containment_files = list(
        (home / "cron" / "quiescence" / "containments").glob("*.json")
    )
    assert len(containment_files) == 1
    allow_termination["value"] = True
    assert q.retry_unconfirmed_containments_once() == 1
    assert containment_files[0].exists() is False
    assert competing.acquire(0.1) is True
    competing.release()


@pytest.mark.skipif(os.name != "nt", reason="Windows process-tree contract")
def test_containment_key_setup_failure_still_retains_admission(
    tmp_path, monkeypatch
):
    import io

    import cron.quiescence as q

    home = tmp_path / "home"
    allow_termination = {"value": False}

    class FakeProcess:
        pid = 444444
        returncode = None
        _handle = 1
        stdout = io.BytesIO(b"")
        stderr = io.BytesIO(b"")

        def wait(self, timeout=None):
            if self.returncode is None:
                raise q.subprocess.TimeoutExpired(["fake"], timeout)
            return self.returncode

        def poll(self):
            return self.returncode

    fake_process = FakeProcess()

    class FailingAssignJob:
        def assign(self, process):
            raise OSError("assign denied")

        def close(self):
            pass

    monkeypatch.setattr(q, "_WindowsKillOnCloseJob", FailingAssignJob)
    monkeypatch.setattr(q.subprocess, "Popen", lambda *args, **kwargs: fake_process)
    monkeypatch.setattr(
        q,
        "_terminate_owned_process",
        lambda process, windows_job=None: (
            setattr(process, "returncode", 1) or True
            if allow_termination["value"]
            else False
        ),
    )
    monkeypatch.setattr(
        q, "_ensure_transport_key", lambda profile: (_ for _ in ()).throw(OSError("key denied"))
    )
    monkeypatch.setattr(
        q,
        "_await_quiescence_ack",
        lambda *args, **kwargs: {
            "request_id": "r-key-fail",
            "request_path": home / "request.json",
            "response_path": home / "response.json",
        },
    )
    argv = [sys.executable, str(tmp_path / "fake.py")]

    result = q.execute_quiescent_child(
        argv,
        expected_argv_sha256=q.canonical_argv_sha256(argv),
        profile_home=home,
        wait_timeout=1,
        child_timeout=0.01,
    )

    assert result.status == "UNKNOWN_HARD_STOP"
    assert fake_process.poll() is None
    competing = q.AdmissionLock(home)
    assert competing.acquire(0.0) is False
    containment_files = list(
        (home / "cron" / "quiescence" / "containments").glob("*.json")
    )
    assert len(containment_files) == 1

    allow_termination["value"] = True
    assert q.retry_unconfirmed_containments_once() == 1
    assert containment_files[0].exists() is False
    assert fake_process.poll() == 1
    assert competing.acquire(0.1) is True
    competing.release()


def test_containment_persistence_and_watchdog_failure_terminates_before_return(
    tmp_path, monkeypatch
):
    import cron.quiescence as q

    home = tmp_path / "profile"
    admission = q.AdmissionLock(home)
    assert admission.acquire(0.1) is True

    class FakeProcess:
        pid = 777

        def __init__(self):
            self.returncode = None

        def poll(self):
            return self.returncode

    process = FakeProcess()

    class FakeJob:
        def close(self):
            pass

    windows_job = FakeJob()
    termination_calls = []

    monkeypatch.setattr(
        q,
        "_ensure_transport_key",
        lambda _profile: (_ for _ in ()).throw(RuntimeError("key unavailable")),
    )
    monkeypatch.setattr(
        q,
        "_atomic_json_write",
        lambda *args, **kwargs: (_ for _ in ()).throw(OSError("atomic unavailable")),
    )
    monkeypatch.setattr(
        q,
        "_write_raw_containment_sentinel",
        lambda *args, **kwargs: (_ for _ in ()).throw(OSError("raw unavailable")),
    )

    class BrokenThread:
        def __init__(self, *args, **kwargs):
            pass

        def start(self):
            raise RuntimeError("thread unavailable")

    monkeypatch.setattr(q.threading, "Thread", BrokenThread)

    def terminate(_process, _windows_job):
        termination_calls.append(True)
        if len(termination_calls) >= 2:
            process.returncode = 1
            return True
        return False

    monkeypatch.setattr(q, "_terminate_owned_process", terminate)
    containment_id = q._retain_unconfirmed_containment(
        home,
        admission,
        process,
        windows_job,
    )

    assert containment_id
    assert process.poll() == 1
    assert len(termination_calls) >= 2
    assert q.retry_unconfirmed_containments_once() == 0
    assert list((home / "cron" / "quiescence" / "containments").glob("*.json")) == []
    competing = q.AdmissionLock(home)
    assert competing.acquire(0.1) is True
    competing.release()


@pytest.mark.skipif(os.name != "nt", reason="Windows process-tree contract")
def test_live_suspended_child_assignment_failure_is_durably_contained(
    tmp_path, monkeypatch
):
    import json

    import psutil

    import cron.quiescence as q

    home = tmp_path / "home"
    script = _script(tmp_path, "import time\ntime.sleep(60)\n")
    argv = [sys.executable, str(script)]
    allow_termination = {"value": False}
    real_terminate = q._terminate_owned_process

    class FailingAssignJob:
        def assign(self, process):
            raise OSError("assign denied")

        def close(self):
            pass

    def controlled_terminate(process, windows_job=None):
        if not allow_termination["value"]:
            return False
        return real_terminate(process, windows_job)

    monkeypatch.setattr(q, "_WindowsKillOnCloseJob", FailingAssignJob)
    monkeypatch.setattr(q, "_terminate_owned_process", controlled_terminate)
    monkeypatch.setattr(
        q,
        "_await_quiescence_ack",
        lambda *args, **kwargs: {
            "request_id": "r-live",
            "request_path": home / "request.json",
            "response_path": home / "response.json",
        },
    )

    try:
        result = q.execute_quiescent_child(
            argv,
            expected_argv_sha256=q.canonical_argv_sha256(argv),
            profile_home=home,
            wait_timeout=1,
            child_timeout=1,
        )
        record_path = next(
            (home / "cron" / "quiescence" / "containments").glob("*.json")
        )
        pid = int(json.loads(record_path.read_text(encoding="utf-8"))["pid"])
        assert result.status == "UNKNOWN_HARD_STOP"
        assert psutil.pid_exists(pid)
        with pytest.raises(RuntimeError, match="live unconfirmed child"):
            q.assert_no_live_unconfirmed_containments(home)

        allow_termination["value"] = True
        assert q.retry_unconfirmed_containments_once() == 1
        assert record_path.exists() is False
        assert not psutil.pid_exists(pid)
    finally:
        allow_termination["value"] = True
        q.retry_unconfirmed_containments_once()


def test_durable_live_containment_blocks_new_owner(tmp_path):
    import json

    import psutil

    import cron.quiescence as q

    home = tmp_path / "home"
    key = q._ensure_transport_key(home)
    pid = os.getpid()
    create_time = psutil.Process(pid).create_time()
    payload = {
        "schema": q._CONTAINMENT_SCHEMA,
        "profile_home_sha256": q.profile_home_sha256(home),
        "pid": pid,
        "create_time": create_time,
        "status": "UNCONFIRMED",
    }
    record_path = home / "cron" / "quiescence" / "containments" / "live.json"
    q._atomic_json_write(
        record_path,
        q._signed_transport_payload(
            payload, key, domain="unconfirmed-containment"
        ),
    )

    with pytest.raises(RuntimeError, match="live unconfirmed child"):
        q.assert_no_live_unconfirmed_containments(home)
    assert json.loads(record_path.read_text(encoding="utf-8"))["pid"] == pid


def test_pipe_capture_failure_is_not_silently_empty():
    import cron.quiescence as q

    class BrokenStream:
        def read(self, _size):
            raise OSError("pipe failed")

        def close(self):
            pass

    capture = q._BoundedPipeCapture(BrokenStream())
    capture.start()
    with pytest.raises(RuntimeError, match="collector failed: OSError"):
        capture.finish()


@pytest.mark.skipif(os.name != "nt", reason="Windows process-tree contract")
def test_public_exec_pipe_failure_returns_hard_stop(tmp_path, monkeypatch):
    import io

    import cron.quiescence as q

    home = tmp_path / "home"

    class BrokenStream:
        def read(self, _size):
            raise OSError("pipe failed")

        def close(self):
            pass

    class FakeProcess:
        pid = 454545
        returncode = 0
        _handle = 1
        stdout = BrokenStream()
        stderr = io.BytesIO(b"")

        def wait(self, timeout=None):
            return 0

        def poll(self):
            return self.returncode

    class FakeJob:
        def assign(self, process):
            pass

        def terminate(self):
            return True

        def wait_empty(self, timeout=5.0):
            return True

        def close(self):
            pass

    monkeypatch.setattr(q, "_WindowsKillOnCloseJob", FakeJob)
    monkeypatch.setattr(q.subprocess, "Popen", lambda *args, **kwargs: FakeProcess())
    monkeypatch.setattr(q, "_resume_suspended_windows_process", lambda pid: None)
    monkeypatch.setattr(
        q,
        "_await_quiescence_ack",
        lambda *args, **kwargs: {
            "request_id": "r3",
            "request_path": home / "request.json",
            "response_path": home / "response.json",
        },
    )
    argv = [sys.executable, str(tmp_path / "fake.py")]

    result = q.execute_quiescent_child(
        argv,
        expected_argv_sha256=q.canonical_argv_sha256(argv),
        profile_home=home,
        wait_timeout=1,
        child_timeout=1,
    )

    assert result.status == "HARD_STOP"
    assert result.exit_code == 70
    assert "collector failed: OSError" in result.stderr


def test_evidence_write_failure_still_releases_lock_and_cleans_transport(
    tmp_path, monkeypatch
):
    import cron.quiescence as q

    home = tmp_path / "home"
    broker, stop, thread = _start_broker(home)
    script = _script(tmp_path, "print('ok')\n")
    argv = [sys.executable, str(script)]
    real_write = q._atomic_json_write

    def fail_evidence(path, payload):
        if path.parent.name == "evidence":
            raise OSError("evidence disk failure")
        return real_write(path, payload)

    monkeypatch.setattr(q, "_atomic_json_write", fail_evidence)
    try:
        result = q.execute_quiescent_child(
            argv,
            expected_argv_sha256=q.canonical_argv_sha256(argv),
            profile_home=home,
            wait_timeout=2,
            child_timeout=2,
        )
        competing = q.AdmissionLock(home)
        assert competing.acquire(0.1)
        competing.release()
    finally:
        _stop_broker(broker, stop, thread)

    assert result.exit_code == 70
    assert result.status == "HARD_STOP"
    root = home / "cron" / "quiescence"
    assert list((root / "requests").glob("*.json")) == []
    assert list((root / "responses").glob("*.json")) == []


def test_cli_strips_separator_and_propagates_child_exit(monkeypatch, capsys):
    import cron.quiescence as q
    from hermes_cli.cron import cron_quiescent_exec

    captured = {}

    def fake(argv, **kwargs):
        captured["argv"] = argv
        return q.QuiescentExecResult("CHILD_FAILED", 7, False, "evidence.json", "out", "err")

    monkeypatch.setattr(q, "execute_quiescent_child", fake)
    args = SimpleNamespace(
        argv=["--", sys.executable, "--version"],
        expected_argv_sha256="a" * 64,
        wait_timeout=1,
        child_timeout=2,
    )
    assert cron_quiescent_exec(args) == 7
    streams = capsys.readouterr()
    assert captured["argv"] == [sys.executable, "--version"]
    assert "out" in streams.out
    assert "err" in streams.err
    assert "evidence.json" in streams.err
