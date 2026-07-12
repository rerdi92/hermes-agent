from __future__ import annotations

import json
import threading

import pytest


def _broker(tmp_path):
    from cron.quiescence import CronBroker, OwnerIdentity, profile_home_sha256

    owner = OwnerIdentity(101, 10.5, profile_home_sha256(tmp_path))
    return CronBroker(
        profile_home=tmp_path,
        owner_identity=owner,
        submit=lambda fn: None,
        runner=lambda job: True,
    )


def test_owner_registration_publishes_ready_only_after_initial_ledger(tmp_path):
    broker = _broker(tmp_path)
    argv = ["pythonw.exe", "-m", "hermes_cli.main", "gateway", "run"]

    path = broker.register_owner(command_kind="CANONICAL_GATEWAY_RUN", argv=argv)

    assert broker.ready is False
    owner_payload = json.loads(path.read_text(encoding="utf-8"))
    ledger_payload = json.loads(broker.ledger_path.read_text(encoding="utf-8"))
    assert owner_payload["schema"] == "hermes.cron.owner-registration.v1"
    assert owner_payload["broker_only"] is True
    assert owner_payload["ready"] is False
    assert owner_payload["owner"] == broker.owner_identity.to_dict()
    assert ledger_payload["owner"] == broker.owner_identity.to_dict()
    rendered = path.read_text(encoding="utf-8")
    assert "argv_sha256" in rendered
    assert "hermes_cli.main" not in rendered

    stop = threading.Event()
    server = threading.Thread(
        target=broker.serve_request_queue, args=(stop,), daemon=True
    )
    server.start()
    assert broker.wait_request_server_started(1.0)
    broker.mark_owner_ready()
    assert broker.ready is True
    assert json.loads(path.read_text(encoding="utf-8"))["ready"] is True
    stop.set()
    server.join(timeout=1.0)
    assert broker.ready is False
    assert json.loads(path.read_text(encoding="utf-8"))["ready"] is False
    assert broker.close_owner() is True


def test_owner_registration_failure_never_leaves_false_ready(tmp_path, monkeypatch):
    broker = _broker(tmp_path)
    monkeypatch.setattr(
        broker,
        "_publish_locked",
        lambda: (_ for _ in ()).throw(OSError("disk")),
    )

    with pytest.raises(OSError, match="disk"):
        broker.register_owner(
            command_kind="CANONICAL_GATEWAY_RUN",
            argv=["pythonw.exe", "-m", "hermes_cli.main", "gateway", "run"],
        )

    assert broker.ready is False
    assert not broker.owner_registry_path.exists()


def test_owner_cleanup_is_exact_identity_and_preserves_foreign_record(tmp_path):
    broker = _broker(tmp_path)
    path = broker.register_owner(
        command_kind="CANONICAL_GATEWAY_RUN",
        argv=["pythonw.exe", "-m", "hermes_cli.main", "gateway", "run"],
    )
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["owner"]["create_time"] = 999.0
    path.write_text(json.dumps(payload), encoding="utf-8")

    assert broker.close_owner() is False
    assert path.exists()
    assert broker.ready is False


def test_gateway_broker_factory_registers_before_return(tmp_path, monkeypatch):
    import cron.quiescence as q
    import gateway.run as gateway_run

    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    import psutil

    current_process = psutil.Process()

    def stable_provider(home, owner, **kwargs):
        row = {
            "pid": owner.pid,
            "ppid": 1,
            "create_time": owner.create_time,
            "username": "HQ\\user",
            "name": "pythonw.exe",
            "exe": str(tmp_path / "pythonw.exe"),
            "argv": ["pythonw.exe", "-m", "hermes_cli.main", "gateway", "run"],
            "cwd": str(tmp_path),
            "profile_home_hash": q.profile_home_sha256(tmp_path),
            "runtime_attested_command_kind": "CANONICAL_GATEWAY_RUN",
        }
        return (lambda: [dict(row)]), "HQ\\user"

    monkeypatch.setattr(q, "build_live_process_snapshot_provider", stable_provider)

    broker = gateway_run._create_gateway_cron_broker(completion_hook=None)

    assert broker.ready is False
    assert broker.owner_identity.pid == current_process.pid
    assert broker.owner_identity.create_time == current_process.create_time()
    assert broker.owner_registry_path.exists()
    assert (
        tmp_path / "cron" / "quiescence" / "process-census" / "latest.json"
    ).exists()
    broker.close_owner()


def test_gateway_broker_factory_hard_stops_on_unsafe_census(tmp_path, monkeypatch):
    import cron.quiescence as q
    import gateway.run as gateway_run

    monkeypatch.setenv("HERMES_HOME", str(tmp_path))

    def unsafe_provider(home, owner, **kwargs):
        base = {
            "ppid": 1,
            "username": "HQ\\user",
            "exe": str(tmp_path / "python.exe"),
            "cwd": str(tmp_path),
            "profile_home_hash": q.profile_home_sha256(tmp_path),
        }
        rows = [
            {
                **base,
                "pid": owner.pid,
                "create_time": owner.create_time,
                "name": "pythonw.exe",
                "argv": [
                    "pythonw.exe",
                    "-m",
                    "hermes_cli.main",
                    "gateway",
                    "run",
                ],
                "runtime_attested_command_kind": "CANONICAL_GATEWAY_RUN",
            },
            {
                **base,
                "pid": owner.pid + 100000,
                "create_time": owner.create_time + 1.0,
                "name": "python.exe",
                "argv": [
                    "python.exe",
                    "-m",
                    "hermes_cli.main",
                    "cron",
                    "tick",
                ],
            },
        ]
        return (lambda: [dict(row) for row in rows]), "HQ\\user"

    monkeypatch.setattr(q, "build_live_process_snapshot_provider", unsafe_provider)

    with pytest.raises(RuntimeError, match="census hard stop"):
        gateway_run._create_gateway_cron_broker(completion_hook=None)
    assert not list((tmp_path / "cron" / "quiescence" / "owners").glob("*.json"))
