import asyncio
from types import SimpleNamespace

from hermes_cli import web_server


def test_desktop_shutdown_file_requires_desktop_owned_absolute_path(monkeypatch, tmp_path):
    marker = tmp_path / "shutdown.json"
    monkeypatch.setenv("HERMES_DESKTOP_SHUTDOWN_FILE", str(marker))
    monkeypatch.delenv("HERMES_DESKTOP", raising=False)

    assert web_server._desktop_shutdown_file_from_env() is None

    monkeypatch.setenv("HERMES_DESKTOP", "1")
    assert web_server._desktop_shutdown_file_from_env() == marker

    monkeypatch.setenv("HERMES_DESKTOP_SHUTDOWN_FILE", "relative/shutdown.json")
    assert web_server._desktop_shutdown_file_from_env() is None


def test_desktop_shutdown_watcher_requests_uvicorn_exit_on_marker(tmp_path):
    marker = tmp_path / "shutdown.json"
    server = SimpleNamespace(should_exit=False)

    async def exercise():
        task = asyncio.create_task(
            web_server._watch_desktop_shutdown(server, marker, poll_interval=0.001)
        )
        await asyncio.sleep(0)
        marker.write_text('{"reason":"desktop-relaunch"}', encoding="utf-8")
        return await asyncio.wait_for(task, timeout=1)

    assert asyncio.run(exercise()) is True
    assert server.should_exit is True
    assert marker.exists(), "Electron owns marker cleanup after child exit"


def test_desktop_shutdown_watcher_stops_without_marker_when_server_is_already_exiting(tmp_path):
    marker = tmp_path / "missing.json"
    server = SimpleNamespace(should_exit=True)

    observed = asyncio.run(
        web_server._watch_desktop_shutdown(server, marker, poll_interval=0.001)
    )

    assert observed is False
    assert marker.exists() is False
