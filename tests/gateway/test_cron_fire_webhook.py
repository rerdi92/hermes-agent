"""Tests for the broker-only Chronos fire endpoint on APIServerAdapter."""

from __future__ import annotations

import pytest
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

from gateway.config import PlatformConfig
from gateway.platforms.api_server import APIServerAdapter, cors_middleware


def _make_adapter() -> APIServerAdapter:
    return APIServerAdapter(PlatformConfig(enabled=True, extra={"key": "sk-secret"}))


def _create_app(adapter: APIServerAdapter) -> web.Application:
    app = web.Application(middlewares=[cors_middleware])
    app["api_server_adapter"] = adapter
    app.router.add_post("/api/cron/fire", adapter._handle_cron_fire)
    return app


@pytest.fixture
def adapter():
    return _make_adapter()


class _DispatchSpy:
    def __init__(self, status="ACCEPTED"):
        self.status = status
        self.fired = []

    def __call__(self, job_id, **kwargs):
        from cron.quiescence import DispatchResult

        self.fired.append((job_id, kwargs))
        values = {
            "status": self.status,
            "job_id": job_id,
            "mode": "provider",
            "request_id": "req",
        }
        if self.status == "ACCEPTED":
            values.update(attempt_token="a", run_token="r")
        return DispatchResult(**values)


def _patch_dispatch(monkeypatch, spy):
    import cron.quiescence as q

    monkeypatch.setattr(q, "request_broker_dispatch", spy)


@pytest.mark.asyncio
async def test_valid_token_accepts_and_fires(adapter, monkeypatch):
    spy = _DispatchSpy()
    _patch_dispatch(monkeypatch, spy)
    monkeypatch.setattr(
        "plugins.cron_providers.chronos.verify.get_fire_verifier",
        lambda: (lambda **kw: {"purpose": "cron_fire", "aud": "agent:x"}),
    )

    app = _create_app(adapter)
    async with TestClient(TestServer(app)) as cli:
        resp = await cli.post(
            "/api/cron/fire",
            headers={"Authorization": "Bearer good"},
            json={"job_id": "abc123"},
        )
        assert resp.status == 202
        data = await resp.json()
        assert data["job_id"] == "abc123"
        assert data["status"] == "ACCEPTED"
        assert data["schema"] == "hermes.cron.dispatch-result.v1"

    assert len(spy.fired) == 1
    assert spy.fired[0][0] == "abc123"
    assert spy.fired[0][1]["mode"] == "provider"
    assert "profile_home" in spy.fired[0][1]


@pytest.mark.asyncio
async def test_invalid_token_401_and_no_fire(adapter, monkeypatch):
    spy = _DispatchSpy()
    _patch_dispatch(monkeypatch, spy)
    monkeypatch.setattr(
        "plugins.cron_providers.chronos.verify.get_fire_verifier",
        lambda: (lambda **kw: None),
    )

    app = _create_app(adapter)
    async with TestClient(TestServer(app)) as cli:
        resp = await cli.post(
            "/api/cron/fire",
            headers={"Authorization": "Bearer forged"},
            json={"job_id": "abc123"},
        )
        assert resp.status == 401
    assert spy.fired == []


@pytest.mark.asyncio
async def test_missing_token_401(adapter, monkeypatch):
    spy = _DispatchSpy()
    _patch_dispatch(monkeypatch, spy)
    app = _create_app(adapter)
    async with TestClient(TestServer(app)) as cli:
        resp = await cli.post("/api/cron/fire", json={"job_id": "abc123"})
        assert resp.status == 401
    assert spy.fired == []


@pytest.mark.asyncio
async def test_missing_job_id_400(adapter, monkeypatch):
    spy = _DispatchSpy()
    _patch_dispatch(monkeypatch, spy)
    monkeypatch.setattr(
        "plugins.cron_providers.chronos.verify.get_fire_verifier",
        lambda: (lambda **kw: {"purpose": "cron_fire"}),
    )

    app = _create_app(adapter)
    async with TestClient(TestServer(app)) as cli:
        resp = await cli.post(
            "/api/cron/fire",
            headers={"Authorization": "Bearer good"},
            json={},
        )
        assert resp.status == 400
    assert spy.fired == []


@pytest.mark.asyncio
async def test_fire_does_not_require_api_server_key(adapter, monkeypatch):
    """The NAS fire JWT, not API_SERVER_KEY, is the endpoint auth boundary."""
    spy = _DispatchSpy()
    _patch_dispatch(monkeypatch, spy)
    monkeypatch.setattr(
        "plugins.cron_providers.chronos.verify.get_fire_verifier",
        lambda: (lambda **kw: {"purpose": "cron_fire"}),
    )

    app = _create_app(adapter)
    async with TestClient(TestServer(app)) as cli:
        resp = await cli.post(
            "/api/cron/fire",
            headers={"Authorization": "Bearer nas-jwt"},
            json={"job_id": "j9"},
        )
        assert resp.status == 202
        data = await resp.json()
        assert data["status"] == "ACCEPTED"
        assert data["schema"] == "hermes.cron.dispatch-result.v1"
    assert len(spy.fired) == 1
    assert spy.fired[0][0] == "j9"
