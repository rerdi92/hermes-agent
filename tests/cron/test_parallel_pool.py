"""Persistent pool units plus broker-only tick regression coverage."""

from __future__ import annotations

from unittest.mock import patch

import pytest


class TestPersistentPool:
    """_get_parallel_pool remains the broker worker submission pool."""

    def test_pool_is_reused(self):
        import cron.scheduler as sched

        sched._parallel_pool = None
        sched._parallel_pool_max_workers = None
        pool1 = sched._get_parallel_pool(4)
        pool2 = sched._get_parallel_pool(4)
        assert pool1 is pool2
        sched._shutdown_parallel_pool()

    def test_pool_is_recreated_on_worker_change(self):
        import cron.scheduler as sched

        sched._parallel_pool = None
        sched._parallel_pool_max_workers = None
        pool1 = sched._get_parallel_pool(2)
        pool2 = sched._get_parallel_pool(4)
        assert pool1 is not pool2
        sched._shutdown_parallel_pool()

    def test_shutdown_clears_pool(self):
        import cron.scheduler as sched

        sched._parallel_pool = None
        sched._parallel_pool_max_workers = None
        sched._get_parallel_pool(2)
        sched._shutdown_parallel_pool()
        assert sched._parallel_pool is None
        assert sched._parallel_pool_max_workers is None


def _due(job_id: str, index: int, *, workdir=None):
    from cron.jobs import DueJob

    job = {"id": job_id, "name": job_id, "workdir": workdir}
    return DueJob(
        job_id,
        str(index) * 64,
        f"slot-{index}",
        f"slot-{index}",
        job,
        {},
    )


def _dispatch(job_id: str, status="ACCEPTED"):
    from cron.quiescence import DispatchResult

    values = {
        "status": status,
        "job_id": job_id,
        "mode": "ticker",
        "request_id": f"req-{job_id}",
    }
    if status == "ACCEPTED":
        values.update(attempt_token=f"a-{job_id}", run_token=f"r-{job_id}")
    return DispatchResult(**values)


@pytest.mark.parametrize("sync", [True, False])
def test_tick_sync_flag_only_controls_caller_wait_not_local_execution(monkeypatch, tmp_path, sync):
    import cron.scheduler as sched
    from cron.jobs import DueScan

    due = tuple(_due(f"job-{index}", index) for index in range(1, 4))
    monkeypatch.setattr(
        "cron.jobs.scan_due_jobs_read_only",
        lambda now=None: DueScan("now", due),
    )
    monkeypatch.setattr(sched, "_get_hermes_home", lambda: tmp_path)
    requested = []
    monkeypatch.setattr(
        "cron.quiescence.request_broker_dispatch",
        lambda job_id, **kwargs: requested.append((job_id, kwargs))
        or _dispatch(job_id),
    )

    with patch("cron.scheduler.run_job") as run, patch(
        "cron.scheduler._get_parallel_pool"
    ) as parallel_pool, patch("cron.scheduler._get_sequential_pool") as sequential_pool:
        count = sched.tick(verbose=False, sync=sync)

    assert count == 3
    assert [item[0] for item in requested] == ["job-1", "job-2", "job-3"]
    assert all(
        kwargs == {"mode": "ticker", "profile_home": tmp_path}
        for _job_id, kwargs in requested
    )
    run.assert_not_called()
    parallel_pool.assert_not_called()
    sequential_pool.assert_not_called()


def test_tick_counts_only_accepted_broker_results(monkeypatch, tmp_path):
    import cron.scheduler as sched
    from cron.jobs import DueScan

    due = (_due("active", 1), _due("new", 2))
    monkeypatch.setattr(
        "cron.jobs.scan_due_jobs_read_only",
        lambda now=None: DueScan("now", due),
    )
    monkeypatch.setattr(sched, "_get_hermes_home", lambda: tmp_path)
    monkeypatch.setattr(
        "cron.quiescence.request_broker_dispatch",
        lambda job_id, **kwargs: _dispatch(
            job_id, "ALREADY_RUNNING" if job_id == "active" else "ACCEPTED"
        ),
    )

    assert sched.tick(verbose=False) == 1


def test_workdir_due_job_still_uses_broker_and_never_local_sequential_pool(
    monkeypatch, tmp_path
):
    import cron.scheduler as sched
    from cron.jobs import DueScan

    due = (_due("workdir", 1, workdir=str(tmp_path)),)
    monkeypatch.setattr(
        "cron.jobs.scan_due_jobs_read_only",
        lambda now=None: DueScan("now", due),
    )
    monkeypatch.setattr(sched, "_get_hermes_home", lambda: tmp_path)
    requested = []
    monkeypatch.setattr(
        "cron.quiescence.request_broker_dispatch",
        lambda job_id, **kwargs: requested.append((job_id, kwargs))
        or _dispatch(job_id),
    )

    with patch("cron.scheduler.run_job") as run, patch(
        "cron.scheduler._get_sequential_pool"
    ) as sequential_pool:
        assert sched.tick(verbose=False, sync=False) == 1

    assert requested[0][0] == "workdir"
    run.assert_not_called()
    sequential_pool.assert_not_called()
