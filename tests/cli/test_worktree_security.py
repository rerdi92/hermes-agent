"""Security-focused integration tests for CLI worktree setup."""

import os
import subprocess
import time
from pathlib import Path

import pytest


@pytest.fixture
def git_repo(tmp_path):
    """Create a temporary git repo for testing real cli._setup_worktree behavior."""
    repo = tmp_path / "test-repo"
    repo.mkdir()
    subprocess.run(["git", "init"], cwd=repo, check=True, capture_output=True)
    subprocess.run(["git", "config", "user.email", "test@test.com"], cwd=repo, check=True, capture_output=True)
    subprocess.run(["git", "config", "user.name", "Test"], cwd=repo, check=True, capture_output=True)
    (repo / "README.md").write_text("# Test Repo\n")
    subprocess.run(["git", "add", "."], cwd=repo, check=True, capture_output=True)
    subprocess.run(["git", "commit", "-m", "Initial commit"], cwd=repo, check=True, capture_output=True)
    return repo


def _force_remove_worktree(info: dict | None) -> None:
    if not info:
        return
    subprocess.run(
        ["git", "worktree", "remove", info["path"], "--force"],
        cwd=info["repo_root"],
        capture_output=True,
        check=False,
    )
    subprocess.run(
        ["git", "branch", "-D", info["branch"]],
        cwd=info["repo_root"],
        capture_output=True,
        check=False,
    )


class TestWorktreeIncludeSecurity:
    def test_rejects_parent_directory_file_traversal(self, git_repo):
        import cli as cli_mod

        outside_file = git_repo.parent / "sensitive.txt"
        outside_file.write_text("SENSITIVE DATA")
        (git_repo / ".worktreeinclude").write_text("../sensitive.txt\n")

        info = None
        try:
            info = cli_mod._setup_worktree(str(git_repo))
            assert info is not None

            wt_path = Path(info["path"])
            assert not (wt_path.parent / "sensitive.txt").exists()
            assert not (wt_path / "../sensitive.txt").resolve().exists()
        finally:
            _force_remove_worktree(info)

    def test_rejects_parent_directory_directory_traversal(self, git_repo):
        import cli as cli_mod

        outside_dir = git_repo.parent / "outside-dir"
        outside_dir.mkdir()
        (outside_dir / "secret.txt").write_text("SENSITIVE DIR DATA")
        (git_repo / ".worktreeinclude").write_text("../outside-dir\n")

        info = None
        try:
            info = cli_mod._setup_worktree(str(git_repo))
            assert info is not None

            wt_path = Path(info["path"])
            escaped_dir = wt_path.parent / "outside-dir"
            assert not escaped_dir.exists()
            assert not escaped_dir.is_symlink()
        finally:
            _force_remove_worktree(info)

    def test_rejects_symlink_that_resolves_outside_repo(self, git_repo):
        import cli as cli_mod

        outside_file = git_repo.parent / "linked-secret.txt"
        outside_file.write_text("LINKED SECRET")
        (git_repo / "leak.txt").symlink_to(outside_file)
        (git_repo / ".worktreeinclude").write_text("leak.txt\n")

        info = None
        try:
            info = cli_mod._setup_worktree(str(git_repo))
            assert info is not None

            assert not (Path(info["path"]) / "leak.txt").exists()
        finally:
            _force_remove_worktree(info)

    def test_allows_valid_file_include(self, git_repo):
        import cli as cli_mod

        (git_repo / ".env").write_text("SECRET=***\n")
        (git_repo / ".worktreeinclude").write_text(".env\n")

        info = None
        try:
            info = cli_mod._setup_worktree(str(git_repo))
            assert info is not None

            copied = Path(info["path"]) / ".env"
            assert copied.exists()
            assert copied.read_text() == "SECRET=***\n"
        finally:
            _force_remove_worktree(info)

    def test_allows_valid_directory_include(self, git_repo):
        import cli as cli_mod

        assets_dir = git_repo / ".venv" / "lib"
        assets_dir.mkdir(parents=True)
        (assets_dir / "marker.txt").write_text("venv marker")
        (git_repo / ".worktreeinclude").write_text(".venv\n")

        info = None
        try:
            info = cli_mod._setup_worktree(str(git_repo))
            assert info is not None

            linked_dir = Path(info["path"]) / ".venv"
            assert linked_dir.is_symlink()
            assert (linked_dir / "lib" / "marker.txt").read_text() == "venv marker"
        finally:
            _force_remove_worktree(info)


class TestWorktreeCleanupSafety:
    """Exercise the production cleanup helper against real git worktrees."""

    def test_preserves_untracked_worktree(self, git_repo):
        import cli as cli_mod

        info = cli_mod._setup_worktree(str(git_repo), sync_base=False)
        assert info is not None
        try:
            marker = Path(info["path"]) / "untracked-work.txt"
            marker.write_text("recoverable work\n")

            cli_mod._cleanup_worktree(info)

            assert Path(info["path"]).exists()
            assert marker.read_text() == "recoverable work\n"
        finally:
            _force_remove_worktree(info)

    def test_preserves_staged_worktree(self, git_repo):
        import cli as cli_mod

        info = cli_mod._setup_worktree(str(git_repo), sync_base=False)
        assert info is not None
        try:
            marker = Path(info["path"]) / "staged-work.txt"
            marker.write_text("staged work\n")
            subprocess.run(
                ["git", "add", marker.name],
                cwd=info["path"],
                capture_output=True,
                check=True,
            )

            cli_mod._cleanup_worktree(info)

            assert Path(info["path"]).exists()
            assert marker.exists()
        finally:
            _force_remove_worktree(info)

    def test_removes_clean_worktree(self, git_repo):
        import cli as cli_mod

        info = cli_mod._setup_worktree(str(git_repo), sync_base=False)
        assert info is not None

        cli_mod._cleanup_worktree(info)

        assert not Path(info["path"]).exists()

    def test_dirty_probe_failure_is_fail_closed(self, git_repo, monkeypatch):
        import cli as cli_mod

        info = cli_mod._setup_worktree(str(git_repo), sync_base=False)
        assert info is not None
        try:
            monkeypatch.setattr(cli_mod, "_worktree_is_dirty", lambda *_args, **_kwargs: True)

            cli_mod._cleanup_worktree(info)

            assert Path(info["path"]).exists()
        finally:
            _force_remove_worktree(info)

    @pytest.mark.parametrize("move_returncode", [0, 1])
    def test_move_failure_preserves_worktree_and_branch(
        self,
        git_repo,
        monkeypatch,
        capsys,
        move_returncode,
    ):
        import cli as cli_mod

        info = cli_mod._setup_worktree(str(git_repo), sync_base=False)
        assert info is not None
        real_run = subprocess.run
        calls = []

        def fake_move(args, **kwargs):
            calls.append(args)
            if args[1:3] == ["worktree", "move"]:
                return subprocess.CompletedProcess(
                    args,
                    move_returncode,
                    "",
                    "simulated move result",
                )
            return real_run(args, **kwargs)

        monkeypatch.setattr(subprocess, "run", fake_move)
        try:
            cli_mod._cleanup_worktree(info)

            output = capsys.readouterr().out
            assert Path(info["path"]).exists()
            assert real_run(
                ["git", "show-ref", "--verify", f"refs/heads/{info['branch']}"],
                cwd=git_repo,
                capture_output=True,
            ).returncode == 0
            assert "Worktree archived for recovery" not in output
            assert not any(args[1:3] == ["worktree", "remove"] for args in calls)
            assert not any(args[1:3] == ["update-ref", "-d"] for args in calls)
            assert not any(args[1:4] == ["branch", "-d", "--"] for args in calls)
        finally:
            real_run(
                ["git", "worktree", "remove", info["path"], "--force"],
                cwd=git_repo,
                capture_output=True,
            )
            real_run(
                ["git", "branch", "-D", info["branch"]],
                cwd=git_repo,
                capture_output=True,
            )

    def test_move_nonzero_with_archive_present_reports_recovery_path(
        self,
        git_repo,
        monkeypatch,
        capsys,
    ):
        import cli as cli_mod

        info = cli_mod._setup_worktree(str(git_repo), sync_base=False)
        assert info is not None
        real_run = subprocess.run
        archive_path = git_repo / ".worktrees" / ".archive" / Path(info["path"]).name

        def move_then_report_failure(args, **kwargs):
            if args[1:3] == ["worktree", "move"]:
                actual = real_run(args, **kwargs)
                assert actual.returncode == 0
                return subprocess.CompletedProcess(
                    args,
                    1,
                    actual.stdout,
                    "simulated nonzero after archive move",
                )
            return real_run(args, **kwargs)

        monkeypatch.setattr(subprocess, "run", move_then_report_failure)
        try:
            cli_mod._cleanup_worktree(info)
            output = capsys.readouterr().out

            assert not Path(info["path"]).exists()
            assert archive_path.exists()
            assert f"Worktree archived for recovery: {info['path']} -> {archive_path}" in output
            assert info["branch"] in output
            assert real_run(
                ["git", "show-ref", "--verify", f"refs/heads/{info['branch']}"],
                cwd=git_repo,
                capture_output=True,
            ).returncode == 0
        finally:
            real_run(
                ["git", "worktree", "remove", str(archive_path), "--force"],
                cwd=git_repo,
                capture_output=True,
            )
            real_run(
                ["git", "branch", "-D", info["branch"]],
                cwd=git_repo,
                capture_output=True,
            )

    def test_preserves_unstaged_tracked_worktree(self, git_repo):
        import cli as cli_mod

        info = cli_mod._setup_worktree(str(git_repo), sync_base=False)
        assert info is not None
        try:
            readme = Path(info["path"]) / "README.md"
            readme.write_text("unstaged recoverable work\n")

            cli_mod._cleanup_worktree(info)

            assert Path(info["path"]).exists()
            assert readme.read_text() == "unstaged recoverable work\n"
        finally:
            _force_remove_worktree(info)

    def test_dirty_probe_fails_closed_on_real_git_error(self, tmp_path):
        import cli as cli_mod

        non_repo = tmp_path / "not-a-repository"
        non_repo.mkdir()

        assert cli_mod._worktree_is_dirty(str(non_repo), timeout=5) is True

    def test_preserves_no_remote_unique_commit(self, git_repo):
        import cli as cli_mod

        info = cli_mod._setup_worktree(str(git_repo), sync_base=False)
        assert info is not None
        try:
            marker = Path(info["path"]) / "unique-commit.txt"
            marker.write_text("only reachable from worktree branch\n")
            subprocess.run(["git", "add", marker.name], cwd=info["path"], check=True)
            subprocess.run(
                ["git", "commit", "-m", "unique worktree commit"],
                cwd=info["path"],
                check=True,
                capture_output=True,
            )

            cli_mod._cleanup_worktree(info)

            assert Path(info["path"]).exists()
            branches = subprocess.run(
                ["git", "branch", "--list", info["branch"]],
                cwd=git_repo,
                capture_output=True,
                text=True,
                check=True,
            )
            assert info["branch"] in branches.stdout
        finally:
            _force_remove_worktree(info)

    def test_preserves_48h_dirty_worktree_on_startup_prune(self, git_repo):
        import cli as cli_mod

        info = cli_mod._setup_worktree(str(git_repo), sync_base=False)
        assert info is not None
        try:
            marker = Path(info["path"]) / "untracked-at-startup.txt"
            marker.write_text("must survive startup prune\n")
            subprocess.run(
                ["git", "worktree", "unlock", info["path"]],
                cwd=git_repo,
                capture_output=True,
                check=True,
            )
            assert cli_mod._worktree_lock_is_live(
                str(git_repo),
                info["path"],
            ) is None
            old_time = time.time() - (48 * 3600)
            os.utime(info["path"], (old_time, old_time))

            cli_mod._prune_stale_worktrees(str(git_repo), max_age_hours=24)

            assert Path(info["path"]).exists()
            assert marker.exists()
        finally:
            _force_remove_worktree(info)

    def test_preserves_unique_orphan_branch_without_remote(self, git_repo):
        import cli as cli_mod

        original_branch = subprocess.run(
            ["git", "branch", "--show-current"],
            cwd=git_repo,
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()
        orphan_branch = "hermes/hermes-unique-orphan"
        subprocess.run(["git", "checkout", "-b", orphan_branch], cwd=git_repo, check=True)
        (git_repo / "orphan-work.txt").write_text("unique orphan commit\n")
        subprocess.run(["git", "add", "orphan-work.txt"], cwd=git_repo, check=True)
        subprocess.run(
            ["git", "commit", "-m", "unique orphan work"],
            cwd=git_repo,
            check=True,
            capture_output=True,
        )
        subprocess.run(["git", "checkout", original_branch], cwd=git_repo, check=True)

        cli_mod._prune_orphaned_branches(str(git_repo))

        branches = subprocess.run(
            ["git", "branch", "--list", orphan_branch],
            cwd=git_repo,
            capture_output=True,
            text=True,
            check=True,
        )
        assert orphan_branch in branches.stdout

    def test_prunes_redundant_orphan_branch_without_remote(self, git_repo):
        import cli as cli_mod

        orphan_branch = "hermes/hermes-redundant-orphan"
        subprocess.run(["git", "branch", orphan_branch, "HEAD"], cwd=git_repo, check=True)

        cli_mod._prune_orphaned_branches(str(git_repo))

        branches = subprocess.run(
            ["git", "branch", "--list", orphan_branch],
            cwd=git_repo,
            capture_output=True,
            text=True,
            check=True,
        )
        assert orphan_branch not in branches.stdout

    def test_preserves_shared_unique_orphan_branch_set(self, git_repo):
        import cli as cli_mod

        original_branch = subprocess.run(
            ["git", "branch", "--show-current"],
            cwd=git_repo,
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()
        first = "hermes/hermes-shared-unique-a"
        second = "hermes/hermes-shared-unique-b"
        subprocess.run(["git", "checkout", "-b", first], cwd=git_repo, check=True)
        (git_repo / "shared-orphan-work.txt").write_text("shared unique commit\n")
        subprocess.run(["git", "add", "shared-orphan-work.txt"], cwd=git_repo, check=True)
        subprocess.run(
            ["git", "commit", "-m", "shared unique orphan work"],
            cwd=git_repo,
            check=True,
            capture_output=True,
        )
        subprocess.run(["git", "branch", second, "HEAD"], cwd=git_repo, check=True)
        subprocess.run(["git", "checkout", original_branch], cwd=git_repo, check=True)

        cli_mod._prune_orphaned_branches(str(git_repo))

        branches = subprocess.run(
            ["git", "branch", "--list", "hermes/hermes-shared-unique-*"],
            cwd=git_repo,
            capture_output=True,
            text=True,
            check=True,
        ).stdout
        assert first in branches
        assert second in branches

    def test_symbolic_ref_hard_failure_preserves_exit_cleanup(self, git_repo, monkeypatch):
        import cli as cli_mod

        info = cli_mod._setup_worktree(str(git_repo), sync_base=False)
        assert info is not None
        marker = Path(info["path"]) / "symbolic-ref-failure.txt"
        marker.write_text("unique commit\n")
        subprocess.run(["git", "add", marker.name], cwd=info["path"], check=True)
        subprocess.run(
            ["git", "commit", "-m", "unique before symbolic-ref failure"],
            cwd=info["path"],
            check=True,
            capture_output=True,
        )
        real_run = subprocess.run

        def fail_symbolic_ref(args, **kwargs):
            if args[1:] == ["symbolic-ref", "--quiet", "HEAD"]:
                return subprocess.CompletedProcess(args, 128, "", "probe failed")
            return real_run(args, **kwargs)

        monkeypatch.setattr(subprocess, "run", fail_symbolic_ref)
        try:
            cli_mod._cleanup_worktree(info)

            assert Path(info["path"]).exists()
            branch = real_run(
                ["git", "show-ref", "--verify", f"refs/heads/{info['branch']}"],
                cwd=git_repo,
                capture_output=True,
            )
            assert branch.returncode == 0
        finally:
            real_run(
                ["git", "worktree", "remove", info["path"], "--force"],
                cwd=git_repo,
                capture_output=True,
            )
            real_run(
                ["git", "branch", "-D", info["branch"]],
                cwd=git_repo,
                capture_output=True,
            )

    def test_symbolic_ref_hard_failure_preserves_startup_prune(self, git_repo, monkeypatch):
        import cli as cli_mod

        info = cli_mod._setup_worktree(str(git_repo), sync_base=False)
        assert info is not None
        marker = Path(info["path"]) / "startup-symbolic-ref-failure.txt"
        marker.write_text("unique commit\n")
        subprocess.run(["git", "add", marker.name], cwd=info["path"], check=True)
        subprocess.run(
            ["git", "commit", "-m", "unique before startup symbolic-ref failure"],
            cwd=info["path"],
            check=True,
            capture_output=True,
        )
        subprocess.run(
            ["git", "worktree", "unlock", info["path"]],
            cwd=git_repo,
            capture_output=True,
        )
        old_time = time.time() - (48 * 3600)
        os.utime(info["path"], (old_time, old_time))
        real_run = subprocess.run

        def fail_symbolic_ref(args, **kwargs):
            if args[1:] == ["symbolic-ref", "--quiet", "HEAD"]:
                return subprocess.CompletedProcess(args, 128, "", "probe failed")
            return real_run(args, **kwargs)

        monkeypatch.setattr(subprocess, "run", fail_symbolic_ref)
        try:
            cli_mod._prune_stale_worktrees(str(git_repo), max_age_hours=24)

            assert Path(info["path"]).exists()
            branch = real_run(
                ["git", "show-ref", "--verify", f"refs/heads/{info['branch']}"],
                cwd=git_repo,
                capture_output=True,
            )
            assert branch.returncode == 0
        finally:
            real_run(
                ["git", "worktree", "remove", info["path"], "--force"],
                cwd=git_repo,
                capture_output=True,
            )
            real_run(
                ["git", "branch", "-D", info["branch"]],
                cwd=git_repo,
                capture_output=True,
            )

    def test_orphan_prune_fails_closed_when_worktree_census_fails(
        self,
        tmp_path,
        monkeypatch,
    ):
        import cli as cli_mod

        calls = []

        def fake_run(args, **_kwargs):
            calls.append(args)
            if args[1:3] == ["branch", "--format=%(refname:short)"]:
                return subprocess.CompletedProcess(args, 0, "hermes/hermes-redundant\n", "")
            if args[1:4] == ["worktree", "list", "--porcelain"]:
                return subprocess.CompletedProcess(args, 128, "", "census failed")
            return subprocess.CompletedProcess(args, 0, "main\n", "")

        monkeypatch.setattr(subprocess, "run", fake_run)

        cli_mod._prune_orphaned_branches(str(tmp_path))

        assert not any(args[1:3] == ["branch", "-D"] for args in calls)

    def test_symbolic_ref_invalid_stdout_is_fail_closed(self, git_repo, monkeypatch):
        import cli as cli_mod

        info = cli_mod._setup_worktree(str(git_repo), sync_base=False)
        assert info is not None
        marker = Path(info["path"]) / "invalid-symbolic-ref.txt"
        marker.write_text("unique commit\n")
        subprocess.run(["git", "add", marker.name], cwd=info["path"], check=True)
        subprocess.run(
            ["git", "commit", "-m", "unique before malformed symbolic ref"],
            cwd=info["path"],
            capture_output=True,
            check=True,
        )
        real_run = subprocess.run

        def malformed_symbolic_ref(args, **kwargs):
            if args[1:] == ["symbolic-ref", "--quiet", "HEAD"]:
                return subprocess.CompletedProcess(
                    args,
                    0,
                    "refs/heads/worker\nrefs/heads/injected\n",
                    "",
                )
            return real_run(args, **kwargs)

        monkeypatch.setattr(subprocess, "run", malformed_symbolic_ref)
        try:
            assert cli_mod._worktree_has_unpushed_commits(info["path"]) is True
        finally:
            real_run(
                ["git", "worktree", "remove", info["path"], "--force"],
                cwd=git_repo,
                capture_output=True,
            )
            real_run(
                ["git", "branch", "-D", info["branch"]],
                cwd=git_repo,
                capture_output=True,
            )

    def test_orphan_prune_rejects_empty_successful_census(self, tmp_path, monkeypatch):
        import cli as cli_mod

        calls = []

        def fake_run(args, **_kwargs):
            calls.append(args)
            if args[1:3] == ["branch", "--format=%(refname:short)"]:
                return subprocess.CompletedProcess(args, 0, "hermes/hermes-redundant\n", "")
            if args[1:4] == ["worktree", "list", "--porcelain"]:
                return subprocess.CompletedProcess(args, 0, "", "")
            return subprocess.CompletedProcess(args, 0, "main\n", "")

        monkeypatch.setattr(subprocess, "run", fake_run)

        cli_mod._prune_orphaned_branches(str(tmp_path))

        assert not any(args[1:3] == ["branch", "-D"] for args in calls)

    def test_preserves_ignored_file_with_no_other_changes(self, git_repo):
        import cli as cli_mod

        gitignore = git_repo / ".gitignore"
        existing = gitignore.read_text() if gitignore.exists() else ""
        gitignore.write_text(existing + "\nignored/\n")
        subprocess.run(["git", "add", ".gitignore"], cwd=git_repo, check=True)
        subprocess.run(
            ["git", "commit", "-m", "ignore generated-looking files"],
            cwd=git_repo,
            capture_output=True,
            check=True,
        )
        info = cli_mod._setup_worktree(str(git_repo), sync_base=False)
        assert info is not None
        try:
            valuable = Path(info["path"]) / "ignored" / "valuable.txt"
            valuable.parent.mkdir()
            valuable.write_text("only copy\n")

            cli_mod._cleanup_worktree(info)

            assert Path(info["path"]).exists()
            assert valuable.read_text() == "only copy\n"
        finally:
            _force_remove_worktree(info)

    def test_exit_cleanup_preserves_commit_created_after_initial_checks(
        self,
        git_repo,
        monkeypatch,
    ):
        import cli as cli_mod

        info = cli_mod._setup_worktree(str(git_repo), sync_base=False)
        assert info is not None
        real_run = subprocess.run
        injected = False

        def inject_after_unlock(args, **kwargs):
            nonlocal injected
            result = real_run(args, **kwargs)
            if (
                not injected
                and args[1:3] == ["worktree", "unlock"]
                and str(args[-1]) == info["path"]
            ):
                injected = True
                marker = Path(info["path"]) / "late-exit-commit.txt"
                marker.write_text("late commit\n")
                real_run(["git", "add", marker.name], cwd=info["path"], check=True)
                real_run(
                    ["git", "commit", "-m", "late exit commit"],
                    cwd=info["path"],
                    capture_output=True,
                    check=True,
                )
            return result

        monkeypatch.setattr(subprocess, "run", inject_after_unlock)
        try:
            cli_mod._cleanup_worktree(info)

            assert injected
            assert Path(info["path"]).exists()
            assert real_run(
                ["git", "show-ref", "--verify", f"refs/heads/{info['branch']}"],
                cwd=git_repo,
                capture_output=True,
            ).returncode == 0
        finally:
            _force_remove_worktree(info)

    def test_startup_prune_preserves_commit_created_after_initial_checks(
        self,
        git_repo,
        monkeypatch,
    ):
        import cli as cli_mod

        info = cli_mod._setup_worktree(str(git_repo), sync_base=False)
        assert info is not None
        subprocess.run(
            ["git", "worktree", "unlock", info["path"]],
            cwd=git_repo,
            capture_output=True,
            check=True,
        )
        old_time = time.time() - (48 * 3600)
        os.utime(info["path"], (old_time, old_time))
        real_run = subprocess.run
        injected = False

        def inject_after_branch_query(args, **kwargs):
            nonlocal injected
            result = real_run(args, **kwargs)
            if (
                not injected
                and args[1:3] == ["branch", "--show-current"]
                and kwargs.get("cwd") == info["path"]
            ):
                injected = True
                marker = Path(info["path"]) / "late-startup-commit.txt"
                marker.write_text("late commit\n")
                real_run(["git", "add", marker.name], cwd=info["path"], check=True)
                real_run(
                    ["git", "commit", "-m", "late startup commit"],
                    cwd=info["path"],
                    capture_output=True,
                    check=True,
                )
            return result

        monkeypatch.setattr(subprocess, "run", inject_after_branch_query)
        try:
            cli_mod._prune_stale_worktrees(str(git_repo), max_age_hours=24)

            assert injected
            assert Path(info["path"]).exists()
            assert real_run(
                ["git", "show-ref", "--verify", f"refs/heads/{info['branch']}"],
                cwd=git_repo,
                capture_output=True,
            ).returncode == 0
        finally:
            _force_remove_worktree(info)

    def test_orphan_archive_rename_preserves_concurrent_ref_advance(
        self,
        git_repo,
        monkeypatch,
    ):
        import cli as cli_mod

        branch = "hermes/hermes-racing-orphan"
        ref = f"refs/heads/{branch}"
        subprocess.run(["git", "branch", branch, "HEAD"], cwd=git_repo, check=True)
        real_run = subprocess.run
        comparison_branch = real_run(
            ["git", "branch", "--show-current"],
            cwd=git_repo,
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()
        comparison_ref = f"refs/heads/{comparison_branch}"
        late_oid = None

        def advance_before_rename(args, **kwargs):
            nonlocal late_oid
            is_archive_rename = args[1:3] == ["branch", "-m"] and args[3] == branch
            if late_oid is None and is_archive_rename:
                parent = real_run(
                    ["git", "rev-parse", ref],
                    cwd=git_repo,
                    capture_output=True,
                    text=True,
                    check=True,
                ).stdout.strip()
                tree = real_run(
                    ["git", "rev-parse", f"{ref}^{{tree}}"],
                    cwd=git_repo,
                    capture_output=True,
                    text=True,
                    check=True,
                ).stdout.strip()
                late_oid = real_run(
                    ["git", "commit-tree", tree, "-p", parent, "-m", "late orphan commit"],
                    cwd=git_repo,
                    capture_output=True,
                    text=True,
                    check=True,
                ).stdout.strip()
                real_run(
                    ["git", "update-ref", ref, late_oid, parent],
                    cwd=git_repo,
                    check=True,
                )
                real_run(
                    ["git", "update-ref", comparison_ref, late_oid, parent],
                    cwd=git_repo,
                    check=True,
                )
                result = real_run(args, **kwargs)
                real_run(
                    ["git", "update-ref", comparison_ref, parent, late_oid],
                    cwd=git_repo,
                    check=True,
                )
                return result
            return real_run(args, **kwargs)

        monkeypatch.setattr(subprocess, "run", advance_before_rename)

        cli_mod._prune_orphaned_branches(str(git_repo))

        assert late_oid
        assert real_run(
            ["git", "show-ref", "--verify", ref],
            cwd=git_repo,
            capture_output=True,
        ).returncode != 0
        archived = real_run(
            [
                "git",
                "for-each-ref",
                "--format=%(objectname) %(refname)",
                "refs/heads/hermes/archive",
            ],
            cwd=git_repo,
            capture_output=True,
            text=True,
            check=True,
        ).stdout.splitlines()
        assert any(
            line.startswith(f"{late_oid} refs/heads/hermes/archive/")
            for line in archived
        )
        containing_refs = real_run(
            ["git", "for-each-ref", "--contains", late_oid, "--format=%(refname)"],
            cwd=git_repo,
            capture_output=True,
            text=True,
            check=True,
        ).stdout.splitlines()
        assert any(ref.startswith("refs/heads/hermes/archive/") for ref in containing_refs)
        assert comparison_ref not in containing_refs

    def test_quarantine_move_preserves_last_moment_untracked_write(
        self,
        git_repo,
        monkeypatch,
    ):
        import cli as cli_mod

        info = cli_mod._setup_worktree(str(git_repo), sync_base=False)
        assert info is not None
        real_run = subprocess.run
        marker = Path(info["path"]) / "last-moment-untracked.txt"
        archive_path = git_repo / ".worktrees" / ".archive" / Path(info["path"]).name
        injected = False

        def inject_before_move(args, **kwargs):
            nonlocal injected
            if not injected and args[1:3] == ["worktree", "move"]:
                injected = True
                marker.write_text("must survive quarantine move\n")
            return real_run(args, **kwargs)

        monkeypatch.setattr(subprocess, "run", inject_before_move)
        try:
            cli_mod._cleanup_worktree(info)

            assert injected
            assert not Path(info["path"]).exists()
            assert archive_path.exists()
            assert (archive_path / marker.name).read_text() == "must survive quarantine move\n"
            assert real_run(
                ["git", "show-ref", "--verify", f"refs/heads/{info['branch']}"],
                cwd=git_repo,
                capture_output=True,
            ).returncode == 0
        finally:
            real_run(
                ["git", "worktree", "remove", str(archive_path), "--force"],
                cwd=git_repo,
                capture_output=True,
            )
            real_run(
                ["git", "branch", "-D", info["branch"]],
                cwd=git_repo,
                capture_output=True,
            )

    def test_clean_cleanup_archives_worktree_branch_and_expected_oid(self, git_repo):
        import cli as cli_mod

        info = cli_mod._setup_worktree(str(git_repo), sync_base=False)
        assert info is not None
        archive_path = git_repo / ".worktrees" / ".archive" / Path(info["path"]).name
        expected_oid = subprocess.run(
            ["git", "rev-parse", f"refs/heads/{info['branch']}"],
            cwd=git_repo,
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()

        cli_mod._cleanup_worktree(info)

        assert not Path(info["path"]).exists()
        assert archive_path.exists()
        assert subprocess.run(
            ["git", "show-ref", "--verify", f"refs/heads/{info['branch']}"],
            cwd=git_repo,
            capture_output=True,
        ).returncode == 0
        archive_refs = subprocess.run(
            ["git", "for-each-ref", "--format=%(objectname)", "refs/hermes/archive"],
            cwd=git_repo,
            capture_output=True,
            text=True,
            check=True,
        ).stdout.splitlines()
        assert expected_oid in archive_refs

    def test_exit_quarantine_preserves_merged_tip_after_comparison_retreat(
        self,
        git_repo,
        monkeypatch,
    ):
        import cli as cli_mod

        info = cli_mod._setup_worktree(str(git_repo), sync_base=False)
        assert info is not None
        branch = info["branch"]
        ref = f"refs/heads/{branch}"
        archive_path = git_repo / ".worktrees" / ".archive" / Path(info["path"]).name
        real_run = subprocess.run
        comparison_branch = real_run(
            ["git", "branch", "--show-current"],
            cwd=git_repo,
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()
        comparison_ref = f"refs/heads/{comparison_branch}"
        late_oid = None

        def advance_before_move(args, **kwargs):
            nonlocal late_oid
            if late_oid is None and args[1:3] == ["worktree", "move"]:
                parent = real_run(
                    ["git", "rev-parse", ref],
                    cwd=git_repo,
                    capture_output=True,
                    text=True,
                    check=True,
                ).stdout.strip()
                tree = real_run(
                    ["git", "rev-parse", f"{parent}^{{tree}}"],
                    cwd=git_repo,
                    capture_output=True,
                    text=True,
                    check=True,
                ).stdout.strip()
                late_oid = real_run(
                    ["git", "commit-tree", tree, "-p", parent, "-m", "late branch advance"],
                    cwd=git_repo,
                    capture_output=True,
                    text=True,
                    check=True,
                ).stdout.strip()
                real_run(
                    ["git", "update-ref", ref, late_oid, parent],
                    cwd=git_repo,
                    capture_output=True,
                    check=True,
                )
                real_run(
                    ["git", "update-ref", comparison_ref, late_oid, parent],
                    cwd=git_repo,
                    capture_output=True,
                    check=True,
                )
                result = real_run(args, **kwargs)
                real_run(
                    ["git", "update-ref", comparison_ref, parent, late_oid],
                    cwd=git_repo,
                    capture_output=True,
                    check=True,
                )
                return result
            return real_run(args, **kwargs)

        monkeypatch.setattr(subprocess, "run", advance_before_move)
        try:
            cli_mod._cleanup_worktree(info)

            assert late_oid is not None
            assert not Path(info["path"]).exists()
            assert archive_path.exists()
            assert real_run(
                ["git", "rev-parse", ref],
                cwd=git_repo,
                capture_output=True,
                text=True,
                check=True,
            ).stdout.strip() == late_oid
            containing_refs = real_run(
                ["git", "for-each-ref", "--contains", late_oid, "--format=%(refname)"],
                cwd=git_repo,
                capture_output=True,
                text=True,
                check=True,
            ).stdout.splitlines()
            assert ref in containing_refs
            assert comparison_ref not in containing_refs
        finally:
            real_run(
                ["git", "worktree", "remove", str(archive_path), "--force"],
                cwd=git_repo,
                capture_output=True,
            )
            real_run(["git", "branch", "-D", branch], cwd=git_repo, capture_output=True)

    def test_exit_quarantine_keeps_registered_branch_without_delete(
        self,
        git_repo,
        monkeypatch,
    ):
        import cli as cli_mod

        info = cli_mod._setup_worktree(str(git_repo), sync_base=False)
        assert info is not None
        branch = info["branch"]
        ref = f"refs/heads/{branch}"
        archive_path = git_repo / ".worktrees" / ".archive" / Path(info["path"]).name
        real_run = subprocess.run
        calls = []

        def record_calls(args, **kwargs):
            calls.append(args)
            return real_run(args, **kwargs)

        monkeypatch.setattr(subprocess, "run", record_calls)
        try:
            cli_mod._cleanup_worktree(info)

            assert not Path(info["path"]).exists()
            assert archive_path.exists()
            assert real_run(
                ["git", "show-ref", "--verify", ref],
                cwd=git_repo,
                capture_output=True,
            ).returncode == 0
            worktree_list = real_run(
                ["git", "worktree", "list", "--porcelain"],
                cwd=git_repo,
                capture_output=True,
                text=True,
                check=True,
            ).stdout
            assert f"worktree {archive_path.as_posix()}" in worktree_list
            assert f"branch {ref}" in worktree_list
            assert not any(args[1:3] == ["update-ref", "-d"] for args in calls)
            assert not any(args[1:4] == ["branch", "-d", "--"] for args in calls)
            assert not any(args[1:3] == ["branch", "-D"] for args in calls)
        finally:
            real_run(
                ["git", "worktree", "remove", str(archive_path), "--force"],
                cwd=git_repo,
                capture_output=True,
            )
            real_run(["git", "branch", "-D", branch], cwd=git_repo, capture_output=True)

    def test_orphan_archive_preserves_late_checked_out_head(
        self,
        git_repo,
        tmp_path,
        monkeypatch,
    ):
        import cli as cli_mod

        branch = "hermes/hermes-late-checkout"
        late_worktree = tmp_path / "late-checkout-worktree"
        subprocess.run(["git", "branch", branch, "HEAD"], cwd=git_repo, check=True)
        real_run = subprocess.run
        attached = False

        def attach_before_rename(args, **kwargs):
            nonlocal attached
            is_archive_rename = args[1:3] == ["branch", "-m"] and args[3] == branch
            if not attached and is_archive_rename:
                attached = True
                real_run(
                    ["git", "worktree", "add", str(late_worktree), branch],
                    cwd=git_repo,
                    capture_output=True,
                    check=True,
                )
            return real_run(args, **kwargs)

        monkeypatch.setattr(subprocess, "run", attach_before_rename)
        final_branch = branch
        try:
            cli_mod._prune_orphaned_branches(str(git_repo))

            assert attached
            assert late_worktree.exists()
            symbolic_ref = real_run(
                ["git", "symbolic-ref", "--quiet", "HEAD"],
                cwd=late_worktree,
                capture_output=True,
                text=True,
                check=True,
            ).stdout.strip()
            assert symbolic_ref.startswith("refs/heads/")
            final_branch = symbolic_ref.removeprefix("refs/heads/")
            assert final_branch == branch or final_branch.startswith("hermes/archive/")
            assert real_run(
                ["git", "show-ref", "--verify", symbolic_ref],
                cwd=git_repo,
                capture_output=True,
            ).returncode == 0
        finally:
            real_run(
                ["git", "worktree", "remove", str(late_worktree), "--force"],
                cwd=git_repo,
                capture_output=True,
            )
            real_run(
                ["git", "branch", "-D", final_branch],
                cwd=git_repo,
                capture_output=True,
            )

    def test_archive_update_ref_failure_preserves_clean_worktree(
        self,
        git_repo,
        monkeypatch,
    ):
        import cli as cli_mod

        info = cli_mod._setup_worktree(str(git_repo), sync_base=False)
        assert info is not None
        real_run = subprocess.run

        def fail_archive_update_ref(args, **kwargs):
            if (
                len(args) > 2
                and args[1] == "update-ref"
                and str(args[2]).startswith("refs/hermes/archive/")
            ):
                return subprocess.CompletedProcess(
                    args,
                    1,
                    stdout="",
                    stderr="injected archive update-ref failure",
                )
            return real_run(args, **kwargs)

        monkeypatch.setattr(subprocess, "run", fail_archive_update_ref)
        try:
            cli_mod._cleanup_worktree(info)

            assert Path(info["path"]).exists()
            assert real_run(
                ["git", "show-ref", "--verify", f"refs/heads/{info['branch']}"],
                cwd=git_repo,
                capture_output=True,
            ).returncode == 0
        finally:
            _force_remove_worktree(info)

    def test_archive_destination_race_is_not_reported_as_archived(
        self,
        git_repo,
        monkeypatch,
    ):
        import cli as cli_mod

        info = cli_mod._setup_worktree(str(git_repo), sync_base=False)
        assert info is not None
        archive_path = git_repo / ".worktrees" / ".archive" / Path(info["path"]).name
        real_run = subprocess.run

        def create_competing_destination(args, **kwargs):
            if args[1:3] == ["worktree", "move"]:
                archive_path.mkdir(parents=True, exist_ok=False)
                return subprocess.CompletedProcess(
                    args,
                    1,
                    stdout="",
                    stderr="injected destination race",
                )
            return real_run(args, **kwargs)

        monkeypatch.setattr(subprocess, "run", create_competing_destination)
        try:
            outcome = cli_mod._archive_worktree_if_unchanged(
                str(git_repo),
                info["path"],
                expected_branch=info["branch"],
            )

            assert outcome["status"] != "archived_branch_preserved"
            assert outcome["status"] == "preserved"
            assert Path(info["path"]).exists()
            assert archive_path.exists()
            registered = real_run(
                ["git", "worktree", "list", "--porcelain"],
                cwd=git_repo,
                capture_output=True,
                text=True,
                check=True,
            ).stdout
            assert f"worktree {archive_path}" not in registered
        finally:
            if archive_path.exists():
                archive_path.rmdir()
            _force_remove_worktree(info)

    def test_unregistered_archive_destination_is_not_reported_as_archived(
        self,
        git_repo,
        monkeypatch,
    ):
        import cli as cli_mod

        info = cli_mod._setup_worktree(str(git_repo), sync_base=False)
        assert info is not None
        source_path = Path(info["path"])
        archive_path = git_repo / ".worktrees" / ".archive" / source_path.name
        real_run = subprocess.run

        def rename_without_git_registration(args, **kwargs):
            if args[1:3] == ["worktree", "move"]:
                archive_path.parent.mkdir(parents=True, exist_ok=True)
                source_path.rename(archive_path)
                return subprocess.CompletedProcess(
                    args,
                    1,
                    stdout="",
                    stderr="injected unregistered rename",
                )
            return real_run(args, **kwargs)

        monkeypatch.setattr(subprocess, "run", rename_without_git_registration)
        try:
            outcome = cli_mod._archive_worktree_if_unchanged(
                str(git_repo),
                info["path"],
                expected_branch=info["branch"],
            )

            assert outcome["status"] == "preserved"
            assert outcome["reason"] == "archive_destination_not_registered"
            assert not source_path.exists()
            assert archive_path.exists()
        finally:
            if archive_path.exists() and not source_path.exists():
                archive_path.rename(source_path)
            _force_remove_worktree(info)

    def test_last_moment_self_ignoring_payload_is_archived(
        self,
        git_repo,
        monkeypatch,
    ):
        import cli as cli_mod

        info = cli_mod._setup_worktree(str(git_repo), sync_base=False)
        assert info is not None
        subprocess.run(
            ["git", "worktree", "unlock", info["path"]],
            cwd=git_repo,
            capture_output=True,
            check=True,
        )
        real_run = subprocess.run
        payload = b"only-copy late ignored payload\n"
        injected = False

        def inject_before_cleanup_command(args, **kwargs):
            nonlocal injected
            is_remove = args[1:3] == ["worktree", "remove"]
            is_move = args[1:3] == ["worktree", "move"]
            if not injected and (is_remove or is_move):
                injected = True
                worktree = Path(info["path"])
                (worktree / ".gitignore").write_text(".gitignore\nignored/\n")
                ignored_dir = worktree / "ignored"
                ignored_dir.mkdir()
                (ignored_dir / "valuable.bin").write_bytes(payload)
            return real_run(args, **kwargs)

        monkeypatch.setattr(subprocess, "run", inject_before_cleanup_command)
        outcome = cli_mod._archive_worktree_if_unchanged(
            str(git_repo),
            info["path"],
            expected_branch=info["branch"],
        )
        try:
            assert injected
            assert outcome["status"] == "archived_branch_preserved"
            archive_path = Path(outcome["path"])
            assert archive_path.exists()
            assert (archive_path / "ignored" / "valuable.bin").read_bytes() == payload
            assert real_run(
                ["git", "show-ref", "--verify", f"refs/heads/{info['branch']}"],
                cwd=git_repo,
                capture_output=True,
            ).returncode == 0
        finally:
            archived = outcome.get("path")
            if archived:
                real_run(
                    ["git", "worktree", "remove", archived, "--force"],
                    cwd=git_repo,
                    capture_output=True,
                )
            _force_remove_worktree(info)

    def test_ignore_capable_clean_worktree_is_not_hard_removed(
        self,
        git_repo,
        monkeypatch,
    ):
        import cli as cli_mod

        (git_repo / ".gitignore").write_text("ignored/\n")
        subprocess.run(["git", "add", ".gitignore"], cwd=git_repo, check=True)
        subprocess.run(
            ["git", "commit", "-m", "add ignore policy"],
            cwd=git_repo,
            capture_output=True,
            check=True,
        )
        info = cli_mod._setup_worktree(str(git_repo), sync_base=False)
        assert info is not None
        real_run = subprocess.run
        archive_path = git_repo / ".worktrees" / ".archive" / Path(info["path"]).name
        remove_attempted = False

        def record_remove(args, **kwargs):
            nonlocal remove_attempted
            if args[1:3] == ["worktree", "remove"]:
                remove_attempted = True
            return real_run(args, **kwargs)

        monkeypatch.setattr(subprocess, "run", record_remove)
        try:
            cli_mod._cleanup_worktree(info)

            assert not Path(info["path"]).exists()
            assert archive_path.exists()
            assert not remove_attempted
            assert real_run(
                ["git", "show-ref", "--verify", f"refs/heads/{info['branch']}"],
                cwd=git_repo,
                capture_output=True,
            ).returncode == 0
        finally:
            real_run(
                ["git", "worktree", "remove", str(archive_path), "--force"],
                cwd=git_repo,
                capture_output=True,
            )
            real_run(
                ["git", "branch", "-D", info["branch"]],
                cwd=git_repo,
                capture_output=True,
            )

    def test_cleanup_failure_message_names_worktree_and_branch(
        self,
        git_repo,
        monkeypatch,
        capsys,
    ):
        import cli as cli_mod

        info = cli_mod._setup_worktree(str(git_repo), sync_base=False)
        assert info is not None
        monkeypatch.setattr(
            cli_mod,
            "_worktree_has_unpushed_commits",
            lambda *_args, **_kwargs: False,
        )
        monkeypatch.setattr(
            cli_mod,
            "_worktree_is_dirty",
            lambda *_args, **_kwargs: False,
        )
        monkeypatch.setattr(
            cli_mod,
            "_worktree_lock_owned_by_current_process",
            lambda *_args, **_kwargs: False,
        )
        monkeypatch.setattr(
            cli_mod,
            "_archive_worktree_if_unchanged",
            lambda *_args, **_kwargs: {
                "status": "failed",
                "branch": info["branch"],
            },
        )
        try:
            cli_mod._cleanup_worktree(info)
            output = capsys.readouterr().out

            assert (
                f"preserving worktree and branch: {info['path']} ({info['branch']})"
                in output
            )
            assert "keeping branch:" not in output
        finally:
            _force_remove_worktree(info)
