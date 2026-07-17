"""Tests for GHSA-96vc-wcxf-jjff and GHSA-qg5c-hvr5-hjgr.

Two related ACP approval-flow issues:
- 96vc: ACP didn't set HERMES_EXEC_ASK, so `check_all_command_guards`
  took the non-interactive auto-approve path and never consulted the
  ACP-supplied callback.
- qg5c: `_approval_callback` was a module-global in terminal_tool;
  overlapping ACP sessions overwrote each other's callback slot.

Both fixed together by:
1. Setting HERMES_EXEC_ASK inside _run_agent (wraps the agent call).
2. Storing the callback in thread-local state so concurrent executor
   threads don't collide.
"""

import threading



class TestThreadLocalApprovalCallback:
    """GHSA-qg5c-hvr5-hjgr: set_approval_callback must be per-thread so
    concurrent ACP sessions don't stomp on each other's handlers."""

    def test_set_and_get_in_same_thread(self):
        from tools.terminal_tool import (
            set_approval_callback,
            _get_approval_callback,
        )

        cb1 = lambda cmd, desc: "once"  # noqa: E731
        set_approval_callback(cb1)
        assert _get_approval_callback() is cb1

    def test_callback_not_visible_in_different_thread(self):
        """Thread A's callback is NOT visible to Thread B."""
        from tools.terminal_tool import (
            set_approval_callback,
            _get_approval_callback,
        )

        cb_a = lambda cmd, desc: "thread_a"  # noqa: E731
        cb_b = lambda cmd, desc: "thread_b"  # noqa: E731

        seen_in_a = []
        seen_in_b = []

        def thread_a():
            set_approval_callback(cb_a)
            # Pause so thread B has time to set its own callback
            import time
            time.sleep(0.05)
            seen_in_a.append(_get_approval_callback())

        def thread_b():
            set_approval_callback(cb_b)
            import time
            time.sleep(0.05)
            seen_in_b.append(_get_approval_callback())

        ta = threading.Thread(target=thread_a)
        tb = threading.Thread(target=thread_b)
        ta.start()
        tb.start()
        ta.join()
        tb.join()

        # Each thread must see ONLY its own callback — not the other's
        assert seen_in_a == [cb_a]
        assert seen_in_b == [cb_b]

    def test_main_thread_callback_not_leaked_to_worker(self):
        """A callback set in the main thread does NOT leak into a
        freshly-spawned worker thread."""
        from tools.terminal_tool import (
            set_approval_callback,
            _get_approval_callback,
        )

        cb_main = lambda cmd, desc: "main"  # noqa: E731
        set_approval_callback(cb_main)

        worker_saw = []

        def worker():
            worker_saw.append(_get_approval_callback())

        t = threading.Thread(target=worker)
        t.start()
        t.join()

        # Worker thread has no callback set — TLS is empty for it
        assert worker_saw == [None]
        # Main thread still has its callback
        assert _get_approval_callback() is cb_main

    def test_sudo_password_callback_also_thread_local(self):
        """Same protection applies to the sudo password callback."""
        from tools.terminal_tool import (
            set_sudo_password_callback,
            _get_sudo_password_callback,
        )

        cb_main = lambda: "main-password"  # noqa: E731
        set_sudo_password_callback(cb_main)

        worker_saw = []

        def worker():
            worker_saw.append(_get_sudo_password_callback())

        t = threading.Thread(target=worker)
        t.start()
        t.join()

        assert worker_saw == [None]
        assert _get_sudo_password_callback() is cb_main

    def test_sudo_password_cache_does_not_leak_across_threads(self):
        """Interactive sudo cache must not bleed into another executor thread."""
        from tools.terminal_tool import (
            _get_cached_sudo_password,
            _reset_cached_sudo_passwords,
            _set_cached_sudo_password,
        )

        _reset_cached_sudo_passwords()
        _set_cached_sudo_password("main-thread-password")

        worker_saw = []

        def worker():
            worker_saw.append(_get_cached_sudo_password())

        t = threading.Thread(target=worker)
        t.start()
        t.join()

        assert worker_saw == [""]
        assert _get_cached_sudo_password() == "main-thread-password"

    def test_sudo_password_cache_isolated_across_acp_sessions_on_same_pool_thread(self):
        """ACP's ThreadPoolExecutor reuses threads. Two ACP sessions that land
        on the same reused thread must not share the interactive sudo password
        cache. The fix wraps each session in contextvars.copy_context() and
        binds HERMES_SESSION_KEY per session, so the cache scope key differs
        across sessions even when the underlying thread is identical.
        """
        import contextvars
        from concurrent.futures import ThreadPoolExecutor

        from gateway.session_context import (
            clear_session_vars,
            set_session_vars,
        )
        from tools.terminal_tool import (
            _get_cached_sudo_password,
            _reset_cached_sudo_passwords,
            _set_cached_sudo_password,
        )

        _reset_cached_sudo_passwords()
        executor = ThreadPoolExecutor(max_workers=1)  # force thread reuse

        runs: list[tuple[str, str, str]] = []  # (session_id, before, after)

        def _simulate_acp_session(session_id: str, write_password: str) -> None:
            tokens = set_session_vars(session_key=session_id)
            try:
                observed_before = _get_cached_sudo_password()
                _set_cached_sudo_password(write_password)
                observed_after = _get_cached_sudo_password()
                runs.append((session_id, observed_before, observed_after))
            finally:
                clear_session_vars(tokens)

        def _run_in_fresh_context(session_id: str, pw: str) -> str:
            ctx = contextvars.copy_context()
            ctx.run(_simulate_acp_session, session_id, pw)
            return session_id

        try:
            executor.submit(_run_in_fresh_context, "acp-session-A", "alpha-secret").result()
            # Same thread. Without the fix B would see "alpha-secret".
            executor.submit(_run_in_fresh_context, "acp-session-B", "bravo-secret").result()
        finally:
            executor.shutdown(wait=True)
            _reset_cached_sudo_passwords()

        assert runs[0] == ("acp-session-A", "", "alpha-secret")
        # Core regression guard: B on the same reused thread must see an empty
        # cache, not A's password.
        assert runs[1] == ("acp-session-B", "", "bravo-secret")


class TestAcpExecAskGate:
    """GHSA-96vc-wcxf-jjff: ACP's _run_agent must set HERMES_INTERACTIVE so
    that tools.approval.check_all_command_guards takes the CLI-interactive
    path (consults the registered callback via prompt_dangerous_approval)
    instead of the non-interactive auto-approve shortcut.

    (HERMES_EXEC_ASK takes the gateway-queue path which requires a
    notify_cb registered in _gateway_notify_cbs — not applicable to ACP,
    which uses a direct callback shape.)"""

    def test_interactive_env_var_routes_to_callback(self, monkeypatch):
        """When HERMES_INTERACTIVE is set and an approval callback is
        registered, a dangerous command must route through the callback."""
        # Clean env
        monkeypatch.delenv("HERMES_INTERACTIVE", raising=False)
        monkeypatch.delenv("HERMES_GATEWAY_SESSION", raising=False)
        monkeypatch.delenv("HERMES_EXEC_ASK", raising=False)
        monkeypatch.delenv("HERMES_YOLO_MODE", raising=False)

        from tools import approval as approval_module

        monkeypatch.setattr(approval_module, "_get_approval_mode", lambda: "manual")
        check_all_command_guards = approval_module.check_all_command_guards

        called_with = []

        def fake_cb(command, description, *, allow_permanent=True):
            called_with.append((command, description))
            return "once"

        # Without HERMES_INTERACTIVE: takes auto-approve path, callback NOT called
        result = check_all_command_guards(
            "rm -rf /tmp/test-exec-ask", "local", approval_callback=fake_cb,
        )
        assert result["approved"] is True
        assert called_with == [], (
            "without HERMES_INTERACTIVE the non-interactive auto-approve "
            "path should fire without consulting the callback"
        )

        # With HERMES_INTERACTIVE: callback IS called, approval flows through it
        monkeypatch.setenv("HERMES_INTERACTIVE", "1")
        called_with.clear()
        result = check_all_command_guards(
            "rm -rf /tmp/test-exec-ask", "local", approval_callback=fake_cb,
        )
        assert called_with, (
            "with HERMES_INTERACTIVE the approval path should consult the "
            "registered callback — this was the ACP bypass in "
            "GHSA-96vc-wcxf-jjff"
        )
        assert result["approved"] is True

    def test_interactive_context_var_routes_to_callback_without_env(
        self, monkeypatch,
    ):
        """Context-local interactive flag must work without touching os.environ.

        Concurrent ACP sessions run on a shared ThreadPoolExecutor, so the
        interactive flag is now a contextvar instead of a process-global env
        var — one session can no longer clobber another's flag mid-run
        (GHSA-96vc-wcxf-jjff).
        """
        monkeypatch.delenv("HERMES_INTERACTIVE", raising=False)
        monkeypatch.delenv("HERMES_GATEWAY_SESSION", raising=False)
        monkeypatch.delenv("HERMES_EXEC_ASK", raising=False)
        monkeypatch.delenv("HERMES_YOLO_MODE", raising=False)

        from tools.approval import (
            check_all_command_guards,
            reset_hermes_interactive_context,
            set_hermes_interactive_context,
        )
        from tools import approval as approval_module

        monkeypatch.setattr(approval_module, "_get_approval_mode", lambda: "manual")

        called_with = []

        def fake_cb(command, description, *, allow_permanent=True):
            called_with.append((command, description))
            return "once"

        tok = set_hermes_interactive_context(True)
        try:
            result = check_all_command_guards(
                "rm -rf /tmp/test-context-interactive",
                "local",
                approval_callback=fake_cb,
            )
        finally:
            reset_hermes_interactive_context(tok)

        assert called_with, (
            "set_hermes_interactive_context(True) should route dangerous "
            "commands through the callback without HERMES_INTERACTIVE in env"
        )
        assert result["approved"] is True

    def test_acp_authority_makes_smart_approve_advisory(self, monkeypatch):
        """ACP's editor callback remains authoritative over Smart APPROVE."""
        monkeypatch.delenv("HERMES_INTERACTIVE", raising=False)
        monkeypatch.delenv("HERMES_GATEWAY_SESSION", raising=False)
        monkeypatch.delenv("HERMES_EXEC_ASK", raising=False)
        monkeypatch.delenv("HERMES_YOLO_MODE", raising=False)

        from tools import approval as approval_module

        monkeypatch.setattr(approval_module, "_get_approval_mode", lambda: "smart")
        monkeypatch.setattr(approval_module, "_smart_approve", lambda *_args: "approve")
        callback_calls = []

        def deny_cb(command, description, *, allow_permanent=True):
            callback_calls.append((command, description, allow_permanent))
            return "deny"

        interactive_token = approval_module.set_hermes_interactive_context(True)
        authority_token = approval_module.set_acp_approval_authority_context(True)
        try:
            result = approval_module.check_all_command_guards(
                "rm -rf /tmp/test-acp-authority",
                "local",
                approval_callback=deny_cb,
            )
        finally:
            approval_module.reset_acp_approval_authority_context(authority_token)
            approval_module.reset_hermes_interactive_context(interactive_token)

        assert len(callback_calls) == 1
        assert result["approved"] is False
        assert result["outcome"] == "denied"
        assert result["user_consent"] is False
        assert "smart_approved" not in result

        # Outside ACP authority, ordinary interactive Smart mode keeps its
        # documented auto-approval behavior and does not invoke the callback.
        monkeypatch.setenv("HERMES_INTERACTIVE", "1")
        callback_calls.clear()
        ordinary_result = approval_module.check_all_command_guards(
            "rm -rf /tmp/test-ordinary-smart",
            "local",
            approval_callback=deny_cb,
        )
        assert callback_calls == []
        assert ordinary_result["approved"] is True
        assert ordinary_result["smart_approved"] is True

    def test_acp_authority_context_isolated_in_shared_executor(self, monkeypatch):
        """Concurrent ACP authority cannot leak into an ordinary Smart turn."""
        from concurrent.futures import ThreadPoolExecutor

        from tools import approval as approval_module

        monkeypatch.delenv("HERMES_INTERACTIVE", raising=False)
        monkeypatch.delenv("HERMES_GATEWAY_SESSION", raising=False)
        monkeypatch.delenv("HERMES_EXEC_ASK", raising=False)
        monkeypatch.setattr(approval_module, "_get_approval_mode", lambda: "smart")
        monkeypatch.setattr(approval_module, "_smart_approve", lambda *_args: "approve")
        barrier = threading.Barrier(2)
        callback_calls = []

        def authoritative_turn():
            interactive = approval_module.set_hermes_interactive_context(True)
            authority = approval_module.set_acp_approval_authority_context(True)
            try:
                barrier.wait(timeout=5)
                return approval_module.check_all_command_guards(
                    "rm -rf /tmp/test-acp-concurrent",
                    "local",
                    approval_callback=lambda *_a, **_k: callback_calls.append("acp") or "deny",
                )
            finally:
                approval_module.reset_acp_approval_authority_context(authority)
                approval_module.reset_hermes_interactive_context(interactive)

        def ordinary_turn():
            interactive = approval_module.set_hermes_interactive_context(True)
            try:
                barrier.wait(timeout=5)
                return approval_module.check_all_command_guards(
                    "rm -rf /tmp/test-ordinary-concurrent",
                    "local",
                    approval_callback=lambda *_a, **_k: callback_calls.append("ordinary") or "deny",
                )
            finally:
                approval_module.reset_hermes_interactive_context(interactive)

        with ThreadPoolExecutor(max_workers=2) as executor:
            authoritative_future = executor.submit(authoritative_turn)
            ordinary_future = executor.submit(ordinary_turn)
            authoritative = authoritative_future.result(timeout=10)
            ordinary = ordinary_future.result(timeout=10)
            residual = [
                executor.submit(
                    approval_module._acp_approval_authority_ctx.get
                ).result(timeout=5)
                for _ in range(2)
            ]

        assert authoritative["approved"] is False
        assert ordinary["approved"] is True
        assert ordinary["smart_approved"] is True
        assert callback_calls == ["acp"]
        assert residual == [False, False]

    def test_acp_authority_without_callback_denies_before_smart(self, monkeypatch):
        """Missing ACP callback is a denial, not auxiliary consent."""
        monkeypatch.delenv("HERMES_INTERACTIVE", raising=False)
        monkeypatch.delenv("HERMES_GATEWAY_SESSION", raising=False)
        monkeypatch.delenv("HERMES_EXEC_ASK", raising=False)
        monkeypatch.delenv("HERMES_YOLO_MODE", raising=False)

        from tools import approval as approval_module

        smart_calls = []
        monkeypatch.setattr(approval_module, "_get_approval_mode", lambda: "smart")
        monkeypatch.setattr(
            approval_module,
            "_smart_approve",
            lambda *_args: smart_calls.append(True) or "approve",
        )
        interactive_token = approval_module.set_hermes_interactive_context(True)
        authority_token = approval_module.set_acp_approval_authority_context(True)
        try:
            result = approval_module.check_all_command_guards(
                "rm -rf /tmp/test-acp-no-callback",
                "local",
                approval_callback=None,
            )
        finally:
            approval_module.reset_acp_approval_authority_context(authority_token)
            approval_module.reset_hermes_interactive_context(interactive_token)

        assert smart_calls == []
        assert result["approved"] is False
        assert result["outcome"] == "denied"
        assert result["user_consent"] is False

    def test_acp_authority_uses_owner_callback_despite_gateway_flags(self, monkeypatch):
        """Leaked gateway env flags cannot divert ACP into the queue path."""
        monkeypatch.setenv("HERMES_GATEWAY_SESSION", "1")
        monkeypatch.setenv("HERMES_EXEC_ASK", "1")
        monkeypatch.delenv("HERMES_YOLO_MODE", raising=False)

        from tools import approval as approval_module

        monkeypatch.setattr(approval_module, "_get_approval_mode", lambda: "smart")
        monkeypatch.setattr(approval_module, "_smart_approve", lambda *_args: "approve")
        callback_calls = []
        interactive = approval_module.set_hermes_interactive_context(True)
        authority = approval_module.set_acp_approval_authority_context(True)
        try:
            result = approval_module.check_all_command_guards(
                "rm -rf /tmp/test-acp-gateway-env",
                "local",
                approval_callback=lambda *_a, **_k: callback_calls.append(True) or "deny",
            )
        finally:
            approval_module.reset_acp_approval_authority_context(authority)
            approval_module.reset_hermes_interactive_context(interactive)

        assert callback_calls == [True]
        assert result["approved"] is False
        assert result["user_consent"] is False

    def test_acp_authority_malformed_callback_choice_denies(self, monkeypatch):
        """Unhashable, missing, and unexpected callback output all deny."""
        monkeypatch.delenv("HERMES_INTERACTIVE", raising=False)
        monkeypatch.delenv("HERMES_GATEWAY_SESSION", raising=False)
        monkeypatch.delenv("HERMES_EXEC_ASK", raising=False)
        monkeypatch.delenv("HERMES_YOLO_MODE", raising=False)

        from tools import approval as approval_module

        monkeypatch.setattr(approval_module, "_get_approval_mode", lambda: "smart")
        monkeypatch.setattr(approval_module, "_smart_approve", lambda *_args: "escalate")
        for malformed in (None, "unexpected", {"choice": "deny"}, []):
            interactive_token = approval_module.set_hermes_interactive_context(True)
            authority_token = approval_module.set_acp_approval_authority_context(True)
            try:
                result = approval_module.check_all_command_guards(
                    "rm -rf /tmp/test-acp-malformed",
                    "local",
                    approval_callback=lambda *_args, _value=malformed, **_kwargs: _value,
                )
            finally:
                approval_module.reset_acp_approval_authority_context(authority_token)
                approval_module.reset_hermes_interactive_context(interactive_token)

            assert result["approved"] is False, repr(malformed)
            assert result["outcome"] == "denied", repr(malformed)
            assert result["user_consent"] is False, repr(malformed)
