"""Regression tests for the streaming single-writer invariant (#65991).

A retry that supersedes a still-live SSE stream must fence the old stream out
of the delta sink; otherwise both streams write into the same turn and the
persisted transcript is two coherent responses interleaved token-by-token.

These tests exercise the real ``AIAgent`` guard helpers and the streaming
consume-loop, asserting that exactly one writer ever reaches the turn.
"""
import threading
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest


def _make_agent():
    from run_agent import AIAgent

    agent = AIAgent(
        api_key="test-key",
        base_url="https://openrouter.ai/api/v1",
        model="test/model",
        quiet_mode=True,
        skip_context_files=True,
        skip_memory=True,
    )
    agent.api_mode = "chat_completions"
    agent._interrupt_requested = False
    return agent


def _chunk(content=None, finish_reason=None, model=None):
    delta = SimpleNamespace(content=content, tool_calls=None, reasoning_content=None, reasoning=None)
    choice = SimpleNamespace(index=0, delta=delta, finish_reason=finish_reason)
    return SimpleNamespace(choices=[choice], model=model, usage=None)


def _tool_chunk(name=None, arguments=None, finish_reason=None, model=None):
    function = SimpleNamespace(name=name, arguments=arguments)
    tool_call = SimpleNamespace(index=0, id="call_1", function=function)
    delta = SimpleNamespace(
        content=None,
        tool_calls=[tool_call],
        reasoning_content=None,
        reasoning=None,
    )
    choice = SimpleNamespace(index=0, delta=delta, finish_reason=finish_reason)
    return SimpleNamespace(choices=[choice], model=model, usage=None)


class TestSingleWriterSink:
    def test_new_claim_cannot_overtake_checked_emission(self):
        """Claim and callback emission have one total order.

        A newer writer that starts after the old writer passed its stale check
        must not emit first and then let the old delta land behind it.
        """
        agent = _make_agent()
        delivered = []
        agent.stream_delta_callback = lambda text: delivered.append(text)
        agent._stream_callback = None
        agent._stream_think_scrubber = None
        agent._stream_context_scrubber = None

        old_in_transform = threading.Event()
        release_old = threading.Event()
        new_claim_started = threading.Event()
        new_claimed = threading.Event()

        def blocking_strip(text):
            if text == "old":
                old_in_transform.set()
                assert release_old.wait(timeout=2)
            return text

        agent._strip_think_blocks = blocking_strip

        def old_writer():
            agent._claim_stream_writer()
            agent._fire_stream_delta("old")

        def new_writer():
            assert old_in_transform.wait(timeout=2)
            new_claim_started.set()
            agent._claim_stream_writer()
            new_claimed.set()
            agent._fire_stream_delta("new")

        old = threading.Thread(target=old_writer)
        new = threading.Thread(target=new_writer)
        old.start()
        assert old_in_transform.wait(timeout=2)
        new.start()
        assert new_claim_started.wait(timeout=2)
        new_overtook_inflight_emission = new_claimed.wait(timeout=0.2)
        release_old.set()
        old.join(timeout=2)
        new.join(timeout=2)

        assert not new_overtook_inflight_emission
        assert delivered == ["old", "new"]
        assert not old.is_alive() and not new.is_alive()

    def test_superseded_writer_deltas_are_dropped(self):
        """A stale writer (older token, other thread) is fenced; only the
        newest writer reaches the callbacks and the accumulated turn text."""
        agent = _make_agent()
        delivered = []
        agent.stream_delta_callback = lambda t: delivered.append(t)
        agent._stream_callback = None

        a_claimed = threading.Event()
        b_claimed = threading.Event()

        def writer_a():
            agent._claim_stream_writer()  # token 1
            a_claimed.set()
            b_claimed.wait(timeout=2)  # let B supersede us first
            # We are now stale — every sink call must be a no-op.
            agent._fire_stream_delta("A-should-drop")
            agent._fire_reasoning_delta("A-reason-drop")
            agent._record_streamed_assistant_text("A-record-drop")

        def writer_b():
            a_claimed.wait(timeout=2)
            agent._claim_stream_writer()  # token 2 — supersedes A
            b_claimed.set()

        tb = threading.Thread(target=writer_b)
        ta = threading.Thread(target=writer_a)
        tb.start()
        ta.start()
        ta.join(timeout=3)
        tb.join(timeout=3)

        assert delivered == [], "a superseded stream must not deliver any deltas"
        assert "A-record-drop" not in (agent._current_streamed_assistant_text or "")
        assert agent._stream_writer_dropped >= 1

    def test_current_writer_is_never_fenced(self):
        """The active writer always delivers — the guard can only drop a
        stream that a *newer* claim has superseded."""
        agent = _make_agent()
        delivered = []
        agent.stream_delta_callback = lambda t: delivered.append(t)
        agent._stream_callback = None

        agent._claim_stream_writer()
        agent._fire_stream_delta("hello ")
        agent._fire_stream_delta("world")

        assert "".join(delivered) == "hello world"
        assert agent._stream_writer_dropped == 0

    def test_non_claiming_thread_is_not_a_writer(self):
        """A thread that never claimed (a non-streaming delta caller) is never
        treated as a stale writer, even after other attempts have claimed."""
        agent = _make_agent()
        delivered = []
        agent.stream_delta_callback = lambda t: delivered.append(t)
        agent._stream_callback = None

        # Some other thread runs a couple of stream attempts and bumps the token.
        def other():
            agent._claim_stream_writer()
            agent._claim_stream_writer()

        t = threading.Thread(target=other)
        t.start()
        t.join(timeout=3)

        # This (main) thread never claimed → not superseded → delivers.
        assert agent._stream_writer_superseded() is False
        agent._fire_stream_delta("plain")
        assert delivered == ["plain"]

    def test_reentrant_claim_stops_old_delta_before_second_callback_and_record(self):
        """A callback-started replacement cannot inherit the outer old delta."""
        agent = _make_agent()
        delivered = []
        agent._stream_think_scrubber = None
        agent._stream_context_scrubber = None

        def display(text):
            delivered.append(("display", text))
            if text == "old":
                agent._claim_stream_writer()
                agent._fire_stream_delta("new")

        agent.stream_delta_callback = display
        agent._stream_callback = lambda text: delivered.append(("tts", text))
        agent._claim_stream_writer()
        agent._fire_stream_delta("old")

        assert delivered == [
            ("display", "old"),
            ("display", "new"),
            ("tts", "new"),
        ]
        assert agent._current_streamed_assistant_text == "new"

    def test_reentrant_claim_during_tail_flush_cannot_reset_new_writer_state(self):
        """A reset callback that starts a writer cannot have its state wiped."""
        agent = _make_agent()
        delivered = []

        class TailScrubber:
            def feed(self, text):
                return text

            def flush(self):
                return "old-tail"

        def display(text):
            delivered.append(("display", text))
            if text == "old-tail":
                agent._claim_stream_writer()
                agent._fire_stream_delta("new")

        agent._stream_think_scrubber = TailScrubber()
        agent._stream_context_scrubber = None
        agent.stream_delta_callback = display
        agent._stream_callback = lambda text: delivered.append(("tts", text))
        agent._claim_stream_writer()
        agent._reset_stream_delivery_tracking()

        assert delivered == [
            ("display", "old-tail"),
            ("display", "new"),
            ("tts", "new"),
        ]
        assert agent._current_streamed_assistant_text == "new"


class TestSingleWriterLoop:
    @patch("run_agent.AIAgent._create_request_openai_client")
    @patch("run_agent.AIAgent._close_request_openai_client")
    def test_consume_loop_stops_when_superseded_mid_stream(self, _close, mock_create):
        """The real streaming loop bails out the moment a newer attempt claims
        the sink, so a superseded stream cannot interleave into the turn."""
        agent = _make_agent()
        delivered = []
        agent.stream_delta_callback = lambda t: delivered.append(t)
        agent._stream_callback = None

        def stream_gen():
            yield _chunk(content="first")
            # A concurrent retry supersedes this stream between chunks.
            agent._claim_stream_writer()
            yield _chunk(content="-stale-tail", finish_reason="stop", model="m")

        mock_client = MagicMock()
        mock_client.chat.completions.create.return_value = stream_gen()
        mock_create.return_value = mock_client

        agent._interruptible_streaming_api_call({})

        assert "".join(delivered) == "first"
        assert "-stale-tail" not in "".join(delivered)

    @patch("run_agent.AIAgent._create_request_openai_client")
    @patch("run_agent.AIAgent._close_request_openai_client")
    def test_tool_suppressed_raw_callback_rechecks_writer_under_emission_lock(
        self, _close, mock_create
    ):
        """A claim after the loop check fences the raw tool-suppressed callback."""
        agent = _make_agent()
        delivered = []
        agent.stream_delta_callback = lambda text: delivered.append(text)
        agent._stream_callback = None
        old_after_loop_check = threading.Event()
        release_old = threading.Event()
        errors = []

        class BlockingSuppressedDelta:
            tool_calls = None
            reasoning_content = None
            reasoning = None

            def __init__(self):
                self._reads = 0

            @property
            def content(self):
                self._reads += 1
                if self._reads == 1:
                    old_after_loop_check.set()
                    assert release_old.wait(timeout=2)
                return "old-suppressed"

        def stream_gen():
            # Establish tool_calls_acc so subsequent content takes the raw,
            # tag-preserving callback branch rather than _fire_stream_delta.
            yield _tool_chunk(name="write_file", arguments="{}")
            yield SimpleNamespace(
                choices=[SimpleNamespace(
                    index=0,
                    delta=BlockingSuppressedDelta(),
                    finish_reason=None,
                )],
                model="m",
                usage=None,
            )
            yield _chunk(finish_reason="tool_calls", model="m")

        mock_client = MagicMock()
        mock_client.chat.completions.create.return_value = stream_gen()
        mock_create.return_value = mock_client

        def old_writer():
            try:
                agent._interruptible_streaming_api_call({})
            except Exception as exc:  # pragma: no cover - assertion aid
                errors.append(exc)

        old = threading.Thread(target=old_writer)
        old.start()
        assert old_after_loop_check.wait(timeout=2)
        # Supersede after the consume-loop check but before the raw callback.
        agent._claim_stream_writer()
        agent._fire_stream_delta("new")
        release_old.set()
        old.join(timeout=3)

        assert not old.is_alive()
        assert errors == []
        assert delivered == ["new"]
        assert "old-suppressed" not in agent._current_streamed_assistant_text


class TestCodexSingleWriter:
    """The codex_responses path claims the sink and stops when superseded,
    matching the chat_completions/anthropic/bedrock parity added in salvage."""

    def _codex_event(self, event_type, **fields):
        return SimpleNamespace(type=event_type, **fields)

    def test_codex_stream_claims_writer_and_stops_when_superseded(self):
        from agent.codex_runtime import run_codex_stream

        agent = _make_agent()
        agent.api_mode = "codex_responses"
        delivered = []
        agent.stream_delta_callback = lambda t: delivered.append(t)
        agent._stream_callback = None

        def event_gen():
            yield self._codex_event(
                "response.output_text.delta", delta="first", item_id="i1",
            )
            # A concurrent retry supersedes this stream between events.
            agent._claim_stream_writer()
            yield self._codex_event(
                "response.output_text.delta", delta="-stale-tail", item_id="i1",
            )
            yield self._codex_event(
                "response.completed",
                response=SimpleNamespace(
                    id="r1", status="completed", output=[], usage=None,
                ),
            )

        mock_client = MagicMock()
        mock_client.responses.create.return_value = event_gen()

        run_codex_stream(agent, {"model": "gpt-5.3-codex"}, client=mock_client)

        assert "".join(delivered) == "first"
        assert "-stale-tail" not in "".join(delivered)

    def test_codex_stream_undisturbed_when_sole_writer(self):
        from agent.codex_runtime import run_codex_stream

        agent = _make_agent()
        agent.api_mode = "codex_responses"
        delivered = []
        agent.stream_delta_callback = lambda t: delivered.append(t)
        agent._stream_callback = None

        def event_gen():
            yield self._codex_event(
                "response.output_text.delta", delta="hello ", item_id="i1",
            )
            yield self._codex_event(
                "response.output_text.delta", delta="world", item_id="i1",
            )
            yield self._codex_event(
                "response.completed",
                response=SimpleNamespace(
                    id="r1", status="completed", output=[], usage=None,
                ),
            )

        mock_client = MagicMock()
        mock_client.responses.create.return_value = event_gen()

        run_codex_stream(agent, {"model": "gpt-5.3-codex"}, client=mock_client)

        assert "".join(delivered) == "hello world"


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-q"]))
