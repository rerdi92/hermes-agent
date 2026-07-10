"""Tests for tools/clarify_tool.py - Interactive clarifying questions."""

import json
from typing import List, Optional


from tools.clarify_tool import (
    clarify_tool,
    check_clarify_requirements,
    MAX_CHOICES,
    CLARIFY_SCHEMA,
    _flatten_choice,
    _load_reoffer_policy,
    cap_clarify_attempt_timeout,
)


class TestClarifyToolBasics:
    """Basic functionality tests for clarify_tool."""

    def test_simple_question_with_callback(self):
        """Should return user response for simple question."""
        def mock_callback(question: str, choices: Optional[List[str]]) -> str:
            assert question == "What color?"
            assert choices is None
            return "blue"

        result = json.loads(clarify_tool("What color?", callback=mock_callback))
        assert result["question"] == "What color?"
        assert result["choices_offered"] is None
        assert result["user_response"] == "blue"

    def test_question_with_choices(self):
        """Should pass choices to callback and return response."""
        def mock_callback(question: str, choices: Optional[List[str]]) -> str:
            assert question == "Pick a number"
            assert choices == ["1", "2", "3"]
            return "2"

        result = json.loads(clarify_tool(
            "Pick a number",
            choices=["1", "2", "3"],
            callback=mock_callback
        ))
        assert result["question"] == "Pick a number"
        assert result["choices_offered"] == ["1", "2", "3"]
        assert result["user_response"] == "2"

    def test_empty_question_returns_error(self):
        """Should return error for empty question."""
        result = json.loads(clarify_tool("", callback=lambda q, c: "ignored"))
        assert "error" in result
        assert "required" in result["error"].lower()

    def test_whitespace_only_question_returns_error(self):
        """Should return error for whitespace-only question."""
        result = json.loads(clarify_tool("   \n\t  ", callback=lambda q, c: "ignored"))
        assert "error" in result

    def test_no_callback_returns_error(self):
        """Should return error when no callback is provided."""
        result = json.loads(clarify_tool("What do you want?"))
        assert "error" in result
        assert "not available" in result["error"].lower()


class TestClarifyToolChoicesValidation:
    """Tests for choices parameter validation."""

    def test_choices_trimmed_to_max(self):
        """Should trim choices to MAX_CHOICES."""
        choices_passed = []

        def mock_callback(question: str, choices: Optional[List[str]]) -> str:
            choices_passed.extend(choices or [])
            return "picked"

        many_choices = ["a", "b", "c", "d", "e", "f", "g"]
        clarify_tool("Pick one", choices=many_choices, callback=mock_callback)

        assert len(choices_passed) == MAX_CHOICES

    def test_empty_choices_become_none(self):
        """Empty choices list should become None (open-ended)."""
        choices_received = ["marker"]

        def mock_callback(question: str, choices: Optional[List[str]]) -> str:
            choices_received.clear()
            if choices is not None:
                choices_received.extend(choices)
            return "answer"

        clarify_tool("Open question?", choices=[], callback=mock_callback)
        assert choices_received == []  # Was cleared, nothing added

    def test_choices_with_only_whitespace_stripped(self):
        """Whitespace-only choices should be stripped out."""
        choices_received = []

        def mock_callback(question: str, choices: Optional[List[str]]) -> str:
            choices_received.extend(choices or [])
            return "answer"

        clarify_tool("Pick", choices=["valid", "  ", "", "also valid"], callback=mock_callback)
        assert choices_received == ["valid", "also valid"]

    def test_invalid_choices_type_returns_error(self):
        """Non-list choices should return error."""
        result = json.loads(clarify_tool(
            "Question?",
            choices="not a list",  # type: ignore
            callback=lambda q, c: "ignored"
        ))
        assert "error" in result
        assert "list" in result["error"].lower()

    def test_choices_converted_to_strings(self):
        """Non-string choices should be converted to strings."""
        choices_received = []

        def mock_callback(question: str, choices: Optional[List[str]]) -> str:
            choices_received.extend(choices or [])
            return "answer"

        clarify_tool("Pick", choices=[1, 2, 3], callback=mock_callback)  # type: ignore
        assert choices_received == ["1", "2", "3"]


class TestClarifyToolCallbackHandling:
    """Tests for callback error handling."""

    def test_callback_exception_returns_error(self):
        """Should return error if callback raises exception."""
        def failing_callback(question: str, choices: Optional[List[str]]) -> str:
            raise RuntimeError("User cancelled")

        result = json.loads(clarify_tool("Question?", callback=failing_callback))
        assert "error" in result
        assert "Failed to get user input" in result["error"]
        assert "User cancelled" in result["error"]

    def test_callback_receives_stripped_question(self):
        """Callback should receive trimmed question."""
        received_question = []

        def mock_callback(question: str, choices: Optional[List[str]]) -> str:
            received_question.append(question)
            return "answer"

        clarify_tool("  Question with spaces  \n", callback=mock_callback)
        assert received_question[0] == "Question with spaces"

    def test_user_response_stripped(self):
        """User response should be stripped of whitespace."""
        def mock_callback(question: str, choices: Optional[List[str]]) -> str:
            return "  response with spaces  \n"

        result = json.loads(clarify_tool("Q?", callback=mock_callback))
        assert result["user_response"] == "response with spaces"


class TestClarifyReofferPolicy:
    """Multiple-choice prompts may re-open after a retryable no-selection."""

    def test_attempts_without_window_fail_safe_to_single_shot(self, monkeypatch):
        monkeypatch.setattr(
            "hermes_cli.config.load_config",
            lambda: {"agent": {"clarify_reoffer_attempts": 3}},
        )

        assert _load_reoffer_policy() == (1, 0)

    def test_policy_hard_caps_high_config(self, monkeypatch):
        monkeypatch.setattr(
            "hermes_cli.config.load_config",
            lambda: {
                "agent": {
                    "clarify_reoffer_attempts": 9,
                    "clarify_reoffer_window_seconds": 9999,
                }
            },
        )

        assert _load_reoffer_policy() == (3, 1200)

    def test_attempt_timeout_caps_only_when_reoffer_enabled(self):
        enabled = {
            "clarify_reoffer_attempts": 3,
            "clarify_reoffer_window_seconds": 1200,
        }
        single_shot = {
            "clarify_reoffer_attempts": 1,
            "clarify_reoffer_window_seconds": 0,
        }

        assert cap_clarify_attempt_timeout(3600, enabled) == 400
        assert cap_clarify_attempt_timeout(3600, single_shot) == 3600

    def test_reoffers_empty_choice_until_answer(self, monkeypatch):
        monkeypatch.setattr("tools.clarify_tool._load_reoffer_policy", lambda: (3, 1200))
        responses = iter(["", "", "approved"])
        calls = []

        def callback(question, choices):
            calls.append((question, choices))
            return next(responses)

        result = json.loads(clarify_tool(
            "Proceed?", choices=["approved", "defer"], callback=callback,
        ))

        assert len(calls) == 3
        assert result["user_response"] == "approved"
        assert result["attempts_used"] == 3
        assert result["selection_status"] == "answered"
        assert result.get("reoffer_exhausted") is not True

    def test_exhausted_choice_returns_explicit_no_consent(self, monkeypatch):
        monkeypatch.setattr("tools.clarify_tool._load_reoffer_policy", lambda: (3, 1200))
        calls = []

        def callback(question, choices):
            calls.append((question, choices))
            return ""

        result = json.loads(clarify_tool(
            "Proceed?", choices=["approve", "defer"], callback=callback,
        ))

        assert len(calls) == 3
        assert result["attempts_used"] == 3
        assert result["selection_status"] == "no_consent"
        assert result["reoffer_exhausted"] is True
        assert result["consent_inferred"] is False
        assert "Do not infer consent" in result["decision_instruction"]

    def test_open_ended_prompt_is_never_reoffered(self, monkeypatch):
        monkeypatch.setattr("tools.clarify_tool._load_reoffer_policy", lambda: (3, 1200))
        calls = []

        def callback(question, choices):
            calls.append((question, choices))
            return ""

        result = json.loads(clarify_tool("Explain", callback=callback))

        assert len(calls) == 1
        assert result["attempts_used"] == 1
        assert result["selection_status"] == "no_selection"

    def test_cancelled_choice_prompt_is_not_reoffered(self, monkeypatch):
        monkeypatch.setattr("tools.clarify_tool._load_reoffer_policy", lambda: (3, 1200))
        calls = []

        def callback(question, choices):
            calls.append((question, choices))
            return "[clarify prompt cancelled]"

        result = json.loads(clarify_tool(
            "Proceed?", choices=["yes", "no"], callback=callback,
        ))

        assert len(calls) == 1
        assert result["selection_status"] == "cancelled"

    def test_known_cli_timeout_sentinel_is_reoffered(self, monkeypatch):
        monkeypatch.setattr("tools.clarify_tool._load_reoffer_policy", lambda: (3, 1200))
        responses = iter([
            "The user did not provide a response within the time limit. Continue safely.",
            "defer",
        ])
        calls = []

        def callback(question, choices):
            calls.append((question, choices))
            return next(responses)

        result = json.loads(clarify_tool(
            "Proceed?", choices=["run", "defer"], callback=callback,
        ))

        assert len(calls) == 2
        assert result["user_response"] == "defer"

    def test_window_expiry_stops_before_next_reoffer(self, monkeypatch):
        monkeypatch.setattr("tools.clarify_tool._load_reoffer_policy", lambda: (3, 1200))
        ticks = iter([100.0, 1301.0])
        monkeypatch.setattr("tools.clarify_tool.time.monotonic", lambda: next(ticks))
        calls = []

        def callback(question, choices):
            calls.append((question, choices))
            return ""

        result = json.loads(clarify_tool(
            "Proceed?", choices=["yes", "no"], callback=callback,
        ))

        assert len(calls) == 1
        assert result["attempts_used"] == 1
        assert result["selection_status"] == "no_consent"
        assert result["reoffer_exhausted"] is True


class TestCheckClarifyRequirements:
    """Tests for the requirements check function."""

    def test_always_returns_true(self):
        """clarify tool has no external requirements."""
        assert check_clarify_requirements() is True


class TestClarifyDictChoices:
    """Dict-shaped choices must be unwrapped to user-facing text at the source.

    LLMs sometimes emit [{"description": "..."}] instead of bare strings. The
    naive str(c) coercion leaked the Python dict repr onto every surface (CLI
    panel, Discord buttons, Telegram list) AND returned it verbatim as the
    user's answer. _flatten_choice normalises at the one platform-agnostic
    entry point so the whole class is fixed in one place.
    """

    def test_flatten_unwraps_label_first(self):
        assert _flatten_choice({"label": "Short", "description": "Long"}) == "Short"

    def test_flatten_unwraps_description_when_no_label(self):
        assert _flatten_choice({"description": "A loose layout"}) == "A loose layout"

    def test_flatten_unwrap_order_label_over_description(self):
        assert _flatten_choice({"description": "verbose", "label": "tight"}) == "tight"

    def test_flatten_drops_name_value_only_dict(self):
        # name/value are component-shaped fields, not user-facing labels —
        # picking them would leak raw enum values / short model ids.
        assert _flatten_choice({"name": "tight", "value": "x"}) == ""

    def test_flatten_prefers_canonical_key_over_name(self):
        assert _flatten_choice({"name": "tight", "description": "Tight desc"}) == "Tight desc"

    def test_flatten_drops_keyless_dict(self):
        assert _flatten_choice({"foo": "bar", "n": 1}) == ""

    def test_flatten_passthrough_string_and_scalar(self):
        assert _flatten_choice("plain") == "plain"
        assert _flatten_choice(7) == "7"
        assert _flatten_choice(None) == ""

    def test_dict_choices_reach_callback_as_clean_text(self):
        """The whole point: the UI callback never sees a dict repr."""
        seen = []

        def cb(question, choices):
            seen.extend(choices or [])
            return choices[0]

        result = json.loads(clarify_tool(
            "Pick a layout",
            choices=[
                {"choice": "Tight", "description": "Tight, covers all 3 points"},
                {"description": "Loose layout"},
                {"name": "modelid", "value": "abc"},  # dropped, not leaked
                "A plain string choice",
            ],
            callback=cb,
        ))  # type: ignore
        assert seen == [
            "Tight, covers all 3 points",
            "Loose layout",
            "A plain string choice",
        ]
        # and the resolved answer is clean text, not a dict repr
        assert result["user_response"] == "Tight, covers all 3 points"
        assert "{" not in result["user_response"]
        assert all("{" not in c for c in result["choices_offered"])


class TestClarifySchema:
    """Tests for the OpenAI function-calling schema."""

    def test_schema_name(self):
        """Schema should have correct name."""
        assert CLARIFY_SCHEMA["name"] == "clarify"

    def test_schema_has_description(self):
        """Schema should have a description."""
        assert "description" in CLARIFY_SCHEMA
        assert len(CLARIFY_SCHEMA["description"]) > 50

    def test_schema_question_required(self):
        """Question parameter should be required."""
        assert "question" in CLARIFY_SCHEMA["parameters"]["required"]

    def test_schema_choices_optional(self):
        """Choices parameter should be optional."""
        assert "choices" not in CLARIFY_SCHEMA["parameters"]["required"]

    def test_schema_choices_max_items(self):
        """Schema should specify max items for choices."""
        choices_spec = CLARIFY_SCHEMA["parameters"]["properties"]["choices"]
        assert choices_spec.get("maxItems") == MAX_CHOICES

    def test_max_choices_is_four(self):
        """MAX_CHOICES constant should be 4."""
        assert MAX_CHOICES == 4
