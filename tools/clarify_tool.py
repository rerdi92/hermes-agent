#!/usr/bin/env python3
"""
Clarify Tool Module - Interactive Clarifying Questions

Allows the agent to present structured multiple-choice questions or open-ended
prompts to the user. In CLI mode, choices are navigable with arrow keys. On
messaging platforms, choices are rendered as a numbered list.

The actual user-interaction logic lives in the platform layer (cli.py for CLI,
gateway/run.py for messaging). This module defines the schema, validation, and
a thin dispatcher that delegates to a platform-provided callback.
"""

import json
import time
from typing import List, Optional, Callable


# Maximum number of predefined choices the agent can offer.
# A 5th "Other (type your answer)" option is always appended by the UI.
MAX_CHOICES = 4
MAX_REOFFER_ATTEMPTS = 3
MAX_REOFFER_WINDOW_SECONDS = 1200
MAX_REOFFER_ATTEMPT_TIMEOUT_SECONDS = 400

_CLARIFY_TIMEOUT_SENTINELS = (
    "[clarify prompt timed out]",
    "[user did not respond within ",
    "the user did not provide a response within the time limit",
)
_CLARIFY_CANCELLED_SENTINEL = "[clarify prompt cancelled]"


def _coerce_reoffer_policy(agent_cfg: dict) -> tuple[int, int]:
    """Validate and hard-cap re-offer settings from an agent config mapping."""
    try:
        attempts = int(agent_cfg.get("clarify_reoffer_attempts", 1))
        window = int(agent_cfg.get("clarify_reoffer_window_seconds", 0))
    except (TypeError, ValueError):
        return 1, 0

    attempts = max(1, min(attempts, MAX_REOFFER_ATTEMPTS))
    window = max(0, min(window, MAX_REOFFER_WINDOW_SECONDS))
    if attempts > 1 and window == 0:
        return 1, 0
    return attempts, window


def _load_reoffer_policy() -> tuple[int, int]:
    """Return the hard-capped ``(attempts, overall_window_seconds)`` policy.

    The defaults preserve historical behavior: one callback, no retry window.
    Re-offers are opt-in and apply only to multiple-choice prompts.
    """
    try:
        from hermes_cli.config import load_config

        cfg = load_config() or {}
        agent_cfg = cfg.get("agent", {}) or {}
    except Exception:
        return 1, 0

    return _coerce_reoffer_policy(agent_cfg)


def cap_clarify_attempt_timeout(timeout: int, agent_cfg: Optional[dict] = None) -> int:
    """Cap one choice-UI wait at 400s only when bounded re-offer is enabled."""
    try:
        normalized_timeout = max(1, int(timeout))
    except (TypeError, ValueError):
        normalized_timeout = MAX_REOFFER_ATTEMPT_TIMEOUT_SECONDS

    policy = _coerce_reoffer_policy(agent_cfg) if agent_cfg is not None else _load_reoffer_policy()
    if policy[0] > 1:
        return min(normalized_timeout, MAX_REOFFER_ATTEMPT_TIMEOUT_SECONDS)
    return normalized_timeout


def _is_retryable_no_selection(response: str) -> bool:
    """True for Skip/empty and known platform timeout sentinels."""
    normalized = str(response or "").strip().lower()
    if not normalized:
        return True
    return any(normalized.startswith(prefix) for prefix in _CLARIFY_TIMEOUT_SENTINELS)


def _selection_status(response: str) -> str:
    normalized = str(response or "").strip().lower()
    if normalized.startswith(_CLARIFY_CANCELLED_SENTINEL):
        return "cancelled"
    if _is_retryable_no_selection(normalized):
        return "no_selection"
    return "answered"


def _flatten_choice(c) -> str:
    """Coerce a single choice into its user-facing display string.

    The schema declares choices as bare strings, but LLMs sometimes emit
    dict-shaped choices like ``[{"description": "..."}]``. A naive ``str(c)``
    turns the whole dict into its Python repr — ``{'description': '...'}`` —
    which then leaks onto every surface that renders the choice (CLI panel,
    Discord buttons, Telegram numbered list) AND is returned verbatim as the
    user's answer. Normalising here, at the one platform-agnostic entry point,
    fixes the whole class in one place instead of per-adapter.

    Dict unwrap order is the canonical LLM tool-call user-facing keys:
    ``label`` → ``description`` → ``text`` → ``title``. ``name`` and ``value``
    are deliberately excluded — they're component-shaped fields that could
    carry raw enum values or short identifiers, not human-readable labels. A
    dict with none of the canonical keys is dropped (returns ""), since a
    garbage label is worse than no choice at all.
    """
    if c is None:
        return ""
    if isinstance(c, str):
        return c.strip()
    if isinstance(c, dict):
        for key in ("label", "description", "text", "title"):
            v = c.get(key)
            if isinstance(v, str) and v.strip():
                return v.strip()
        return ""
    if isinstance(c, (list, tuple)):
        return " ".join(_flatten_choice(x) for x in c).strip()
    return str(c).strip()


def clarify_tool(
    question: str,
    choices: Optional[List[str]] = None,
    callback: Optional[Callable] = None,
) -> str:
    """
    Ask the user a question, optionally with multiple-choice options.

    Args:
        question: The question text to present.
        choices:  Up to 4 predefined answer choices. When omitted the
                  question is purely open-ended.
        callback: Platform-provided function that handles the actual UI
                  interaction. Signature: callback(question, choices) -> str.
                  Injected by the agent runner (cli.py / gateway).

    Returns:
        JSON string with the user's response.
    """
    if not question or not question.strip():
        return tool_error("Question text is required.")

    question = question.strip()

    # Validate and trim choices
    if choices is not None:
        if not isinstance(choices, list):
            return tool_error("choices must be a list of strings.")
        # LLMs sometimes emit dict-shaped choices (e.g. [{"description": "..."}])
        # instead of bare strings. _flatten_choice unwraps them to their
        # user-facing text here — the single platform-agnostic entry point —
        # so the CLI panel, Discord buttons, and Telegram list all render clean
        # text and the resolved answer is never a raw Python dict repr.
        choices = [s for s in (_flatten_choice(c) for c in choices) if s]
        if len(choices) > MAX_CHOICES:
            choices = choices[:MAX_CHOICES]
        if not choices:
            choices = None  # empty list → open-ended

    if callback is None:
        return json.dumps(
            {"error": "Clarify tool is not available in this execution context."},
            ensure_ascii=False,
        )

    configured_attempts, window_seconds = _load_reoffer_policy()
    configured_attempts = min(max(1, configured_attempts), MAX_REOFFER_ATTEMPTS)
    window_seconds = min(max(0, window_seconds), MAX_REOFFER_WINDOW_SECONDS)
    max_attempts = configured_attempts if choices is not None else 1
    deadline = time.monotonic() + window_seconds if window_seconds > 0 else None
    attempts_used = 0
    user_response = ""

    for attempt_index in range(max_attempts):
        if attempt_index > 0 and deadline is not None and time.monotonic() >= deadline:
            break
        try:
            user_response = callback(question, choices)
        except Exception as exc:
            return json.dumps(
                {"error": f"Failed to get user input: {exc}"},
                ensure_ascii=False,
            )
        attempts_used += 1
        if not _is_retryable_no_selection(user_response):
            break

    status = _selection_status(user_response)
    no_consent = choices is not None and status == "no_selection"
    if no_consent:
        status = "no_consent"

    result = {
        "question": question,
        "choices_offered": choices,
        "user_response": str(user_response).strip(),
        "attempts_used": attempts_used,
        "selection_status": status,
    }
    if no_consent:
        result.update({
            "reoffer_exhausted": True,
            "consent_inferred": False,
            "decision_instruction": (
                "No selection was captured. Do not infer consent or execute side effects; "
                "leave the pending action paused."
            ),
        })
    return json.dumps(result, ensure_ascii=False)


def check_clarify_requirements() -> bool:
    """Clarify tool has no external requirements -- always available."""
    return True


# =============================================================================
# OpenAI Function-Calling Schema
# =============================================================================

CLARIFY_SCHEMA = {
    "name": "clarify",
    "description": (
        "Ask the user a question when you need clarification, feedback, or a "
        "decision before proceeding. Supports two modes:\n\n"
        "1. **Multiple choice** — provide up to 4 choices. The user picks one "
        "or types their own answer via a 5th 'Other' option.\n"
        "2. **Open-ended** — omit choices entirely. The user types a free-form "
        "response.\n\n"
        "CRITICAL: when you are offering options, put each option ONLY in the "
        "`choices` array — NEVER enumerate the options inside the `question` "
        "text. The UI renders `choices` as selectable rows; options written "
        "into the question string render as dead prose the user can't pick. "
        "Right: question='Which deployment target?', choices=['staging', "
        "'prod']. Wrong: question='Which target? 1) staging 2) prod', choices=[].\n\n"
        "Use this tool when:\n"
        "- The task is ambiguous and you need the user to choose an approach\n"
        "- You want post-task feedback ('How did that work out?')\n"
        "- You want to offer to save a skill or update memory\n"
        "- A decision has meaningful trade-offs the user should weigh in on\n\n"
        "Do NOT use this tool for simple yes/no confirmation of dangerous "
        "commands (the terminal tool handles that). Prefer making a reasonable "
        "default choice yourself when the decision is low-stakes."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "question": {
                "type": "string",
                "description": (
                    "The question itself, and ONLY the question (e.g. 'Which "
                    "deployment target?'). Do NOT embed the answer options here "
                    "— pass them as separate elements in `choices`."
                ),
            },
            "choices": {
                "type": "array",
                "items": {"type": "string"},
                "maxItems": MAX_CHOICES,
                "description": (
                    "REQUIRED whenever you are presenting selectable options: "
                    "each distinct option is its own array element (up to 4). "
                    "The UI renders these as pickable rows and auto-appends an "
                    "'Other (type your answer)' option. Omit this parameter "
                    "entirely ONLY for a genuinely open-ended free-text question."
                ),
            },
        },
        "required": ["question"],
    },
}


# --- Registry ---
from tools.registry import registry, tool_error

registry.register(
    name="clarify",
    toolset="clarify",
    schema=CLARIFY_SCHEMA,
    handler=lambda args, **kw: clarify_tool(
        question=args.get("question", ""),
        choices=args.get("choices"),
        callback=kw.get("callback")),
    check_fn=check_clarify_requirements,
    emoji="❓",
)
