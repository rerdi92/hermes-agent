"""Desktop auto-skill compact payload tests."""

from pathlib import Path
import threading

from tui_gateway import server


def test_auto_skills_prompt_uses_compact_payload(monkeypatch, tmp_path):
    """Desktop FLT/ULW/ULR toggles must not inject full SKILL.md bodies."""
    from agent import skill_commands

    huge_body = "HEAVY_SKILL_BODY " * 2000
    skill_dir = tmp_path / "heavy-skill"
    skill_dir.mkdir()

    def fake_load(name, task_id=None):
        return (
            {
                "name": name,
                "description": f"Description for {name}",
                "content": huge_body,
                "linked_files": {},
            },
            skill_dir,
            name,
        )

    monkeypatch.setattr(skill_commands, "_load_skill_payload", fake_load)

    msg = server._auto_skills_prompt(
        "make Hermes lighter",
        ["hq-agent-collaboration", "ulw", "ultraresearch"],
        task_id="desktop-session",
    )

    assert isinstance(msg, str)
    assert "hq-agent-collaboration" in msg
    assert "ulw" in msg
    assert "ultraresearch" in msg
    assert "make Hermes lighter" in msg
    assert "HEAVY_SKILL_BODY" not in msg
    assert len(msg) < 3000


def test_auto_skills_prompt_ignores_unknown_and_deduplicates(monkeypatch, tmp_path):
    from agent import skill_commands

    skill_dir = tmp_path / "skill"
    skill_dir.mkdir()
    loaded_names: list[str] = []

    def fake_load(name, task_id=None):
        loaded_names.append(name)
        if name == "missing":
            return None
        return ({"name": name, "description": "desc", "content": "x" * 10000}, skill_dir, name)

    monkeypatch.setattr(skill_commands, "_load_skill_payload", fake_load)

    msg = server._auto_skills_prompt("do it", ["ulw", "missing", "ulw"], task_id="s")

    assert loaded_names == ["ulw"]
    assert msg.count("Skill: ulw") == 1
    assert "missing" not in msg
    assert msg.endswith("do it")


def test_busy_queue_preserves_auto_skills_until_drained(monkeypatch, tmp_path):
    """A mid-turn Desktop mode prompt must not lose auto_skills while queued."""
    from agent import skill_commands

    skill_dir = tmp_path / "skill"
    skill_dir.mkdir()

    def fake_load(name, task_id=None):
        return ({"name": name, "description": "desc", "content": "x" * 10000}, skill_dir, name)

    captured: list[str] = []
    session = {
        "history_lock": threading.RLock(),
        "running": True,
        "session_key": "desktop-session",
    }

    monkeypatch.setattr(skill_commands, "_load_skill_payload", fake_load)
    monkeypatch.setattr(server, "_load_busy_input_mode", lambda: "queue")
    monkeypatch.setattr(
        server,
        "_run_prompt_submit",
        lambda _rid, _sid, _session, text: captured.append(text),
    )

    server._handle_busy_submit(
        "rid",
        "sid",
        session,
        "do it",
        None,
        auto_skills=["ulw"],
    )

    session["running"] = False
    assert server._drain_queued_prompt("rid2", "sid", session) is True

    assert len(captured) == 1
    assert "Skill: ulw" in captured[0]
    assert "HEAVY_SKILL_BODY" not in captured[0]
    assert captured[0].endswith("do it")
