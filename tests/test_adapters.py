import pytest
from deskbridge.agent.adapters import build_command, KNOWN_ADAPTERS


def test_claude_code_command():
    cmd = build_command("claude-code", "/repo", "do the thing")
    assert cmd == ["claude", "--project", "/repo", "--message", "do the thing"]


def test_codex_command():
    cmd = build_command("codex", "/repo", "do the thing")
    assert cmd == ["codex", "--cwd", "/repo", "--no-interactive", "do the thing"]


def test_gemini_command():
    cmd = build_command("gemini", "/repo", "do the thing")
    assert cmd == ["gemini", "--yolo", "-p", "do the thing"]


def test_unknown_adapter_raises():
    with pytest.raises(ValueError, match="Unknown adapter"):
        build_command("hermes", "/repo", "do the thing")


def test_known_adapters_contains_all_three():
    assert KNOWN_ADAPTERS == {"claude-code", "codex", "gemini"}
