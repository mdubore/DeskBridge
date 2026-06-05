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


def test_hermes_command():
    cmd = build_command("hermes", "/repo", "do the thing")
    assert cmd == ["hermes", "chat", "-Q", "-q", "do the thing"]


def test_openclaw_command():
    cmd = build_command("openclaw", "/repo", "do the thing", agent_id="my-agent")
    assert cmd == ["openclaw", "agent", "--agent", "my-agent", "--local", "--message", "do the thing"]


def test_openclaw_missing_agent_id_raises():
    with pytest.raises(ValueError, match="agent_id is required"):
        build_command("openclaw", "/repo", "do the thing")


def test_unknown_adapter_raises():
    with pytest.raises(ValueError, match="voltron"):
        build_command("voltron", "/repo", "do the thing")


def test_known_adapters_matches_build_command():
    for adapter in KNOWN_ADAPTERS:
        if adapter == "openclaw":
            # openclaw requires agent_id; config validator ensures it's non-None at runtime
            cmd = build_command(adapter, "/repo", "test", agent_id="test-agent")
            assert "--agent" in cmd
            assert "test-agent" in cmd
        else:
            cmd = build_command(adapter, "/repo", "test")
        assert isinstance(cmd, list)
        assert len(cmd) > 0
