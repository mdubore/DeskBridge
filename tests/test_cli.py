import textwrap
import pytest
from click.testing import CliRunner
from deskbridge.cli import cli


@pytest.fixture
def config_file(tmp_path, monkeypatch):
    monkeypatch.setenv("ALICE", "passphrase")
    content = textwrap.dedent(f"""
        [supervisor]
        db_path = "{tmp_path}/test.db"

        [mcp]
        command = "nostrdesk-mcp"

        [[identities]]
        label = "alice"
        npub = "npub1alice"
        passphrase_ref = "env:ALICE"
    """)
    cfg = tmp_path / "config.toml"
    cfg.write_text(content)
    return cfg


def test_cli_has_start_command():
    runner = CliRunner()
    result = runner.invoke(cli, ["start", "--help"])
    assert result.exit_code == 0
    assert "config" in result.output.lower() or "--config" in result.output


def test_cli_has_status_command():
    runner = CliRunner()
    result = runner.invoke(cli, ["status", "--help"])
    assert result.exit_code == 0


def test_cli_start_missing_config_exits_nonzero():
    runner = CliRunner()
    result = runner.invoke(cli, ["start", "--config", "/nonexistent/config.toml"])
    assert result.exit_code != 0


def test_cli_status_no_db_shows_not_running(config_file):
    runner = CliRunner()
    result = runner.invoke(cli, ["status", "--config", str(config_file)])
    assert result.exit_code == 0
    assert "not running" in result.output.lower() or "no database" in result.output.lower()
