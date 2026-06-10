import textwrap
import pytest
import aiosqlite
from click.testing import CliRunner
from deskbridge.cli import cli
from deskbridge.db.schema import apply_schema


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
    assert "--config" in result.output


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


async def test_cli_status_shows_accounts(config_file, tmp_path):
    db_path = tmp_path / "test.db"
    async with aiosqlite.connect(db_path) as conn:
        await apply_schema(conn)
        await conn.execute(
            "INSERT INTO accounts (id, npub, label, passphrase_ref, session_id, health) "
            "VALUES ('acc-alice', 'npub1alice', 'alice', 'env:ALICE', 'sess-123', 'ok')"
        )
        await conn.commit()

    runner = CliRunner()
    result = runner.invoke(cli, ["status", "--config", str(config_file)])
    assert result.exit_code == 0
    assert "alice" in result.output
    assert "ok" in result.output
    assert "sess-123" in result.output


async def test_status_shows_all_sections(config_file, tmp_path):
    db_path = tmp_path / "test.db"
    async with aiosqlite.connect(db_path) as conn:
        conn.row_factory = aiosqlite.Row
        await apply_schema(conn)
        await conn.execute(
            "INSERT INTO accounts (id, npub, label, passphrase_ref, health) "
            "VALUES ('acc-alice', 'npub1alice', 'alice', 'env:X', 'ok')"
        )
        await conn.execute(
            "INSERT INTO work_items (id, source_type, source_id, identity_id, "
            "status, idempotency_key, summary) "
            "VALUES ('wi-1', 'dm', 'msg-1', 'acc-alice', 'pending', 'k1', 'fix the bug')"
        )
        await conn.execute(
            "INSERT INTO approvals (id, action_description, status) "
            "VALUES ('appr-1', 'pay invoice', 'pending')"
        )
        await conn.execute(
            "INSERT INTO agent_runs (id, work_item_id, adapter_type, status) "
            "VALUES ('run-1', 'wi-1', 'claude-code', 'done')"
        )
        await conn.execute(
            "INSERT INTO cursors (id, cursor_type, identity_id, raw_json) "
            "VALUES ('cur-1', 'dm', 'acc-alice', '{}')"
        )
        await conn.commit()

    runner = CliRunner()
    result = runner.invoke(cli, ["status", "--config", str(config_file)])
    assert result.exit_code == 0
    assert "Accounts" in result.output
    assert "Work Queue" in result.output
    assert "Approvals" in result.output
    assert "Recent Runs" in result.output
    assert "Watchers" in result.output
    assert "alice" in result.output
    assert "claude-code" in result.output
    assert "done" in result.output
