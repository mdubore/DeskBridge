import asyncio
import pytest
import aiosqlite
from unittest.mock import AsyncMock, MagicMock, patch
from contextlib import contextmanager
from deskbridge.supervisor import Supervisor
from deskbridge.config import DeskBridgeConfig, SupervisorConfig, McpConfig, IdentityConfig


def make_config(tmp_path) -> DeskBridgeConfig:
    return DeskBridgeConfig(
        supervisor=SupervisorConfig(
            db_path=str(tmp_path / "test.db"),
            heartbeat_interval_secs=0.05,
        ),
        mcp=McpConfig(command="nostrdesk-mcp"),
        identities=[
            IdentityConfig(label="alice", npub="npub1alice", passphrase_ref="env:ALICE")
        ],
    )


@pytest.fixture
def mock_broker():
    broker = AsyncMock()
    broker.unlock_all = AsyncMock()
    broker.refresh_if_needed = AsyncMock()
    return broker


@pytest.fixture
def mock_client_ctx():
    ctx = AsyncMock()
    ctx.__aenter__ = AsyncMock(return_value=AsyncMock())
    ctx.__aexit__ = AsyncMock(return_value=False)
    return ctx


async def test_supervisor_starts_and_stops(tmp_path, monkeypatch, mock_broker, mock_client_ctx):
    monkeypatch.setenv("ALICE", "pass")
    config = make_config(tmp_path)

    with patch("deskbridge.supervisor.McpClient") as MockMcpClient, \
         patch("deskbridge.supervisor.SessionBroker", return_value=mock_broker):
        mock_instance = MockMcpClient.return_value
        mock_instance.connect.return_value = mock_client_ctx

        supervisor = Supervisor(config=config)

        async def stop_after_short_delay():
            await asyncio.sleep(0.1)
            supervisor.request_shutdown()

        await asyncio.gather(
            supervisor.run(),
            stop_after_short_delay(),
        )

    mock_broker.unlock_all.assert_called_once()

    async with aiosqlite.connect(tmp_path / "test.db") as conn:
        conn.row_factory = aiosqlite.Row
        row_cursor = await conn.execute(
            "SELECT id FROM accounts WHERE id = 'acc-alice'"
        )
        row = await row_cursor.fetchone()
    assert row is not None, "bootstrap_accounts_from_config must insert the account row on startup"


async def test_supervisor_calls_refresh_on_heartbeat(tmp_path, monkeypatch, mock_broker, mock_client_ctx):
    monkeypatch.setenv("ALICE", "pass")
    config = make_config(tmp_path)

    with patch("deskbridge.supervisor.McpClient") as MockMcpClient, \
         patch("deskbridge.supervisor.SessionBroker", return_value=mock_broker), \
         patch("deskbridge.supervisor.apply_schema", new=AsyncMock()), \
         patch("deskbridge.supervisor.bootstrap_accounts_from_config", new=AsyncMock()):
        mock_instance = MockMcpClient.return_value
        mock_instance.connect.return_value = mock_client_ctx

        supervisor = Supervisor(config=config)

        async def stop_after_heartbeats():
            await asyncio.sleep(0.3)
            supervisor.request_shutdown()

        await asyncio.gather(
            supervisor.run(),
            stop_after_heartbeats(),
        )

    assert mock_broker.refresh_if_needed.call_count >= 3
