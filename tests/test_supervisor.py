import asyncio
import pytest
import aiosqlite
from unittest.mock import AsyncMock, MagicMock, patch, Mock, ANY
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
         patch("deskbridge.supervisor.bootstrap_accounts_from_config", new=AsyncMock()), \
         patch("deskbridge.supervisor.Store") as MockStore:
        mock_instance = MockMcpClient.return_value
        mock_instance.connect.return_value = mock_client_ctx

        mock_store_instance = AsyncMock()
        mock_store_instance.get_project_groups = AsyncMock(return_value=[])
        MockStore.return_value = mock_store_instance

        supervisor = Supervisor(config=config)

        async def stop_after_heartbeats():
            await asyncio.sleep(0.3)
            supervisor.request_shutdown()

        await asyncio.gather(
            supervisor.run(),
            stop_after_heartbeats(),
        )

    assert mock_broker.refresh_if_needed.call_count >= 3


async def test_supervisor_spawns_and_cancels_dm_tasks(tmp_path, monkeypatch, mock_broker, mock_client_ctx):
    monkeypatch.setenv("ALICE", "pass")
    config = make_config(tmp_path)

    with patch("deskbridge.supervisor.McpClient") as MockMcpClient, \
         patch("deskbridge.supervisor.SessionBroker", return_value=mock_broker), \
         patch("deskbridge.supervisor.apply_schema", new=AsyncMock()), \
         patch("deskbridge.supervisor.bootstrap_accounts_from_config", new=AsyncMock()), \
         patch("deskbridge.supervisor.DmWatcher") as MockDmWatcher, \
         patch("deskbridge.supervisor.OutboxDrainer") as MockOutboxDrainer, \
         patch("deskbridge.supervisor.WorkItemPoller") as MockWorkItemPoller, \
         patch("deskbridge.supervisor.GroupWatcher") as MockGroupWatcher, \
         patch("deskbridge.supervisor.Store") as MockStore:

        mock_instance = MockMcpClient.return_value
        mock_instance.connect.return_value = mock_client_ctx

        async def never_finishes():
            await asyncio.Event().wait()

        MockDmWatcher.return_value.run = Mock(side_effect=never_finishes)
        MockOutboxDrainer.return_value.run = Mock(side_effect=never_finishes)
        MockWorkItemPoller.return_value.run = Mock(side_effect=never_finishes)

        mock_store_instance = AsyncMock()
        mock_store_instance.get_project_groups = AsyncMock(return_value=[])
        MockStore.return_value = mock_store_instance

        supervisor = Supervisor(config=config)

        async def stop():
            await asyncio.sleep(0.1)
            supervisor.request_shutdown()

        await asyncio.gather(supervisor.run(), stop())

    MockDmWatcher.assert_called_once_with(
        identity_label="alice",
        store=ANY,
        client=ANY,
        broker=mock_broker,
        shutdown_event=ANY,
        operator_npub=None,
    )
    MockOutboxDrainer.assert_called_once_with(
        store=ANY,
        client=ANY,
        broker=mock_broker,
        identities=config.identities,
        shutdown_event=ANY,
    )
    MockDmWatcher.return_value.run.assert_called_once()
    MockOutboxDrainer.return_value.run.assert_called_once()


async def test_supervisor_spawns_and_cancels_poller_tasks(tmp_path, monkeypatch, mock_broker, mock_client_ctx):
    monkeypatch.setenv("ALICE", "pass")
    config = make_config(tmp_path)

    with patch("deskbridge.supervisor.McpClient") as MockMcpClient, \
         patch("deskbridge.supervisor.SessionBroker", return_value=mock_broker), \
         patch("deskbridge.supervisor.apply_schema", new=AsyncMock()), \
         patch("deskbridge.supervisor.bootstrap_accounts_from_config", new=AsyncMock()), \
         patch("deskbridge.supervisor.DmWatcher") as MockDmWatcher, \
         patch("deskbridge.supervisor.OutboxDrainer") as MockOutboxDrainer, \
         patch("deskbridge.supervisor.WorkItemPoller") as MockWorkItemPoller, \
         patch("deskbridge.supervisor.GroupWatcher") as MockGroupWatcher, \
         patch("deskbridge.supervisor.Store") as MockStore:

        mock_instance = MockMcpClient.return_value
        mock_instance.connect.return_value = mock_client_ctx

        async def never_finishes():
            await asyncio.Event().wait()

        MockDmWatcher.return_value.run = Mock(side_effect=never_finishes)
        MockOutboxDrainer.return_value.run = Mock(side_effect=never_finishes)
        MockWorkItemPoller.return_value.run = Mock(side_effect=never_finishes)

        mock_store_instance = AsyncMock()
        mock_store_instance.get_project_groups = AsyncMock(return_value=[])
        MockStore.return_value = mock_store_instance

        supervisor = Supervisor(config=config)

        async def stop():
            await asyncio.sleep(0.1)
            supervisor.request_shutdown()

        await asyncio.gather(supervisor.run(), stop())

    MockWorkItemPoller.assert_called_once_with(
        identity_label="alice",
        store=ANY,
        client=ANY,
        broker=mock_broker,
        config=config,
        shutdown_event=ANY,
    )
    MockWorkItemPoller.return_value.run.assert_called_once()


async def test_supervisor_spawns_group_watcher_when_groups_configured(tmp_path, monkeypatch, mock_broker, mock_client_ctx):
    monkeypatch.setenv("ALICE", "pass")
    config = make_config(tmp_path)

    with patch("deskbridge.supervisor.McpClient") as MockMcpClient, \
         patch("deskbridge.supervisor.SessionBroker", return_value=mock_broker), \
         patch("deskbridge.supervisor.apply_schema", new=AsyncMock()), \
         patch("deskbridge.supervisor.bootstrap_accounts_from_config", new=AsyncMock()), \
         patch("deskbridge.supervisor.DmWatcher") as MockDmWatcher, \
         patch("deskbridge.supervisor.OutboxDrainer") as MockOutboxDrainer, \
         patch("deskbridge.supervisor.WorkItemPoller") as MockWorkItemPoller, \
         patch("deskbridge.supervisor.GroupWatcher") as MockGroupWatcher, \
         patch("deskbridge.supervisor.Store") as MockStore:

        mock_instance = MockMcpClient.return_value
        mock_instance.connect.return_value = mock_client_ctx

        async def never_finishes():
            await asyncio.Event().wait()

        MockDmWatcher.return_value.run = Mock(side_effect=never_finishes)
        MockOutboxDrainer.return_value.run = Mock(side_effect=never_finishes)
        MockWorkItemPoller.return_value.run = Mock(side_effect=never_finishes)
        MockGroupWatcher.return_value.run = Mock(side_effect=never_finishes)

        # get_project_groups returns a non-empty list so GroupWatcher is spawned
        mock_store_instance = AsyncMock()
        mock_store_instance.get_project_groups = AsyncMock(return_value=["grp-1"])
        MockStore.return_value = mock_store_instance

        supervisor = Supervisor(config=config)

        async def stop():
            await asyncio.sleep(0.1)
            supervisor.request_shutdown()

        await asyncio.gather(supervisor.run(), stop())

    MockGroupWatcher.assert_called_once_with(
        identity_label="alice",
        identity_npub="npub1alice",
        operator_npub=None,
        group_ids=["grp-1"],
        store=ANY,
        client=ANY,
        broker=mock_broker,
        shutdown_event=ANY,
    )
    MockGroupWatcher.return_value.run.assert_called_once()


async def test_supervisor_no_group_watcher_when_no_groups(tmp_path, monkeypatch, mock_broker, mock_client_ctx):
    monkeypatch.setenv("ALICE", "pass")
    config = make_config(tmp_path)

    with patch("deskbridge.supervisor.McpClient") as MockMcpClient, \
         patch("deskbridge.supervisor.SessionBroker", return_value=mock_broker), \
         patch("deskbridge.supervisor.apply_schema", new=AsyncMock()), \
         patch("deskbridge.supervisor.bootstrap_accounts_from_config", new=AsyncMock()), \
         patch("deskbridge.supervisor.DmWatcher") as MockDmWatcher, \
         patch("deskbridge.supervisor.OutboxDrainer") as MockOutboxDrainer, \
         patch("deskbridge.supervisor.WorkItemPoller") as MockWorkItemPoller, \
         patch("deskbridge.supervisor.GroupWatcher") as MockGroupWatcher, \
         patch("deskbridge.supervisor.Store") as MockStore:

        mock_instance = MockMcpClient.return_value
        mock_instance.connect.return_value = mock_client_ctx

        async def never_finishes():
            await asyncio.Event().wait()

        MockDmWatcher.return_value.run = Mock(side_effect=never_finishes)
        MockOutboxDrainer.return_value.run = Mock(side_effect=never_finishes)
        MockWorkItemPoller.return_value.run = Mock(side_effect=never_finishes)

        # get_project_groups returns empty — no GroupWatcher should be spawned
        mock_store_instance = AsyncMock()
        mock_store_instance.get_project_groups = AsyncMock(return_value=[])
        MockStore.return_value = mock_store_instance

        supervisor = Supervisor(config=config)

        async def stop():
            await asyncio.sleep(0.1)
            supervisor.request_shutdown()

        await asyncio.gather(supervisor.run(), stop())

    MockGroupWatcher.assert_not_called()


async def test_supervisor_spawns_approval_watcher(tmp_path, monkeypatch, mock_broker, mock_client_ctx):
    monkeypatch.setenv("ALICE", "pass")
    config = make_config(tmp_path)

    with patch("deskbridge.supervisor.McpClient") as MockMcpClient, \
         patch("deskbridge.supervisor.SessionBroker", return_value=mock_broker), \
         patch("deskbridge.supervisor.apply_schema", new=AsyncMock()), \
         patch("deskbridge.supervisor.bootstrap_accounts_from_config", new=AsyncMock()), \
         patch("deskbridge.supervisor.DmWatcher") as MockDmWatcher, \
         patch("deskbridge.supervisor.OutboxDrainer") as MockOutboxDrainer, \
         patch("deskbridge.supervisor.WorkItemPoller") as MockWorkItemPoller, \
         patch("deskbridge.supervisor.GroupWatcher") as MockGroupWatcher, \
         patch("deskbridge.supervisor.ApprovalRequestWatcher") as MockApprovalWatcher, \
         patch("deskbridge.supervisor.Store") as MockStore:

        mock_instance = MockMcpClient.return_value
        mock_instance.connect.return_value = mock_client_ctx

        async def never_finishes():
            await asyncio.Event().wait()

        MockDmWatcher.return_value.run = Mock(side_effect=never_finishes)
        MockOutboxDrainer.return_value.run = Mock(side_effect=never_finishes)
        MockWorkItemPoller.return_value.run = Mock(side_effect=never_finishes)
        MockApprovalWatcher.return_value.run = Mock(side_effect=never_finishes)

        mock_store_instance = AsyncMock()
        mock_store_instance.get_project_groups = AsyncMock(return_value=[])
        MockStore.return_value = mock_store_instance

        supervisor = Supervisor(config=config)

        async def stop():
            await asyncio.sleep(0.1)
            supervisor.request_shutdown()

        await asyncio.gather(supervisor.run(), stop())

    MockApprovalWatcher.assert_called_once_with(
        identity_label="alice",
        operator_npub=None,
        store=ANY,
        client=ANY,
        broker=mock_broker,
        shutdown_event=ANY,
    )
    MockApprovalWatcher.return_value.run.assert_called_once()
