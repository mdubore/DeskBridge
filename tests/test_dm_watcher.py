import asyncio
import json
import pytest
from unittest.mock import AsyncMock, MagicMock
from deskbridge.dm.watcher import DmWatcher
from deskbridge.mcp.client import McpToolError
from deskbridge.mcp.errors import RoutingDecision
from deskbridge.models import McpError, McpErrorCategory


def make_watcher(store, shutdown_event, *, call_tool_mock, session_id="sess-123"):
    mock_client = MagicMock()
    mock_client.call_tool = call_tool_mock
    mock_broker = MagicMock()
    mock_broker.get_session_id = AsyncMock(return_value=session_id)
    return DmWatcher(
        identity_label="alice",
        store=store,
        client=mock_client,
        broker=mock_broker,
        shutdown_event=shutdown_event,
        poll_timeout_secs=1,
    )


async def test_dm_watcher_creates_work_item_and_cursor(store):
    shutdown_event = asyncio.Event()

    async def one_shot(tool_name, arguments):
        shutdown_event.set()
        return {
            "messages": [{"id": "msg-1", "content": "hello operator"}],
            "last_message_id": "msg-1",
            "last_created_at": "2026-01-01T00:00:00Z",
        }

    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    watcher = make_watcher(store, shutdown_event, call_tool_mock=AsyncMock(side_effect=one_shot))
    await watcher.run()

    async with store._conn.execute("SELECT * FROM work_items") as cur:
        rows = await cur.fetchall()
    assert len(rows) == 1
    assert rows[0]["source_type"] == "dm"
    assert rows[0]["source_id"] == "msg-1"
    assert rows[0]["idempotency_key"] == "msg-1"
    assert rows[0]["identity_id"] == "acc-alice"
    assert rows[0]["summary"] == "hello operator"

    cursor_row = await store.get_cursor("dm_watcher", "acc-alice")
    assert cursor_row is not None
    assert cursor_row["last_entity_id"] == "msg-1"
    assert cursor_row["last_created_at"] == "2026-01-01T00:00:00Z"


async def test_dm_watcher_empty_response_no_row_no_cursor(store):
    shutdown_event = asyncio.Event()

    async def one_shot(tool_name, arguments):
        shutdown_event.set()
        return {"messages": [], "last_message_id": None, "last_created_at": None}

    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    watcher = make_watcher(store, shutdown_event, call_tool_mock=AsyncMock(side_effect=one_shot))
    await watcher.run()

    async with store._conn.execute("SELECT COUNT(*) FROM work_items") as cur:
        row = await cur.fetchone()
    assert row[0] == 0

    cursor_row = await store.get_cursor("dm_watcher", "acc-alice")
    assert cursor_row is None


async def test_dm_watcher_idempotent_on_replay(store):
    shutdown_event = asyncio.Event()
    call_count = 0

    async def two_shot(tool_name, arguments):
        nonlocal call_count
        call_count += 1
        if call_count >= 2:
            shutdown_event.set()
        return {
            "messages": [{"id": "msg-1", "content": "hello"}],
            "last_message_id": "msg-1",
            "last_created_at": "2026-01-01T00:00:00Z",
        }

    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    watcher = make_watcher(store, shutdown_event, call_tool_mock=AsyncMock(side_effect=two_shot))
    await watcher.run()

    async with store._conn.execute("SELECT COUNT(*) FROM work_items") as cur:
        row = await cur.fetchone()
    assert row[0] == 1


async def test_dm_watcher_reauth_does_not_crash(store):
    shutdown_event = asyncio.Event()
    call_count = 0

    async def reauth_then_stop(tool_name, arguments):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise McpToolError(
                mcp_error=McpError(category=McpErrorCategory.INVALID_SESSION, message="reauth"),
                routing=RoutingDecision.REAUTH,
            )
        shutdown_event.set()
        return {"messages": [], "last_message_id": None, "last_created_at": None}

    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    watcher = make_watcher(store, shutdown_event, call_tool_mock=AsyncMock(side_effect=reauth_then_stop))
    await watcher.run()
    assert call_count == 2


async def test_dm_watcher_reject_exits_cleanly(store):
    shutdown_event = asyncio.Event()

    async def reject(tool_name, arguments):
        raise McpToolError(
            mcp_error=McpError(category=McpErrorCategory.UNSUPPORTED_STATE, message="rejected"),
            routing=RoutingDecision.REJECT,
        )

    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    mock_client = MagicMock()
    mock_client.call_tool = AsyncMock(side_effect=reject)
    mock_broker = MagicMock()
    mock_broker.get_session_id = AsyncMock(return_value="sess-123")
    watcher = DmWatcher(
        identity_label="alice",
        store=store,
        client=mock_client,
        broker=mock_broker,
        shutdown_event=shutdown_event,
        poll_timeout_secs=1,
    )
    await watcher.run()
    assert not shutdown_event.is_set()


async def test_dm_watcher_no_session_skips_poll(store):
    shutdown_event = asyncio.Event()
    mock_client = MagicMock()
    mock_client.call_tool = AsyncMock()
    mock_broker = MagicMock()
    mock_broker.get_session_id = AsyncMock(return_value=None)

    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    watcher = DmWatcher(
        identity_label="alice",
        store=store,
        client=mock_client,
        broker=mock_broker,
        shutdown_event=shutdown_event,
        poll_timeout_secs=1,
    )

    async def stop():
        await asyncio.sleep(0.05)
        shutdown_event.set()

    await asyncio.gather(watcher.run(), stop())
    mock_client.call_tool.assert_not_awaited()
