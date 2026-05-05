import asyncio
import uuid
from unittest.mock import AsyncMock, MagicMock
from deskbridge.dm.approval_watcher import ApprovalRequestWatcher
from deskbridge.mcp.client import McpToolError
from deskbridge.mcp.errors import RoutingDecision
from deskbridge.models import McpError, McpErrorCategory

OPERATOR_NPUB = "npub1op"


def make_watcher(store, shutdown_event, *, call_tool_mock, session_id="sess-123",
                 operator_npub=OPERATOR_NPUB):
    mock_client = MagicMock()
    mock_client.call_tool = call_tool_mock
    mock_broker = MagicMock()
    mock_broker.get_session_id = AsyncMock(return_value=session_id)
    return ApprovalRequestWatcher(
        identity_label="alice",
        operator_npub=operator_npub,
        store=store,
        client=mock_client,
        broker=mock_broker,
        shutdown_event=shutdown_event,
        poll_timeout_secs=1,
    )


def _approval_response(*requests, last_id="req-1"):
    return {
        "requests": list(requests),
        "last_request_id": last_id,
    }


def _req(id="req-1", description="Force push to main", triggered_by_tool="git_push",
         created_at="2026-01-01T00:00:00Z"):
    return {"id": id, "description": description,
            "triggered_by_tool": triggered_by_tool, "created_at": created_at}


async def test_approval_watcher_new_request_inserts_approval(store):
    shutdown_event = asyncio.Event()

    async def one_shot(tool_name, arguments):
        shutdown_event.set()
        return _approval_response(_req())

    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    watcher = make_watcher(store, shutdown_event, call_tool_mock=AsyncMock(side_effect=one_shot))
    await watcher.run()

    async with store._conn.execute("SELECT * FROM approvals WHERE mcp_approval_id='req-1'") as cur:
        row = await cur.fetchone()
    assert row is not None
    assert row["mcp_approval_id"] == "req-1"
    assert row["action_description"] == "Force push to main"
    assert row["status"] == "pending"


async def test_approval_watcher_new_request_notifies_operator(store):
    shutdown_event = asyncio.Event()

    async def one_shot(tool_name, arguments):
        shutdown_event.set()
        return _approval_response(_req(id="req-1", description="Delete production DB"))

    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    watcher = make_watcher(store, shutdown_event, call_tool_mock=AsyncMock(side_effect=one_shot))
    await watcher.run()

    async with store._conn.execute("SELECT message_text, dest_pubkey FROM outbox") as cur:
        rows = await cur.fetchall()
    assert len(rows) == 1
    assert "Delete production DB" in rows[0]["message_text"]
    assert rows[0]["dest_pubkey"] == OPERATOR_NPUB


async def test_approval_watcher_cursor_advances(store):
    shutdown_event = asyncio.Event()

    async def one_shot(tool_name, arguments):
        shutdown_event.set()
        return _approval_response(_req(id="req-42"), last_id="req-42")

    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    watcher = make_watcher(store, shutdown_event, call_tool_mock=AsyncMock(side_effect=one_shot))
    await watcher.run()

    cursor_row = await store.get_cursor("approval_watcher", "acc-alice")
    assert cursor_row is not None
    assert cursor_row["last_entity_id"] == "req-42"


async def test_approval_watcher_empty_response_no_insert_no_cursor(store):
    shutdown_event = asyncio.Event()

    async def one_shot(tool_name, arguments):
        shutdown_event.set()
        return {"requests": [], "last_request_id": None}

    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    watcher = make_watcher(store, shutdown_event, call_tool_mock=AsyncMock(side_effect=one_shot))
    await watcher.run()

    async with store._conn.execute("SELECT COUNT(*) FROM approvals") as cur:
        row = await cur.fetchone()
    assert row[0] == 0
    assert await store.get_cursor("approval_watcher", "acc-alice") is None


async def test_approval_watcher_no_session_skips_poll(store):
    shutdown_event = asyncio.Event()
    mock_client = MagicMock()
    mock_client.call_tool = AsyncMock()
    mock_broker = MagicMock()
    mock_broker.get_session_id = AsyncMock(return_value=None)

    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    watcher = ApprovalRequestWatcher(
        identity_label="alice",
        operator_npub=OPERATOR_NPUB,
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


async def test_approval_watcher_reject_exits_cleanly(store):
    shutdown_event = asyncio.Event()

    async def reject(tool_name, arguments):
        raise McpToolError(
            mcp_error=McpError(category=McpErrorCategory.UNSUPPORTED_STATE, message="rejected"),
            routing=RoutingDecision.REJECT,
        )

    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    watcher = make_watcher(store, shutdown_event, call_tool_mock=AsyncMock(side_effect=reject))
    await watcher.run()
    assert not shutdown_event.is_set()
