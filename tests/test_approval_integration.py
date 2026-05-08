import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from deskbridge.dm.approval_watcher import ApprovalRequestWatcher
from deskbridge.dm.watcher import DmWatcher
from deskbridge.mcp.client import McpToolError
from deskbridge.mcp.errors import RoutingDecision
from deskbridge.models import McpError, McpErrorCategory

OPERATOR_NPUB = "npub1op"
SESSION_ID = "session-123"


def make_dm_watcher(store, shutdown_event, *, dm_messages, respond_result=None,
                    respond_side_effect=None, session_id=SESSION_ID):
    respond_calls = []

    async def call_tool_side_effect(tool_name, arguments):
        if tool_name == "wait_for_new_dms":
            shutdown_event.set()
            return {
                "messages": dm_messages,
                "last_message_id": dm_messages[-1]["id"] if dm_messages else None,
                "last_created_at": "2026-01-01T00:00:00Z",
            }
        elif tool_name == "respond_to_approval":
            respond_calls.append(arguments)
            if respond_side_effect is not None:
                raise respond_side_effect
            return respond_result or {"ok": True, "approval_request_id": arguments["approval_request_id"], "status": "approved"}

    mock_client = MagicMock()
    mock_client.call_tool = AsyncMock(side_effect=call_tool_side_effect)
    mock_broker = MagicMock()
    mock_broker.get_session_id = AsyncMock(return_value=session_id)
    watcher = DmWatcher(
        identity_label="alice",
        store=store,
        client=mock_client,
        broker=mock_broker,
        shutdown_event=shutdown_event,
        operator_npub=OPERATOR_NPUB,
        poll_timeout_secs=1,
    )
    return watcher, respond_calls


async def test_approval_round_trip(store):
    """Full flow: watcher polls → DB row + outbox DM → second poll deduplicates → operator approves → respond_to_approval called with correct args."""
    await store.upsert_account(
        id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X"
    )

    pending_req = {
        "id": "req-abc",
        "tool_name": "pay_invoice",
        "display_payload_json": '{"amount": 100}',
        "expires_at": 1770000000,
    }

    # --- Step 1: First watcher poll ---
    shutdown1 = asyncio.Event()
    call_count = 0

    async def list_one_then_stop(tool_name, arguments):
        nonlocal call_count
        call_count += 1
        if call_count >= 2:
            shutdown1.set()
        return [pending_req]

    mock_client1 = MagicMock()
    mock_client1.call_tool = AsyncMock(side_effect=list_one_then_stop)
    mock_broker1 = MagicMock()
    mock_broker1.get_session_id = AsyncMock(return_value=SESSION_ID)
    watcher1 = ApprovalRequestWatcher(
        identity_label="alice",
        operator_npub=OPERATOR_NPUB,
        store=store,
        client=mock_client1,
        broker=mock_broker1,
        shutdown_event=shutdown1,
        poll_interval_secs=0.01,
    )
    await watcher1.run()

    # Assert: exactly one approvals row
    async with store._conn.execute(
        "SELECT * FROM approvals WHERE mcp_approval_id='req-abc'"
    ) as cur:
        rows = await cur.fetchall()
    assert len(rows) == 1
    assert rows[0]["status"] == "pending"

    # Assert: exactly one outbox DM
    async with store._conn.execute(
        "SELECT COUNT(*) FROM outbox WHERE idempotency_key='approval-notify-req-abc'"
    ) as cur:
        row = await cur.fetchone()
    assert row[0] == 1

    # --- Step 2: Operator "approve" DM ---
    shutdown2 = asyncio.Event()
    dm_messages = [{"id": "msg-op-1", "content": "approve", "sender_pubkey": OPERATOR_NPUB}]

    dm_watcher, respond_calls = make_dm_watcher(
        store, shutdown2,
        dm_messages=dm_messages,
        respond_result={
            "ok": True,
            "approval_request_id": "req-abc",
            "status": "approved",
        },
    )
    await dm_watcher.run()

    # Assert: respond_to_approval called with exact args
    assert len(respond_calls) == 1
    assert respond_calls[0]["session_id"] == SESSION_ID
    assert respond_calls[0]["approval_request_id"] == "req-abc"
    assert respond_calls[0]["approved"] is True
    assert respond_calls[0]["note"] is None

    # Assert: local approval resolved
    async with store._conn.execute(
        "SELECT status FROM approvals WHERE mcp_approval_id='req-abc'"
    ) as cur:
        row = await cur.fetchone()
    assert row["status"] == "approved"

    # Assert: operator reply outbox message says "Approved."
    async with store._conn.execute(
        "SELECT message_text FROM outbox WHERE idempotency_key='approve-reply-msg-op-1'"
    ) as cur:
        row = await cur.fetchone()
    assert row is not None
    assert row["message_text"] == "Approved."


async def test_approval_sdk_exception_terminal(store):
    """SDK exception with .data (Path 2) flows through real McpClient.call_tool and is handled
    as approval_expired by DmWatcher — not as unknown/internal error.

    This test uses a real McpClient with a mocked _session so that McpClient.call_tool
    actually executes its SDK exception catch path.
    """
    from deskbridge.mcp.client import McpClient

    await store.upsert_account(
        id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X"
    )

    await store.insert_approval(
        id="local-appr-1",
        mcp_approval_id="req-sdk-exc",
        work_item_id=None,
        action_description="pay_invoice: ...",
        scope=None,
        request_text=None,
        expires_at=None,
        identity_id="acc-alice",
    )

    # SDK exception that carries .data — this is what NostrDesk raises in Path 2
    sdk_exc = Exception("approval window closed")
    sdk_exc.data = {
        "category": "approval_expired",
        "approval_request_id": "req-sdk-exc",
    }

    # Real McpClient with a mocked _session so call_tool actually runs
    real_client = McpClient.__new__(McpClient)
    real_client._session = MagicMock()

    dm_call_count = 0

    async def session_call_tool(tool_name, arguments):
        nonlocal dm_call_count
        if tool_name == "wait_for_new_dms":
            dm_call_count += 1
            # Return a fake result object (isError=False, content=[msg])
            fake_result = MagicMock()
            fake_result.isError = False
            fake_result.content = [MagicMock(text='{"messages": [{"id": "msg-op-2", "content": "approve", "sender_pubkey": "' + OPERATOR_NPUB + '"}], "last_message_id": "msg-op-2", "last_created_at": "2026-01-01T00:00:00Z"}')]
            return fake_result
        elif tool_name == "respond_to_approval":
            raise sdk_exc  # triggers Path 2 catch in McpClient.call_tool

    real_client._session.call_tool = AsyncMock(side_effect=session_call_tool)

    shutdown = asyncio.Event()
    mock_broker = MagicMock()
    mock_broker.get_session_id = AsyncMock(return_value=SESSION_ID)

    dm_watcher = DmWatcher(
        identity_label="alice",
        store=store,
        client=real_client,
        broker=mock_broker,
        shutdown_event=shutdown,
        operator_npub=OPERATOR_NPUB,
        poll_timeout_secs=1,
    )

    async def stop_after_first_dm():
        while dm_call_count == 0:
            await asyncio.sleep(0.01)
        # Poll until the DB write lands (DmWatcher may still be in flight)
        for _ in range(50):
            await asyncio.sleep(0.02)
            async with store._conn.execute(
                "SELECT status FROM approvals WHERE mcp_approval_id='req-sdk-exc'"
            ) as cur:
                row = await cur.fetchone()
            if row and row["status"] != "pending":
                break
        shutdown.set()

    await asyncio.gather(dm_watcher.run(), stop_after_first_dm())

    # McpClient.call_tool must have caught the SDK exception and converted it to McpToolError
    # with category="approval_expired". DmWatcher must handle it as terminal.
    async with store._conn.execute(
        "SELECT status FROM approvals WHERE mcp_approval_id='req-sdk-exc'"
    ) as cur:
        row = await cur.fetchone()
    assert row["status"] == "rejected"   # NOT "pending"

    async with store._conn.execute(
        "SELECT message_text FROM outbox WHERE idempotency_key='approve-reply-msg-op-2'"
    ) as cur:
        row = await cur.fetchone()
    assert row is not None
    assert "already resolved or" in row["message_text"] and "expired" in row["message_text"]
