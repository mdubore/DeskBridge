import asyncio
from unittest.mock import AsyncMock, MagicMock

from deskbridge.dm.approval_decision_poller import ApprovalDecisionPoller
from deskbridge.mcp.client import McpToolError
from deskbridge.mcp.errors import RoutingDecision
from deskbridge.models import McpError, McpErrorCategory


def make_poller(store, shutdown_event, *, call_tool_mock=None, session_id="sess-123"):
    mock_client = MagicMock()
    mock_client.call_tool = call_tool_mock or AsyncMock()
    mock_broker = MagicMock()
    mock_broker.get_session_id = AsyncMock(return_value=session_id)
    poller = ApprovalDecisionPoller(
        identity_label="alice",
        store=store,
        client=mock_client,
        broker=mock_broker,
        shutdown_event=shutdown_event,
        poll_interval_secs=0.05,
    )
    return poller, mock_client


async def _seed_account(store):
    await store.upsert_account(
        id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X"
    )


async def _seed_requested_approval(db_conn, *, id="appr-1", identity_id="acc-alice",
                                   status="approve_requested", mcp_approval_id=None):
    await db_conn.execute(
        "INSERT INTO approvals (id, identity_id, mcp_approval_id, action_description, status) "
        "VALUES (?, ?, ?, 'pay invoice', ?)",
        (id, identity_id, mcp_approval_id, status),
    )
    await db_conn.commit()


async def test_local_approve_request_resolves_approved(store, db_conn):
    await _seed_account(store)
    await _seed_requested_approval(db_conn)
    poller, client = make_poller(store, asyncio.Event())
    await poller._poll_once()
    row = await store.get_approval("appr-1")
    assert row["status"] == "approved"
    client.call_tool.assert_not_awaited()
    audits = await store.get_audit_events("approval_resolved")
    assert len(audits) == 1


async def test_local_reject_request_resolves_rejected(store, db_conn):
    await _seed_account(store)
    await _seed_requested_approval(db_conn, status="reject_requested")
    poller, _client = make_poller(store, asyncio.Event())
    await poller._poll_once()
    row = await store.get_approval("appr-1")
    assert row["status"] == "rejected"


async def test_mcp_correlated_decision_forwards_to_mcp(store, db_conn):
    await _seed_account(store)
    await _seed_requested_approval(db_conn, mcp_approval_id="req-9")
    call_tool = AsyncMock(return_value={
        "ok": True, "approval_request_id": "req-9", "status": "approved",
    })
    poller, client = make_poller(store, asyncio.Event(), call_tool_mock=call_tool)
    await poller._poll_once()
    client.call_tool.assert_awaited_once()
    tool_name, args = client.call_tool.await_args.args
    assert tool_name == "respond_to_approval"
    assert args["approval_request_id"] == "req-9"
    assert args["approved"] is True
    row = await store.get_approval("appr-1")
    assert row["status"] == "approved"


async def test_mcp_failure_leaves_row_requested_for_retry(store, db_conn):
    await _seed_account(store)
    await _seed_requested_approval(db_conn, mcp_approval_id="req-9")
    err = McpToolError(
        mcp_error=McpError(category=McpErrorCategory.TRANSIENT_TRANSPORT, message="boom"),
        routing=RoutingDecision.RETRY,
    )
    poller, _client = make_poller(
        store, asyncio.Event(), call_tool_mock=AsyncMock(side_effect=err)
    )
    await poller._poll_once()
    row = await store.get_approval("appr-1")
    assert row["status"] == "approve_requested"


async def test_no_session_skips_mcp_correlated_row(store, db_conn):
    await _seed_account(store)
    await _seed_requested_approval(db_conn, mcp_approval_id="req-9")
    poller, client = make_poller(store, asyncio.Event(), session_id=None)
    await poller._poll_once()
    client.call_tool.assert_not_awaited()
    row = await store.get_approval("appr-1")
    assert row["status"] == "approve_requested"


async def test_ignores_other_identity_rows(store, db_conn):
    await _seed_account(store)
    await store.upsert_account(
        id="acc-bob", npub="npub1bob", label="bob", passphrase_ref="env:Y"
    )
    await _seed_requested_approval(db_conn, identity_id="acc-bob")
    poller, _client = make_poller(store, asyncio.Event())
    await poller._poll_once()
    row = await store.get_approval("appr-1")
    assert row["status"] == "approve_requested"


async def test_run_loop_processes_and_stops_on_shutdown(store, db_conn):
    await _seed_account(store)
    await _seed_requested_approval(db_conn)
    shutdown = asyncio.Event()
    poller, _client = make_poller(store, shutdown)

    async def stop():
        await asyncio.sleep(0.12)
        shutdown.set()

    await asyncio.gather(poller.run(), stop())
    row = await store.get_approval("appr-1")
    assert row["status"] == "approved"
