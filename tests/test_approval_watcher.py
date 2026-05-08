import asyncio
import json
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
        poll_interval_secs=0.05,
    )


def _req(id="req-1", tool_name="pay_invoice", display_payload_json='{"amount": 100}',
         request_payload_json=None, expires_at=None):
    req = {"id": id, "tool_name": tool_name, "display_payload_json": display_payload_json}
    if request_payload_json is not None:
        req["request_payload_json"] = request_payload_json
    if expires_at is not None:
        req["expires_at"] = expires_at
    return req


async def test_approval_watcher_new_request_inserts_approval(store):
    shutdown_event = asyncio.Event()

    async def one_shot(tool_name, arguments):
        shutdown_event.set()
        return [_req()]

    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    watcher = make_watcher(store, shutdown_event, call_tool_mock=AsyncMock(side_effect=one_shot))
    await watcher.run()

    async with store._conn.execute("SELECT * FROM approvals WHERE mcp_approval_id='req-1'") as cur:
        row = await cur.fetchone()
    assert row is not None
    assert row["mcp_approval_id"] == "req-1"
    assert "pay_invoice" in row["action_description"]
    assert row["status"] == "pending"


async def test_approval_watcher_new_request_notifies_operator(store):
    shutdown_event = asyncio.Event()

    async def one_shot(tool_name, arguments):
        shutdown_event.set()
        return [_req(id="req-1", display_payload_json='{"action": "Delete production DB"}')]

    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    watcher = make_watcher(store, shutdown_event, call_tool_mock=AsyncMock(side_effect=one_shot))
    await watcher.run()

    async with store._conn.execute("SELECT message_text, dest_pubkey FROM outbox") as cur:
        rows = await cur.fetchall()
    assert len(rows) == 1
    assert "Delete production DB" in rows[0]["message_text"]
    assert rows[0]["dest_pubkey"] == OPERATOR_NPUB


async def test_approval_watcher_empty_response_no_insert(store):
    shutdown_event = asyncio.Event()

    async def one_shot(tool_name, arguments):
        shutdown_event.set()
        return []

    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    watcher = make_watcher(store, shutdown_event, call_tool_mock=AsyncMock(side_effect=one_shot))
    await watcher.run()

    async with store._conn.execute("SELECT COUNT(*) FROM approvals") as cur:
        row = await cur.fetchone()
    assert row[0] == 0


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
        poll_interval_secs=0.05,
    )

    async def stop():
        await asyncio.sleep(0.05)
        shutdown_event.set()

    await asyncio.gather(watcher.run(), stop())
    mock_client.call_tool.assert_not_awaited()


async def test_approval_watcher_null_work_item_approval_is_visible_to_dm_watcher(store):
    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    shutdown_event = asyncio.Event()

    async def one_shot(tool_name, arguments):
        shutdown_event.set()
        return [_req(id="req-null-wi", tool_name="risky_tool")]

    watcher = make_watcher(store, shutdown_event, call_tool_mock=AsyncMock(side_effect=one_shot))
    await watcher.run()

    row = await store.get_pending_approval("acc-alice")
    assert row is not None
    assert row["mcp_approval_id"] == "req-null-wi"
    assert row["identity_id"] == "acc-alice"


async def test_approval_watcher_idempotent_on_replay(store):
    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    call_count = 0
    shutdown_event = asyncio.Event()

    async def two_shot(tool_name, arguments):
        nonlocal call_count
        call_count += 1
        if call_count >= 2:
            shutdown_event.set()
        return [_req(id="req-replay")]

    watcher = make_watcher(store, shutdown_event, call_tool_mock=AsyncMock(side_effect=two_shot))
    await watcher.run()

    async with store._conn.execute(
        "SELECT COUNT(*) FROM approvals WHERE mcp_approval_id='req-replay'"
    ) as cur:
        row = await cur.fetchone()
    assert row[0] == 1


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


async def test_approval_watcher_dedup_no_second_outbox_row(store):
    """Second poll with same id must not create a second outbox row."""
    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    call_count = 0
    shutdown_event = asyncio.Event()

    async def two_shot(tool_name, arguments):
        nonlocal call_count
        call_count += 1
        if call_count >= 2:
            shutdown_event.set()
        return [_req(id="req-dedup")]

    watcher = make_watcher(store, shutdown_event, call_tool_mock=AsyncMock(side_effect=two_shot))
    await watcher.run()

    async with store._conn.execute(
        "SELECT COUNT(*) FROM outbox WHERE idempotency_key='approval-notify-req-dedup'"
    ) as cur:
        row = await cur.fetchone()
    assert row[0] == 1


async def test_approval_watcher_row_missing_id_is_skipped(store):
    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    shutdown_event = asyncio.Event()

    async def one_shot(tool_name, arguments):
        shutdown_event.set()
        return [{"tool_name": "pay_invoice"}]  # missing "id"

    watcher = make_watcher(store, shutdown_event, call_tool_mock=AsyncMock(side_effect=one_shot))
    await watcher.run()

    async with store._conn.execute("SELECT COUNT(*) FROM approvals") as cur:
        row = await cur.fetchone()
    assert row[0] == 0


async def test_approval_watcher_row_missing_tool_name_uses_unknown(store):
    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    shutdown_event = asyncio.Event()

    async def one_shot(tool_name, arguments):
        shutdown_event.set()
        return [{"id": "req-notool", "display_payload_json": '{"x": 1}'}]

    watcher = make_watcher(store, shutdown_event, call_tool_mock=AsyncMock(side_effect=one_shot))
    await watcher.run()

    async with store._conn.execute(
        "SELECT action_description FROM approvals WHERE mcp_approval_id='req-notool'"
    ) as cur:
        row = await cur.fetchone()
    assert row is not None
    assert "unknown tool" in row["action_description"]


async def test_approval_watcher_invalid_display_payload_json_uses_raw_wrapper(store):
    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    shutdown_event = asyncio.Event()

    async def one_shot(tool_name, arguments):
        shutdown_event.set()
        return [_req(id="req-bad-json", display_payload_json="not valid json {{")]

    watcher = make_watcher(store, shutdown_event, call_tool_mock=AsyncMock(side_effect=one_shot))
    await watcher.run()

    async with store._conn.execute("SELECT message_text FROM outbox") as cur:
        rows = await cur.fetchall()
    assert len(rows) == 1
    assert "not valid json {{" in rows[0]["message_text"]
    assert "raw_display_payload" in rows[0]["message_text"]


async def test_approval_watcher_missing_display_payload_shows_unavailable(store):
    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    shutdown_event = asyncio.Event()

    async def one_shot(tool_name, arguments):
        shutdown_event.set()
        return [{"id": "req-nodisplay", "tool_name": "pay_invoice"}]  # no display_payload_json

    watcher = make_watcher(store, shutdown_event, call_tool_mock=AsyncMock(side_effect=one_shot))
    await watcher.run()

    async with store._conn.execute("SELECT message_text FROM outbox") as cur:
        rows = await cur.fetchall()
    assert len(rows) == 1
    assert "(details unavailable)" in rows[0]["message_text"]


async def test_approval_watcher_expires_at_unix_converted_to_iso(store):
    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    shutdown_event = asyncio.Event()

    async def one_shot(tool_name, arguments):
        shutdown_event.set()
        return [_req(id="req-ts", expires_at=1770000000)]

    watcher = make_watcher(store, shutdown_event, call_tool_mock=AsyncMock(side_effect=one_shot))
    await watcher.run()

    async with store._conn.execute(
        "SELECT expires_at FROM approvals WHERE mcp_approval_id='req-ts'"
    ) as cur:
        row = await cur.fetchone()
    assert row is not None
    assert row["expires_at"] is not None
    assert "T" in row["expires_at"]
    assert "Z" in row["expires_at"]


async def test_approval_watcher_absent_expires_at_stored_as_none(store):
    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    shutdown_event = asyncio.Event()

    async def one_shot(tool_name, arguments):
        shutdown_event.set()
        return [_req(id="req-no-ts")]

    watcher = make_watcher(store, shutdown_event, call_tool_mock=AsyncMock(side_effect=one_shot))
    await watcher.run()

    async with store._conn.execute(
        "SELECT expires_at FROM approvals WHERE mcp_approval_id='req-no-ts'"
    ) as cur:
        row = await cur.fetchone()
    assert row is not None
    assert row["expires_at"] is None


async def test_approval_watcher_pathological_expires_at_stored_as_none(store):
    """datetime.fromtimestamp on an extreme value must not abort the row — falls back to None."""
    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    shutdown_event = asyncio.Event()

    async def one_shot(tool_name, arguments):
        shutdown_event.set()
        return [_req(id="req-bad-ts", expires_at=99999999999999999)]

    watcher = make_watcher(store, shutdown_event, call_tool_mock=AsyncMock(side_effect=one_shot))
    await watcher.run()

    async with store._conn.execute(
        "SELECT expires_at FROM approvals WHERE mcp_approval_id='req-bad-ts'"
    ) as cur:
        row = await cur.fetchone()
    assert row is not None
    assert row["expires_at"] is None


async def test_approval_watcher_non_string_display_payload_does_not_abort_row(store):
    """display_payload_json that is not a string (e.g. an int) must not abort the row."""
    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    shutdown_event = asyncio.Event()

    async def one_shot(tool_name, arguments):
        shutdown_event.set()
        return [{"id": "req-bad-display", "tool_name": "pay_invoice", "display_payload_json": 12345}]

    watcher = make_watcher(store, shutdown_event, call_tool_mock=AsyncMock(side_effect=one_shot))
    await watcher.run()

    async with store._conn.execute(
        "SELECT action_description FROM approvals WHERE mcp_approval_id='req-bad-display'"
    ) as cur:
        row = await cur.fetchone()
    assert row is not None
    assert "12345" in row["action_description"]


async def test_approval_watcher_unexpected_response_shape_logs_and_skips(store):
    """Non-list response from list_pending_approvals must log a warning and insert nothing."""
    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    shutdown_event = asyncio.Event()

    async def one_shot(tool_name, arguments):
        shutdown_event.set()
        return {"unexpected": "dict", "requests": []}

    watcher = make_watcher(store, shutdown_event, call_tool_mock=AsyncMock(side_effect=one_shot))
    await watcher.run()

    async with store._conn.execute("SELECT COUNT(*) FROM approvals") as cur:
        row = await cur.fetchone()
    assert row[0] == 0


async def test_approval_watcher_non_dict_row_skipped_does_not_abort_batch(store):
    """Non-dict items in a list response must be skipped; valid dict items still processed."""
    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    shutdown_event = asyncio.Event()

    async def one_shot(tool_name, arguments):
        shutdown_event.set()
        return [
            "unexpected_string",
            None,
            _req(id="req-valid", display_payload_json='{"action": "Valid action"}'),
        ]

    watcher = make_watcher(store, shutdown_event, call_tool_mock=AsyncMock(side_effect=one_shot))
    await watcher.run()

    async with store._conn.execute("SELECT COUNT(*) FROM approvals WHERE mcp_approval_id='req-valid'") as cur:
        row = await cur.fetchone()
    assert row[0] == 1


async def test_approval_watcher_request_payload_not_in_dm(store):
    """request_payload_json must ONLY appear in approvals.request_text, never in DMs."""
    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    shutdown_event = asyncio.Event()

    async def one_shot(tool_name, arguments):
        shutdown_event.set()
        return [{
            "id": "req-safety",
            "tool_name": "pay_invoice",
            "request_payload_json": '{"amount": 1000, "dest": "secret_wallet"}',
        }]

    watcher = make_watcher(store, shutdown_event, call_tool_mock=AsyncMock(side_effect=one_shot))
    await watcher.run()

    async with store._conn.execute(
        "SELECT action_description, request_text FROM approvals WHERE mcp_approval_id='req-safety'"
    ) as cur:
        appr = await cur.fetchone()
    async with store._conn.execute("SELECT message_text FROM outbox") as cur:
        outbox_rows = await cur.fetchall()

    assert appr["request_text"] == '{"amount": 1000, "dest": "secret_wallet"}'
    assert "1000" not in appr["action_description"]
    assert "secret_wallet" not in appr["action_description"]
    assert len(outbox_rows) == 1
    assert "1000" not in outbox_rows[0]["message_text"]
    assert "secret_wallet" not in outbox_rows[0]["message_text"]
