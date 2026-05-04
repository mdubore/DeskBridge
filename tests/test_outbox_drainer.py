import asyncio
import json
import pytest
from unittest.mock import AsyncMock, MagicMock
from deskbridge.dm.outbox import OutboxDrainer
from deskbridge.mcp.client import McpToolError
from deskbridge.mcp.errors import RoutingDecision
from deskbridge.models import McpError, McpErrorCategory
from deskbridge.config import IdentityConfig


ALICE = IdentityConfig(label="alice", npub="npub1alice", passphrase_ref="env:X")


async def _seed_outbox(conn, *, id, idempotency_key, dest_pubkey="npub1dest",
                       delivery_attempts=0, delivery_status="pending", identity_id="acc-alice"):
    await conn.execute(
        """
        INSERT INTO outbox
            (id, identity_id, dest_pubkey, message_text, idempotency_key,
             delivery_status, delivery_attempts)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (id, identity_id, dest_pubkey, "hello", idempotency_key,
         delivery_status, delivery_attempts),
    )
    await conn.commit()


def make_drainer(store, db_conn, *, send_dm_mock, session_id="sess-123",
                 drain_interval=0.01, max_attempts=3):
    mock_client = MagicMock()
    mock_client.call_tool = send_dm_mock
    mock_broker = MagicMock()
    mock_broker.get_session_id = AsyncMock(return_value=session_id)
    shutdown_event = asyncio.Event()
    drainer = OutboxDrainer(
        store=store,
        client=mock_client,
        broker=mock_broker,
        identities=[ALICE],
        shutdown_event=shutdown_event,
        drain_interval_secs=drain_interval,
        max_attempts=max_attempts,
    )
    return drainer, shutdown_event, db_conn


async def test_outbox_drainer_delivers_message(store, db_conn):
    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    await _seed_outbox(db_conn, id="ob-1", idempotency_key="k1")

    async def send_and_stop(tool_name, arguments):
        drainer._shutdown_event.set()
        return {"delivered": True}

    drainer, shutdown_event, _ = make_drainer(
        store, db_conn, send_dm_mock=AsyncMock(side_effect=send_and_stop)
    )
    await drainer.run()

    async with db_conn.execute("SELECT delivery_status, delivery_attempts FROM outbox WHERE id='ob-1'") as cur:
        row = await cur.fetchone()
    assert row["delivery_status"] == "delivered"
    assert row["delivery_attempts"] == 1


async def test_outbox_drainer_transient_failure_increments(store, db_conn):
    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    await _seed_outbox(db_conn, id="ob-1", idempotency_key="k1")

    async def fail_and_stop(tool_name, arguments):
        drainer._shutdown_event.set()
        raise McpToolError(
            mcp_error=McpError(category=McpErrorCategory.TRANSIENT_TRANSPORT, message="timeout"),
            routing=RoutingDecision.RETRY,
        )

    drainer, shutdown_event, _ = make_drainer(
        store, db_conn, send_dm_mock=AsyncMock(side_effect=fail_and_stop), max_attempts=3
    )
    await drainer.run()

    async with db_conn.execute("SELECT delivery_status, delivery_attempts FROM outbox WHERE id='ob-1'") as cur:
        row = await cur.fetchone()
    assert row["delivery_status"] == "pending"
    assert row["delivery_attempts"] == 1


async def test_outbox_drainer_last_attempt_marks_failed(store, db_conn):
    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    await _seed_outbox(db_conn, id="ob-1", idempotency_key="k1", delivery_attempts=2)

    async def fail_and_stop(tool_name, arguments):
        drainer._shutdown_event.set()
        raise McpToolError(
            mcp_error=McpError(category=McpErrorCategory.TRANSIENT_TRANSPORT, message="timeout"),
            routing=RoutingDecision.RETRY,
        )

    drainer, shutdown_event, _ = make_drainer(
        store, db_conn, send_dm_mock=AsyncMock(side_effect=fail_and_stop), max_attempts=3
    )
    await drainer.run()

    async with db_conn.execute("SELECT delivery_status, delivery_attempts FROM outbox WHERE id='ob-1'") as cur:
        row = await cur.fetchone()
    assert row["delivery_status"] == "failed"
    assert row["delivery_attempts"] == 3


async def test_outbox_drainer_reject_marks_failed_immediately(store, db_conn):
    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    await _seed_outbox(db_conn, id="ob-1", idempotency_key="k1", delivery_attempts=0)

    async def reject_and_stop(tool_name, arguments):
        drainer._shutdown_event.set()
        raise McpToolError(
            mcp_error=McpError(category=McpErrorCategory.UNSUPPORTED_STATE, message="nope"),
            routing=RoutingDecision.REJECT,
        )

    drainer, shutdown_event, _ = make_drainer(
        store, db_conn, send_dm_mock=AsyncMock(side_effect=reject_and_stop), max_attempts=3
    )
    await drainer.run()

    async with db_conn.execute("SELECT delivery_status, delivery_attempts FROM outbox WHERE id='ob-1'") as cur:
        row = await cur.fetchone()
    assert row["delivery_status"] == "failed"


async def test_outbox_drainer_exhausted_row_not_fetched(store, db_conn):
    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    await _seed_outbox(db_conn, id="ob-1", idempotency_key="k1", delivery_attempts=3)

    mock_client = MagicMock()
    mock_client.call_tool = AsyncMock()
    mock_broker = MagicMock()
    mock_broker.get_session_id = AsyncMock(return_value="sess-123")
    shutdown_event = asyncio.Event()
    shutdown_event.set()
    drainer = OutboxDrainer(
        store=store,
        client=mock_client,
        broker=mock_broker,
        identities=[ALICE],
        shutdown_event=shutdown_event,
        drain_interval_secs=0.01,
        max_attempts=3,
    )
    await drainer.run()
    mock_client.call_tool.assert_not_awaited()


async def test_outbox_drainer_skips_no_dest_pubkey(store, db_conn):
    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    await db_conn.execute(
        "INSERT INTO outbox (id, identity_id, dest_group_id, message_text, idempotency_key) "
        "VALUES ('ob-1', 'acc-alice', 'grp-1', 'hello', 'k1')"
    )
    await db_conn.commit()

    mock_client = MagicMock()
    mock_client.call_tool = AsyncMock()
    mock_broker = MagicMock()
    mock_broker.get_session_id = AsyncMock(return_value="sess-123")
    shutdown_event = asyncio.Event()
    shutdown_event.set()
    drainer = OutboxDrainer(
        store=store,
        client=mock_client,
        broker=mock_broker,
        identities=[ALICE],
        shutdown_event=shutdown_event,
        drain_interval_secs=0.01,
    )
    await drainer.run()
    mock_client.call_tool.assert_not_awaited()


async def test_outbox_drainer_skips_unknown_identity(store, db_conn):
    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    await store.upsert_account(id="acc-bob", npub="npub1bob", label="bob", passphrase_ref="env:Y")
    await _seed_outbox(db_conn, id="ob-1", idempotency_key="k1", identity_id="acc-bob")

    mock_client = MagicMock()
    mock_client.call_tool = AsyncMock()
    mock_broker = MagicMock()
    mock_broker.get_session_id = AsyncMock(return_value="sess-123")
    shutdown_event = asyncio.Event()
    shutdown_event.set()
    drainer = OutboxDrainer(
        store=store,
        client=mock_client,
        broker=mock_broker,
        identities=[ALICE],  # bob not in identities list
        shutdown_event=shutdown_event,
        drain_interval_secs=0.01,
    )
    await drainer.run()
    mock_client.call_tool.assert_not_awaited()


async def test_outbox_drainer_skips_no_session(store, db_conn):
    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    await _seed_outbox(db_conn, id="ob-1", idempotency_key="k1")

    mock_client = MagicMock()
    mock_client.call_tool = AsyncMock()
    mock_broker = MagicMock()
    mock_broker.get_session_id = AsyncMock(return_value=None)
    shutdown_event = asyncio.Event()
    shutdown_event.set()
    drainer = OutboxDrainer(
        store=store,
        client=mock_client,
        broker=mock_broker,
        identities=[ALICE],
        shutdown_event=shutdown_event,
        drain_interval_secs=0.01,
    )
    await drainer.run()
    mock_client.call_tool.assert_not_awaited()
