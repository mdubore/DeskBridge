import asyncio
import uuid
from unittest.mock import AsyncMock, MagicMock
from deskbridge.dm.watcher import DmWatcher
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
    return DmWatcher(
        identity_label="alice",
        store=store,
        client=mock_client,
        broker=mock_broker,
        shutdown_event=shutdown_event,
        operator_npub=operator_npub,
        poll_timeout_secs=1,
    )


def _dm_response(*messages, last_id="msg-1"):
    return {
        "messages": list(messages),
        "last_message_id": last_id,
        "last_created_at": "2026-01-01T00:00:00Z",
    }


def _msg(id="msg-1", content="fix the auth bug", sender_pubkey=OPERATOR_NPUB):
    return {"id": id, "content": content, "sender_pubkey": sender_pubkey}


# ---------------------------------------------------------------------------
# Existing MCP-level tests (updated to include sender_pubkey in messages)
# ---------------------------------------------------------------------------

async def test_dm_watcher_creates_work_item_and_cursor(store):
    shutdown_event = asyncio.Event()

    async def one_shot(tool_name, arguments):
        shutdown_event.set()
        return _dm_response(_msg(content="hello operator"))

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
    assert await store.get_cursor("dm_watcher", "acc-alice") is None


async def test_dm_watcher_idempotent_on_replay(store):
    shutdown_event = asyncio.Event()
    call_count = 0

    async def two_shot(tool_name, arguments):
        nonlocal call_count
        call_count += 1
        if call_count >= 2:
            shutdown_event.set()
        return _dm_response(_msg(content="hello"))

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
    watcher = make_watcher(store, shutdown_event, call_tool_mock=AsyncMock(side_effect=reject))
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
        operator_npub=OPERATOR_NPUB,
        poll_timeout_secs=1,
    )

    async def stop():
        await asyncio.sleep(0.05)
        shutdown_event.set()

    await asyncio.gather(watcher.run(), stop())
    mock_client.call_tool.assert_not_awaited()


# ---------------------------------------------------------------------------
# Authorization tests
# ---------------------------------------------------------------------------

async def test_dm_watcher_unauthorized_sender_ignored(store):
    shutdown_event = asyncio.Event()

    async def one_shot(tool_name, arguments):
        shutdown_event.set()
        return _dm_response(_msg(sender_pubkey="npub1stranger"))

    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    watcher = make_watcher(store, shutdown_event, call_tool_mock=AsyncMock(side_effect=one_shot))
    await watcher.run()

    async with store._conn.execute("SELECT COUNT(*) FROM work_items") as cur:
        row = await cur.fetchone()
    assert row[0] == 0


async def test_dm_watcher_no_operator_npub_ignores_all(store):
    shutdown_event = asyncio.Event()

    async def one_shot(tool_name, arguments):
        shutdown_event.set()
        return _dm_response(_msg())

    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    watcher = make_watcher(store, shutdown_event,
                            call_tool_mock=AsyncMock(side_effect=one_shot),
                            operator_npub=None)
    await watcher.run()

    async with store._conn.execute("SELECT COUNT(*) FROM work_items") as cur:
        row = await cur.fetchone()
    assert row[0] == 0


# ---------------------------------------------------------------------------
# Intent routing tests
# ---------------------------------------------------------------------------

async def test_dm_watcher_status_intent_queues_reply_no_tasks(store):
    shutdown_event = asyncio.Event()

    async def one_shot(tool_name, arguments):
        shutdown_event.set()
        return _dm_response(_msg(content="status please"))

    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    watcher = make_watcher(store, shutdown_event, call_tool_mock=AsyncMock(side_effect=one_shot))
    await watcher.run()

    async with store._conn.execute("SELECT COUNT(*) FROM work_items") as cur:
        row = await cur.fetchone()
    assert row[0] == 0

    async with store._conn.execute("SELECT message_text, dest_pubkey FROM outbox") as cur:
        rows = await cur.fetchall()
    assert len(rows) == 1
    assert "No tasks" in rows[0]["message_text"]
    assert rows[0]["dest_pubkey"] == OPERATOR_NPUB


async def test_dm_watcher_status_intent_reports_latest_task(store, db_conn):
    shutdown_event = asyncio.Event()

    async def one_shot(tool_name, arguments):
        shutdown_event.set()
        return _dm_response(_msg(id="msg-2", content="status please"))

    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    await db_conn.execute(
        "INSERT INTO work_items (id, source_type, source_id, identity_id, status, summary, idempotency_key) "
        "VALUES ('wi-1', 'dm', 'wi-1', 'acc-alice', 'done', 'fix auth bug', 'idem-wi-1')"
    )
    await db_conn.commit()

    watcher = make_watcher(store, shutdown_event, call_tool_mock=AsyncMock(side_effect=one_shot))
    await watcher.run()

    async with store._conn.execute("SELECT message_text FROM outbox") as cur:
        rows = await cur.fetchall()
    assert len(rows) == 1
    assert "fix auth bug" in rows[0]["message_text"]
    assert "done" in rows[0]["message_text"]


async def test_dm_watcher_cancel_intent_marks_cancel_requested(store, db_conn):
    shutdown_event = asyncio.Event()

    async def one_shot(tool_name, arguments):
        shutdown_event.set()
        return _dm_response(_msg(id="msg-2", content="cancel that task"))

    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    await db_conn.execute(
        "INSERT INTO work_items (id, source_type, source_id, identity_id, status, summary, idempotency_key) "
        "VALUES ('wi-1', 'dm', 'wi-1', 'acc-alice', 'dispatched', 'fix auth bug', 'idem-wi-1')"
    )
    await db_conn.commit()

    watcher = make_watcher(store, shutdown_event, call_tool_mock=AsyncMock(side_effect=one_shot))
    await watcher.run()

    async with store._conn.execute("SELECT status FROM work_items WHERE id='wi-1'") as cur:
        row = await cur.fetchone()
    assert row["status"] == "cancel_requested"

    async with store._conn.execute("SELECT message_text FROM outbox") as cur:
        rows = await cur.fetchall()
    assert len(rows) == 1
    assert "Cancel requested" in rows[0]["message_text"]


async def test_dm_watcher_cancel_no_active_task_sends_reply(store):
    shutdown_event = asyncio.Event()

    async def one_shot(tool_name, arguments):
        shutdown_event.set()
        return _dm_response(_msg(content="abort the run"))

    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    watcher = make_watcher(store, shutdown_event, call_tool_mock=AsyncMock(side_effect=one_shot))
    await watcher.run()

    async with store._conn.execute("SELECT message_text FROM outbox") as cur:
        rows = await cur.fetchall()
    assert len(rows) == 1
    assert "No active task" in rows[0]["message_text"]


async def test_dm_watcher_approve_intent_resolves_approval(store, db_conn):
    shutdown_event = asyncio.Event()

    async def one_shot(tool_name, arguments):
        shutdown_event.set()
        return _dm_response(_msg(id="msg-2", content="yes go ahead"))

    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    await db_conn.execute(
        "INSERT INTO work_items (id, source_type, source_id, identity_id, status, idempotency_key) "
        "VALUES ('wi-1', 'dm', 'wi-1', 'acc-alice', 'pending', 'idem-wi-1')"
    )
    await db_conn.execute(
        "INSERT INTO approvals (id, work_item_id, action_description, status) "
        "VALUES ('appr-1', 'wi-1', 'do something', 'pending')"
    )
    await db_conn.commit()

    watcher = make_watcher(store, shutdown_event, call_tool_mock=AsyncMock(side_effect=one_shot))
    await watcher.run()

    async with store._conn.execute("SELECT status, resolved_at FROM approvals WHERE id='appr-1'") as cur:
        row = await cur.fetchone()
    assert row["status"] == "approved"
    assert row["resolved_at"] is not None


async def test_dm_watcher_reject_intent_resolves_approval(store, db_conn):
    shutdown_event = asyncio.Event()

    async def one_shot(tool_name, arguments):
        shutdown_event.set()
        return _dm_response(_msg(id="msg-2", content="no, reject that"))

    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    await db_conn.execute(
        "INSERT INTO work_items (id, source_type, source_id, identity_id, status, idempotency_key) "
        "VALUES ('wi-1', 'dm', 'wi-1', 'acc-alice', 'pending', 'idem-wi-1')"
    )
    await db_conn.execute(
        "INSERT INTO approvals (id, work_item_id, action_description, status) "
        "VALUES ('appr-1', 'wi-1', 'do something', 'pending')"
    )
    await db_conn.commit()

    watcher = make_watcher(store, shutdown_event, call_tool_mock=AsyncMock(side_effect=one_shot))
    await watcher.run()

    async with store._conn.execute("SELECT status FROM approvals WHERE id='appr-1'") as cur:
        row = await cur.fetchone()
    assert row["status"] == "rejected"


async def test_dm_watcher_approve_no_pending_sends_reply(store):
    shutdown_event = asyncio.Event()

    async def one_shot(tool_name, arguments):
        shutdown_event.set()
        return _dm_response(_msg(content="approve"))

    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    watcher = make_watcher(store, shutdown_event, call_tool_mock=AsyncMock(side_effect=one_shot))
    await watcher.run()

    async with store._conn.execute("SELECT message_text FROM outbox") as cur:
        rows = await cur.fetchall()
    assert len(rows) == 1
    assert "No pending approval" in rows[0]["message_text"]


async def test_dm_watcher_approve_calls_resolve_when_mcp_approval_id_set(store, db_conn):
    shutdown_event = asyncio.Event()
    resolve_calls = []

    async def call_tool_side_effect(tool_name, arguments):
        if tool_name == "wait_for_new_dms":
            shutdown_event.set()
            return _dm_response(_msg(id="msg-2", content="yes go ahead"))
        elif tool_name == "resolve_approval_request":
            resolve_calls.append(arguments)
            return {"ok": True}

    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    await db_conn.execute(
        "INSERT INTO work_items (id, source_type, source_id, identity_id, status, idempotency_key) "
        "VALUES ('wi-1', 'dm', 'wi-1', 'acc-alice', 'pending', 'idem-wi-1')"
    )
    await db_conn.execute(
        "INSERT INTO approvals (id, work_item_id, action_description, status, mcp_approval_id) "
        "VALUES ('appr-1', 'wi-1', 'force push', 'pending', 'req-ext-1')"
    )
    await db_conn.commit()

    watcher = make_watcher(store, shutdown_event,
                           call_tool_mock=AsyncMock(side_effect=call_tool_side_effect))
    await watcher.run()

    assert len(resolve_calls) == 1
    assert resolve_calls[0]["request_id"] == "req-ext-1"
    assert resolve_calls[0]["decision"] == "approved"
    assert resolve_calls[0]["session_id"] == "sess-123"


async def test_dm_watcher_approve_skips_resolve_when_no_mcp_approval_id(store, db_conn):
    shutdown_event = asyncio.Event()
    resolve_calls = []

    async def call_tool_side_effect(tool_name, arguments):
        if tool_name == "wait_for_new_dms":
            shutdown_event.set()
            return _dm_response(_msg(id="msg-2", content="yes go ahead"))
        elif tool_name == "resolve_approval_request":
            resolve_calls.append(arguments)
            return {"ok": True}

    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    await db_conn.execute(
        "INSERT INTO work_items (id, source_type, source_id, identity_id, status, idempotency_key) "
        "VALUES ('wi-1', 'dm', 'wi-1', 'acc-alice', 'pending', 'idem-wi-1')"
    )
    await db_conn.execute(
        "INSERT INTO approvals (id, work_item_id, action_description, status) "
        "VALUES ('appr-1', 'wi-1', 'force push', 'pending')"
    )
    await db_conn.commit()

    watcher = make_watcher(store, shutdown_event,
                           call_tool_mock=AsyncMock(side_effect=call_tool_side_effect))
    await watcher.run()

    assert len(resolve_calls) == 0


async def test_dm_watcher_reject_calls_resolve_when_mcp_approval_id_set(store, db_conn):
    shutdown_event = asyncio.Event()
    resolve_calls = []

    async def call_tool_side_effect(tool_name, arguments):
        if tool_name == "wait_for_new_dms":
            shutdown_event.set()
            return _dm_response(_msg(id="msg-2", content="no, reject that"))
        elif tool_name == "resolve_approval_request":
            resolve_calls.append(arguments)
            return {"ok": True}

    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    await db_conn.execute(
        "INSERT INTO work_items (id, source_type, source_id, identity_id, status, idempotency_key) "
        "VALUES ('wi-1', 'dm', 'wi-1', 'acc-alice', 'pending', 'idem-wi-1')"
    )
    await db_conn.execute(
        "INSERT INTO approvals (id, work_item_id, action_description, status, mcp_approval_id) "
        "VALUES ('appr-1', 'wi-1', 'force push', 'pending', 'req-ext-2')"
    )
    await db_conn.commit()

    watcher = make_watcher(store, shutdown_event,
                           call_tool_mock=AsyncMock(side_effect=call_tool_side_effect))
    await watcher.run()

    assert len(resolve_calls) == 1
    assert resolve_calls[0]["request_id"] == "req-ext-2"
    assert resolve_calls[0]["decision"] == "rejected"
