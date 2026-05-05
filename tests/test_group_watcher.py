import asyncio
from unittest.mock import AsyncMock, MagicMock
from deskbridge.dm.group_watcher import GroupWatcher
from deskbridge.mcp.client import McpToolError
from deskbridge.mcp.errors import RoutingDecision
from deskbridge.models import McpError, McpErrorCategory


IDENTITY_NPUB = "npub1alice"
OPERATOR_NPUB = "npub1op"
GROUP_ID = "grp-1"


def make_group_watcher(store, shutdown_event, *, call_tool_mock, session_id="sess-123",
                        operator_npub=OPERATOR_NPUB, group_ids=None):
    mock_client = MagicMock()
    mock_client.call_tool = call_tool_mock
    mock_broker = MagicMock()
    mock_broker.get_session_id = AsyncMock(return_value=session_id)
    return GroupWatcher(
        identity_label="alice",
        identity_npub=IDENTITY_NPUB,
        operator_npub=operator_npub,
        group_ids=group_ids or [GROUP_ID],
        store=store,
        client=mock_client,
        broker=mock_broker,
        shutdown_event=shutdown_event,
        poll_timeout_secs=1,
    )


def _msg(id="gmsg-1", content="fix the auth bug", sender_pubkey=OPERATOR_NPUB, group_id=GROUP_ID,
         identity_npub=IDENTITY_NPUB):
    return {
        "id": id,
        "content": f"nostr:{identity_npub} {content}",
        "sender_pubkey": sender_pubkey,
        "group_id": group_id,
    }


def _group_response(*messages, last_id="gmsg-1"):
    return {
        "messages": list(messages),
        "last_message_id": last_id,
        "last_created_at": "2026-01-01T00:00:00Z",
    }


# ---------------------------------------------------------------------------
# Authorization and mention filtering
# ---------------------------------------------------------------------------

async def test_group_watcher_task_creates_work_item(store):
    shutdown_event = asyncio.Event()

    async def one_shot(tool_name, arguments):
        shutdown_event.set()
        return _group_response(_msg(content="fix the auth bug"))

    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    watcher = make_group_watcher(store, shutdown_event, call_tool_mock=AsyncMock(side_effect=one_shot))
    await watcher.run()

    async with store._conn.execute("SELECT COUNT(*) FROM work_items") as cur:
        row = await cur.fetchone()
    assert row[0] == 1


async def test_group_watcher_unauthorized_sender_ignored(store):
    shutdown_event = asyncio.Event()

    async def one_shot(tool_name, arguments):
        shutdown_event.set()
        return _group_response(_msg(sender_pubkey="npub1stranger"))

    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    watcher = make_group_watcher(store, shutdown_event, call_tool_mock=AsyncMock(side_effect=one_shot))
    await watcher.run()

    async with store._conn.execute("SELECT COUNT(*) FROM work_items") as cur:
        row = await cur.fetchone()
    assert row[0] == 0


async def test_group_watcher_no_mention_ignored(store):
    shutdown_event = asyncio.Event()

    async def one_shot(tool_name, arguments):
        shutdown_event.set()
        # Message does NOT contain identity_npub
        return _group_response({
            "id": "gmsg-1",
            "content": "hey everyone, fix the auth bug",
            "sender_pubkey": OPERATOR_NPUB,
            "group_id": GROUP_ID,
        })

    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    watcher = make_group_watcher(store, shutdown_event, call_tool_mock=AsyncMock(side_effect=one_shot))
    await watcher.run()

    async with store._conn.execute("SELECT COUNT(*) FROM work_items") as cur:
        row = await cur.fetchone()
    assert row[0] == 0


async def test_group_watcher_no_operator_npub_ignores_all(store):
    shutdown_event = asyncio.Event()

    async def one_shot(tool_name, arguments):
        shutdown_event.set()
        return _group_response(_msg())

    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    watcher = make_group_watcher(store, shutdown_event,
                                  call_tool_mock=AsyncMock(side_effect=one_shot),
                                  operator_npub=None)
    await watcher.run()

    async with store._conn.execute("SELECT COUNT(*) FROM work_items") as cur:
        row = await cur.fetchone()
    assert row[0] == 0


# ---------------------------------------------------------------------------
# Intent routing
# ---------------------------------------------------------------------------

async def test_group_watcher_status_intent_replies_to_group(store):
    shutdown_event = asyncio.Event()

    async def one_shot(tool_name, arguments):
        shutdown_event.set()
        return _group_response(_msg(content="status"))

    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    watcher = make_group_watcher(store, shutdown_event, call_tool_mock=AsyncMock(side_effect=one_shot))
    await watcher.run()

    async with store._conn.execute("SELECT dest_group_id, dest_pubkey, message_text FROM outbox") as cur:
        rows = await cur.fetchall()
    assert len(rows) == 1
    assert rows[0]["dest_group_id"] == GROUP_ID
    assert rows[0]["dest_pubkey"] is None
    assert "No tasks" in rows[0]["message_text"]


async def test_group_watcher_cancel_intent_marks_cancel_requested(store, db_conn):
    shutdown_event = asyncio.Event()

    async def one_shot(tool_name, arguments):
        shutdown_event.set()
        return _group_response(_msg(id="gmsg-2", content="cancel"))

    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    await db_conn.execute(
        "INSERT INTO work_items (id, source_type, source_id, identity_id, status, summary, idempotency_key) "
        "VALUES ('wi-1', 'dm', 'wi-1', 'acc-alice', 'dispatched', 'fix auth', 'idem-wi-1')"
    )
    await db_conn.commit()

    watcher = make_group_watcher(store, shutdown_event, call_tool_mock=AsyncMock(side_effect=one_shot))
    await watcher.run()

    async with store._conn.execute("SELECT status FROM work_items WHERE id='wi-1'") as cur:
        row = await cur.fetchone()
    assert row["status"] == "cancel_requested"


async def test_group_watcher_no_session_skips_poll(store):
    shutdown_event = asyncio.Event()
    mock_client = MagicMock()
    mock_client.call_tool = AsyncMock()
    mock_broker = MagicMock()
    mock_broker.get_session_id = AsyncMock(return_value=None)

    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    watcher = GroupWatcher(
        identity_label="alice",
        identity_npub=IDENTITY_NPUB,
        operator_npub=OPERATOR_NPUB,
        group_ids=[GROUP_ID],
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


async def test_group_watcher_reject_exits_cleanly(store):
    shutdown_event = asyncio.Event()

    async def reject(tool_name, arguments):
        raise McpToolError(
            mcp_error=McpError(category=McpErrorCategory.UNSUPPORTED_STATE, message="rejected"),
            routing=RoutingDecision.REJECT,
        )

    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    watcher = make_group_watcher(store, shutdown_event, call_tool_mock=AsyncMock(side_effect=reject))
    await watcher.run()
    assert not shutdown_event.is_set()
