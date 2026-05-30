import asyncio
from unittest.mock import AsyncMock, MagicMock, ANY

import pytest

from deskbridge.dm.kanban_watcher import KanbanWatcher
from deskbridge.mcp.client import McpToolError
from deskbridge.models import McpError, McpErrorCategory
from deskbridge.mcp.errors import RoutingDecision


def make_watcher(
    *,
    boards=None,
    operator_npub=None,
    poll_interval=0.01,
    session_id="sess-1",
):
    store = MagicMock()
    store.upsert_work_item = AsyncMock(return_value=True)
    store.insert_outbox_item = AsyncMock()

    client = MagicMock()
    client.call_tool = AsyncMock(return_value=[])

    broker = MagicMock()
    broker.get_session_id = AsyncMock(return_value=session_id)

    shutdown = asyncio.Event()

    watcher = KanbanWatcher(
        account_id="acc-alice",
        identity_label="alice",
        store=store,
        client=client,
        broker=broker,
        shutdown_event=shutdown,
        boards=boards if boards is not None else ["ch-1"],
        operator_npub=operator_npub,
        poll_interval_secs=poll_interval,
    )
    return watcher, store, client, broker, shutdown


async def test_new_card_inserts_work_item_and_sends_operator_dm():
    watcher, store, client, broker, shutdown = make_watcher(operator_npub="npub1op")
    card = {"id": "card-1", "title": "Fix auth bug", "description": "details here"}

    async def one_poll(*args, **kwargs):
        shutdown.set()
        return [card]

    client.call_tool = AsyncMock(side_effect=one_poll)

    await watcher.run()

    store.upsert_work_item.assert_awaited_once_with(
        id=ANY,
        source_type="kanban",
        source_id="card-1",
        identity_id="acc-alice",
        idempotency_key="kanban-card-1",
        summary="Fix auth bug",
        payload_json="details here",
    )
    store.insert_outbox_item.assert_awaited_once()
    call_args = store.insert_outbox_item.call_args.args
    assert call_args[2] == "npub1op"
    assert "Fix auth bug" in call_args[3]
    assert "card-1" in call_args[3]
    assert call_args[4] == "kanban-notify-card-1"


async def test_second_poll_same_card_is_idempotent():
    watcher, store, client, broker, shutdown = make_watcher(operator_npub="npub1op")
    card = {"id": "card-1", "title": "Fix bug", "description": ""}

    call_count = 0

    async def two_polls(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count >= 2:
            shutdown.set()
        return [card]

    client.call_tool = AsyncMock(side_effect=two_polls)
    store.upsert_work_item = AsyncMock(side_effect=[True, False])

    await watcher.run()

    assert store.upsert_work_item.await_count == 2
    assert store.insert_outbox_item.await_count == 1


async def test_multiple_boards_processes_cards_from_all():
    watcher, store, client, broker, shutdown = make_watcher(
        boards=["ch-1", "ch-2"],
        operator_npub=None,
    )

    call_count = 0

    async def board_polls(tool_name, params):
        nonlocal call_count
        call_count += 1
        if call_count >= 2:
            shutdown.set()
        channel = params["channel_id"]
        if channel == "ch-1":
            return [{"id": "card-A", "title": "Task A"}]
        return [{"id": "card-B", "title": "Task B"}]

    client.call_tool = AsyncMock(side_effect=board_polls)

    await watcher.run()

    assert store.upsert_work_item.await_count == 2
    source_ids = [c.kwargs["source_id"] for c in store.upsert_work_item.call_args_list]
    assert "card-A" in source_ids
    assert "card-B" in source_ids


async def test_card_missing_id_is_skipped_others_processed():
    watcher, store, client, broker, shutdown = make_watcher()

    async def one_poll(*args, **kwargs):
        shutdown.set()
        return [
            {"title": "no id here"},
            {"id": "card-1", "title": "Valid card"},
        ]

    client.call_tool = AsyncMock(side_effect=one_poll)

    await watcher.run()

    store.upsert_work_item.assert_awaited_once()
    assert store.upsert_work_item.call_args.kwargs["source_id"] == "card-1"


async def test_card_missing_title_uses_untitled():
    watcher, store, client, broker, shutdown = make_watcher()

    async def one_poll(*args, **kwargs):
        shutdown.set()
        return [{"id": "card-1"}]

    client.call_tool = AsyncMock(side_effect=one_poll)

    await watcher.run()

    store.upsert_work_item.assert_awaited_once()
    assert store.upsert_work_item.call_args.kwargs["summary"] == "(untitled)"


async def test_no_session_skips_poll():
    watcher, store, client, broker, shutdown = make_watcher(session_id=None)

    async def stop():
        await asyncio.sleep(0.05)
        shutdown.set()

    await asyncio.gather(watcher.run(), stop())

    client.call_tool.assert_not_awaited()


async def test_mcp_tool_error_logs_warning_and_continues():
    watcher, store, client, broker, shutdown = make_watcher()

    call_count = 0

    async def error_then_stop(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise McpToolError(
                mcp_error=McpError(message="board poll failed"),
                routing=RoutingDecision.RETRY,
            )
        shutdown.set()
        return []

    client.call_tool = AsyncMock(side_effect=error_then_stop)

    await watcher.run()

    assert call_count == 2
    store.upsert_work_item.assert_not_awaited()


async def test_no_operator_npub_skips_outbox_dm():
    watcher, store, client, broker, shutdown = make_watcher(operator_npub=None)

    async def one_poll(*args, **kwargs):
        shutdown.set()
        return [{"id": "card-1", "title": "Task"}]

    client.call_tool = AsyncMock(side_effect=one_poll)

    await watcher.run()

    store.upsert_work_item.assert_awaited_once()
    store.insert_outbox_item.assert_not_awaited()


async def test_shutdown_during_sleep_exits_cleanly():
    watcher, store, client, broker, shutdown = make_watcher(poll_interval=5.0)
    client.call_tool = AsyncMock(return_value=[])

    async def stop():
        await asyncio.sleep(0.05)
        shutdown.set()

    await asyncio.gather(watcher.run(), stop())


async def test_mcp_tool_error_reject_stops_watcher():
    watcher, store, client, broker, shutdown = make_watcher()

    async def reject_poll(*args, **kwargs):
        raise McpToolError(
            mcp_error=McpError(
                category=McpErrorCategory.INVALID_SESSION,
                message="auth rejected",
            ),
            routing=RoutingDecision.REJECT,
        )

    client.call_tool = AsyncMock(side_effect=reject_poll)

    # Watcher should exit on its own without needing shutdown set externally
    async def force_stop():
        await asyncio.sleep(0.3)
        shutdown.set()  # safety net so test doesn't hang

    await asyncio.gather(watcher.run(), force_stop())

    # call_tool called exactly once (rejected on first attempt, watcher exited)
    assert client.call_tool.await_count == 1
    store.upsert_work_item.assert_not_awaited()
