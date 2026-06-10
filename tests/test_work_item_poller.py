import asyncio
from unittest.mock import AsyncMock, MagicMock, patch, ANY

from deskbridge.agent.poller import WorkItemPoller
from deskbridge.config import (
    DeskBridgeConfig, SupervisorConfig, McpConfig, IdentityConfig, ProjectConfig
)

ALICE = IdentityConfig(label="alice", npub="npub1alice", passphrase_ref="env:X")
PROJ = ProjectConfig(
    id="proj-1", name="MyProj", repo_path="/repo",
    identity="alice", escalation_dm_target="npub1op", adapter="claude-code",
)


def make_config(projects=None):
    return DeskBridgeConfig(
        supervisor=SupervisorConfig(db_path="/tmp/test.db"),
        mcp=McpConfig(command="nostrdesk-mcp"),
        identities=[ALICE],
        projects=projects if projects is not None else [PROJ],
    )


def _row(**kwargs):
    m = MagicMock()
    m.__getitem__ = MagicMock(side_effect=lambda k: kwargs[k])
    return m


def make_store(*, project_row=None, pending_items=None, claim_result=True):
    store = MagicMock()
    store.get_project_for_identity = AsyncMock(return_value=project_row)
    store.get_pending_work_items = AsyncMock(return_value=pending_items or [])
    store.claim_work_item = AsyncMock(return_value=claim_result)
    store.get_work_item = AsyncMock(return_value=None)
    store.log_audit = AsyncMock()
    store.retry_work_item = AsyncMock()
    return store


def make_poller(store, shutdown, *, poll_interval=0.01, projects=None):
    return WorkItemPoller(
        identity_label="alice",
        store=store,
        client=MagicMock(),
        broker=MagicMock(),
        config=make_config(projects=projects),
        shutdown_event=shutdown,
        poll_interval_secs=poll_interval,
    )


async def test_poller_no_project_skips_claim():
    shutdown = asyncio.Event()
    store = make_store(project_row=None)

    async def stop():
        await asyncio.sleep(0.05)
        shutdown.set()

    poller = make_poller(store, shutdown)
    await asyncio.gather(poller.run(), stop())

    store.claim_work_item.assert_not_awaited()


async def test_poller_pending_item_claimed_and_runner_spawned():
    shutdown = asyncio.Event()
    project_row = _row(id="proj-1")
    work_item = _row(id="wi-1", source_type="dm", source_id="msg-1",
                     summary="fix bug", payload_json="{}", attempt_count=0)
    store = make_store(project_row=project_row, pending_items=[work_item])

    async def fake_run():
        shutdown.set()

    with patch("deskbridge.agent.poller.AgentRunner") as MockRunner:
        MockRunner.return_value.run = AsyncMock(side_effect=fake_run)
        poller = make_poller(store, shutdown)
        await poller.run()

    store.claim_work_item.assert_awaited_once_with("wi-1")
    MockRunner.assert_called_once_with(
        work_item=work_item,
        project=PROJ,
        run_id=ANY,
        store=store,
        client=ANY,
        broker=ANY,
    )


async def test_poller_skips_when_active_run_in_flight():
    shutdown = asyncio.Event()
    project_row = _row(id="proj-1")
    work_item = _row(id="wi-1", source_type="dm", source_id="msg-1",
                     summary="fix bug", payload_json="{}", attempt_count=0)
    store = make_store(project_row=project_row, pending_items=[work_item])

    poller = make_poller(store, shutdown)
    # Simulate an active run that never finishes
    blocking_task = asyncio.create_task(asyncio.Event().wait())
    poller._active_run_task = blocking_task

    async def stop():
        await asyncio.sleep(0.05)
        shutdown.set()

    await asyncio.gather(poller.run(), stop())
    blocking_task.cancel()
    await asyncio.gather(blocking_task, return_exceptions=True)

    store.claim_work_item.assert_not_awaited()


async def test_poller_shutdown_exits_cleanly():
    shutdown = asyncio.Event()
    store = make_store(project_row=None)

    poller = make_poller(store, shutdown)

    async def stop():
        await asyncio.sleep(0.02)
        shutdown.set()

    await asyncio.gather(poller.run(), stop())
    # Verifying run() returns cleanly — no exception is the assertion


async def test_poller_cancellation_cancels_active_runner():
    shutdown = asyncio.Event()
    project_row = _row(id="proj-1")
    work_item = _row(id="wi-1", source_type="dm", source_id="msg-1",
                     summary="fix bug", payload_json="{}", attempt_count=0)
    store = make_store(project_row=project_row, pending_items=[work_item])

    poller = make_poller(store, shutdown)
    blocking_task = asyncio.create_task(asyncio.Event().wait())
    poller._active_run_task = blocking_task

    poller_task = asyncio.create_task(poller.run())
    await asyncio.sleep(0.02)
    poller_task.cancel()
    await asyncio.gather(poller_task, return_exceptions=True)

    assert blocking_task.cancelled()


async def test_poller_completes_work_item_when_project_config_missing():
    shutdown = asyncio.Event()
    project_row = _row(id="proj-missing")  # DB has project but config has no matching entry
    work_item = _row(id="wi-1", source_type="dm", source_id="msg-1",
                     summary="fix bug", payload_json="{}", attempt_count=0)
    store = make_store(project_row=project_row, pending_items=[work_item], claim_result=True)
    store.complete_work_item = AsyncMock()

    call_count = 0
    async def get_pending_side_effect(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count > 1:
            return []  # Return empty on second call so poller exits loop
        return [work_item]

    store.get_pending_work_items = AsyncMock(side_effect=get_pending_side_effect)

    async def stop():
        await asyncio.sleep(0.05)
        shutdown.set()

    # Config has PROJ (id="proj-1") but project_row has id="proj-missing" — no match
    poller = make_poller(store, shutdown)
    await asyncio.gather(poller.run(), stop())

    store.complete_work_item.assert_awaited_once_with("wi-1", "failed")


async def test_poller_cancels_runner_when_work_item_cancel_requested():
    store = MagicMock()
    project_row = _row(id="proj-1")
    cancel_work_item_row = _row(
        id="wi-1", status="cancel_requested", summary="fix auth bug",
        attempt_count=0,
    )

    async def fake_get_work_item(id):
        return cancel_work_item_row

    store.get_project_for_identity = AsyncMock(return_value=project_row)
    store.get_pending_work_items = AsyncMock(return_value=[])
    store.get_work_item = AsyncMock(side_effect=fake_get_work_item)
    store.complete_work_item = AsyncMock()
    store.log_audit = AsyncMock()

    config = make_config()
    mock_client = MagicMock()
    mock_broker = MagicMock()
    mock_broker.get_session_id = AsyncMock(return_value="sess-1")
    shutdown_event = asyncio.Event()

    poller = WorkItemPoller(
        identity_label="alice",
        store=store,
        client=mock_client,
        broker=mock_broker,
        config=config,
        shutdown_event=shutdown_event,
        poll_interval_secs=0.01,
    )

    # Inject an active (never-finishing) task and its work item ID
    async def never_finishes():
        await asyncio.Event().wait()

    poller._active_run_task = asyncio.create_task(never_finishes())
    poller._active_work_item_id = "wi-1"

    async def stop():
        await asyncio.sleep(0.05)
        shutdown_event.set()

    await asyncio.gather(poller.run(), stop())

    store.complete_work_item.assert_awaited_once_with("wi-1", "cancelled")
    assert poller._active_run_task is None
    assert poller._active_work_item_id is None


async def test_kanban_work_item_claim_calls_update_board_card_with_configured_column():
    shutdown = asyncio.Event()
    project_row = _row(id="proj-1")
    kanban_item = _row(
        id="wi-1", source_type="kanban", source_id="card-1",
        summary="Fix auth", payload_json="{}", attempt_count=0,
    )
    store = MagicMock()
    store.get_project_for_identity = AsyncMock(return_value=project_row)
    store.get_pending_work_items = AsyncMock(return_value=[kanban_item])
    store.claim_work_item = AsyncMock(return_value=True)
    store.get_work_item = AsyncMock(return_value=None)
    store.log_audit = AsyncMock()

    broker = MagicMock()
    broker.get_session_id = AsyncMock(return_value="sess-1")

    client = MagicMock()
    client.call_tool = AsyncMock()

    async def fake_run():
        shutdown.set()

    with patch("deskbridge.agent.poller.AgentRunner") as MockRunner:
        MockRunner.return_value.run = AsyncMock(side_effect=fake_run)
        poller = WorkItemPoller(
            identity_label="alice",
            store=store,
            client=client,
            broker=broker,
            config=make_config(),
            shutdown_event=shutdown,
            poll_interval_secs=0.01,
            kanban_column_in_progress="doing",
            kanban_column_done="finished",
        )
        await poller.run()

    client.call_tool.assert_awaited_once_with(
        "update_board_card",
        {
            "session_id": "sess-1",
            "card_id": "card-1",
            "column": "doing",
            "idempotency_key": "deskbridge-wi-1-in-progress",
        },
    )


async def test_kanban_work_item_completion_calls_update_board_card_with_configured_column():
    shutdown = asyncio.Event()
    project_row = _row(id="proj-1")
    kanban_item = _row(
        id="wi-1", source_type="kanban", source_id="card-1",
        summary="Fix auth", payload_json="{}", status="done", attempt_count=0,
    )
    store = MagicMock()
    store.get_project_for_identity = AsyncMock(return_value=project_row)
    store.get_pending_work_items = AsyncMock(return_value=[])
    store.get_work_item = AsyncMock(return_value=kanban_item)
    store.log_audit = AsyncMock()

    broker = MagicMock()
    broker.get_session_id = AsyncMock(return_value="sess-1")

    client = MagicMock()
    client.call_tool = AsyncMock()

    poller = WorkItemPoller(
        identity_label="alice",
        store=store,
        client=client,
        broker=broker,
        config=make_config(),
        shutdown_event=shutdown,
        poll_interval_secs=0.01,
        kanban_column_in_progress="doing",
        kanban_column_done="finished",
    )

    # Inject a completed runner task so _poll_once sees it as done
    completed_task = asyncio.create_task(asyncio.sleep(0))
    await asyncio.sleep(0)  # yield control
    await asyncio.sleep(0)  # yield again to let task complete
    poller._active_run_task = completed_task
    poller._active_work_item_id = "wi-1"

    await poller._poll_once()

    client.call_tool.assert_awaited_once_with(
        "update_board_card",
        {
            "session_id": "sess-1",
            "card_id": "card-1",
            "column": "finished",
            "idempotency_key": "deskbridge-wi-1-done",
        },
    )


async def test_dm_work_item_claim_does_not_call_update_board_card():
    shutdown = asyncio.Event()
    project_row = _row(id="proj-1")
    dm_item = _row(
        id="wi-2", source_type="dm", source_id="msg-1",
        summary="Fix bug", payload_json="{}", attempt_count=0,
    )
    store = MagicMock()
    store.get_project_for_identity = AsyncMock(return_value=project_row)
    store.get_pending_work_items = AsyncMock(return_value=[dm_item])
    store.claim_work_item = AsyncMock(return_value=True)
    store.get_work_item = AsyncMock(return_value=None)
    store.log_audit = AsyncMock()

    broker = MagicMock()
    broker.get_session_id = AsyncMock(return_value="sess-1")

    client = MagicMock()
    client.call_tool = AsyncMock()

    async def fake_run():
        shutdown.set()

    with patch("deskbridge.agent.poller.AgentRunner") as MockRunner:
        MockRunner.return_value.run = AsyncMock(side_effect=fake_run)
        poller = WorkItemPoller(
            identity_label="alice",
            store=store,
            client=client,
            broker=broker,
            config=make_config(),
            shutdown_event=shutdown,
            poll_interval_secs=0.01,
        )
        await poller.run()

    client.call_tool.assert_not_awaited()


async def test_update_board_card_error_on_claim_does_not_block_dispatch():
    shutdown = asyncio.Event()
    project_row = _row(id="proj-1")
    kanban_item = _row(
        id="wi-1", source_type="kanban", source_id="card-1",
        summary="Fix", payload_json="{}", attempt_count=0,
    )
    store = MagicMock()
    store.get_project_for_identity = AsyncMock(return_value=project_row)
    store.get_pending_work_items = AsyncMock(return_value=[kanban_item])
    store.claim_work_item = AsyncMock(return_value=True)
    store.get_work_item = AsyncMock(return_value=None)
    store.log_audit = AsyncMock()

    broker = MagicMock()
    broker.get_session_id = AsyncMock(return_value="sess-1")

    client = MagicMock()
    client.call_tool = AsyncMock(side_effect=Exception("network error"))

    runner_called = False

    async def fake_run():
        nonlocal runner_called
        runner_called = True
        shutdown.set()

    with patch("deskbridge.agent.poller.AgentRunner") as MockRunner:
        MockRunner.return_value.run = AsyncMock(side_effect=fake_run)
        poller = WorkItemPoller(
            identity_label="alice",
            store=store,
            client=client,
            broker=broker,
            config=make_config(),
            shutdown_event=shutdown,
            poll_interval_secs=0.01,
            kanban_column_in_progress="doing",
            kanban_column_done="finished",
        )
        await poller.run()

    store.claim_work_item.assert_awaited_once_with("wi-1")
    assert runner_called, "AgentRunner.run should be called despite update_board_card failure"


async def test_no_session_on_sync_logs_warning_and_dispatch_proceeds():
    shutdown = asyncio.Event()
    project_row = _row(id="proj-1")
    kanban_item = _row(
        id="wi-1", source_type="kanban", source_id="card-1",
        summary="Fix", payload_json="{}", attempt_count=0,
    )
    store = MagicMock()
    store.get_project_for_identity = AsyncMock(return_value=project_row)
    store.get_pending_work_items = AsyncMock(return_value=[kanban_item])
    store.claim_work_item = AsyncMock(return_value=True)
    store.get_work_item = AsyncMock(return_value=None)
    store.log_audit = AsyncMock()

    broker = MagicMock()
    broker.get_session_id = AsyncMock(return_value=None)

    client = MagicMock()
    client.call_tool = AsyncMock()

    runner_called = False

    async def fake_run():
        nonlocal runner_called
        runner_called = True
        shutdown.set()

    with patch("deskbridge.agent.poller.AgentRunner") as MockRunner:
        MockRunner.return_value.run = AsyncMock(side_effect=fake_run)
        poller = WorkItemPoller(
            identity_label="alice",
            store=store,
            client=client,
            broker=broker,
            config=make_config(),
            shutdown_event=shutdown,
            poll_interval_secs=0.01,
            kanban_column_in_progress="doing",
            kanban_column_done="finished",
        )
        await poller.run()

    client.call_tool.assert_not_awaited()
    store.claim_work_item.assert_awaited_once_with("wi-1")
    assert runner_called, "AgentRunner.run should be called even when session is unavailable"


async def test_failed_run_below_max_attempts_requeues():
    shutdown = asyncio.Event()
    project_row = _row(id="proj-1")
    work_item = _row(
        id="wi-1", source_type="dm", source_id="msg-1",
        summary="fix bug", payload_json="{}", attempt_count=0,
    )
    completed = _row(
        id="wi-1", source_type="dm", source_id="msg-1",
        status="failed", attempt_count=0,
    )
    store = make_store(project_row=project_row, pending_items=[work_item])
    store.get_work_item = AsyncMock(return_value=completed)
    store.complete_work_item = AsyncMock()

    # Return item only once; after retry_work_item is called the cooldown filter
    # would exclude it in production — simulate that here so the poller doesn't
    # re-dispatch the same item on subsequent poll cycles.
    pending_call_count = 0
    async def pending_once(*args, **kwargs):
        nonlocal pending_call_count
        pending_call_count += 1
        return [work_item] if pending_call_count == 1 else []
    store.get_pending_work_items = AsyncMock(side_effect=pending_once)

    async def fake_run():
        pass  # completes immediately, runner writes status=failed to DB

    with patch("deskbridge.agent.poller.AgentRunner") as MockRunner:
        MockRunner.return_value.run = AsyncMock(side_effect=fake_run)

        async def stop():
            await asyncio.sleep(0.08)
            shutdown.set()

        poller = make_poller(store, shutdown)
        await asyncio.gather(poller.run(), stop())

    store.retry_work_item.assert_awaited_once()
    call_args = store.retry_work_item.call_args
    assert call_args[0][0] == "wi-1"  # id
    # second arg is next_retry_at — just verify it's a non-empty string
    assert isinstance(call_args[0][1], str) and len(call_args[0][1]) > 0


async def test_failed_run_at_max_attempts_stays_failed():
    # attempt_count=2, max_agent_attempts=3 → 2+1 == 3, not < 3 → terminal
    shutdown = asyncio.Event()
    project_row = _row(id="proj-1")
    work_item = _row(
        id="wi-1", source_type="dm", source_id="msg-1",
        summary="fix bug", payload_json="{}", attempt_count=2,
    )
    completed = _row(
        id="wi-1", source_type="dm", source_id="msg-1",
        status="failed", attempt_count=2,
    )
    store = make_store(project_row=project_row, pending_items=[work_item])
    store.get_work_item = AsyncMock(return_value=completed)
    store.complete_work_item = AsyncMock()

    # Return item only once to avoid repeated dispatches within the test window.
    pending_call_count = 0
    async def pending_once(*args, **kwargs):
        nonlocal pending_call_count
        pending_call_count += 1
        return [work_item] if pending_call_count == 1 else []
    store.get_pending_work_items = AsyncMock(side_effect=pending_once)

    async def fake_run():
        pass

    with patch("deskbridge.agent.poller.AgentRunner") as MockRunner:
        MockRunner.return_value.run = AsyncMock(side_effect=fake_run)

        async def stop():
            await asyncio.sleep(0.08)
            shutdown.set()

        poller = make_poller(store, shutdown)
        await asyncio.gather(poller.run(), stop())

    store.retry_work_item.assert_not_awaited()
