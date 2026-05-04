import asyncio
from unittest.mock import AsyncMock, MagicMock, patch, ANY

from deskbridge.agent.poller import WorkItemPoller
from deskbridge.config import (
    DeskBridgeConfig, SupervisorConfig, McpConfig, IdentityConfig, ProjectConfig
)

ALICE = IdentityConfig(label="alice", npub="npub1alice", passphrase_ref="env:X")
PROJ = ProjectConfig(
    id="proj-1", name="MyProj", repo_path="/repo",
    identity="alice", escalation_dm_target="npub1op", agents=["claude-code"],
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
                     summary="fix bug", payload_json="{}")
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
                     summary="fix bug", payload_json="{}")
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
                     summary="fix bug", payload_json="{}")
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
                     summary="fix bug", payload_json="{}")
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
