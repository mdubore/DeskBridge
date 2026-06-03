import asyncio
import json
from unittest.mock import AsyncMock, MagicMock

from deskbridge.agent.checkin_scheduler import ScheduledCheckInWatcher


def make_store(*, dispatched_item=None, upsert_result=True):
    store = MagicMock()
    store.get_latest_dispatched_work_item = AsyncMock(return_value=dispatched_item)
    store.upsert_work_item = AsyncMock(return_value=upsert_result)
    return store


def make_watcher(store, shutdown, *, interval_hours=24.0, prompt="Check status."):
    return ScheduledCheckInWatcher(
        identity_label="alice",
        identity_id="acc-alice",
        operator_npub="npub1op",
        interval_hours=interval_hours,
        prompt=prompt,
        store=store,
        client=MagicMock(),
        broker=MagicMock(),
        shutdown_event=shutdown,
    )


async def test_checkin_queues_work_item_on_startup():
    store = make_store()
    shutdown = asyncio.Event()
    watcher = make_watcher(store, shutdown)

    async def stop():
        await asyncio.sleep(0.05)
        shutdown.set()

    await asyncio.gather(watcher.run(), stop())

    store.upsert_work_item.assert_awaited_once()
    call_kwargs = store.upsert_work_item.call_args.kwargs
    assert call_kwargs["source_type"] == "scheduled"
    assert call_kwargs["source_id"] == "scheduled"
    assert call_kwargs["identity_id"] == "acc-alice"
    assert call_kwargs["summary"] == "Check status."
    payload = json.loads(call_kwargs["payload_json"])
    assert payload["operator_npub"] == "npub1op"
    assert payload["prompt"] == "Check status."


async def test_checkin_skips_when_agent_busy():
    active_run = MagicMock()
    store = make_store(dispatched_item=active_run)
    shutdown = asyncio.Event()
    watcher = make_watcher(store, shutdown)

    async def stop():
        await asyncio.sleep(0.05)
        shutdown.set()

    await asyncio.gather(watcher.run(), stop())

    store.upsert_work_item.assert_not_awaited()


async def test_checkin_noop_when_idempotency_key_exists():
    store = make_store(upsert_result=False)
    shutdown = asyncio.Event()
    watcher = make_watcher(store, shutdown)

    async def stop():
        await asyncio.sleep(0.05)
        shutdown.set()

    await asyncio.gather(watcher.run(), stop())

    store.upsert_work_item.assert_awaited_once()


async def test_checkin_exception_does_not_crash_loop():
    store = MagicMock()
    store.get_latest_dispatched_work_item = AsyncMock(side_effect=Exception("db error"))
    store.upsert_work_item = AsyncMock(return_value=True)
    shutdown = asyncio.Event()
    watcher = make_watcher(store, shutdown)

    async def stop():
        await asyncio.sleep(0.05)
        shutdown.set()

    await asyncio.gather(watcher.run(), stop())


async def test_checkin_shutdown_stops_loop_without_initial_sleep():
    store = make_store()
    shutdown = asyncio.Event()
    # 24-hour interval: if watcher slept first, this test would hang
    watcher = make_watcher(store, shutdown, interval_hours=24.0)

    async def stop():
        await asyncio.sleep(0.05)
        shutdown.set()

    await asyncio.gather(watcher.run(), stop())
    # Completing in 50ms with a 24h interval proves no initial sleep
    store.upsert_work_item.assert_awaited_once()


async def test_checkin_idempotency_key_uses_time_bucket():
    store = make_store()
    shutdown = asyncio.Event()
    watcher = make_watcher(store, shutdown, interval_hours=1.0)

    async def stop():
        await asyncio.sleep(0.05)
        shutdown.set()

    await asyncio.gather(watcher.run(), stop())

    call_kwargs = store.upsert_work_item.call_args.kwargs
    key = call_kwargs["idempotency_key"]
    assert key.startswith("checkin-acc-alice-")
    bucket_str = key[len("checkin-acc-alice-"):]
    assert bucket_str.isdigit()


async def test_checkin_direct_check_and_queue_uses_given_bucket():
    store = make_store()
    shutdown = asyncio.Event()
    watcher = make_watcher(store, shutdown)

    await watcher._check_and_queue(current_bucket=99)

    call_kwargs = store.upsert_work_item.call_args.kwargs
    assert call_kwargs["idempotency_key"] == "checkin-acc-alice-99"
    assert call_kwargs["source_type"] == "scheduled"


async def test_checkin_summary_truncated_to_200_chars():
    store = make_store()
    shutdown = asyncio.Event()
    long_prompt = "x" * 300
    watcher = make_watcher(store, shutdown, prompt=long_prompt)

    await watcher._check_and_queue(current_bucket=1)

    call_kwargs = store.upsert_work_item.call_args.kwargs
    assert len(call_kwargs["summary"]) == 200
    assert call_kwargs["summary"] == "x" * 200
    payload = json.loads(call_kwargs["payload_json"])
    assert len(payload["prompt"]) == 300
