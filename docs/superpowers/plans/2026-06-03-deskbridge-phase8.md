# DeskBridge Phase 8: Scheduled Project Check-ins — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `ScheduledCheckInWatcher` that fires configurable autonomous check-ins on a time interval without requiring an operator DM.

**Architecture:** A new `ScheduledCheckInWatcher` asyncio task checks the current interval bucket on startup (no initial sleep), inserts a `source_type="scheduled"` work item if no agent is already running, then sleeps to the next bucket boundary. `AgentRunner` gains a step 10 that DMes the `operator_npub` stored in the work item's `payload_json` when a scheduled item completes. The Supervisor spawns the watcher per-identity when `check_in_interval_hours` is set in config.

**Tech Stack:** Python asyncio, Pydantic, aiosqlite, structlog — no new dependencies.

---

## File Structure

| File | Action | Responsibility |
|---|---|---|
| `deskbridge/config.py` | Modify | Add `check_in_interval_hours` and `check_in_prompt` to `ProjectConfig` |
| `deskbridge/agent/checkin_scheduler.py` | Create | `ScheduledCheckInWatcher` — bucket-aligned interval scheduling |
| `deskbridge/agent/runner.py` | Modify | Step 10: DM `operator_npub` when `source_type == "scheduled"` |
| `deskbridge/supervisor.py` | Modify | Spawn `ScheduledCheckInWatcher` per identity, add to cancel list |
| `tests/test_config.py` | Modify | Validation tests for new `ProjectConfig` fields |
| `tests/test_checkin_scheduler.py` | Create | Unit tests for `ScheduledCheckInWatcher` |
| `tests/test_agent_runner.py` | Modify | Tests for scheduled completion DM |
| `tests/test_supervisor.py` | Modify | Tests for conditional watcher spawn |

---

## Task 1: Add `check_in_interval_hours` and `check_in_prompt` to `ProjectConfig`

**Files:**
- Modify: `deskbridge/config.py` (around line 69 — `ProjectConfig` class)
- Modify: `tests/test_config.py`

- [ ] **Step 1: Write failing tests**

Add to `tests/test_config.py` (existing `MINIMAL_CONFIG` and `load_config` import already present):

```python
def test_project_config_check_in_interval_hours_accepted(tmp_path):
    cfg_file = tmp_path / "config.toml"
    cfg_file.write_text(MINIMAL_CONFIG.rstrip() + "\ncheck_in_interval_hours = 24.0\n")
    config = load_config(cfg_file)
    assert config.projects[0].check_in_interval_hours == 24.0


def test_project_config_check_in_fields_absent_by_default(tmp_path):
    cfg_file = tmp_path / "config.toml"
    cfg_file.write_text(MINIMAL_CONFIG)
    config = load_config(cfg_file)
    assert config.projects[0].check_in_interval_hours is None
    assert config.projects[0].check_in_prompt == (
        "Perform a project status check-in and report any blockers or progress."
    )


def test_project_config_check_in_interval_zero_rejected(tmp_path):
    cfg_file = tmp_path / "config.toml"
    cfg_file.write_text(MINIMAL_CONFIG.rstrip() + "\ncheck_in_interval_hours = 0.0\n")
    with pytest.raises(ConfigError):
        load_config(cfg_file)


def test_project_config_check_in_interval_negative_rejected(tmp_path):
    cfg_file = tmp_path / "config.toml"
    cfg_file.write_text(MINIMAL_CONFIG.rstrip() + "\ncheck_in_interval_hours = -1.0\n")
    with pytest.raises(ConfigError):
        load_config(cfg_file)
```

Also add `import pytest` to `tests/test_config.py` at the top if not already present (check: it is already present on line 2).

- [ ] **Step 2: Run tests to verify they fail**

```bash
pytest tests/test_config.py::test_project_config_check_in_interval_hours_accepted \
       tests/test_config.py::test_project_config_check_in_fields_absent_by_default \
       tests/test_config.py::test_project_config_check_in_interval_zero_rejected \
       tests/test_config.py::test_project_config_check_in_interval_negative_rejected \
       -v
```

Expected: 4 FAILED (AttributeError on `check_in_interval_hours`)

- [ ] **Step 3: Add fields to `ProjectConfig`**

In `deskbridge/config.py`, the `ProjectConfig` class ends at line 81 with `kanban_column_done`. Add after that line:

```python
class ProjectConfig(BaseModel):
    id: str
    name: str
    repo_path: str
    identity: str
    escalation_dm_target: str
    agents: list[str] = Field(default_factory=lambda: ["codex", "claude-code"])
    allowed_autonomous_actions: list[str] = Field(
        default_factory=lambda: ["read", "send_dm", "update_task_status"]
    )
    boards: list[str] = Field(default_factory=list)
    kanban_column_in_progress: str = "in_progress"
    kanban_column_done: str = "done"
    check_in_interval_hours: float | None = Field(default=None, gt=0)
    check_in_prompt: str = (
        "Perform a project status check-in and report any blockers or progress."
    )
```

`Field` is already imported at the top of `deskbridge/config.py` (`from pydantic import BaseModel, Field, ValidationError, field_validator`).

- [ ] **Step 4: Run tests to verify they pass**

```bash
pytest tests/test_config.py::test_project_config_check_in_interval_hours_accepted \
       tests/test_config.py::test_project_config_check_in_fields_absent_by_default \
       tests/test_config.py::test_project_config_check_in_interval_zero_rejected \
       tests/test_config.py::test_project_config_check_in_interval_negative_rejected \
       -v
```

Expected: 4 PASSED

- [ ] **Step 5: Run the full test suite to check for regressions**

```bash
pytest tests/test_config.py -v
```

Expected: all tests PASSED

- [ ] **Step 6: Commit**

```bash
git add deskbridge/config.py tests/test_config.py
git commit -m "feat: add check_in_interval_hours and check_in_prompt to ProjectConfig"
```

---

## Task 2: Implement `ScheduledCheckInWatcher`

**Files:**
- Create: `deskbridge/agent/checkin_scheduler.py`
- Create: `tests/test_checkin_scheduler.py`

- [ ] **Step 1: Write failing tests**

Create `tests/test_checkin_scheduler.py`:

```python
import asyncio
import json
from unittest.mock import AsyncMock, MagicMock, ANY

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
    # If exception crashed loop, run() would raise — reaching here means it survived


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
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
pytest tests/test_checkin_scheduler.py -v
```

Expected: 7 FAILED (ModuleNotFoundError: `deskbridge.agent.checkin_scheduler`)

- [ ] **Step 3: Implement `ScheduledCheckInWatcher`**

Create `deskbridge/agent/checkin_scheduler.py`:

```python
import asyncio
import json
import time
from datetime import datetime, timezone
from uuid import uuid4

import structlog

from deskbridge.db.store import Store
from deskbridge.mcp.client import McpClient
from deskbridge.mcp.session import SessionBroker

log = structlog.get_logger()


class ScheduledCheckInWatcher:
    def __init__(
        self,
        identity_label: str,
        identity_id: str,
        operator_npub: str,
        interval_hours: float,
        prompt: str,
        store: Store,
        client: McpClient,
        broker: SessionBroker,
        shutdown_event: asyncio.Event,
    ) -> None:
        self._identity_label = identity_label
        self._identity_id = identity_id
        self._operator_npub = operator_npub
        self._interval_secs = interval_hours * 3600
        self._prompt = prompt
        self._store = store
        self._client = client
        self._broker = broker
        self._shutdown_event = shutdown_event

    async def run(self) -> None:
        log.info("checkin_watcher_started", identity=self._identity_label)
        try:
            while not self._shutdown_event.is_set():
                current_time = time.time()
                current_bucket = int(current_time // self._interval_secs)
                try:
                    await self._check_and_queue(current_bucket)
                except Exception:
                    log.warning(
                        "checkin_watcher_error",
                        identity=self._identity_label,
                        exc_info=True,
                    )
                next_bucket_time = (current_bucket + 1) * self._interval_secs
                sleep_duration = next_bucket_time - time.time()
                log.debug(
                    "checkin_sleeping",
                    next_checkin_utc=datetime.fromtimestamp(
                        next_bucket_time, tz=timezone.utc
                    ).strftime("%Y-%m-%dT%H:%M:%SZ"),
                    sleep_secs=round(sleep_duration, 1),
                )
                try:
                    await asyncio.wait_for(
                        self._shutdown_event.wait(), timeout=sleep_duration
                    )
                except asyncio.TimeoutError:
                    pass
        finally:
            log.info("checkin_watcher_stopped", identity=self._identity_label)

    async def _check_and_queue(self, current_bucket: int) -> None:
        active = await self._store.get_latest_dispatched_work_item(self._identity_id)
        if active is not None:
            log.info("checkin_skipped_agent_busy", identity=self._identity_label)
            return

        inserted = await self._store.upsert_work_item(
            id=str(uuid4()),
            source_type="scheduled",
            source_id="scheduled",
            identity_id=self._identity_id,
            summary=self._prompt[:200],
            payload_json=json.dumps(
                {"prompt": self._prompt, "operator_npub": self._operator_npub}
            ),
            idempotency_key=f"checkin-{self._identity_id}-{current_bucket}",
        )
        if inserted:
            log.info("checkin_work_item_created", identity=self._identity_label)
        else:
            log.info("checkin_already_queued", identity=self._identity_label)
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
pytest tests/test_checkin_scheduler.py -v
```

Expected: 7 PASSED

- [ ] **Step 5: Commit**

```bash
git add deskbridge/agent/checkin_scheduler.py tests/test_checkin_scheduler.py
git commit -m "feat: add ScheduledCheckInWatcher with bucket-aligned interval scheduling"
```

---

## Task 3: `AgentRunner` step 10 — DM operator on scheduled completion

**Files:**
- Modify: `deskbridge/agent/runner.py`
- Modify: `tests/test_agent_runner.py`

**Note:** The spec places this DM in `WorkItemPoller`, but `result_text` (the agent's output) is only available inside `AgentRunner._do_run`. Placing it in `runner.py` as step 10 follows the exact same pattern as the existing step 8 (`escalation_dm_target`) and requires no new store methods.

- [ ] **Step 1: Write failing tests**

Add to `tests/test_agent_runner.py`:

```python
async def test_scheduled_completion_dms_operator_npub():
    work_item = _row(
        id="wi-1",
        source_type="scheduled",
        source_id="scheduled",
        summary="Check status.",
        payload_json='{"prompt": "Check status.", "operator_npub": "npub1op"}',
    )
    store = make_store()
    proc = make_proc(returncode=0, stdout_lines=[b"check-in complete\n"])

    with patch("asyncio.create_subprocess_exec", return_value=proc):
        runner = make_runner(work_item, project=PROJ_NO_DM, store=store)
        await runner.run()

    outbox_calls = store.insert_outbox_item.call_args_list
    checkin_calls = [
        c for c in outbox_calls
        if "checkin_result" in (c.kwargs.get("idempotency_key") or "")
    ]
    assert len(checkin_calls) == 1
    assert checkin_calls[0].kwargs["dest_pubkey"] == "npub1op"
    assert "check-in complete" in checkin_calls[0].kwargs["message_text"]


async def test_non_scheduled_completion_does_not_send_checkin_dm():
    work_item = _row(
        id="wi-1",
        source_type="dm",
        source_id="msg-1",
        summary="fix bug",
        payload_json="{}",
    )
    store = make_store()
    proc = make_proc(returncode=0)

    with patch("asyncio.create_subprocess_exec", return_value=proc):
        runner = make_runner(work_item, project=PROJ_NO_DM, store=store)
        await runner.run()

    outbox_calls = store.insert_outbox_item.call_args_list
    checkin_calls = [
        c for c in outbox_calls
        if "checkin_result" in (c.kwargs.get("idempotency_key") or "")
    ]
    assert len(checkin_calls) == 0
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
pytest tests/test_agent_runner.py::test_scheduled_completion_dms_operator_npub \
       tests/test_agent_runner.py::test_non_scheduled_completion_does_not_send_checkin_dm \
       -v
```

Expected: 2 FAILED (`checkin_calls` empty — step 10 does not yet exist)

- [ ] **Step 3: Add `import json` and step 10 to `runner.py`**

Add `import json` to the imports at the top of `deskbridge/agent/runner.py` (after `import collections`):

```python
import asyncio
import collections
import json
import structlog
from datetime import datetime, timezone
from uuid import uuid4
```

Add step 10 at the end of `_do_run`, after the existing step 9 block (after line 172):

```python
        # 10. Notify operator_npub for scheduled check-ins
        if work_item["source_type"] == "scheduled":
            payload = json.loads(work_item["payload_json"])
            operator_npub = payload.get("operator_npub")
            if operator_npub:
                await self._store.insert_outbox_item(
                    id=str(uuid4()),
                    identity_id=f"acc-{project.identity}",
                    dest_pubkey=operator_npub,
                    message_text=result_text,
                    idempotency_key=f"deskbridge:{run_id}:checkin_result",
                )
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
pytest tests/test_agent_runner.py::test_scheduled_completion_dms_operator_npub \
       tests/test_agent_runner.py::test_non_scheduled_completion_does_not_send_checkin_dm \
       -v
```

Expected: 2 PASSED

- [ ] **Step 5: Run full runner test suite for regressions**

```bash
pytest tests/test_agent_runner.py -v
```

Expected: all PASSED

- [ ] **Step 6: Commit**

```bash
git add deskbridge/agent/runner.py tests/test_agent_runner.py
git commit -m "feat: DM operator_npub when scheduled check-in completes (AgentRunner step 10)"
```

---

## Task 4: Wire `ScheduledCheckInWatcher` into `Supervisor`

**Files:**
- Modify: `deskbridge/supervisor.py`
- Modify: `tests/test_supervisor.py`

- [ ] **Step 1: Write failing tests**

Add to `tests/test_supervisor.py`. The existing imports already include `DeskBridgeConfig`, `SupervisorConfig`, `McpConfig`, `IdentityConfig`, `ProjectConfig`, `asyncio`, `AsyncMock`, `MagicMock`, `patch`, `Mock`, `ANY`.

```python
async def test_supervisor_spawns_checkin_watcher_when_interval_configured(
    tmp_path, monkeypatch, mock_broker, mock_client_ctx
):
    monkeypatch.setenv("ALICE", "pass")
    config = DeskBridgeConfig(
        supervisor=SupervisorConfig(
            db_path=str(tmp_path / "test.db"), heartbeat_interval_secs=0.05
        ),
        mcp=McpConfig(command="nostrdesk-mcp"),
        identities=[
            IdentityConfig(
                label="alice",
                npub="npub1alice",
                passphrase_ref="env:ALICE",
                operator_npub="npub1op",
            )
        ],
        projects=[
            ProjectConfig(
                id="proj-1",
                name="Proj",
                repo_path="/repo",
                identity="alice",
                escalation_dm_target="npub1op",
                check_in_interval_hours=24.0,
            )
        ],
    )

    with patch("deskbridge.supervisor.McpClient") as MockMcpClient, \
         patch("deskbridge.supervisor.SessionBroker", return_value=mock_broker), \
         patch("deskbridge.supervisor.apply_schema", new=AsyncMock()), \
         patch("deskbridge.supervisor.bootstrap_accounts_from_config", new=AsyncMock()), \
         patch("deskbridge.supervisor.DmWatcher") as MockDmWatcher, \
         patch("deskbridge.supervisor.OutboxDrainer") as MockOutboxDrainer, \
         patch("deskbridge.supervisor.WorkItemPoller") as MockWorkItemPoller, \
         patch("deskbridge.supervisor.ApprovalRequestWatcher") as MockApprovalWatcher, \
         patch("deskbridge.supervisor.ScheduledCheckInWatcher") as MockCheckInWatcher, \
         patch("deskbridge.supervisor.Store") as MockStore:

        MockMcpClient.return_value.connect.return_value = mock_client_ctx

        async def never_finishes():
            await asyncio.Event().wait()

        MockDmWatcher.return_value.run = Mock(side_effect=never_finishes)
        MockOutboxDrainer.return_value.run = Mock(side_effect=never_finishes)
        MockWorkItemPoller.return_value.run = Mock(side_effect=never_finishes)
        MockApprovalWatcher.return_value.run = Mock(side_effect=never_finishes)
        MockCheckInWatcher.return_value.run = Mock(side_effect=never_finishes)

        mock_store_instance = AsyncMock()
        mock_store_instance.get_project_groups = AsyncMock(return_value=[])
        mock_store_instance.get_project_for_identity = AsyncMock(return_value=None)
        MockStore.return_value = mock_store_instance

        supervisor = Supervisor(config=config)

        async def stop():
            await asyncio.sleep(0.1)
            supervisor.request_shutdown()

        await asyncio.gather(supervisor.run(), stop())

    MockCheckInWatcher.assert_called_once_with(
        identity_label="alice",
        identity_id="acc-alice",
        operator_npub="npub1op",
        interval_hours=24.0,
        prompt="Perform a project status check-in and report any blockers or progress.",
        store=ANY,
        client=ANY,
        broker=mock_broker,
        shutdown_event=ANY,
    )
    MockCheckInWatcher.return_value.run.assert_called_once()


async def test_supervisor_no_checkin_watcher_when_interval_not_configured(
    tmp_path, monkeypatch, mock_broker, mock_client_ctx
):
    monkeypatch.setenv("ALICE", "pass")
    config = make_config(tmp_path)  # no projects → no check_in_interval_hours

    with patch("deskbridge.supervisor.McpClient") as MockMcpClient, \
         patch("deskbridge.supervisor.SessionBroker", return_value=mock_broker), \
         patch("deskbridge.supervisor.apply_schema", new=AsyncMock()), \
         patch("deskbridge.supervisor.bootstrap_accounts_from_config", new=AsyncMock()), \
         patch("deskbridge.supervisor.DmWatcher") as MockDmWatcher, \
         patch("deskbridge.supervisor.OutboxDrainer") as MockOutboxDrainer, \
         patch("deskbridge.supervisor.WorkItemPoller") as MockWorkItemPoller, \
         patch("deskbridge.supervisor.ScheduledCheckInWatcher") as MockCheckInWatcher, \
         patch("deskbridge.supervisor.Store") as MockStore:

        MockMcpClient.return_value.connect.return_value = mock_client_ctx

        async def never_finishes():
            await asyncio.Event().wait()

        MockDmWatcher.return_value.run = Mock(side_effect=never_finishes)
        MockOutboxDrainer.return_value.run = Mock(side_effect=never_finishes)
        MockWorkItemPoller.return_value.run = Mock(side_effect=never_finishes)

        mock_store_instance = AsyncMock()
        mock_store_instance.get_project_groups = AsyncMock(return_value=[])
        mock_store_instance.get_project_for_identity = AsyncMock(return_value=None)
        MockStore.return_value = mock_store_instance

        supervisor = Supervisor(config=config)

        async def stop():
            await asyncio.sleep(0.1)
            supervisor.request_shutdown()

        await asyncio.gather(supervisor.run(), stop())

    MockCheckInWatcher.assert_not_called()
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
pytest tests/test_supervisor.py::test_supervisor_spawns_checkin_watcher_when_interval_configured \
       tests/test_supervisor.py::test_supervisor_no_checkin_watcher_when_interval_not_configured \
       -v
```

Expected: 2 FAILED (ImportError or `assert_called_once` failing — `ScheduledCheckInWatcher` not yet imported/used in supervisor)

- [ ] **Step 3: Wire `ScheduledCheckInWatcher` into `supervisor.py`**

Add the import at the top of `deskbridge/supervisor.py`, after the existing watcher imports:

```python
from deskbridge.dm.kanban_watcher import KanbanWatcher
from deskbridge.dm.outbox import OutboxDrainer
from deskbridge.agent.poller import WorkItemPoller
from deskbridge.agent.checkin_scheduler import ScheduledCheckInWatcher
from deskbridge.mcp import McpClient, SessionBroker
```

Add `checkin_watcher_tasks` to the task list declarations (around line 68, alongside the other list declarations):

```python
watcher_tasks: list[asyncio.Task] = []
group_watcher_tasks: list[asyncio.Task] = []
approval_watcher_tasks: list[asyncio.Task] = []
drainer_task: asyncio.Task | None = None
poller_tasks: list[asyncio.Task] = []
kanban_watcher_tasks: list[asyncio.Task] = []
checkin_watcher_tasks: list[asyncio.Task] = []
```

Add the checkin watcher loop after the existing `group_watcher_tasks` loop (after the `if groups:` block, before `interval = self._config.supervisor.heartbeat_interval_secs`):

```python
                    for identity in self._config.identities:
                        project_cfg = next(
                            (p for p in self._config.projects if p.identity == identity.label),
                            None,
                        )
                        if (
                            project_cfg
                            and project_cfg.check_in_interval_hours
                            and identity.operator_npub
                        ):
                            checkin_watcher_tasks.append(
                                asyncio.create_task(
                                    ScheduledCheckInWatcher(
                                        identity_label=identity.label,
                                        identity_id=f"acc-{identity.label}",
                                        operator_npub=identity.operator_npub,
                                        interval_hours=project_cfg.check_in_interval_hours,
                                        prompt=project_cfg.check_in_prompt,
                                        store=store,
                                        client=client,
                                        broker=broker,
                                        shutdown_event=self._shutdown_event,
                                    ).run(),
                                    name=f"checkin_watcher_{identity.label}",
                                )
                            )
```

Update the `finally` cancel list (currently around line 189) to include `checkin_watcher_tasks`:

```python
                    tasks_to_cancel = (
                        watcher_tasks
                        + group_watcher_tasks
                        + approval_watcher_tasks
                        + poller_tasks
                        + kanban_watcher_tasks
                        + checkin_watcher_tasks
                        + ([drainer_task] if drainer_task is not None else [])
                    )
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
pytest tests/test_supervisor.py::test_supervisor_spawns_checkin_watcher_when_interval_configured \
       tests/test_supervisor.py::test_supervisor_no_checkin_watcher_when_interval_not_configured \
       -v
```

Expected: 2 PASSED

- [ ] **Step 5: Run full supervisor test suite for regressions**

```bash
pytest tests/test_supervisor.py -v
```

Expected: all PASSED

- [ ] **Step 6: Run full test suite**

```bash
pytest --tb=short -q
```

Expected: all 255+ tests pass (no regressions)

- [ ] **Step 7: Commit**

```bash
git add deskbridge/supervisor.py tests/test_supervisor.py
git commit -m "feat: wire ScheduledCheckInWatcher into Supervisor"
```
