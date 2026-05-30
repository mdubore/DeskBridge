# DeskBridge Phase 7: Kanban Task Coordination — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Poll NostrDesk kanban boards for cards assigned to each identity, insert them as work items, and write column status back to the board as work progresses.

**Architecture:** Five targeted changes — add three fields to `ProjectConfig`, extend `upsert_work_item` to return `bool`, create `KanbanWatcher`, add `_sync_card_column` writeback to `WorkItemPoller`, and wire both into `Supervisor`. No schema changes.

**Tech Stack:** Python, aiosqlite, asyncio, structlog, pydantic, McpClient, pytest-asyncio (`asyncio_mode = auto`)

---

## File Map

| File | Action | Summary |
|---|---|---|
| `deskbridge/config.py` | Modify | Add `boards`, `kanban_column_in_progress`, `kanban_column_done` to `ProjectConfig` |
| `deskbridge/db/store.py` | Modify | `upsert_work_item` returns `bool` (True if newly inserted) |
| `deskbridge/dm/kanban_watcher.py` | Create | 30s interval poll watcher for assigned board cards |
| `deskbridge/agent/poller.py` | Modify | `_sync_card_column`, two writeback call sites, two new constructor params |
| `deskbridge/supervisor.py` | Modify | Spawn `KanbanWatcher` per identity; pass column params to `WorkItemPoller` |
| `tests/test_config.py` | Modify | Three new config field tests |
| `tests/test_store.py` | Modify | Two new return-value tests for `upsert_work_item` |
| `tests/test_kanban_watcher.py` | Create | Nine unit tests for `KanbanWatcher` |
| `tests/test_work_item_poller.py` | Modify | Five writeback tests |
| `tests/test_supervisor.py` | Modify | Update poller assertion; add `get_project_for_identity` mocks; two KanbanWatcher spawn tests |

---

## Task 1: Config — kanban fields on `ProjectConfig`

**Files:**
- Modify: `deskbridge/config.py`
- Modify: `tests/test_config.py`

- [ ] **Step 1: Write the failing tests**

Add to the bottom of `tests/test_config.py`:

```python
def test_project_config_boards_defaults_to_empty():
    from deskbridge.config import ProjectConfig
    proj = ProjectConfig(
        id="p1", name="N", repo_path="/r",
        identity="alice", escalation_dm_target="npub1op"
    )
    assert proj.boards == []


def test_project_config_kanban_columns_default():
    from deskbridge.config import ProjectConfig
    proj = ProjectConfig(
        id="p1", name="N", repo_path="/r",
        identity="alice", escalation_dm_target="npub1op"
    )
    assert proj.kanban_column_in_progress == "in_progress"
    assert proj.kanban_column_done == "done"


def test_project_config_kanban_fields_parse_from_toml(tmp_path):
    content = textwrap.dedent("""
        [supervisor]
        db_path = "/tmp/test.db"

        [mcp]
        command = "nostrdesk-mcp"

        [[identities]]
        label = "alice"
        npub = "npub1alice"
        passphrase_ref = "env:ALICE_PASSPHRASE"

        [[projects]]
        id = "proj-1"
        name = "Test Project"
        repo_path = "/tmp/repo"
        identity = "alice"
        escalation_dm_target = "npub1human"
        boards = ["ch-abc", "ch-def"]
        kanban_column_in_progress = "doing"
        kanban_column_done = "finished"
    """)
    cfg_file = tmp_path / "config.toml"
    cfg_file.write_text(content)
    config = load_config(cfg_file)
    proj = config.projects[0]
    assert proj.boards == ["ch-abc", "ch-def"]
    assert proj.kanban_column_in_progress == "doing"
    assert proj.kanban_column_done == "finished"
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cd /home/missydog/Desktop/Learnding/Tools/DeskBridge
pytest tests/test_config.py::test_project_config_boards_defaults_to_empty tests/test_config.py::test_project_config_kanban_columns_default tests/test_config.py::test_project_config_kanban_fields_parse_from_toml -v
```

Expected: FAIL — `ProjectConfig` has no `boards`, `kanban_column_in_progress`, or `kanban_column_done` attributes.

- [ ] **Step 3: Add the three fields to `ProjectConfig`**

In `deskbridge/config.py`, replace the `ProjectConfig` class body (around line 69–79) with:

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
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
pytest tests/test_config.py -v
```

Expected: All config tests pass.

- [ ] **Step 5: Commit**

```bash
git add deskbridge/config.py tests/test_config.py
git commit -m "feat: add boards and kanban column fields to ProjectConfig"
```

---

## Task 2: Store — `upsert_work_item` returns `bool`

**Files:**
- Modify: `deskbridge/db/store.py`
- Modify: `tests/test_store.py`

- [ ] **Step 1: Write the failing tests**

Add to the bottom of `tests/test_store.py`:

```python
async def test_upsert_work_item_returns_true_on_first_insert(store: Store):
    await store.upsert_account(
        id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X"
    )
    result = await store.upsert_work_item(
        id="wi-1",
        source_type="kanban",
        source_id="card-1",
        identity_id="acc-alice",
        summary="Fix bug",
        payload_json="{}",
        idempotency_key="kanban-card-1",
    )
    assert result is True


async def test_upsert_work_item_returns_false_on_duplicate(store: Store):
    await store.upsert_account(
        id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X"
    )
    await store.upsert_work_item(
        id="wi-1",
        source_type="kanban",
        source_id="card-1",
        identity_id="acc-alice",
        summary="Fix bug",
        payload_json="{}",
        idempotency_key="kanban-card-1",
    )
    result = await store.upsert_work_item(
        id="wi-2",
        source_type="kanban",
        source_id="card-1",
        identity_id="acc-alice",
        summary="Fix bug",
        payload_json="{}",
        idempotency_key="kanban-card-1",
    )
    assert result is False
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
pytest tests/test_store.py::test_upsert_work_item_returns_true_on_first_insert tests/test_store.py::test_upsert_work_item_returns_false_on_duplicate -v
```

Expected: FAIL — `upsert_work_item` returns `None`.

- [ ] **Step 3: Change `upsert_work_item` to return `bool`**

In `deskbridge/db/store.py`, replace the `upsert_work_item` method (lines 166–185):

```python
async def upsert_work_item(
    self,
    id: str,
    source_type: str,
    source_id: str,
    identity_id: str,
    summary: str,
    payload_json: str,
    idempotency_key: str,
) -> bool:
    async with self._conn.execute(
        """
        INSERT OR IGNORE INTO work_items
            (id, source_type, source_id, identity_id, summary, payload_json, idempotency_key)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (id, source_type, source_id, identity_id, summary, payload_json, idempotency_key),
    ) as cur:
        inserted = cur.rowcount == 1
    await self._conn.commit()
    return inserted
```

- [ ] **Step 4: Run all store tests**

```bash
pytest tests/test_store.py -v
```

Expected: All pass. The two new tests pass; existing tests that ignore the return value are unaffected.

- [ ] **Step 5: Commit**

```bash
git add deskbridge/db/store.py tests/test_store.py
git commit -m "feat: upsert_work_item returns bool indicating whether row was newly inserted"
```

---

## Task 3: KanbanWatcher

**Files:**
- Create: `deskbridge/dm/kanban_watcher.py`
- Create: `tests/test_kanban_watcher.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/test_kanban_watcher.py`:

```python
import asyncio
from unittest.mock import AsyncMock, MagicMock, ANY

import pytest

from deskbridge.dm.kanban_watcher import KanbanWatcher
from deskbridge.mcp.client import McpToolError
from deskbridge.models import McpError
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
```

- [ ] **Step 2: Run tests to verify they all fail**

```bash
pytest tests/test_kanban_watcher.py -v
```

Expected: FAIL — `ModuleNotFoundError: No module named 'deskbridge.dm.kanban_watcher'`.

- [ ] **Step 3: Create `deskbridge/dm/kanban_watcher.py`**

```python
import asyncio
import uuid
import structlog

from deskbridge.db.store import Store
from deskbridge.mcp.client import McpClient, McpToolError
from deskbridge.mcp.session import SessionBroker

log = structlog.get_logger()


class KanbanWatcher:
    def __init__(
        self,
        account_id: str,
        identity_label: str,
        store: Store,
        client: McpClient,
        broker: SessionBroker,
        shutdown_event: asyncio.Event,
        boards: list[str],
        operator_npub: str | None = None,
        poll_interval_secs: float = 30.0,
    ) -> None:
        self._account_id = account_id
        self._identity_label = identity_label
        self._store = store
        self._client = client
        self._broker = broker
        self._shutdown_event = shutdown_event
        self._boards = boards
        self._operator_npub = operator_npub
        self._poll_interval_secs = poll_interval_secs

    async def run(self) -> None:
        log.info("kanban_watcher_started", identity=self._identity_label)

        while not self._shutdown_event.is_set():
            session_id = await self._broker.get_session_id(self._identity_label)
            if session_id is None:
                log.debug("kanban_watcher_no_session", identity=self._identity_label)
                await self._sleep()
                continue

            for board_channel_id in self._boards:
                try:
                    cards = await self._client.call_tool(
                        "list_assigned_board_cards",
                        {
                            "session_id": session_id,
                            "channel_id": board_channel_id,
                            "limit": 50,
                        },
                    )
                    if not isinstance(cards, list):
                        log.warning(
                            "kanban_watcher_unexpected_response",
                            identity=self._identity_label,
                            board=board_channel_id,
                            result_type=type(cards).__name__,
                        )
                        continue
                    for card in cards:
                        await self._process_card(card)
                except McpToolError:
                    log.warning(
                        "kanban_watcher_poll_error",
                        identity=self._identity_label,
                        board=board_channel_id,
                    )
                except Exception:
                    log.exception(
                        "kanban_watcher_unexpected_error",
                        identity=self._identity_label,
                        board=board_channel_id,
                    )

            await self._sleep()

        log.info("kanban_watcher_stopped", identity=self._identity_label)

    async def _process_card(self, card: object) -> None:
        if not isinstance(card, dict):
            log.warning("kanban_watcher_non_dict_card", identity=self._identity_label)
            return

        card_id = card.get("id")
        if not card_id:
            log.warning("kanban_watcher_card_missing_id", identity=self._identity_label)
            return

        title = card.get("title") or "(untitled)"
        description = card.get("description") or ""

        idempotency_key = f"kanban-{card_id}"
        inserted = await self._store.upsert_work_item(
            id=str(uuid.uuid4()),
            source_type="kanban",
            source_id=card_id,
            identity_id=self._account_id,
            idempotency_key=idempotency_key,
            summary=title,
            payload_json=description,
        )

        if inserted:
            log.info(
                "kanban_watcher_new_card",
                identity=self._identity_label,
                card_id=card_id,
            )
            if self._operator_npub:
                message = f"New task assigned: {title}\n\nCard ID: {card_id}"
                await self._store.insert_outbox_item(
                    str(uuid.uuid4()),
                    self._account_id,
                    self._operator_npub,
                    message,
                    f"kanban-notify-{card_id}",
                )

    async def _sleep(self) -> None:
        try:
            await asyncio.wait_for(
                self._shutdown_event.wait(), timeout=self._poll_interval_secs
            )
        except asyncio.TimeoutError:
            pass
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
pytest tests/test_kanban_watcher.py -v
```

Expected: All 9 tests pass.

- [ ] **Step 5: Run the full suite to confirm no regressions**

```bash
pytest -v
```

Expected: All tests pass.

- [ ] **Step 6: Commit**

```bash
git add deskbridge/dm/kanban_watcher.py tests/test_kanban_watcher.py
git commit -m "feat: add KanbanWatcher — polls assigned board cards and dedupes via idempotency key"
```

---

## Task 4: WorkItemPoller writeback

**Files:**
- Modify: `deskbridge/agent/poller.py`
- Modify: `tests/test_work_item_poller.py`

- [ ] **Step 1: Write the failing tests**

Add to the bottom of `tests/test_work_item_poller.py`:

```python
async def test_kanban_work_item_claim_calls_update_board_card_with_configured_column():
    shutdown = asyncio.Event()
    project_row = _row(id="proj-1")
    kanban_item = _row(
        id="wi-1", source_type="kanban", source_id="card-1",
        summary="Fix auth", payload_json="{}",
    )
    store = MagicMock()
    store.get_project_for_identity = AsyncMock(return_value=project_row)
    store.get_pending_work_items = AsyncMock(return_value=[kanban_item])
    store.claim_work_item = AsyncMock(return_value=True)
    store.get_work_item = AsyncMock(return_value=None)

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
        summary="Fix auth", payload_json="{}", status="done",
    )
    store = MagicMock()
    store.get_project_for_identity = AsyncMock(return_value=project_row)
    store.get_pending_work_items = AsyncMock(return_value=[])
    store.get_work_item = AsyncMock(return_value=kanban_item)

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
    await asyncio.sleep(0)  # allow the task to finish
    poller._active_run_task = completed_task
    poller._active_work_item_id = "wi-1"

    async def stop():
        await asyncio.sleep(0.05)
        shutdown.set()

    await asyncio.gather(poller.run(), stop())

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
        summary="Fix bug", payload_json="{}",
    )
    store = MagicMock()
    store.get_project_for_identity = AsyncMock(return_value=project_row)
    store.get_pending_work_items = AsyncMock(return_value=[dm_item])
    store.claim_work_item = AsyncMock(return_value=True)
    store.get_work_item = AsyncMock(return_value=None)

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
        summary="Fix", payload_json="{}",
    )
    store = MagicMock()
    store.get_project_for_identity = AsyncMock(return_value=project_row)
    store.get_pending_work_items = AsyncMock(return_value=[kanban_item])
    store.claim_work_item = AsyncMock(return_value=True)
    store.get_work_item = AsyncMock(return_value=None)

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
        summary="Fix", payload_json="{}",
    )
    store = MagicMock()
    store.get_project_for_identity = AsyncMock(return_value=project_row)
    store.get_pending_work_items = AsyncMock(return_value=[kanban_item])
    store.claim_work_item = AsyncMock(return_value=True)
    store.get_work_item = AsyncMock(return_value=None)

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
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
pytest tests/test_work_item_poller.py::test_kanban_work_item_claim_calls_update_board_card_with_configured_column tests/test_work_item_poller.py::test_kanban_work_item_completion_calls_update_board_card_with_configured_column tests/test_work_item_poller.py::test_dm_work_item_claim_does_not_call_update_board_card tests/test_work_item_poller.py::test_update_board_card_error_on_claim_does_not_block_dispatch tests/test_work_item_poller.py::test_no_session_on_sync_logs_warning_and_dispatch_proceeds -v
```

Expected: FAIL — `WorkItemPoller.__init__` has no `kanban_column_in_progress` or `kanban_column_done` parameters and `_sync_card_column` does not exist.

- [ ] **Step 3: Update `WorkItemPoller` in `deskbridge/agent/poller.py`**

Replace the full file contents:

```python
import asyncio
import structlog
from uuid import uuid4

from deskbridge.agent.runner import AgentRunner
from deskbridge.config import DeskBridgeConfig
from deskbridge.db.store import Store
from deskbridge.mcp.client import McpClient
from deskbridge.mcp.session import SessionBroker

log = structlog.get_logger()


class WorkItemPoller:
    def __init__(
        self,
        identity_label: str,
        store: Store,
        client: McpClient,
        broker: SessionBroker,
        config: DeskBridgeConfig,
        shutdown_event: asyncio.Event,
        poll_interval_secs: float = 10.0,
        kanban_column_in_progress: str = "in_progress",
        kanban_column_done: str = "done",
    ) -> None:
        self._identity_label = identity_label
        self._account_id = f"acc-{identity_label}"
        self._store = store
        self._client = client
        self._broker = broker
        self._config = config
        self._shutdown_event = shutdown_event
        self._poll_interval_secs = poll_interval_secs
        self._kanban_column_in_progress = kanban_column_in_progress
        self._kanban_column_done = kanban_column_done
        self._active_run_task: asyncio.Task | None = None
        self._active_work_item_id: str | None = None

    async def run(self) -> None:
        log.info("work_item_poller_started", identity=self._identity_label)
        try:
            while not self._shutdown_event.is_set():
                await self._poll_once()
                try:
                    await asyncio.wait_for(
                        self._shutdown_event.wait(), timeout=self._poll_interval_secs
                    )
                except asyncio.TimeoutError:
                    pass
        finally:
            if self._active_run_task is not None and not self._active_run_task.done():
                self._active_run_task.cancel()
                await asyncio.gather(self._active_run_task, return_exceptions=True)
            log.info("work_item_poller_stopped", identity=self._identity_label)

    async def _poll_once(self) -> None:
        # Detect completed runner and fire done writeback for kanban items
        if self._active_run_task is not None and self._active_run_task.done():
            if self._active_work_item_id is not None:
                completed_item = await self._store.get_work_item(self._active_work_item_id)
                if (
                    completed_item is not None
                    and completed_item["source_type"] == "kanban"
                    and completed_item["status"] in ("done", "failed")
                ):
                    await self._sync_card_column(
                        completed_item["source_id"],
                        column=self._kanban_column_done,
                        idempotency_key=f"deskbridge-{self._active_work_item_id}-done",
                    )
            self._active_run_task = None
            self._active_work_item_id = None

        # Cancel runner if operator requested cancellation
        if (
            self._active_run_task is not None
            and not self._active_run_task.done()
            and self._active_work_item_id is not None
        ):
            row = await self._store.get_work_item(self._active_work_item_id)
            if row is not None and row["status"] == "cancel_requested":
                self._active_run_task.cancel()
                await asyncio.gather(self._active_run_task, return_exceptions=True)
                await self._store.complete_work_item(self._active_work_item_id, "cancelled")
                self._active_run_task = None
                self._active_work_item_id = None
                return

        project = await self._store.get_project_for_identity(self._account_id)
        if project is None:
            log.warning("work_item_poller_no_project", identity=self._identity_label)
            return

        rows = await self._store.get_pending_work_items(self._account_id, limit=10)
        for row in rows:
            if self._active_run_task is not None and not self._active_run_task.done():
                break  # one-at-a-time: wait for current runner to finish

            claimed = await self._store.claim_work_item(row["id"])
            if not claimed:
                continue

            if row["source_type"] == "kanban":
                await self._sync_card_column(
                    row["source_id"],
                    column=self._kanban_column_in_progress,
                    idempotency_key=f"deskbridge-{row['id']}-in-progress",
                )

            project_cfg = next(
                (p for p in self._config.projects if p.id == project["id"]), None
            )
            if project_cfg is None:
                log.error(
                    "work_item_poller_no_project_config",
                    identity=self._identity_label,
                    project_id=project["id"],
                )
                try:
                    await self._store.complete_work_item(row["id"], "failed")
                except Exception:
                    log.exception(
                        "work_item_poller_complete_failed",
                        identity=self._identity_label,
                        work_item_id=row["id"],
                    )
                continue

            runner = AgentRunner(
                work_item=row,
                project=project_cfg,
                run_id=str(uuid4()),
                store=self._store,
                client=self._client,
                broker=self._broker,
            )
            self._active_run_task = asyncio.create_task(
                runner.run(), name=f"agent_run_{row['id']}"
            )
            self._active_work_item_id = row["id"]
            break

    async def _sync_card_column(
        self, card_id: str, column: str, idempotency_key: str
    ) -> None:
        session_id = await self._broker.get_session_id(self._identity_label)
        if session_id is None:
            log.warning("kanban_sync_no_session", card_id=card_id, column=column)
            return
        try:
            await self._client.call_tool(
                "update_board_card",
                {
                    "session_id": session_id,
                    "card_id": card_id,
                    "column": column,
                    "idempotency_key": idempotency_key,
                },
            )
            log.info("kanban_card_column_updated", card_id=card_id, column=column)
        except Exception:
            log.warning("kanban_sync_failed", card_id=card_id, column=column)
```

- [ ] **Step 4: Run all work_item_poller tests**

```bash
pytest tests/test_work_item_poller.py -v
```

Expected: All pass — five new kanban writeback tests and all six existing tests.

- [ ] **Step 5: Run the full suite to confirm no regressions**

```bash
pytest -v
```

Expected: All tests pass.

- [ ] **Step 6: Commit**

```bash
git add deskbridge/agent/poller.py tests/test_work_item_poller.py
git commit -m "feat: WorkItemPoller writes kanban card column on claim and completion"
```

---

## Task 5: Supervisor wiring

**Files:**
- Modify: `deskbridge/supervisor.py`
- Modify: `tests/test_supervisor.py`

### Background

After Task 4 adds `kanban_column_in_progress`/`kanban_column_done` params to `WorkItemPoller`, the Supervisor must pass them from `ProjectConfig`. The Supervisor also needs to call `store.get_project_for_identity(account_id)` to check for boards. All existing supervisor tests that use a mocked `Store` (an `AsyncMock`) must add `get_project_for_identity = AsyncMock(return_value=None)` to prevent `json.loads` from receiving a `MagicMock`. The existing `test_supervisor_spawns_and_cancels_poller_tasks` assertion must be updated to include the new params.

- [ ] **Step 1: Write the failing tests**

**1a — Update ALL existing mock_store_instance setups** in `tests/test_supervisor.py` by adding `mock_store_instance.get_project_for_identity = AsyncMock(return_value=None)` immediately after each `mock_store_instance = AsyncMock()` line. There are five such tests:

- `test_supervisor_calls_refresh_on_heartbeat`
- `test_supervisor_spawns_and_cancels_dm_tasks`
- `test_supervisor_spawns_and_cancels_poller_tasks`
- `test_supervisor_spawns_group_watcher_when_groups_configured`
- `test_supervisor_no_group_watcher_when_no_groups`
- `test_supervisor_spawns_approval_watcher`

Each block currently looks like:
```python
mock_store_instance = AsyncMock()
mock_store_instance.get_project_groups = AsyncMock(return_value=[])
MockStore.return_value = mock_store_instance
```

Change to:
```python
mock_store_instance = AsyncMock()
mock_store_instance.get_project_groups = AsyncMock(return_value=[])
mock_store_instance.get_project_for_identity = AsyncMock(return_value=None)
MockStore.return_value = mock_store_instance
```

**1b — Update the WorkItemPoller assertion** in `test_supervisor_spawns_and_cancels_poller_tasks`. Find:

```python
MockWorkItemPoller.assert_called_once_with(
    identity_label="alice",
    store=ANY,
    client=ANY,
    broker=mock_broker,
    config=config,
    shutdown_event=ANY,
)
```

Replace with:

```python
MockWorkItemPoller.assert_called_once_with(
    identity_label="alice",
    store=ANY,
    client=ANY,
    broker=mock_broker,
    config=config,
    shutdown_event=ANY,
    kanban_column_in_progress="in_progress",
    kanban_column_done="done",
)
```

(The `make_config` fixture has no projects, so `project_cfg` is `None` and the defaults apply.)

**1c — Add two new KanbanWatcher tests** at the bottom of `tests/test_supervisor.py`. First add `ProjectConfig` to the import at the top:

```python
from deskbridge.config import DeskBridgeConfig, SupervisorConfig, McpConfig, IdentityConfig, ProjectConfig
```

Then add:

```python
async def test_supervisor_spawns_kanban_watcher_when_boards_configured(
    tmp_path, monkeypatch, mock_broker, mock_client_ctx
):
    monkeypatch.setenv("ALICE", "pass")
    config = DeskBridgeConfig(
        supervisor=SupervisorConfig(
            db_path=str(tmp_path / "test.db"), heartbeat_interval_secs=0.05
        ),
        mcp=McpConfig(command="nostrdesk-mcp"),
        identities=[IdentityConfig(label="alice", npub="npub1alice", passphrase_ref="env:ALICE")],
        projects=[ProjectConfig(
            id="proj-1", name="Proj", repo_path="/repo",
            identity="alice", escalation_dm_target="npub1op",
            boards=["ch-abc"],
            kanban_column_in_progress="doing",
            kanban_column_done="finished",
        )],
    )

    with patch("deskbridge.supervisor.McpClient") as MockMcpClient, \
         patch("deskbridge.supervisor.SessionBroker", return_value=mock_broker), \
         patch("deskbridge.supervisor.apply_schema", new=AsyncMock()), \
         patch("deskbridge.supervisor.bootstrap_accounts_from_config", new=AsyncMock()), \
         patch("deskbridge.supervisor.DmWatcher") as MockDmWatcher, \
         patch("deskbridge.supervisor.OutboxDrainer") as MockOutboxDrainer, \
         patch("deskbridge.supervisor.WorkItemPoller") as MockWorkItemPoller, \
         patch("deskbridge.supervisor.GroupWatcher") as MockGroupWatcher, \
         patch("deskbridge.supervisor.ApprovalRequestWatcher") as MockApprovalWatcher, \
         patch("deskbridge.supervisor.KanbanWatcher") as MockKanbanWatcher, \
         patch("deskbridge.supervisor.Store") as MockStore:

        MockMcpClient.return_value.connect.return_value = mock_client_ctx

        async def never_finishes():
            await asyncio.Event().wait()

        MockDmWatcher.return_value.run = Mock(side_effect=never_finishes)
        MockOutboxDrainer.return_value.run = Mock(side_effect=never_finishes)
        MockWorkItemPoller.return_value.run = Mock(side_effect=never_finishes)
        MockApprovalWatcher.return_value.run = Mock(side_effect=never_finishes)
        MockKanbanWatcher.return_value.run = Mock(side_effect=never_finishes)

        mock_store_instance = AsyncMock()
        mock_store_instance.get_project_groups = AsyncMock(return_value=[])
        mock_store_instance.get_project_for_identity = AsyncMock(
            return_value={"boards_json": '["ch-abc"]'}
        )
        MockStore.return_value = mock_store_instance

        supervisor = Supervisor(config=config)

        async def stop():
            await asyncio.sleep(0.1)
            supervisor.request_shutdown()

        await asyncio.gather(supervisor.run(), stop())

    MockKanbanWatcher.assert_called_once_with(
        account_id="acc-alice",
        identity_label="alice",
        store=ANY,
        client=ANY,
        broker=mock_broker,
        shutdown_event=ANY,
        boards=["ch-abc"],
        operator_npub=None,
    )
    MockKanbanWatcher.return_value.run.assert_called_once()


async def test_supervisor_does_not_spawn_kanban_watcher_when_no_boards(
    tmp_path, monkeypatch, mock_broker, mock_client_ctx
):
    monkeypatch.setenv("ALICE", "pass")
    config = make_config(tmp_path)

    with patch("deskbridge.supervisor.McpClient") as MockMcpClient, \
         patch("deskbridge.supervisor.SessionBroker", return_value=mock_broker), \
         patch("deskbridge.supervisor.apply_schema", new=AsyncMock()), \
         patch("deskbridge.supervisor.bootstrap_accounts_from_config", new=AsyncMock()), \
         patch("deskbridge.supervisor.DmWatcher") as MockDmWatcher, \
         patch("deskbridge.supervisor.OutboxDrainer") as MockOutboxDrainer, \
         patch("deskbridge.supervisor.WorkItemPoller") as MockWorkItemPoller, \
         patch("deskbridge.supervisor.GroupWatcher") as MockGroupWatcher, \
         patch("deskbridge.supervisor.KanbanWatcher") as MockKanbanWatcher, \
         patch("deskbridge.supervisor.Store") as MockStore:

        MockMcpClient.return_value.connect.return_value = mock_client_ctx

        async def never_finishes():
            await asyncio.Event().wait()

        MockDmWatcher.return_value.run = Mock(side_effect=never_finishes)
        MockOutboxDrainer.return_value.run = Mock(side_effect=never_finishes)
        MockWorkItemPoller.return_value.run = Mock(side_effect=never_finishes)

        mock_store_instance = AsyncMock()
        mock_store_instance.get_project_groups = AsyncMock(return_value=[])
        mock_store_instance.get_project_for_identity = AsyncMock(
            return_value={"boards_json": "[]"}
        )
        MockStore.return_value = mock_store_instance

        supervisor = Supervisor(config=config)

        async def stop():
            await asyncio.sleep(0.1)
            supervisor.request_shutdown()

        await asyncio.gather(supervisor.run(), stop())

    MockKanbanWatcher.assert_not_called()
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
pytest tests/test_supervisor.py -v
```

Expected: 
- `test_supervisor_spawns_and_cancels_poller_tasks` fails — WorkItemPoller not called with kanban column params.
- Two new KanbanWatcher tests fail — `KanbanWatcher` not imported in supervisor.
- Some other tests may error with `json.loads` if `get_project_for_identity` isn't mocked — the Step 1a changes fix that.

After Step 1a (adding the `get_project_for_identity` mocks) and Step 1b (updating poller assertion), the expected failures are limited to:
- The poller assertion test (missing kanban params in call)
- The two new KanbanWatcher tests

- [ ] **Step 3: Update `deskbridge/supervisor.py`**

Replace the full file contents:

```python
import asyncio
import json
import signal
import threading
import structlog
from pathlib import Path

import aiosqlite

from deskbridge.config import DeskBridgeConfig
from deskbridge.db.schema import apply_schema
from deskbridge.db.store import Store, bootstrap_accounts_from_config
from deskbridge.dm.watcher import DmWatcher
from deskbridge.dm.group_watcher import GroupWatcher
from deskbridge.dm.approval_watcher import ApprovalRequestWatcher
from deskbridge.dm.kanban_watcher import KanbanWatcher
from deskbridge.dm.outbox import OutboxDrainer
from deskbridge.agent.poller import WorkItemPoller
from deskbridge.mcp import McpClient, SessionBroker

log = structlog.get_logger()


class Supervisor:
    def __init__(self, config: DeskBridgeConfig) -> None:
        self._config = config
        self._shutdown_event = asyncio.Event()

    def request_shutdown(self) -> None:
        log.info("shutdown_requested")
        self._shutdown_event.set()

    async def run(self) -> None:
        db_path = Path(self._config.supervisor.db_path).expanduser()
        db_path.parent.mkdir(parents=True, exist_ok=True)

        async with aiosqlite.connect(db_path) as conn:
            conn.row_factory = aiosqlite.Row
            await apply_schema(conn)
            store = Store(conn)
            await bootstrap_accounts_from_config(store=store, config=self._config)

            mcp_cfg = self._config.mcp
            client = McpClient(
                command=mcp_cfg.command,
                args=mcp_cfg.args,
                startup_timeout_secs=mcp_cfg.startup_timeout_secs,
            )

            async with client.connect():
                broker = SessionBroker(
                    store=store,
                    client=client,
                    identities=self._config.identities,
                )

                is_main = threading.current_thread() is threading.main_thread()
                if is_main:
                    loop = asyncio.get_running_loop()
                    for sig in (signal.SIGTERM, signal.SIGINT):
                        loop.add_signal_handler(sig, self.request_shutdown)

                watcher_tasks: list[asyncio.Task] = []
                group_watcher_tasks: list[asyncio.Task] = []
                approval_watcher_tasks: list[asyncio.Task] = []
                drainer_task: asyncio.Task | None = None
                poller_tasks: list[asyncio.Task] = []
                kanban_watcher_tasks: list[asyncio.Task] = []

                try:
                    await broker.unlock_all()
                    log.info("supervisor_started")

                    watcher_tasks = [
                        asyncio.create_task(
                            DmWatcher(
                                identity_label=identity.label,
                                store=store,
                                client=client,
                                broker=broker,
                                shutdown_event=self._shutdown_event,
                                operator_npub=identity.operator_npub,
                            ).run(),
                            name=f"dm_watcher_{identity.label}",
                        )
                        for identity in self._config.identities
                    ]
                    approval_watcher_tasks = [
                        asyncio.create_task(
                            ApprovalRequestWatcher(
                                identity_label=identity.label,
                                operator_npub=identity.operator_npub,
                                store=store,
                                client=client,
                                broker=broker,
                                shutdown_event=self._shutdown_event,
                            ).run(),
                            name=f"approval_watcher_{identity.label}",
                        )
                        for identity in self._config.identities
                    ]
                    drainer_task = asyncio.create_task(
                        OutboxDrainer(
                            store=store,
                            client=client,
                            broker=broker,
                            identities=self._config.identities,
                            shutdown_event=self._shutdown_event,
                        ).run(),
                        name="outbox_drainer",
                    )

                    for identity in self._config.identities:
                        account_id = f"acc-{identity.label}"
                        project_cfg = next(
                            (p for p in self._config.projects if p.identity == identity.label),
                            None,
                        )
                        poller_tasks.append(asyncio.create_task(
                            WorkItemPoller(
                                identity_label=identity.label,
                                store=store,
                                client=client,
                                broker=broker,
                                config=self._config,
                                shutdown_event=self._shutdown_event,
                                kanban_column_in_progress=(
                                    project_cfg.kanban_column_in_progress
                                    if project_cfg else "in_progress"
                                ),
                                kanban_column_done=(
                                    project_cfg.kanban_column_done
                                    if project_cfg else "done"
                                ),
                            ).run(),
                            name=f"work_item_poller_{identity.label}",
                        ))

                        project_row = await store.get_project_for_identity(account_id)
                        if project_row and project_row["boards_json"]:
                            boards = json.loads(project_row["boards_json"])
                            if boards:
                                kanban_watcher_tasks.append(asyncio.create_task(
                                    KanbanWatcher(
                                        account_id=account_id,
                                        identity_label=identity.label,
                                        store=store,
                                        client=client,
                                        broker=broker,
                                        shutdown_event=self._shutdown_event,
                                        boards=boards,
                                        operator_npub=identity.operator_npub,
                                    ).run(),
                                    name=f"kanban_watcher_{identity.label}",
                                ))

                    for identity in self._config.identities:
                        groups = await store.get_project_groups(f"acc-{identity.label}")
                        if groups:
                            group_watcher_tasks.append(
                                asyncio.create_task(
                                    GroupWatcher(
                                        identity_label=identity.label,
                                        identity_npub=identity.npub,
                                        operator_npub=identity.operator_npub,
                                        group_ids=groups,
                                        store=store,
                                        client=client,
                                        broker=broker,
                                        shutdown_event=self._shutdown_event,
                                    ).run(),
                                    name=f"group_watcher_{identity.label}",
                                )
                            )

                    interval = self._config.supervisor.heartbeat_interval_secs
                    while not self._shutdown_event.is_set():
                        await broker.refresh_if_needed()
                        try:
                            await asyncio.wait_for(
                                self._shutdown_event.wait(),
                                timeout=float(interval),
                            )
                        except asyncio.TimeoutError:
                            pass

                    log.info("supervisor_stopped")
                finally:
                    tasks_to_cancel = (
                        watcher_tasks
                        + group_watcher_tasks
                        + approval_watcher_tasks
                        + poller_tasks
                        + kanban_watcher_tasks
                        + ([drainer_task] if drainer_task is not None else [])
                    )
                    for task in tasks_to_cancel:
                        task.cancel()
                    if tasks_to_cancel:
                        await asyncio.gather(*tasks_to_cancel, return_exceptions=True)
                    if is_main:
                        for sig in (signal.SIGTERM, signal.SIGINT):
                            loop.remove_signal_handler(sig)
```

- [ ] **Step 4: Run all supervisor tests**

```bash
pytest tests/test_supervisor.py -v
```

Expected: All pass — all existing tests plus the two new KanbanWatcher spawn tests.

- [ ] **Step 5: Run the full test suite**

```bash
pytest -v
```

Expected: All tests pass with no regressions.

- [ ] **Step 6: Commit**

```bash
git add deskbridge/supervisor.py tests/test_supervisor.py
git commit -m "feat: Supervisor spawns KanbanWatcher per identity with boards configured"
```

---

## Self-Review

**Spec coverage:**

| Spec requirement | Task |
|---|---|
| `boards`, `kanban_column_in_progress`, `kanban_column_done` on `ProjectConfig` | Task 1 |
| `upsert_work_item` returns `bool` | Task 2 |
| `KanbanWatcher` constructor with `account_id` explicit injection | Task 3 |
| 30s poll loop with per-board `list_assigned_board_cards` | Task 3 |
| `_process_card`: idempotency key, fallback title, outbox DM | Task 3 |
| All 9 KanbanWatcher test cases | Task 3 |
| `WorkItemPoller` gains `kanban_column_in_progress`/`kanban_column_done` params | Task 4 |
| `_sync_card_column` method | Task 4 |
| In-progress writeback after successful claim | Task 4 |
| Done writeback when runner task completes | Task 4 |
| All 5 WorkItemPoller writeback test cases | Task 4 |
| Supervisor spawns `KanbanWatcher` per identity with boards | Task 5 |
| Supervisor passes column params to `WorkItemPoller` | Task 5 |
| `kanban_watcher_tasks` cancelled on shutdown | Task 5 |

**Placeholder scan:** No TBDs or incomplete sections found.

**Type consistency:** `upsert_work_item` signature is consistent across Task 2 (implementation), Task 3 (`_process_card` call), and Task 3 (test assertions). `KanbanWatcher` constructor signature is consistent between Task 3 (implementation) and Task 5 (Supervisor instantiation). `WorkItemPoller` new params are consistent between Task 4 (implementation) and Task 5 (Supervisor instantiation + test assertion).

---

**Plan complete and saved to `docs/superpowers/plans/2026-05-30-deskbridge-phase7.md`. Two execution options:**

**1. Subagent-Driven (recommended)** — Fresh subagent per task, spec + quality review between tasks

**2. Inline Execution** — Execute tasks in this session using executing-plans

**Which approach?**
