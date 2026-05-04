# DeskBridge Phase 3: Agent Task Routing — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Poll `work_items` rows per identity, dispatch a subprocess coding agent for each, heartbeat progress, write the result to `agent_runs` and `work_items`, send a DM via the outbox, and optionally update a kanban board card via MCP.

**Architecture:** Two new asyncio components — `WorkItemPoller` (per-identity poll loop) and `AgentRunner` (run-to-completion subprocess manager) — live in a new `deskbridge/agent/` package. The supervisor spawns one `WorkItemPoller` per identity after `unlock_all()`, mirroring the existing `DmWatcher` pattern. The poller enforces one active `AgentRunner` at a time per identity. Seven new `Store` methods support these components.

**Tech Stack:** aiosqlite, structlog, asyncio, `asyncio.create_subprocess_exec`, `collections.deque`, `McpClient.call_tool`, pytest-asyncio (`asyncio_mode = auto`).

---

## File Map

| File | Action | Responsibility |
|---|---|---|
| `deskbridge/agent/__init__.py` | Create | Package marker |
| `deskbridge/agent/poller.py` | Create | `WorkItemPoller` — per-identity poll loop |
| `deskbridge/agent/runner.py` | Create | `AgentRunner` — subprocess lifecycle |
| `deskbridge/db/store.py` | Modify | 7 new Store methods |
| `deskbridge/supervisor.py` | Modify | Spawn `WorkItemPoller` tasks after `unlock_all()` |
| `tests/test_store.py` | Modify | Append tests for the 7 new Store methods |
| `tests/test_work_item_poller.py` | Create | `WorkItemPoller` unit tests |
| `tests/test_agent_runner.py` | Create | `AgentRunner` unit tests |
| `tests/test_supervisor.py` | Modify | Append one test: pollers spawned and cancelled |

---

## Task 1: Store additions

**Files:**
- Modify: `deskbridge/db/store.py` (append after `update_outbox_delivery`)
- Modify: `tests/test_store.py` (append)

- [ ] **Step 1: Append the failing tests to `tests/test_store.py`**

Append this block directly after the last test in the file:

```python
# ---------------------------------------------------------------------------
# Helpers for Phase 3 store tests
# ---------------------------------------------------------------------------

async def _seed_project(conn, *, id="proj-1", identity_id="acc-alice") -> None:
    await conn.execute(
        "INSERT INTO projects (id, name, repo_path, identity_id, agents_json) VALUES (?, ?, ?, ?, ?)",
        (id, "MyProject", "/repo/myproject", identity_id, '["claude-code"]'),
    )
    await conn.commit()


async def _seed_work_item(store, *, id="wi-1", identity_id="acc-alice",
                          status="pending", idempotency_key="k1") -> None:
    await store.upsert_work_item(
        id=id, source_type="dm", source_id="msg-1",
        identity_id=identity_id, summary="fix the bug",
        payload_json='{"key": "value"}', idempotency_key=idempotency_key,
    )
    if status != "pending":
        async with store._conn.execute(
            "UPDATE work_items SET status = ? WHERE id = ?", (status, id)
        ):
            pass
        await store._conn.commit()


# ---------------------------------------------------------------------------
# get_pending_work_items
# ---------------------------------------------------------------------------

async def test_get_pending_work_items_returns_pending(store: Store, db_conn):
    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    await _seed_work_item(store)
    rows = await store.get_pending_work_items("acc-alice", limit=10)
    assert len(rows) == 1
    assert rows[0]["id"] == "wi-1"


async def test_get_pending_work_items_excludes_dispatched(store: Store, db_conn):
    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    await _seed_work_item(store, status="dispatched")
    rows = await store.get_pending_work_items("acc-alice", limit=10)
    assert rows == []


async def test_get_pending_work_items_excludes_other_identity(store: Store, db_conn):
    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    await store.upsert_account(id="acc-bob", npub="npub1bob", label="bob", passphrase_ref="env:Y")
    await _seed_work_item(store, identity_id="acc-bob")
    rows = await store.get_pending_work_items("acc-alice", limit=10)
    assert rows == []


# ---------------------------------------------------------------------------
# claim_work_item
# ---------------------------------------------------------------------------

async def test_claim_work_item_returns_true_and_transitions_status(store: Store, db_conn):
    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    await _seed_work_item(store)
    claimed = await store.claim_work_item("wi-1")
    assert claimed is True
    async with db_conn.execute("SELECT status FROM work_items WHERE id = 'wi-1'") as cur:
        row = await cur.fetchone()
    assert row["status"] == "dispatched"


async def test_claim_work_item_second_call_returns_false(store: Store, db_conn):
    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    await _seed_work_item(store)
    first = await store.claim_work_item("wi-1")
    second = await store.claim_work_item("wi-1")
    assert first is True
    assert second is False


# ---------------------------------------------------------------------------
# upsert_agent_run
# ---------------------------------------------------------------------------

async def test_upsert_agent_run_inserts_row(store: Store, db_conn):
    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    await _seed_work_item(store)
    await store.claim_work_item("wi-1")
    await store.upsert_agent_run("run-1", "wi-1", "claude-code")
    async with db_conn.execute("SELECT * FROM agent_runs WHERE id = 'run-1'") as cur:
        row = await cur.fetchone()
    assert row is not None
    assert row["adapter_type"] == "claude-code"
    assert row["status"] == "running"


async def test_upsert_agent_run_is_noop_on_duplicate(store: Store, db_conn):
    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    await _seed_work_item(store)
    await store.claim_work_item("wi-1")
    await store.upsert_agent_run("run-1", "wi-1", "claude-code")
    await store.upsert_agent_run("run-1", "wi-1", "codex")  # second call is a no-op
    async with db_conn.execute("SELECT adapter_type FROM agent_runs WHERE id = 'run-1'") as cur:
        row = await cur.fetchone()
    assert row["adapter_type"] == "claude-code"  # unchanged


# ---------------------------------------------------------------------------
# update_agent_run
# ---------------------------------------------------------------------------

async def test_update_agent_run_sets_heartbeat(store: Store, db_conn):
    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    await _seed_work_item(store)
    await store.claim_work_item("wi-1")
    await store.upsert_agent_run("run-1", "wi-1", "claude-code")
    await store.update_agent_run("run-1", heartbeat_at="2026-05-03T12:00:00Z")
    async with db_conn.execute("SELECT heartbeat_at, status FROM agent_runs WHERE id='run-1'") as cur:
        row = await cur.fetchone()
    assert row["heartbeat_at"] == "2026-05-03T12:00:00Z"
    assert row["status"] == "running"  # unchanged


async def test_update_agent_run_sets_status_and_result(store: Store, db_conn):
    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    await _seed_work_item(store)
    await store.claim_work_item("wi-1")
    await store.upsert_agent_run("run-1", "wi-1", "claude-code")
    await store.update_agent_run("run-1", status="done", result_summary="all good")
    async with db_conn.execute("SELECT status, result_summary FROM agent_runs WHERE id='run-1'") as cur:
        row = await cur.fetchone()
    assert row["status"] == "done"
    assert row["result_summary"] == "all good"


async def test_update_agent_run_raises_if_no_fields(store: Store):
    with pytest.raises(ValueError, match="at least one field"):
        await store.update_agent_run("run-1")


# ---------------------------------------------------------------------------
# complete_work_item
# ---------------------------------------------------------------------------

async def test_complete_work_item_sets_status(store: Store, db_conn):
    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    await _seed_work_item(store)
    await store.complete_work_item("wi-1", "done")
    async with db_conn.execute("SELECT status FROM work_items WHERE id='wi-1'") as cur:
        row = await cur.fetchone()
    assert row["status"] == "done"


# ---------------------------------------------------------------------------
# get_project_for_identity
# ---------------------------------------------------------------------------

async def test_get_project_for_identity_returns_row(store: Store, db_conn):
    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    await _seed_project(db_conn, identity_id="acc-alice")
    row = await store.get_project_for_identity("acc-alice")
    assert row is not None
    assert row["id"] == "proj-1"


async def test_get_project_for_identity_returns_none_when_missing(store: Store):
    result = await store.get_project_for_identity("acc-nobody")
    assert result is None


# ---------------------------------------------------------------------------
# insert_outbox_item
# ---------------------------------------------------------------------------

async def test_insert_outbox_item_inserts_row(store: Store, db_conn):
    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    await store.insert_outbox_item(
        id="ob-1", identity_id="acc-alice", dest_pubkey="npub1op",
        message_text="all done", idempotency_key="idem-1",
    )
    async with db_conn.execute("SELECT * FROM outbox WHERE id='ob-1'") as cur:
        row = await cur.fetchone()
    assert row is not None
    assert row["message_text"] == "all done"
    assert row["delivery_status"] == "pending"


async def test_insert_outbox_item_idempotent_on_same_key(store: Store, db_conn):
    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    await store.insert_outbox_item("ob-1", "acc-alice", "npub1op", "first", "idem-1")
    await store.insert_outbox_item("ob-2", "acc-alice", "npub1op", "second", "idem-1")
    async with db_conn.execute("SELECT COUNT(*) FROM outbox") as cur:
        row = await cur.fetchone()
    assert row[0] == 1
```

- [ ] **Step 2: Run the new tests to confirm they all fail**

```bash
pytest tests/test_store.py -k "pending_work_items or claim_work_item or agent_run or complete_work_item or project_for_identity or outbox_item" -v 2>&1 | tail -30
```

Expected: all new tests FAIL with `AttributeError: 'Store' object has no attribute '...'`

- [ ] **Step 3: Append the 7 new Store methods to `deskbridge/db/store.py`**

Append after the `update_outbox_delivery` method (before `bootstrap_accounts_from_config`):

```python
    async def get_pending_work_items(
        self, identity_id: str, limit: int = 10
    ) -> list[aiosqlite.Row]:
        async with self._conn.execute(
            """
            SELECT * FROM work_items
            WHERE status = 'pending' AND identity_id = ?
            ORDER BY priority, created_at
            LIMIT ?
            """,
            (identity_id, limit),
        ) as cur:
            return await cur.fetchall()

    async def claim_work_item(self, id: str) -> bool:
        async with self._conn.execute(
            """
            UPDATE work_items
            SET status = 'dispatched',
                updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
            WHERE id = ? AND status = 'pending'
            """,
            (id,),
        ) as cur:
            claimed = cur.rowcount > 0
        await self._conn.commit()
        return claimed

    async def upsert_agent_run(
        self, id: str, work_item_id: str, adapter_type: str
    ) -> None:
        async with self._conn.execute(
            "INSERT OR IGNORE INTO agent_runs (id, work_item_id, adapter_type) VALUES (?, ?, ?)",
            (id, work_item_id, adapter_type),
        ):
            pass
        await self._conn.commit()

    async def update_agent_run(
        self,
        id: str,
        *,
        status: str | None = None,
        result_summary: str | None = None,
        heartbeat_at: str | None = None,
    ) -> None:
        if status is None and result_summary is None and heartbeat_at is None:
            raise ValueError("at least one field must be provided to update_agent_run")
        parts = []
        params: list = []
        if status is not None:
            parts.append("status = ?")
            params.append(status)
        if result_summary is not None:
            parts.append("result_summary = ?")
            params.append(result_summary)
        if heartbeat_at is not None:
            parts.append("heartbeat_at = ?")
            params.append(heartbeat_at)
        parts.append("updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')")
        params.append(id)
        sql = f"UPDATE agent_runs SET {', '.join(parts)} WHERE id = ?"
        async with self._conn.execute(sql, params):
            pass
        await self._conn.commit()

    async def complete_work_item(self, id: str, status: str) -> None:
        async with self._conn.execute(
            """
            UPDATE work_items
            SET status = ?,
                updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
            WHERE id = ?
            """,
            (status, id),
        ):
            pass
        await self._conn.commit()

    async def get_project_for_identity(self, identity_id: str) -> aiosqlite.Row | None:
        async with self._conn.execute(
            "SELECT * FROM projects WHERE identity_id = ? LIMIT 1",
            (identity_id,),
        ) as cur:
            return await cur.fetchone()

    async def insert_outbox_item(
        self,
        id: str,
        identity_id: str,
        dest_pubkey: str,
        message_text: str,
        idempotency_key: str,
    ) -> None:
        async with self._conn.execute(
            """
            INSERT OR IGNORE INTO outbox
                (id, identity_id, dest_pubkey, message_text, idempotency_key)
            VALUES (?, ?, ?, ?, ?)
            """,
            (id, identity_id, dest_pubkey, message_text, idempotency_key),
        ):
            pass
        await self._conn.commit()
```

- [ ] **Step 4: Run the new tests and verify they all pass**

```bash
pytest tests/test_store.py -v 2>&1 | tail -20
```

Expected: all tests PASS.

- [ ] **Step 5: Run the full test suite to catch regressions**

```bash
pytest -v 2>&1 | tail -20
```

Expected: all tests PASS.

- [ ] **Step 6: Commit**

```bash
git add deskbridge/db/store.py tests/test_store.py
git commit -m "feat: add 7 new Store methods for Phase 3 agent routing"
```

---

## Task 2: WorkItemPoller

**Files:**
- Create: `deskbridge/agent/__init__.py`
- Create: `deskbridge/agent/poller.py`
- Create: `tests/test_work_item_poller.py`

- [ ] **Step 1: Create the package marker**

Create `deskbridge/agent/__init__.py` — empty file, no content needed.

- [ ] **Step 2: Write the failing tests in `tests/test_work_item_poller.py`**

Create this file:

```python
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
        MockRunner.return_value.run = MagicMock(side_effect=fake_run)
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
    # Just verifying run() returns cleanly — no assertion needed beyond no exception
```

- [ ] **Step 3: Run the tests to confirm they fail with ImportError**

```bash
pytest tests/test_work_item_poller.py -v 2>&1 | tail -20
```

Expected: FAIL with `ModuleNotFoundError: No module named 'deskbridge.agent.poller'`

- [ ] **Step 4: Create `deskbridge/agent/poller.py`**

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
    ) -> None:
        self._identity_label = identity_label
        self._account_id = f"acc-{identity_label}"
        self._store = store
        self._client = client
        self._broker = broker
        self._config = config
        self._shutdown_event = shutdown_event
        self._poll_interval_secs = poll_interval_secs
        self._active_run_task: asyncio.Task | None = None

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

            project_cfg = next(
                (p for p in self._config.projects if p.id == project["id"]), None
            )
            if project_cfg is None:
                log.error(
                    "work_item_poller_no_project_config",
                    identity=self._identity_label,
                    project_id=project["id"],
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
            break
```

- [ ] **Step 5: Run the poller tests**

```bash
pytest tests/test_work_item_poller.py -v 2>&1 | tail -20
```

Expected: all 4 tests PASS. If `ModuleNotFoundError: No module named 'deskbridge.agent.runner'` appears, it means runner.py doesn't exist yet — create an empty stub:

```python
# deskbridge/agent/runner.py  (temporary stub — replaced in Task 3)
class AgentRunner:
    def __init__(self, **kwargs): pass
    async def run(self): pass
```

Re-run after creating the stub if needed.

- [ ] **Step 6: Run the full test suite**

```bash
pytest -v 2>&1 | tail -20
```

Expected: all tests PASS.

- [ ] **Step 7: Commit**

```bash
git add deskbridge/agent/__init__.py deskbridge/agent/poller.py tests/test_work_item_poller.py
git commit -m "feat: add WorkItemPoller per-identity poll loop"
```

---

## Task 3: AgentRunner

**Files:**
- Create (replace stub if it exists): `deskbridge/agent/runner.py`
- Create: `tests/test_agent_runner.py`

- [ ] **Step 1: Write the failing tests in `tests/test_agent_runner.py`**

```python
import asyncio
import collections
from unittest.mock import AsyncMock, MagicMock, patch, call

from deskbridge.agent.runner import AgentRunner
from deskbridge.config import ProjectConfig

PROJ = ProjectConfig(
    id="proj-1", name="MyProj", repo_path="/repo",
    identity="alice", escalation_dm_target="npub1op", agents=["claude-code"],
)
PROJ_NO_DM = ProjectConfig(
    id="proj-1", name="MyProj", repo_path="/repo",
    identity="alice", escalation_dm_target="", agents=["claude-code"],
)


def _row(**kwargs):
    m = MagicMock()
    m.__getitem__ = MagicMock(side_effect=lambda k: kwargs[k])
    m.get = MagicMock(side_effect=lambda k, d=None: kwargs.get(k, d))
    return m


def make_store():
    store = MagicMock()
    store.upsert_agent_run = AsyncMock()
    store.update_agent_run = AsyncMock()
    store.complete_work_item = AsyncMock()
    store.insert_outbox_item = AsyncMock()
    return store


def make_broker(session_id="sess-1"):
    broker = MagicMock()
    broker.get_session_id = AsyncMock(return_value=session_id)
    return broker


async def _stdout(*lines):
    for line in lines:
        yield line


def make_proc(*, returncode=0, stdout_lines=None):
    proc = MagicMock()
    proc.returncode = returncode
    proc.terminate = MagicMock()
    proc.kill = MagicMock()
    proc.stdout = _stdout(*(stdout_lines or [b"agent output\n"]))
    proc.wait = AsyncMock(return_value=returncode)
    return proc


def make_runner(work_item, project=PROJ, *, store=None, broker=None, client=None,
                timeout_secs=5.0, heartbeat_interval_secs=9999.0):
    return AgentRunner(
        work_item=work_item,
        project=project,
        run_id="run-1",
        store=store or make_store(),
        client=client or MagicMock(),
        broker=broker or make_broker(),
        timeout_secs=timeout_secs,
        heartbeat_interval_secs=heartbeat_interval_secs,
    )


async def test_runner_success_marks_done_and_writes_outbox():
    work_item = _row(id="wi-1", source_type="dm", source_id="msg-1",
                     summary="fix bug", payload_json="{}")
    store = make_store()
    proc = make_proc(returncode=0, stdout_lines=[b"line 1\n", b"line 2\n"])

    with patch("asyncio.create_subprocess_exec", return_value=proc):
        runner = make_runner(work_item, store=store)
        await runner.run()

    store.upsert_agent_run.assert_awaited_once_with(
        id="run-1", work_item_id="wi-1", adapter_type="claude-code"
    )
    store.update_agent_run.assert_awaited()
    # Last call to update_agent_run should include status='done'
    final_call_kwargs = store.update_agent_run.call_args_list[-1].kwargs
    assert final_call_kwargs["status"] == "done"
    store.complete_work_item.assert_awaited_once_with("wi-1", status="done")
    store.insert_outbox_item.assert_awaited_once()
    outbox_kwargs = store.insert_outbox_item.call_args.kwargs
    assert outbox_kwargs["dest_pubkey"] == "npub1op"
    assert outbox_kwargs["idempotency_key"] == "deskbridge:run-1:result_notify"


async def test_runner_nonzero_exit_marks_failed():
    work_item = _row(id="wi-1", source_type="dm", source_id="msg-1",
                     summary="fix bug", payload_json="{}")
    store = make_store()
    proc = make_proc(returncode=1, stdout_lines=[b"error output\n"])

    with patch("asyncio.create_subprocess_exec", return_value=proc):
        runner = make_runner(work_item, store=store)
        await runner.run()

    final_call_kwargs = store.update_agent_run.call_args_list[-1].kwargs
    assert final_call_kwargs["status"] == "failed"
    store.complete_work_item.assert_awaited_once_with("wi-1", status="failed")
    store.insert_outbox_item.assert_awaited_once()


async def test_runner_timeout_sends_sigterm_marks_interrupted():
    work_item = _row(id="wi-1", source_type="dm", source_id="msg-1",
                     summary="fix bug", payload_json="{}")
    store = make_store()
    proc = make_proc(stdout_lines=[])

    call_count = 0

    async def hanging_wait():
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            await asyncio.sleep(9999)  # triggers TimeoutError on first call
        # second call (after SIGTERM) returns immediately

    proc.wait = hanging_wait

    with patch("asyncio.create_subprocess_exec", return_value=proc):
        runner = make_runner(work_item, store=store, timeout_secs=0.05)
        await runner.run()

    proc.terminate.assert_called_once()
    final_call_kwargs = store.update_agent_run.call_args_list[-1].kwargs
    assert final_call_kwargs["status"] == "interrupted"
    store.complete_work_item.assert_awaited_once_with("wi-1", status="interrupted")


async def test_runner_cancelled_sends_sigterm_and_reraises():
    work_item = _row(id="wi-1", source_type="dm", source_id="msg-1",
                     summary="fix bug", payload_json="{}")
    store = make_store()
    proc = make_proc(stdout_lines=[])

    async def hang():
        await asyncio.sleep(9999)

    proc.wait = hang

    with patch("asyncio.create_subprocess_exec", return_value=proc):
        runner = make_runner(work_item, store=store, timeout_secs=9999.0)
        task = asyncio.create_task(runner.run())
        await asyncio.sleep(0.02)  # let subprocess "start"
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass  # expected

    proc.terminate.assert_called_once()
    # complete_work_item must NOT be called — CancelledError skips steps 6-9
    store.complete_work_item.assert_not_awaited()


async def test_runner_kanban_source_updates_board_card():
    work_item = _row(id="wi-1", source_type="kanban", source_id="card-42",
                     summary="kanban task", payload_json="{}")
    store = make_store()
    broker = make_broker(session_id="sess-1")
    client = MagicMock()
    client.call_tool = AsyncMock(return_value={"updated": True})
    proc = make_proc(returncode=0, stdout_lines=[b"done\n"])

    with patch("asyncio.create_subprocess_exec", return_value=proc):
        runner = make_runner(work_item, store=store, broker=broker, client=client)
        await runner.run()

    client.call_tool.assert_awaited_once()
    board_call_kwargs = client.call_tool.call_args
    assert board_call_kwargs[0][0] == "update_board_card"
    assert board_call_kwargs[0][1]["card_id"] == "card-42"
    assert board_call_kwargs[0][1]["idempotency_key"] == "deskbridge:run-1:board_update"


async def test_runner_no_session_skips_board_update():
    work_item = _row(id="wi-1", source_type="kanban", source_id="card-42",
                     summary="kanban task", payload_json="{}")
    store = make_store()
    broker = make_broker(session_id=None)
    client = MagicMock()
    client.call_tool = AsyncMock()
    proc = make_proc(returncode=0, stdout_lines=[b"done\n"])

    with patch("asyncio.create_subprocess_exec", return_value=proc):
        runner = make_runner(work_item, store=store, broker=broker, client=client)
        await runner.run()  # must not raise

    client.call_tool.assert_not_awaited()


async def test_runner_empty_escalation_target_skips_outbox():
    work_item = _row(id="wi-1", source_type="dm", source_id="msg-1",
                     summary="fix bug", payload_json="{}")
    store = make_store()
    proc = make_proc(returncode=0, stdout_lines=[b"done\n"])

    with patch("asyncio.create_subprocess_exec", return_value=proc):
        runner = make_runner(work_item, project=PROJ_NO_DM, store=store)
        await runner.run()

    store.insert_outbox_item.assert_not_awaited()


async def test_runner_unexpected_exception_marks_failed():
    work_item = _row(id="wi-1", source_type="dm", source_id="msg-1",
                     summary="fix bug", payload_json="{}")
    store = make_store()
    store.upsert_agent_run = AsyncMock(side_effect=RuntimeError("db exploded"))

    with patch("asyncio.create_subprocess_exec", side_effect=RuntimeError("no process")):
        runner = make_runner(work_item, store=store)
        await runner.run()  # must not raise

    store.complete_work_item.assert_awaited_once_with("wi-1", "failed")
```

- [ ] **Step 2: Run the tests to confirm they fail**

```bash
pytest tests/test_agent_runner.py -v 2>&1 | tail -20
```

Expected: all tests FAIL (ImportError or AttributeError depending on whether the stub exists).

- [ ] **Step 3: Create `deskbridge/agent/runner.py`** (replace stub if present)

```python
import asyncio
import collections
import structlog
from datetime import datetime, timezone
from uuid import uuid4

from deskbridge.config import ProjectConfig
from deskbridge.db.store import Store
from deskbridge.mcp.client import McpClient
from deskbridge.mcp.session import SessionBroker

log = structlog.get_logger()

_ADAPTER_CLI: dict[str, str] = {
    "claude-code": "claude",
    "codex": "codex",
}


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class AgentRunner:
    def __init__(
        self,
        work_item,
        project: ProjectConfig,
        run_id: str,
        store: Store,
        client: McpClient,
        broker: SessionBroker,
        timeout_secs: float = 600.0,
        heartbeat_interval_secs: float = 30.0,
    ) -> None:
        self._work_item = work_item
        self._project = project
        self._run_id = run_id
        self._store = store
        self._client = client
        self._broker = broker
        self._timeout_secs = timeout_secs
        self._heartbeat_interval_secs = heartbeat_interval_secs

    async def run(self) -> None:
        try:
            await self._do_run()
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("agent_runner_unexpected_error", run_id=self._run_id)
            try:
                await self._store.complete_work_item(self._work_item["id"], "failed")
            except Exception:
                log.exception("agent_runner_complete_failed_on_error", run_id=self._run_id)
            if self._project.escalation_dm_target:
                try:
                    await self._store.insert_outbox_item(
                        id=str(uuid4()),
                        identity_id=f"acc-{self._project.identity}",
                        dest_pubkey=self._project.escalation_dm_target,
                        message_text="Agent run failed with unexpected error",
                        idempotency_key=f"deskbridge:{self._run_id}:result_notify",
                    )
                except Exception:
                    log.exception("agent_runner_outbox_failed_on_error", run_id=self._run_id)

    async def _do_run(self) -> None:
        work_item = self._work_item
        project = self._project
        run_id = self._run_id

        # 1. Record the run
        await self._store.upsert_agent_run(
            id=run_id,
            work_item_id=work_item["id"],
            adapter_type=project.agents[0],
        )

        # 2. Build CLI command
        adapter = project.agents[0]
        cli = _ADAPTER_CLI.get(adapter, adapter)
        prompt = f"{work_item['summary']}\n\n{work_item['payload_json']}"[:4000]
        if adapter == "claude-code":
            cmd = ["claude", "--project", project.repo_path, "--message", prompt]
        else:
            cmd = ["codex", "--dir", project.repo_path, prompt]

        # 3. Spawn subprocess
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
            cwd=project.repo_path,
        )

        # 4. Start concurrent tasks
        output_buf: collections.deque = collections.deque(maxlen=200)

        async def _drain() -> None:
            async for line in proc.stdout:
                output_buf.append(line.decode(errors="replace").rstrip())

        async def _heartbeat() -> None:
            while True:
                await asyncio.sleep(self._heartbeat_interval_secs)
                await self._store.update_agent_run(run_id, heartbeat_at=_now_iso())

        drain_task = asyncio.create_task(_drain())
        heartbeat_task = asyncio.create_task(_heartbeat())

        # 5. Wait for process with timeout / cancellation handling
        final_status = "failed"
        try:
            await asyncio.wait_for(proc.wait(), timeout=self._timeout_secs)
            final_status = "done" if proc.returncode == 0 else "failed"
        except asyncio.TimeoutError:
            proc.terminate()
            try:
                await asyncio.wait_for(proc.wait(), timeout=10)
            except asyncio.TimeoutError:
                proc.kill()
            final_status = "interrupted"
        except asyncio.CancelledError:
            proc.terminate()
            try:
                await asyncio.wait_for(proc.wait(), timeout=10)
            except asyncio.TimeoutError:
                proc.kill()
            final_status = "interrupted"
            raise  # propagate — steps 6-9 are skipped
        finally:
            heartbeat_task.cancel()
            await asyncio.gather(heartbeat_task, drain_task, return_exceptions=True)

        # 6. Record final run result
        result_text = "\n".join(output_buf)[-2000:]
        await self._store.update_agent_run(run_id, status=final_status, result_summary=result_text)

        # 7. Update work item
        await self._store.complete_work_item(work_item["id"], status=final_status)

        # 8. Notify operator via DM (best-effort)
        if project.escalation_dm_target:
            await self._store.insert_outbox_item(
                id=str(uuid4()),
                identity_id=f"acc-{project.identity}",
                dest_pubkey=project.escalation_dm_target,
                message_text=result_text,
                idempotency_key=f"deskbridge:{run_id}:result_notify",
            )

        # 9. Update kanban board card (best-effort, only for kanban-sourced items)
        if work_item["source_type"] == "kanban" and work_item["source_id"] is not None:
            session_id = await self._broker.get_session_id(project.identity)
            if session_id is None:
                log.warning("agent_runner_no_session_for_board_update", run_id=run_id)
            else:
                try:
                    await self._client.call_tool(
                        "update_board_card",
                        {
                            "session_id": session_id,
                            "card_id": work_item["source_id"],
                            "description": "\n".join(output_buf)[-500:],
                            "idempotency_key": f"deskbridge:{run_id}:board_update",
                        },
                    )
                except Exception:
                    log.exception("agent_runner_board_update_failed", run_id=run_id)
```

- [ ] **Step 4: Run the AgentRunner tests**

```bash
pytest tests/test_agent_runner.py -v 2>&1 | tail -30
```

Expected: all 8 tests PASS.

- [ ] **Step 5: Run the full suite**

```bash
pytest -v 2>&1 | tail -20
```

Expected: all tests PASS.

- [ ] **Step 6: Commit**

```bash
git add deskbridge/agent/runner.py tests/test_agent_runner.py
git commit -m "feat: add AgentRunner subprocess lifecycle manager"
```

---

## Task 4: Supervisor wiring

**Files:**
- Modify: `deskbridge/supervisor.py`
- Modify: `tests/test_supervisor.py`

- [ ] **Step 1: Append the failing test to `tests/test_supervisor.py`**

Append after the last test in the file:

```python
async def test_supervisor_spawns_and_cancels_poller_tasks(tmp_path, monkeypatch, mock_broker, mock_client_ctx):
    monkeypatch.setenv("ALICE", "pass")
    config = make_config(tmp_path)

    with patch("deskbridge.supervisor.McpClient") as MockMcpClient, \
         patch("deskbridge.supervisor.SessionBroker", return_value=mock_broker), \
         patch("deskbridge.supervisor.apply_schema", new=AsyncMock()), \
         patch("deskbridge.supervisor.bootstrap_accounts_from_config", new=AsyncMock()), \
         patch("deskbridge.supervisor.DmWatcher") as MockDmWatcher, \
         patch("deskbridge.supervisor.OutboxDrainer") as MockOutboxDrainer, \
         patch("deskbridge.supervisor.WorkItemPoller") as MockWorkItemPoller:

        mock_instance = MockMcpClient.return_value
        mock_instance.connect.return_value = mock_client_ctx

        async def never_finishes():
            await asyncio.Event().wait()

        MockDmWatcher.return_value.run = Mock(side_effect=never_finishes)
        MockOutboxDrainer.return_value.run = Mock(side_effect=never_finishes)
        MockWorkItemPoller.return_value.run = Mock(side_effect=never_finishes)

        supervisor = Supervisor(config=config)

        async def stop():
            await asyncio.sleep(0.1)
            supervisor.request_shutdown()

        await asyncio.gather(supervisor.run(), stop())

    MockWorkItemPoller.assert_called_once_with(
        identity_label="alice",
        store=ANY,
        client=ANY,
        broker=mock_broker,
        config=config,
        shutdown_event=ANY,
    )
    MockWorkItemPoller.return_value.run.assert_called_once()
```

- [ ] **Step 2: Run the new test to confirm it fails**

```bash
pytest tests/test_supervisor.py::test_supervisor_spawns_and_cancels_poller_tasks -v 2>&1 | tail -20
```

Expected: FAIL — `WorkItemPoller` not imported in supervisor, assertion fails.

- [ ] **Step 3: Update `deskbridge/supervisor.py`**

Add the import at the top of the file (after the existing imports):

```python
from deskbridge.agent.poller import WorkItemPoller
```

Initialize `poller_tasks` before the `try` block. The current code has:

```python
watcher_tasks: list[asyncio.Task] = []
drainer_task: asyncio.Task | None = None
```

Change it to:

```python
watcher_tasks: list[asyncio.Task] = []
drainer_task: asyncio.Task | None = None
poller_tasks: list[asyncio.Task] = []
```

Inside the `try` block, after creating `drainer_task`, add:

```python
                    poller_tasks = [
                        asyncio.create_task(
                            WorkItemPoller(
                                identity_label=identity.label,
                                store=store,
                                client=client,
                                broker=broker,
                                config=self._config,
                                shutdown_event=self._shutdown_event,
                            ).run(),
                            name=f"work_item_poller_{identity.label}",
                        )
                        for identity in self._config.identities
                    ]
```

In the `finally` block, change:

```python
                    tasks_to_cancel = watcher_tasks + (
                        [drainer_task] if drainer_task is not None else []
                    )
```

to:

```python
                    tasks_to_cancel = watcher_tasks + poller_tasks + (
                        [drainer_task] if drainer_task is not None else []
                    )
```

- [ ] **Step 4: Run the supervisor tests**

```bash
pytest tests/test_supervisor.py -v 2>&1 | tail -20
```

Expected: all 4 supervisor tests PASS.

- [ ] **Step 5: Run the full test suite**

```bash
pytest -v 2>&1 | tail -20
```

Expected: all tests PASS. Note the count — Phase 2 ended with 87 tests; Phase 3 adds approximately 27 new tests.

- [ ] **Step 6: Commit**

```bash
git add deskbridge/supervisor.py deskbridge/agent/poller.py tests/test_supervisor.py
git commit -m "feat: wire WorkItemPoller into supervisor for Phase 3 agent routing"
```

---

## Self-Review

**Spec coverage check:**

| Spec requirement | Covered by |
|---|---|
| `get_pending_work_items` | Task 1 |
| `claim_work_item` (race-safe, `cur.rowcount`) | Task 1 |
| `upsert_agent_run` (`INSERT OR IGNORE`) | Task 1 |
| `update_agent_run` (dynamic, ValueError on no fields) | Task 1 |
| `complete_work_item` | Task 1 |
| `get_project_for_identity` | Task 1 |
| `insert_outbox_item` (`INSERT OR IGNORE`) | Task 1 |
| `WorkItemPoller` per-identity loop | Task 2 |
| One-at-a-time enforcement via `_active_run_task` | Task 2 |
| `AgentRunner` spawned as asyncio.Task | Task 2 |
| Poller `finally` cancels active runner | Task 2 |
| `AgentRunner` subprocess lifecycle | Task 3 |
| `collections.deque(maxlen=200)` output buffer | Task 3 |
| Timeout → SIGTERM → SIGKILL → `interrupted` | Task 3 |
| CancelledError → SIGTERM → SIGKILL → re-raise | Task 3 |
| Steps 6–9 skipped on CancelledError | Task 3 |
| Adapter mapping `"claude-code"` → `claude` | Task 3 |
| `escalation_dm_target` empty → skip outbox | Task 3 |
| Kanban source → `update_board_card` MCP call | Task 3 |
| No session → board update skipped | Task 3 |
| Board update failure → log, do not raise | Task 3 |
| Unexpected exception → `failed`, outbox attempted | Task 3 |
| Supervisor spawns one poller per identity | Task 4 |
| Poller tasks cancelled in `finally` | Task 4 |

All requirements covered.
