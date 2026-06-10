# DeskBridge Phase 11: Operability Triad Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add bounded work-item retry, comprehensive audit logging, and an enhanced `deskbridge status` CLI so the daemon is operationally trustworthy.

**Architecture:** Schema migrations add `attempt_count` and `next_retry_at` to `work_items`; the poller re-queues failed items with a 60-second cooldown up to `max_agent_attempts` total runs; `Store.log_audit` call sites are added at every operational seam (runner, poller, approval watcher, DM watcher, outbox); the `status` CLI command grows from one query to five sections.

**Tech Stack:** Python 3.12, aiosqlite, pydantic v2, click, structlog, pytest-asyncio (asyncio_mode=auto), uv.

---

## File Map

| File | Change |
|---|---|
| `deskbridge/db/schema.py` | Add 2 migrations: `attempt_count`, `next_retry_at` |
| `deskbridge/db/store.py` | Add `retry_work_item`; update `get_pending_work_items` SQL |
| `deskbridge/config.py` | Add `max_agent_attempts: int = Field(default=3, ge=1)` |
| `deskbridge/agent/poller.py` | `_active_project_cfg` field; retry block; dispatch audit; `_iso_offset` helper |
| `deskbridge/agent/runner.py` | `agent_run_started` and `agent_run_finished` audit calls |
| `deskbridge/dm/approval_watcher.py` | `approval_requested` audit after `insert_approval` |
| `deskbridge/dm/watcher.py` | `approval_resolved` audit after each `resolve_approval` call |
| `deskbridge/dm/outbox.py` | `outbox_delivered` and `outbox_delivery_failed` audit calls |
| `deskbridge/cli.py` | Replace `_show_status` with 5-section output |
| `tests/test_store.py` | Tests for `retry_work_item`, `next_retry_at` filter, `attempt_count` |
| `tests/test_config.py` | Tests for `max_agent_attempts` default and validation |
| `tests/test_work_item_poller.py` | Update `make_store`; add retry and terminal tests |
| `tests/test_agent_runner.py` | Add `log_audit = AsyncMock()` to `make_store` |
| `tests/test_cli.py` | Replace raw-sqlite account test; add all-sections test |

---

### Task 1: Schema migrations and Store retry support

**Files:**
- Modify: `deskbridge/db/schema.py`
- Modify: `deskbridge/db/store.py`
- Test: `tests/test_store.py`

- [ ] **Step 1: Write the failing tests**

Add these four tests to `tests/test_store.py` after the existing `claim_work_item` tests (around line 365):

```python
async def test_retry_work_item_resets_to_pending_and_increments_attempt_count(store: Store, db_conn):
    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    await _seed_work_item(store, status="failed")
    await store.retry_work_item("wi-1", "2030-01-01T00:01:00Z")
    async with db_conn.execute(
        "SELECT status, attempt_count, next_retry_at FROM work_items WHERE id='wi-1'"
    ) as cur:
        row = await cur.fetchone()
    assert row["status"] == "pending"
    assert row["attempt_count"] == 1
    assert row["next_retry_at"] == "2030-01-01T00:01:00Z"


async def test_get_pending_work_items_skips_cooling_down(store: Store, db_conn):
    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    await _seed_work_item(store)
    async with db_conn.execute(
        "UPDATE work_items SET next_retry_at='2099-01-01T00:00:00Z' WHERE id='wi-1'"
    ) as _:
        pass
    await db_conn.commit()
    rows = await store.get_pending_work_items("acc-alice", limit=10)
    assert rows == []


async def test_get_pending_work_items_includes_elapsed_cooldown(store: Store, db_conn):
    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    await _seed_work_item(store)
    async with db_conn.execute(
        "UPDATE work_items SET next_retry_at='2000-01-01T00:00:00Z' WHERE id='wi-1'"
    ) as _:
        pass
    await db_conn.commit()
    rows = await store.get_pending_work_items("acc-alice", limit=10)
    assert len(rows) == 1
    assert rows[0]["id"] == "wi-1"


async def test_claim_work_item_does_not_change_attempt_count(store: Store, db_conn):
    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    await _seed_work_item(store)
    await store.claim_work_item("wi-1")
    async with db_conn.execute(
        "SELECT attempt_count FROM work_items WHERE id='wi-1'"
    ) as cur:
        row = await cur.fetchone()
    assert row["attempt_count"] == 0
```

- [ ] **Step 2: Run tests to verify they fail**

```
uv run pytest tests/test_store.py::test_retry_work_item_resets_to_pending_and_increments_attempt_count tests/test_store.py::test_get_pending_work_items_skips_cooling_down tests/test_store.py::test_get_pending_work_items_includes_elapsed_cooldown tests/test_store.py::test_claim_work_item_does_not_change_attempt_count -v
```

Expected: 4 failures — `retry_work_item` does not exist; `next_retry_at` column does not exist.

- [ ] **Step 3: Add migrations to `deskbridge/db/schema.py`**

Change:
```python
_MIGRATIONS = [
    "ALTER TABLE projects ADD COLUMN adapter TEXT NOT NULL DEFAULT 'claude-code'",
    "ALTER TABLE projects ADD COLUMN openclaw_agent_id TEXT",
]
```
To:
```python
_MIGRATIONS = [
    "ALTER TABLE projects ADD COLUMN adapter TEXT NOT NULL DEFAULT 'claude-code'",
    "ALTER TABLE projects ADD COLUMN openclaw_agent_id TEXT",
    "ALTER TABLE work_items ADD COLUMN attempt_count INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE work_items ADD COLUMN next_retry_at TEXT",
]
```

- [ ] **Step 4: Add `retry_work_item` and update `get_pending_work_items` in `deskbridge/db/store.py`**

Replace `get_pending_work_items` (currently at line 257):
```python
async def get_pending_work_items(
    self, identity_id: str, limit: int = 10
) -> list[aiosqlite.Row]:
    async with self._conn.execute(
        """
        SELECT * FROM work_items
        WHERE status = 'pending' AND identity_id = ?
          AND (next_retry_at IS NULL OR next_retry_at <= strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
        ORDER BY priority, created_at
        LIMIT ?
        """,
        (identity_id, limit),
    ) as cur:
        return await cur.fetchall()
```

Add `retry_work_item` immediately after `complete_work_item` (currently ends around line 335):
```python
async def retry_work_item(self, id: str, next_retry_at: str) -> None:
    async with self._conn.execute(
        """
        UPDATE work_items
        SET status = 'pending',
            attempt_count = attempt_count + 1,
            next_retry_at = ?,
            updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
        WHERE id = ?
        """,
        (next_retry_at, id),
    ):
        pass
    await self._conn.commit()
```

- [ ] **Step 5: Run tests to verify they pass**

```
uv run pytest tests/test_store.py::test_retry_work_item_resets_to_pending_and_increments_attempt_count tests/test_store.py::test_get_pending_work_items_skips_cooling_down tests/test_store.py::test_get_pending_work_items_includes_elapsed_cooldown tests/test_store.py::test_claim_work_item_does_not_change_attempt_count -v
```

Expected: 4 PASS.

- [ ] **Step 6: Run full store test suite to check for regressions**

```
uv run pytest tests/test_store.py -v
```

Expected: all PASS.

- [ ] **Step 7: Commit**

```bash
git add deskbridge/db/schema.py deskbridge/db/store.py tests/test_store.py
git commit -m "feat: add attempt_count/next_retry_at columns and retry_work_item store method"
```

---

### Task 2: Config — `max_agent_attempts`

**Files:**
- Modify: `deskbridge/config.py`
- Test: `tests/test_config.py`

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_config.py` after the existing adapter tests:

```python
def test_max_agent_attempts_default_is_3(tmp_path):
    cfg_file = tmp_path / "config.toml"
    cfg_file.write_text(MINIMAL_CONFIG)
    config = load_config(cfg_file)
    assert config.projects[0].max_agent_attempts == 3


def test_max_agent_attempts_zero_raises(tmp_path):
    bad = MINIMAL_CONFIG.replace(
        'escalation_dm_target = "npub1human"',
        'escalation_dm_target = "npub1human"\nmax_agent_attempts = 0',
    )
    cfg_file = tmp_path / "config.toml"
    cfg_file.write_text(bad)
    with pytest.raises(ConfigError):
        load_config(cfg_file)


def test_max_agent_attempts_custom_value_accepted(tmp_path):
    custom = MINIMAL_CONFIG.replace(
        'escalation_dm_target = "npub1human"',
        'escalation_dm_target = "npub1human"\nmax_agent_attempts = 5',
    )
    cfg_file = tmp_path / "config.toml"
    cfg_file.write_text(custom)
    config = load_config(cfg_file)
    assert config.projects[0].max_agent_attempts == 5
```

- [ ] **Step 2: Run tests to verify they fail**

```
uv run pytest tests/test_config.py::test_max_agent_attempts_default_is_3 tests/test_config.py::test_max_agent_attempts_zero_raises tests/test_config.py::test_max_agent_attempts_custom_value_accepted -v
```

Expected: 3 failures — `ProjectConfig` has no `max_agent_attempts` attribute.

- [ ] **Step 3: Add `max_agent_attempts` to `ProjectConfig` in `deskbridge/config.py`**

In `ProjectConfig`, add after `check_in_prompt`:
```python
max_agent_attempts: int = Field(default=3, ge=1)
```

The `Field` import is already present at line 6. No other changes needed.

- [ ] **Step 4: Run tests to verify they pass**

```
uv run pytest tests/test_config.py::test_max_agent_attempts_default_is_3 tests/test_config.py::test_max_agent_attempts_zero_raises tests/test_config.py::test_max_agent_attempts_custom_value_accepted -v
```

Expected: 3 PASS.

- [ ] **Step 5: Run full config test suite**

```
uv run pytest tests/test_config.py -v
```

Expected: all PASS.

- [ ] **Step 6: Commit**

```bash
git add deskbridge/config.py tests/test_config.py
git commit -m "feat: add max_agent_attempts config field (default 3, ge=1)"
```

---

### Task 3: Poller — retry logic and dispatch audit

**Files:**
- Modify: `deskbridge/agent/poller.py`
- Test: `tests/test_work_item_poller.py`

Context: `poller.py` dispatches work items and detects completed runs. Phase 11 adds:
- `self._active_project_cfg` to cache the current project config so it's available in the completed-run detection block
- Retry logic: on `failed`, if `attempt_count + 1 < max_agent_attempts`, call `retry_work_item`; otherwise leave as terminal
- Audit events: `work_item_dispatched` when claiming, `work_item_retry_queued` or `work_item_terminal` after completion
- `_iso_offset` module-level helper for the 60-second cooldown timestamp

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_work_item_poller.py`:

First, update `make_store` to include `log_audit` and `retry_work_item` so audit calls don't fail silently across all tests:

```python
def make_store(*, project_row=None, pending_items=None, claim_result=True):
    store = MagicMock()
    store.get_project_for_identity = AsyncMock(return_value=project_row)
    store.get_pending_work_items = AsyncMock(return_value=pending_items or [])
    store.claim_work_item = AsyncMock(return_value=claim_result)
    store.log_audit = AsyncMock()
    store.retry_work_item = AsyncMock()
    return store
```

Then add the two retry tests after the existing tests:

```python
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
```

Note: `PROJ` in `test_work_item_poller.py` has `max_agent_attempts=3` by default (the new field defaults to 3). No change to `PROJ` needed.

- [ ] **Step 2: Run tests to verify they fail**

```
uv run pytest tests/test_work_item_poller.py::test_failed_run_below_max_attempts_requeues tests/test_work_item_poller.py::test_failed_run_at_max_attempts_stays_failed -v
```

Expected: 2 failures — `retry_work_item` is never called (not yet implemented).

- [ ] **Step 3: Rewrite `deskbridge/agent/poller.py`**

Replace the entire file with:

```python
import asyncio
import json
import structlog
from datetime import datetime, timezone, timedelta
from uuid import uuid4

from deskbridge.agent.runner import AgentRunner
from deskbridge.config import DeskBridgeConfig, ProjectConfig
from deskbridge.db.store import Store
from deskbridge.mcp.client import McpClient
from deskbridge.mcp.session import SessionBroker

log = structlog.get_logger()


def _iso_offset(seconds: int) -> str:
    return (datetime.now(timezone.utc) + timedelta(seconds=seconds)).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )


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
        self._active_project_cfg: ProjectConfig | None = None

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
            log.warning("kanban_sync_failed", card_id=card_id, column=column, exc_info=True)

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
        # Detect completed runner
        if self._active_run_task is not None and self._active_run_task.done():
            if self._active_work_item_id is not None:
                completed_item = await self._store.get_work_item(self._active_work_item_id)
                if completed_item is not None:
                    final_status = completed_item["status"]
                    retried = False

                    if final_status == "failed":
                        if (
                            self._active_project_cfg is not None
                            and completed_item["attempt_count"] + 1
                            < self._active_project_cfg.max_agent_attempts
                        ):
                            next_retry_at = _iso_offset(60)
                            await self._store.retry_work_item(
                                completed_item["id"], next_retry_at
                            )
                            retried = True
                            try:
                                await self._store.log_audit(
                                    id=str(uuid4()),
                                    event_type="work_item_retry_queued",
                                    work_item_id=completed_item["id"],
                                    payload_json=json.dumps({
                                        "attempt_count": completed_item["attempt_count"],
                                        "next_retry_at": next_retry_at,
                                    }),
                                )
                            except Exception:
                                log.warning(
                                    "audit_log_failed",
                                    event_type="work_item_retry_queued",
                                )
                        else:
                            try:
                                await self._store.log_audit(
                                    id=str(uuid4()),
                                    event_type="work_item_terminal",
                                    work_item_id=completed_item["id"],
                                    payload_json=json.dumps({
                                        "status": "failed",
                                        "attempt_count": completed_item["attempt_count"],
                                    }),
                                )
                            except Exception:
                                log.warning(
                                    "audit_log_failed", event_type="work_item_terminal"
                                )
                    elif final_status in ("done", "interrupted", "cancelled"):
                        try:
                            await self._store.log_audit(
                                id=str(uuid4()),
                                event_type="work_item_terminal",
                                work_item_id=completed_item["id"],
                                payload_json=json.dumps({
                                    "status": final_status,
                                    "attempt_count": completed_item["attempt_count"],
                                }),
                            )
                        except Exception:
                            log.warning(
                                "audit_log_failed",
                                event_type="work_item_terminal",
                                status=final_status,
                            )

                    if (
                        not retried
                        and final_status in ("done", "failed")
                        and completed_item["source_type"] == "kanban"
                    ):
                        await self._sync_card_column(
                            completed_item["source_id"],
                            column=self._kanban_column_done,
                            idempotency_key=f"deskbridge-{self._active_work_item_id}-done",
                        )
            self._active_run_task = None
            self._active_work_item_id = None
            self._active_project_cfg = None

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
                self._active_project_cfg = None
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

            try:
                await self._store.log_audit(
                    id=str(uuid4()),
                    event_type="work_item_dispatched",
                    work_item_id=row["id"],
                    payload_json=json.dumps({
                        "adapter": project_cfg.adapter,
                        "attempt_count": row["attempt_count"],
                    }),
                )
            except Exception:
                log.warning(
                    "audit_log_failed",
                    event_type="work_item_dispatched",
                    work_item_id=row["id"],
                )

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
            self._active_project_cfg = project_cfg
            break
```

- [ ] **Step 4: Run the two new tests**

```
uv run pytest tests/test_work_item_poller.py::test_failed_run_below_max_attempts_requeues tests/test_work_item_poller.py::test_failed_run_at_max_attempts_stays_failed -v
```

Expected: 2 PASS.

- [ ] **Step 5: Run full poller test suite**

```
uv run pytest tests/test_work_item_poller.py -v
```

Expected: all PASS.

- [ ] **Step 6: Commit**

```bash
git add deskbridge/agent/poller.py tests/test_work_item_poller.py
git commit -m "feat: add work item retry with 60s cooldown and dispatch audit events"
```

---

### Task 4: Runner audit events

**Files:**
- Modify: `deskbridge/agent/runner.py`
- Modify: `tests/test_agent_runner.py` (update `make_store` mock)

Context: `runner.py` imports `json` and `uuid4` already. The two audit calls are fire-and-forget, wrapped in `try/except`. No new tests are added — only `make_store` in the test file gains `log_audit = AsyncMock()` so that `await store.log_audit(...)` doesn't raise a `TypeError` when called from the runner.

- [ ] **Step 1: Update `make_store` in `tests/test_agent_runner.py`**

Change:
```python
def make_store():
    store = MagicMock()
    store.upsert_agent_run = AsyncMock()
    store.update_agent_run = AsyncMock()
    store.complete_work_item = AsyncMock()
    store.insert_outbox_item = AsyncMock()
    return store
```
To:
```python
def make_store():
    store = MagicMock()
    store.upsert_agent_run = AsyncMock()
    store.update_agent_run = AsyncMock()
    store.complete_work_item = AsyncMock()
    store.insert_outbox_item = AsyncMock()
    store.log_audit = AsyncMock()
    return store
```

- [ ] **Step 2: Run existing runner tests to confirm no regressions before touching runner.py**

```
uv run pytest tests/test_agent_runner.py -v
```

Expected: all PASS.

- [ ] **Step 3: Add `agent_run_started` audit call to `deskbridge/agent/runner.py`**

In `_do_run`, add this block between step 2 (build command) and step 3 (spawn subprocess), after the `cmd = build_command(...)` line and before `proc = await asyncio.create_subprocess_exec(...)`:

```python
        # 2.5. Emit audit event before spawning
        try:
            await self._store.log_audit(
                id=str(uuid4()),
                event_type="agent_run_started",
                identity_id=f"acc-{project.identity}",
                project_id=project.id,
                work_item_id=work_item["id"],
                payload_json=json.dumps({
                    "run_id": run_id,
                    "adapter": project.adapter,
                }),
            )
        except Exception:
            log.warning("audit_log_failed", event_type="agent_run_started", run_id=run_id)
```

- [ ] **Step 4: Add `agent_run_finished` audit call to `deskbridge/agent/runner.py`**

In step 6 of `_do_run`, after `await self._store.update_agent_run(run_id, status=final_status, result_summary=result_text)`, add:

```python
        try:
            await self._store.log_audit(
                id=str(uuid4()),
                event_type="agent_run_finished",
                identity_id=f"acc-{project.identity}",
                project_id=project.id,
                work_item_id=work_item["id"],
                payload_json=json.dumps({
                    "run_id": run_id,
                    "status": final_status,
                    "returncode": proc.returncode,
                }),
            )
        except Exception:
            log.warning("audit_log_failed", event_type="agent_run_finished", run_id=run_id)
```

- [ ] **Step 5: Run runner tests**

```
uv run pytest tests/test_agent_runner.py -v
```

Expected: all PASS.

- [ ] **Step 6: Commit**

```bash
git add deskbridge/agent/runner.py tests/test_agent_runner.py
git commit -m "feat: add agent_run_started and agent_run_finished audit events to runner"
```

---

### Task 5: Approval watcher and DM watcher audit events

**Files:**
- Modify: `deskbridge/dm/approval_watcher.py`
- Modify: `deskbridge/dm/watcher.py`

Context: Both files use real store fixtures in tests (not mocks), so no test file changes are needed. All audit calls are fire-and-forget in `try/except`. `approval_watcher.py` already imports `uuid` and `json`. `watcher.py` already imports `uuid` and `json`.

- [ ] **Step 1: Add `approval_requested` to `deskbridge/dm/approval_watcher.py`**

In `_process_approval`, the `insert_approval` call uses `str(uuid.uuid4())` for the local ID. Extract that ID so it can be logged. Replace:

```python
        await self._store.insert_approval(
            id=str(uuid.uuid4()),
            mcp_approval_id=req_id,
            work_item_id=work_item_id,
            action_description=action_description,
            scope=None,
            request_text=request_payload,
            expires_at=expires_at_iso,
            identity_id=self._account_id,
        )
```

With:

```python
        local_approval_id = str(uuid.uuid4())
        await self._store.insert_approval(
            id=local_approval_id,
            mcp_approval_id=req_id,
            work_item_id=work_item_id,
            action_description=action_description,
            scope=None,
            request_text=request_payload,
            expires_at=expires_at_iso,
            identity_id=self._account_id,
        )
        try:
            await self._store.log_audit(
                id=str(uuid.uuid4()),
                event_type="approval_requested",
                identity_id=self._account_id,
                payload_json=json.dumps({
                    "approval_id": local_approval_id,
                    "mcp_approval_id": req_id,
                }),
            )
        except Exception:
            log.warning("audit_log_failed", event_type="approval_requested", req_id=req_id)
```

- [ ] **Step 2: Run approval watcher tests**

```
uv run pytest tests/test_approval_watcher.py -v
```

Expected: all PASS.

- [ ] **Step 3: Add `approval_resolved` after each `resolve_approval` call in `deskbridge/dm/watcher.py`**

There are six call sites. Add the same `try/except` audit block after each one.

**Site 1** — in `_handle_approve`, after `await self._store.resolve_approval(row["id"], "approved")`:
```python
                    await self._store.resolve_approval(row["id"], "approved")
                    try:
                        await self._store.log_audit(
                            id=str(uuid.uuid4()),
                            event_type="approval_resolved",
                            identity_id=self._account_id,
                            work_item_id=row["work_item_id"],
                            payload_json=json.dumps({"approval_id": row["id"], "resolution": "approved"}),
                        )
                    except Exception:
                        log.warning("audit_log_failed", event_type="approval_resolved")
                    reply = "Approved."
```

**Site 2** — in `_handle_reject`, after `await self._store.resolve_approval(row["id"], "rejected")`:
```python
                    await self._store.resolve_approval(row["id"], "rejected")
                    try:
                        await self._store.log_audit(
                            id=str(uuid.uuid4()),
                            event_type="approval_resolved",
                            identity_id=self._account_id,
                            work_item_id=row["work_item_id"],
                            payload_json=json.dumps({"approval_id": row["id"], "resolution": "rejected"}),
                        )
                    except Exception:
                        log.warning("audit_log_failed", event_type="approval_resolved")
                    reply = "Rejected."
```

**Site 3** — in `_call_respond_to_approval`, after the main `resolve_approval(row["id"], local_status)` (the one after the `response_status == "approved"` / `"denied"` check, around line 303):
```python
            await self._store.resolve_approval(row["id"], local_status)
            try:
                await self._store.log_audit(
                    id=str(uuid.uuid4()),
                    event_type="approval_resolved",
                    identity_id=self._account_id,
                    work_item_id=row["work_item_id"],
                    payload_json=json.dumps({"approval_id": row["id"], "resolution": local_status}),
                )
            except Exception:
                log.warning("audit_log_failed", event_type="approval_resolved")
            log.info(
                "dm_watcher_approval_resolved",
                ...
```

**Site 4** — in the `approval_already_resolved` branch, after `resolve_approval(row["id"], local_status)` (around line 326):
```python
                await self._store.resolve_approval(row["id"], local_status)
                try:
                    await self._store.log_audit(
                        id=str(uuid.uuid4()),
                        event_type="approval_resolved",
                        identity_id=self._account_id,
                        work_item_id=row["work_item_id"],
                        payload_json=json.dumps({"approval_id": row["id"], "resolution": local_status}),
                    )
                except Exception:
                    log.warning("audit_log_failed", event_type="approval_resolved")
                log.warning(
                    "dm_watcher_approval_already_resolved",
```

**Site 5** — in the `approval_expired` branch, after `resolve_approval(row["id"], "rejected")`:
```python
                await self._store.resolve_approval(row["id"], "rejected")
                try:
                    await self._store.log_audit(
                        id=str(uuid.uuid4()),
                        event_type="approval_resolved",
                        identity_id=self._account_id,
                        work_item_id=row["work_item_id"],
                        payload_json=json.dumps({"approval_id": row["id"], "resolution": "rejected"}),
                    )
                except Exception:
                    log.warning("audit_log_failed", event_type="approval_resolved")
                log.warning(
                    "dm_watcher_approval_expired",
```

**Site 6** — in the `approval_not_found` branch, after `resolve_approval(row["id"], "rejected")`:
```python
                await self._store.resolve_approval(row["id"], "rejected")
                try:
                    await self._store.log_audit(
                        id=str(uuid.uuid4()),
                        event_type="approval_resolved",
                        identity_id=self._account_id,
                        work_item_id=row["work_item_id"],
                        payload_json=json.dumps({"approval_id": row["id"], "resolution": "rejected"}),
                    )
                except Exception:
                    log.warning("audit_log_failed", event_type="approval_resolved")
                log.error(
                    "dm_watcher_approval_not_found",
```

- [ ] **Step 4: Run the approval integration test**

```
uv run pytest tests/test_approval_integration.py -v
```

Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add deskbridge/dm/approval_watcher.py deskbridge/dm/watcher.py
git commit -m "feat: add approval_requested and approval_resolved audit events"
```

---

### Task 6: Outbox audit events

**Files:**
- Modify: `deskbridge/dm/outbox.py`

Context: `outbox.py` imports `json` already; add `uuid4`. Two audit events: `outbox_delivered` on successful send, `outbox_delivery_failed` when permanently failed.

- [ ] **Step 1: Add `uuid4` import to `deskbridge/dm/outbox.py`**

Change:
```python
import asyncio
import json
import structlog
```
To:
```python
import asyncio
import json
import structlog
from uuid import uuid4
```

- [ ] **Step 2: Add `outbox_delivered` after successful send in `_drain_row`**

After `log.info("outbox_drainer_delivered", id=row["id"])`, add:
```python
            try:
                await self._store.log_audit(
                    id=str(uuid4()),
                    event_type="outbox_delivered",
                    identity_id=row["identity_id"],
                    payload_json=json.dumps({
                        "outbox_id": row["id"],
                        "dest_pubkey": row["dest_pubkey"],
                    }),
                )
            except Exception:
                log.warning("audit_log_failed", event_type="outbox_delivered")
```

- [ ] **Step 3: Add `outbox_delivery_failed` after permanent failure in `_drain_row`**

In the `except McpToolError` block, after `await self._store.update_outbox_delivery(row["id"], final_status, error_json)`, add:

```python
            if is_permanent:
                try:
                    await self._store.log_audit(
                        id=str(uuid4()),
                        event_type="outbox_delivery_failed",
                        identity_id=row["identity_id"],
                        payload_json=json.dumps({
                            "outbox_id": row["id"],
                            "attempts": row["delivery_attempts"] + 1,
                        }),
                    )
                except Exception:
                    log.warning("audit_log_failed", event_type="outbox_delivery_failed")
```

- [ ] **Step 4: Run outbox tests**

```
uv run pytest tests/test_outbox_drainer.py -v
```

Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add deskbridge/dm/outbox.py
git commit -m "feat: add outbox_delivered and outbox_delivery_failed audit events"
```

---

### Task 7: Enhanced `deskbridge status` CLI

**Files:**
- Modify: `deskbridge/cli.py`
- Test: `tests/test_cli.py`

Context: Replace `_show_status` with a 5-section version. The existing test `test_cli_status_shows_accounts` seeds a bare SQLite DB without `apply_schema`, which will break when `_show_status` queries `work_items`, `approvals`, `agent_runs`, and `cursors`. Replace that test with a version that uses `aiosqlite` + `apply_schema`.

- [ ] **Step 1: Replace `test_cli_status_shows_accounts` and add `test_status_shows_all_sections` in `tests/test_cli.py`**

Add `import aiosqlite` and `from deskbridge.db.schema import apply_schema` to the top of the test file.

Replace:
```python
def test_cli_status_shows_accounts(config_file, tmp_path):
    import sqlite3 as _sqlite3
    db_path = tmp_path / "test.db"
    conn = _sqlite3.connect(db_path)
    conn.execute(
        "CREATE TABLE accounts "
        "(id TEXT, npub TEXT, label TEXT, passphrase_ref TEXT, "
        "session_id TEXT, health TEXT NOT NULL DEFAULT 'unknown', "
        "last_unlocked_at TEXT, created_at TEXT)"
    )
    conn.execute(
        "INSERT INTO accounts (id, npub, label, passphrase_ref, session_id, health) "
        "VALUES ('acc-alice', 'npub1alice', 'alice', 'env:ALICE', 'sess-123', 'ok')"
    )
    conn.commit()
    conn.close()

    runner = CliRunner()
    result = runner.invoke(cli, ["status", "--config", str(config_file)])
    assert result.exit_code == 0
    assert "alice" in result.output
    assert "ok" in result.output
    assert "sess-123" in result.output
```

With:
```python
async def test_cli_status_shows_accounts(config_file, tmp_path):
    db_path = tmp_path / "test.db"
    async with aiosqlite.connect(db_path) as conn:
        await apply_schema(conn)
        await conn.execute(
            "INSERT INTO accounts (id, npub, label, passphrase_ref, session_id, health) "
            "VALUES ('acc-alice', 'npub1alice', 'alice', 'env:ALICE', 'sess-123', 'ok')"
        )
        await conn.commit()

    runner = CliRunner()
    result = runner.invoke(cli, ["status", "--config", str(config_file)])
    assert result.exit_code == 0
    assert "alice" in result.output
    assert "ok" in result.output
    assert "sess-123" in result.output


async def test_status_shows_all_sections(config_file, tmp_path):
    db_path = tmp_path / "test.db"
    async with aiosqlite.connect(db_path) as conn:
        conn.row_factory = aiosqlite.Row
        await apply_schema(conn)
        await conn.execute(
            "INSERT INTO accounts (id, npub, label, passphrase_ref, health) "
            "VALUES ('acc-alice', 'npub1alice', 'alice', 'env:X', 'ok')"
        )
        await conn.execute(
            "INSERT INTO work_items (id, source_type, source_id, identity_id, "
            "status, idempotency_key, summary) "
            "VALUES ('wi-1', 'dm', 'msg-1', 'acc-alice', 'pending', 'k1', 'fix the bug')"
        )
        await conn.execute(
            "INSERT INTO approvals (id, action_description, status) "
            "VALUES ('appr-1', 'pay invoice', 'pending')"
        )
        await conn.execute(
            "INSERT INTO agent_runs (id, work_item_id, adapter_type, status) "
            "VALUES ('run-1', 'wi-1', 'claude-code', 'done')"
        )
        await conn.execute(
            "INSERT INTO cursors (id, cursor_type, identity_id, raw_json) "
            "VALUES ('cur-1', 'dm', 'acc-alice', '{}')"
        )
        await conn.commit()

    runner = CliRunner()
    result = runner.invoke(cli, ["status", "--config", str(config_file)])
    assert result.exit_code == 0
    assert "Accounts" in result.output
    assert "Work Queue" in result.output
    assert "Approvals" in result.output
    assert "Recent Runs" in result.output
    assert "Watchers" in result.output
    assert "alice" in result.output
    assert "claude-code" in result.output
    assert "done" in result.output
```

- [ ] **Step 2: Run tests to verify they fail**

```
uv run pytest tests/test_cli.py::test_cli_status_shows_accounts tests/test_cli.py::test_status_shows_all_sections -v
```

Expected: `test_cli_status_shows_accounts` may pass or fail depending on existing table presence; `test_status_shows_all_sections` fails because `_show_status` only outputs accounts.

- [ ] **Step 3: Replace `_show_status` in `deskbridge/cli.py`**

Replace the entire `_show_status` function:

```python
async def _show_status(db_path: Path) -> None:
    async with aiosqlite.connect(db_path) as conn:
        conn.row_factory = aiosqlite.Row

        # Accounts
        async with conn.execute(
            "SELECT label, health, session_id, last_unlocked_at FROM accounts"
        ) as cursor:
            accounts = await cursor.fetchall()
        click.echo("Accounts")
        if not accounts:
            click.echo("  (none)")
        else:
            for row in accounts:
                click.echo(
                    f"  [{row['health']}] {row['label']}  "
                    f"session={row['session_id'] or 'none'}  "
                    f"last_unlocked={row['last_unlocked_at'] or 'never'}"
                )

        # Work Queue
        async with conn.execute(
            "SELECT status, COUNT(*) AS n FROM work_items GROUP BY status"
        ) as cursor:
            counts = {r["status"]: r["n"] for r in await cursor.fetchall()}
        click.echo(
            f"\nWork Queue\n"
            f"  pending={counts.get('pending', 0)}"
            f"  dispatched={counts.get('dispatched', 0)}"
            f"  done={counts.get('done', 0)}"
            f"  failed={counts.get('failed', 0)}"
            f"  cancelled={counts.get('cancelled', 0)}"
        )

        # Approvals
        async with conn.execute(
            "SELECT COUNT(*) AS n FROM approvals WHERE status = 'pending'"
        ) as cursor:
            row = await cursor.fetchone()
        click.echo(f"\nApprovals\n  pending={row['n'] if row else 0}")

        # Recent Runs
        async with conn.execute(
            """
            SELECT ar.status, ar.adapter_type, ar.updated_at, wi.summary
            FROM agent_runs ar
            LEFT JOIN work_items wi ON wi.id = ar.work_item_id
            ORDER BY ar.updated_at DESC
            LIMIT 5
            """
        ) as cursor:
            runs = await cursor.fetchall()
        click.echo("\nRecent Runs (last 5)")
        if not runs:
            click.echo("  (none)")
        else:
            for run in runs:
                summary = (run["summary"] or "")[:40]
                click.echo(
                    f"  {run['status']:<12} {run['adapter_type']:<14}"
                    f" {run['updated_at']}  \"{summary}\""
                )

        # Watchers
        async with conn.execute(
            "SELECT cursor_type, identity_id, updated_at FROM cursors ORDER BY cursor_type"
        ) as cursor:
            cursors = await cursor.fetchall()
        click.echo("\nWatchers (last cursor update)")
        if not cursors:
            click.echo("  (none)")
        else:
            for c in cursors:
                click.echo(
                    f"  {c['cursor_type']:<12} {c['identity_id']:<20} {c['updated_at']}"
                )
```

- [ ] **Step 4: Run CLI tests**

```
uv run pytest tests/test_cli.py -v
```

Expected: all PASS.

- [ ] **Step 5: Run full test suite**

```
uv run pytest -v
```

Expected: all PASS (296 + new tests).

- [ ] **Step 6: Commit**

```bash
git add deskbridge/cli.py tests/test_cli.py
git commit -m "feat: expand deskbridge status to five-section operational dashboard"
```

---

## Self-Review

**Spec coverage:**
- ✅ Schema migrations for `attempt_count` and `next_retry_at` — Task 1
- ✅ `retry_work_item` store method — Task 1
- ✅ `get_pending_work_items` cooldown filter — Task 1
- ✅ `max_agent_attempts` config field — Task 2
- ✅ Poller retry logic (`attempt_count + 1 < max_agent_attempts`) — Task 3
- ✅ `work_item_dispatched` audit — Task 3
- ✅ `work_item_retry_queued` audit — Task 3
- ✅ `work_item_terminal` audit — Task 3
- ✅ `agent_run_started` audit — Task 4
- ✅ `agent_run_finished` audit — Task 4
- ✅ `approval_requested` audit — Task 5
- ✅ `approval_resolved` audit (6 call sites) — Task 5
- ✅ `outbox_delivered` audit — Task 6
- ✅ `outbox_delivery_failed` audit — Task 6
- ✅ Five-section `status` CLI — Task 7

**Type consistency:** `retry_work_item(id: str, next_retry_at: str)` matches usage in both store (Task 1) and poller (Task 3). `max_agent_attempts` is `int` throughout.

**Retry guard:** `completed_item["attempt_count"] + 1 < self._active_project_cfg.max_agent_attempts` — with `attempt_count=0` on first failure and `max_agent_attempts=3`: 0+1=1 < 3 → retry; after two retries `attempt_count=2`: 2+1=3, not < 3 → terminal. Correct: 3 total runs.
