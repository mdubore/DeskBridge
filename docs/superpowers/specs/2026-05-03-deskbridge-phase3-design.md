# DeskBridge Phase 3: Agent Task Routing — Design Spec

**Goal:** Read pending `work_items` rows, route them to the correct project by identity, dispatch a coding agent subprocess, and on completion notify the operator via DM and update the kanban board card.

**Architecture:** Two new asyncio components — `WorkItemPoller` (one per identity) and `AgentRunner` (one per dispatched work item) — run inside the supervisor alongside the existing `DmWatcher` and `OutboxDrainer`. The supervisor spawns one `WorkItemPoller` per identity after `unlock_all()`, following the same pattern as `DmWatcher`. `AgentRunner` is a coroutine spawned by the poller, not a long-lived loop.

**Tech stack:** Same as Phase 1/2 — aiosqlite, structlog, asyncio, `McpClient.call_tool`, `asyncio.create_subprocess_exec` for agent processes, pytest-asyncio (`asyncio_mode = auto`).

---

## Scope

**In scope:**
- `WorkItemPoller`: per-identity asyncio task, polls `work_items WHERE status='pending'`, routes by identity → project, enforces one-agent-at-a-time per project, spawns `AgentRunner`
- `AgentRunner`: manages one subprocess run-to-completion — launches CLI, heartbeats `agent_runs`, captures output, writes result to `work_items`, writes outbox row for DM, calls `update_board_card` via MCP if source is kanban
- `Store` additions: `get_pending_work_items`, `claim_work_item`, `upsert_agent_run`, `update_agent_run`, `complete_work_item`, `get_project_for_identity`, `insert_outbox_item`
- Supervisor wiring: spawn one `WorkItemPoller` per identity, cancel on shutdown

**Out of scope:**
- Parsing DM content for intent or commands (Phase 4)
- Group message routing (Phase 4)
- Multi-project-per-identity routing
- Agent approval workflows (schema has `approvals` table but Phase 3 does not use it)
- `update_board_card` retry logic — board update is best-effort; DM delivery is handled by existing `OutboxDrainer`

---

## File Map

| File | Action | Responsibility |
|---|---|---|
| `deskbridge/agent/__init__.py` | Create | Package marker |
| `deskbridge/agent/poller.py` | Create | `WorkItemPoller` — per-identity poll loop |
| `deskbridge/agent/runner.py` | Create | `AgentRunner` — subprocess lifecycle |
| `deskbridge/db/store.py` | Modify | Add 7 new Store methods |
| `deskbridge/supervisor.py` | Modify | Spawn `WorkItemPoller` tasks after `unlock_all()` |
| `tests/test_work_item_poller.py` | Create | `WorkItemPoller` unit tests |
| `tests/test_agent_runner.py` | Create | `AgentRunner` unit tests |
| `tests/test_store.py` | Modify | Append tests for 6 new Store methods |
| `tests/test_supervisor.py` | Modify | Append test: pollers spawned and cancelled |

---

## Component Design

### Store additions (`deskbridge/db/store.py`)

Six new methods following the existing `async with self._conn.execute(...): pass` and `fetchall`/`fetchone` patterns.

**`get_pending_work_items`** — returns pending items for one identity, ordered by priority then creation time:
```python
async def get_pending_work_items(
    self,
    identity_id: str,
    limit: int = 10,
) -> list[aiosqlite.Row]:
```
SQL: `SELECT * FROM work_items WHERE status = 'pending' AND identity_id = ? ORDER BY priority, created_at LIMIT ?`

**`claim_work_item`** — conditional update; returns `True` if the row was claimed, `False` if already taken:
```python
async def claim_work_item(self, id: str) -> bool:
```
SQL: `UPDATE work_items SET status = 'dispatched', updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now') WHERE id = ? AND status = 'pending'`
Use `async with self._conn.execute(sql, params) as cur: return cur.rowcount > 0`. The `AND status = 'pending'` guard makes this race-safe: a second caller gets `rowcount = 0` and returns `False`.

**`upsert_agent_run`** — insert a new `agent_runs` row:
```python
async def upsert_agent_run(
    self,
    id: str,
    work_item_id: str,
    adapter_type: str,
) -> None:
```
SQL: `INSERT OR IGNORE INTO agent_runs (id, work_item_id, adapter_type) VALUES (?, ?, ?)` — `INSERT OR IGNORE` so a restart after crash does not fail if the row was already created.

**`update_agent_run`** — update heartbeat, status, and result fields:
```python
async def update_agent_run(
    self,
    id: str,
    *,
    status: str | None = None,
    result_summary: str | None = None,
    heartbeat_at: str | None = None,
) -> None:
```
SQL builds dynamically: only set columns where the argument is not None. Always sets `updated_at = strftime(...)`. Raises `ValueError` if all three optional args are `None` — callers must provide at least one field to update.

**`complete_work_item`** — set final status on a work item:
```python
async def complete_work_item(self, id: str, status: str) -> None:
```
SQL: `UPDATE work_items SET status = ?, updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now') WHERE id = ?`

**`get_project_for_identity`** — returns the single project row for an identity, or `None`:
```python
async def get_project_for_identity(self, identity_id: str) -> aiosqlite.Row | None:
```
SQL: `SELECT * FROM projects WHERE identity_id = ? LIMIT 1`

---

### WorkItemPoller (`deskbridge/agent/poller.py`)

One instance per identity. Runs as an asyncio task spawned by the supervisor.

```python
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

    async def run(self) -> None:
        ...
```

`_account_id = f"acc-{identity_label}"` — consistent with Phase 1/2 convention.

**Loop behavior** (`while not shutdown_event.is_set()`):

1. Fetch `project = await store.get_project_for_identity(self._account_id)` — if `None`, log warning and sleep `poll_interval_secs`, continue
2. Fetch `rows = await store.get_pending_work_items(self._account_id, limit=10)`
3. For each row:
   - If `self._active_run_task is not None and not self._active_run_task.done()`: skip (one-at-a-time per poller/identity)
   - `claimed = await store.claim_work_item(row["id"])` — if not claimed, skip
   - Resolve `ProjectConfig` from `self._config.projects` by `next((p for p in self._config.projects if p.id == project["id"]), None)` — if `None`, log error and skip (DB row exists but config entry missing)
   - Build `AgentRunner(work_item=row, project=project_cfg, run_id=str(uuid4()), store=store, client=client, broker=broker)`, spawn as `asyncio.create_task(runner.run(), name=f"agent_run_{row['id']}")`; store in `self._active_run_task`
   - Break after spawning one (process one at a time; remaining pending items will be picked up next cycle)
4. Sleep using `asyncio.wait_for(self._shutdown_event.wait(), timeout=self._poll_interval_secs)` with `except asyncio.TimeoutError: pass`

`_active_run_task: asyncio.Task | None = None` — initialized to `None` in `__init__`.

`run()` must have a `finally` block: if `_active_run_task` is not None and not done, cancel it and `await asyncio.gather(self._active_run_task, return_exceptions=True)` before returning. This ensures the subprocess inside `AgentRunner` is terminated before the poller exits.

---

### AgentRunner (`deskbridge/agent/runner.py`)

One coroutine per dispatched work item. Not a loop — runs to completion.

```python
class AgentRunner:
    def __init__(
        self,
        work_item: aiosqlite.Row,
        project: ProjectConfig,
        run_id: str,
        store: Store,
        client: McpClient,
        broker: SessionBroker,
        timeout_secs: float = 600.0,
        heartbeat_interval_secs: float = 30.0,
    ) -> None:

    async def run(self) -> None:
        ...
```

**`run()` behavior:**

1. `await store.upsert_agent_run(id=self._run_id, work_item_id=work_item["id"], adapter_type=self._project.agents[0])`
2. Build CLI command. Map `project.agents[0]` (adapter type stored in config) to the CLI executable:

   | Config value | CLI executable |
   |---|---|
   | `"claude-code"` | `claude` |
   | `"codex"` | `codex` |

   Commands:
   - `claude-code` adapter: `["claude", "--project", project.repo_path, "--message", prompt]`
   - `codex` adapter: `["codex", "--dir", project.repo_path, prompt]`
   - `prompt` = `f"{work_item['summary']}\n\n{work_item['payload_json']}"` truncated to 4000 chars

3. `proc = await asyncio.create_subprocess_exec(*cmd, stdout=PIPE, stderr=STDOUT, cwd=project.repo_path)`

4. Start two concurrent asyncio tasks (do **not** `await` them yet):
   - **Output drainer task**: reads `proc.stdout` line-by-line, appends each decoded line to a `collections.deque(maxlen=200)` (≈4000 chars at 20 chars/line average); exits when `proc.stdout` returns EOF. Final buffer content is `"\n".join(output_buf)`.
   - **Heartbeat task**: loops — calls `store.update_agent_run(run_id, heartbeat_at=now_iso())`, then `asyncio.sleep(heartbeat_interval_secs)` wrapped in `asyncio.wait_for` — until cancelled.

5. Wait for the process, handle timeout and cancellation:
   ```python
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
       raise  # re-raise so the task is properly cancelled
   finally:
       heartbeat_task.cancel()
       await asyncio.gather(heartbeat_task, drain_task, return_exceptions=True)
   ```
   The `finally` block cancels the heartbeat task and awaits both tasks so all stdout is drained before continuing. `CancelledError` is **re-raised** after cleanup so asyncio's task cancellation propagates correctly.

   Steps 6–9 run **after** the `try/except/finally` block above (they execute on normal exit and on timeout; they do **not** execute when `CancelledError` is re-raised):

6. `await store.update_agent_run(run_id, status=final_status, result_summary="\n".join(output_buf)[-2000:])`
7. `await store.complete_work_item(work_item["id"], status=final_status)`
8. Write outbox DM — only if `project.escalation_dm_target` is non-empty:
   ```python
   if project.escalation_dm_target:
       identity_id = f"acc-{project.identity}"
       await store.insert_outbox_item(
           id=str(uuid4()),
           identity_id=identity_id,
           dest_pubkey=project.escalation_dm_target,
           message_text="\n".join(output_buf)[-2000:],
           idempotency_key=f"deskbridge:{run_id}:result_notify",
       )
   ```
9. If `work_item["source_type"] == "kanban"` and `work_item["source_id"]` is not None:
   - `session_id = await broker.get_session_id(project.identity)` — if None, log and skip
   - `await client.call_tool("update_board_card", {"session_id": session_id, "card_id": work_item["source_id"], "description": "\n".join(output_buf)[-500:], "idempotency_key": f"deskbridge:{run_id}:board_update"})`
   - On any error: log, do not raise — board update is best-effort

The entire `run()` body (steps 1–9) is wrapped in `try/except Exception` at the outermost level: on unexpected error, log with `exc_info=True`, call `complete_work_item(id, 'failed')`, and attempt the outbox write (if `escalation_dm_target` is set).

---

### Store method: `insert_outbox_item`

Rather than writing to `outbox` directly via raw SQL in `AgentRunner`, add one more Store method:

```python
async def insert_outbox_item(
    self,
    id: str,
    identity_id: str,
    dest_pubkey: str,
    message_text: str,
    idempotency_key: str,
) -> None:
```
SQL: `INSERT OR IGNORE INTO outbox (id, identity_id, dest_pubkey, message_text, idempotency_key) VALUES (?, ?, ?, ?, ?)`

`INSERT OR IGNORE` on `idempotency_key` — safe on retry if `AgentRunner` is restarted.

---

### Supervisor changes (`deskbridge/supervisor.py`)

Same pattern as `DmWatcher` — spawn one `WorkItemPoller` per identity after `unlock_all()`:

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

Add `poller_tasks` to the `finally` block cancel/gather alongside `watcher_tasks` and `drainer_task`. Initialize `poller_tasks: list[asyncio.Task] = []` before the `try` block.

---

## Data Flow

```
work_items (status=pending)
    │  get_pending_work_items (per identity)
    ▼
WorkItemPoller
    │  claim_work_item → status=dispatched
    │  upsert_agent_run
    ▼
AgentRunner
    │  asyncio.create_subprocess_exec (claude / codex)
    │  heartbeat → agent_runs.heartbeat_at
    │  output buffer
    ▼
process exits
    │  update_agent_run (status, result_summary)
    │  complete_work_item (status=done/failed/interrupted)
    │  insert_outbox_item → OutboxDrainer → send_dm
    │  update_board_card (MCP, best-effort, kanban source only)
    ▼
operator notified via DM
```

---

## Error Handling

| Scenario | Behavior |
|---|---|
| No project for identity | Log warning, sleep, continue — item stays `pending` |
| Active run still in flight | Skip this poll cycle — item stays `pending`, picked up next cycle |
| `claim_work_item` returns False | Another process claimed it — skip |
| Subprocess exits non-zero | `agent_runs.status = 'failed'`, `work_items.status = 'failed'`, DM operator |
| Subprocess timeout (default 10 min) | SIGTERM → wait 10s → SIGKILL, mark `interrupted`, DM operator |
| `shutdown_event` fires mid-run | SIGTERM → SIGKILL, mark `interrupted`, supervisor exits cleanly |
| `update_board_card` MCP fails | Log error, continue — DM already sent, board update is best-effort |
| `insert_outbox_item` fails | Log error — operator not notified via DM for this run |
| Unexpected exception in `AgentRunner.run()` | Log with `exc_info=True`, `complete_work_item('failed')`, attempt outbox write |

---

## Testing Strategy

**Store tests** (`tests/test_store.py`) — real aiosqlite via `db_conn` fixture:
- `get_pending_work_items`: returns pending rows for identity, excludes dispatched/done
- `claim_work_item`: returns `True` and transitions status; second call on same row returns `False`
- `upsert_agent_run`: inserts row; second call with same id is a no-op (`INSERT OR IGNORE`)
- `update_agent_run`: updates only provided fields; leaves others unchanged
- `complete_work_item`: sets final status
- `get_project_for_identity`: returns project row; returns `None` when not found
- `insert_outbox_item`: inserts row; second call with same idempotency_key is a no-op

**WorkItemPoller tests** (`tests/test_work_item_poller.py`) — `AsyncMock` for `Store`, real `asyncio.Event`:
- Pending item + no project → no claim made
- Pending item + project → item claimed, AgentRunner spawned
- Active run still in flight → second item skipped
- Shutdown event → loop exits cleanly

**AgentRunner tests** (`tests/test_agent_runner.py`) — mock `asyncio.create_subprocess_exec`, mock `Store`, mock `McpClient`:
- Success path (exit 0) → `done` status, outbox row written, board card updated (if kanban)
- Non-zero exit → `failed` status, outbox row written
- Timeout → `interrupted` status, SIGTERM sent
- Shutdown mid-run → `interrupted`, SIGTERM sent
- No session for board update → board update skipped, no crash
- `escalation_dm_target` empty → outbox row not written, no crash
- Unexpected exception → `failed`, outbox write attempted

**Supervisor tests** (`tests/test_supervisor.py`) — append one test: pollers spawned and cancelled, same pattern as existing DmWatcher/OutboxDrainer test.

---

## Invariants and Constraints

- `claim_work_item` uses `WHERE status = 'pending'` — only one caller can claim a given item; `conn.total_changes` check makes this race-safe within a single aiosqlite connection
- One active `AgentRunner` task per `WorkItemPoller` — enforced by `_active_run_task.done()` check before spawning
- `insert_outbox_item` uses `INSERT OR IGNORE` on `idempotency_key` — safe if `AgentRunner` restarts mid-run
- `update_board_card` is called at most once per run and is best-effort — failure does not affect work item status or DM delivery
- All loop sleeps use `asyncio.wait_for(shutdown_event.wait(), timeout=N)` — never `asyncio.sleep`
- Agent subprocess is always killed before `AgentRunner.run()` returns — no orphaned processes on shutdown
- `AgentRunner` uses a `collections.deque(maxlen=200)` to accumulate stdout lines — bounded memory, no post-hoc slicing of a large string

## Idempotency Key Format

Outbox rows written by `AgentRunner`:
```
deskbridge:<run_id>:result_notify
```

Board card update idempotency key:
```
deskbridge:<run_id>:board_update
```
