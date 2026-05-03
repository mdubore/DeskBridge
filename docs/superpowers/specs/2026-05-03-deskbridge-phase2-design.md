# DeskBridge Phase 2: DM Automation — Design Spec

**Goal:** Add inbound DM watching (per-identity, cursor-persisted), outbound DM delivery (outbox drain loop), and the Store methods both need — so the supervisor can receive operator commands as `work_items` rows and send autonomous notifications via `outbox`.

**Architecture:** Three new components run as asyncio tasks inside the supervisor alongside the existing heartbeat loop. `DmWatcher` (one per identity) long-polls `wait_for_new_dms` and writes inbound DMs to `work_items`. `OutboxDrainer` (one shared instance) polls the `outbox` table every 5 seconds and calls `send_dm`. Both components depend on new `Store` methods; neither knows about each other.

**Tech stack:** Same as Phase 1 — aiosqlite, structlog, mcp ClientSession via `McpClient.call_tool`, pytest-asyncio (`asyncio_mode = auto`).

---

## Scope

**In scope:**
- `DmWatcher`: per-identity asyncio task, `wait_for_new_dms` long-poll, cursor persistence, `work_items` row creation
- `OutboxDrainer`: shared asyncio task, 5-second drain loop, `send_dm` with idempotency key, delivery status tracking, max 3 attempts then mark `'failed'`
- `Store` additions: `upsert_work_item`, `get_pending_outbox_items`, `update_outbox_delivery`
- Supervisor wiring: spawn watcher tasks and drainer task after `unlock_all()`, cancel cleanly on shutdown

**Out of scope:**
- Parsing or routing inbound DM content (Phase 4)
- Group message watching (Phase 4)
- Agent acting on `work_items` rows (Phase 3)
- `dest_group_id` outbox rows — outbox drainer handles `dest_pubkey` only; group sends come in Phase 4
- `get_session_status` / `get_relay_status` in heartbeat (Phase 3)

---

## File Map

| File | Status | Responsibility |
|---|---|---|
| `deskbridge/dm/__init__.py` | Create | Package marker |
| `deskbridge/dm/watcher.py` | Create | `DmWatcher` — per-identity long-poll loop |
| `deskbridge/dm/outbox.py` | Create | `OutboxDrainer` — shared drain loop |
| `deskbridge/db/store.py` | Modify | Add `upsert_work_item`, `get_pending_outbox_items`, `update_outbox_delivery` |
| `deskbridge/supervisor.py` | Modify | Spawn and cancel DM watcher + outbox drainer tasks |
| `tests/test_dm_watcher.py` | Create | DmWatcher unit tests |
| `tests/test_outbox_drainer.py` | Create | OutboxDrainer unit tests |
| `tests/test_store.py` | Modify | Tests for new Store methods |
| `tests/test_supervisor.py` | Modify | Supervisor spawns watcher/drainer tasks |

---

## Component Design

### Store additions (`deskbridge/db/store.py`)

Three new methods, all following the existing `async with self._conn.execute(...): pass` pattern.

**`upsert_work_item`** — inserts or ignores on `idempotency_key` conflict (a replayed cursor must not create duplicate rows):
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
) -> None:
```
SQL: `INSERT OR IGNORE INTO work_items (id, source_type, source_id, identity_id, summary, payload_json, idempotency_key) VALUES (?, ?, ?, ?, ?, ?, ?)`.

**`get_pending_outbox_items`** — returns rows where `delivery_status = 'pending'` AND `delivery_attempts < max_attempts`. Rows that have exhausted retries are marked `'failed'` by `update_outbox_delivery` and never appear here again.
```python
async def get_pending_outbox_items(self, max_attempts: int = 3) -> list[aiosqlite.Row]:
```
SQL: `SELECT * FROM outbox WHERE delivery_status = 'pending' AND delivery_attempts < ? ORDER BY created_at`.

**`update_outbox_delivery`** — increments `delivery_attempts` via SQL (not Python read-modify-write), sets `delivery_status` and `delivery_result_json`, sets `delivered_at` when status is `'delivered'`:
```python
async def update_outbox_delivery(
    self,
    id: str,
    delivery_status: str,   # 'delivered' | 'pending' | 'failed'
    delivery_result_json: str,
) -> None:
```
SQL:
```sql
UPDATE outbox
SET delivery_status = ?,
    delivery_attempts = delivery_attempts + 1,
    delivery_result_json = ?,
    delivered_at = CASE WHEN ? = 'delivered' THEN strftime('%Y-%m-%dT%H:%M:%SZ', 'now') ELSE NULL END
WHERE id = ?
```

---

### DmWatcher (`deskbridge/dm/watcher.py`)

One instance per identity. Runs as an asyncio task spawned by the supervisor.

```python
class DmWatcher:
    def __init__(
        self,
        identity_label: str,
        store: Store,
        client: McpClient,
        broker: SessionBroker,
        shutdown_event: asyncio.Event,
        poll_timeout_secs: int = 30,
    ) -> None:

    async def run(self) -> None:
        ...
```

`account_id` is derived as `f"acc-{self._identity_label}"` — consistent with the Phase 1 `acc-{label}` convention throughout.

**Loop behavior:**

Before entering the loop, load the persisted cursor once:
```python
cursor_row = await self._store.get_cursor(cursor_type="dm_watcher", identity_id=account_id)
after_message_id = cursor_row["last_entity_id"] if cursor_row else None
after_created_at = cursor_row["last_created_at"] if cursor_row else None
```

Then loop until `shutdown_event.is_set()`:

1. Get `session_id = await self._broker.get_session_id(self._identity_label)` — if `None`, sleep 5s and `continue` (session not yet unlocked or degraded; heartbeat will re-unlock)
2. Call `client.call_tool("wait_for_new_dms", {"session_id": session_id, "after_message_id": after_message_id, "after_created_at": after_created_at, "timeout_seconds": self._poll_timeout_secs})`
   - **Contract assumption:** response shape is `{"messages": [...], "last_message_id": str|null, "last_created_at": str|null}`. Each message has at minimum `"id"` and `"content"` fields. Verify against actual nostrdesk-mcp schema when writing tests; adjust field names if they differ.
3. For each message in `result["messages"]`:
   - Call `store.upsert_work_item(id=uuid4(), source_type="dm", source_id=message["id"], identity_id=account_id, summary=message["content"][:200], payload_json=json.dumps(message), idempotency_key=message["id"])`
4. If `result["messages"]` is non-empty and `result.get("last_message_id")` is not None, update in-memory cursor and persist:
   ```python
   after_message_id = result["last_message_id"]
   after_created_at = result.get("last_created_at")
   await store.upsert_cursor(cursor_type="dm_watcher", identity_id=account_id,
       last_entity_id=after_message_id, last_created_at=after_created_at,
       last_imported_at=None, raw_json=json.dumps(result))
   ```
   Do **not** update the cursor on an empty response or timeout — no progress was made.
5. On `McpToolError` with routing `REAUTH`: log warning, sleep 5s — heartbeat will re-unlock
6. On `McpToolError` with routing `REJECT`: log error and **stop the loop** for this identity — a reject means the MCP server has permanently refused this poll. The supervisor will not restart this task automatically; operator intervention is needed.
7. On `McpToolError` with other routing: log error, sleep 5s, continue
8. On any unexpected `Exception`: log with `exc_info=True`, sleep 5s, continue — a bad message must not kill the watcher

**Cursor persistence invariant:** cursor is saved *after* all `work_items` rows are written for that batch. If the process crashes between writing work items and saving the cursor, the same DMs will be seen again on restart — `INSERT OR IGNORE` on `idempotency_key` makes that safe.

---

### OutboxDrainer (`deskbridge/dm/outbox.py`)

One shared instance. Runs as a single asyncio task.

```python
class OutboxDrainer:
    def __init__(
        self,
        store: Store,
        client: McpClient,
        broker: SessionBroker,
        identities: list[IdentityConfig],
        shutdown_event: asyncio.Event,
        drain_interval_secs: float = 5.0,
        max_attempts: int = 3,
    ) -> None:

    async def run(self) -> None:
        ...
```

**Identity label lookup:** at init time, build `self._account_to_label: dict[str, str]` from `identities`:
```python
self._account_to_label = {f"acc-{i.label}": i.label for i in identities}
```
Used to resolve `row["identity_id"]` → label for `broker.get_session_id`.

**Loop behavior** (runs until `shutdown_event.is_set()`):

1. Fetch `rows = await store.get_pending_outbox_items(max_attempts=self._max_attempts)`
2. For each row:
   - Guard: if `not row["dest_pubkey"]`, log at debug level and skip — group-only rows and malformed rows are not for this drainer
   - Look up `label = self._account_to_label.get(row["identity_id"])`. If missing, log error and skip.
   - Get `session_id = await self._broker.get_session_id(label)` — if `None`, skip row (will retry next drain)
   - Call `client.call_tool("send_dm", {"session_id": session_id, "recipient_pubkey": row["dest_pubkey"], "content": row["message_text"], "idempotency_key": row["idempotency_key"]})`
   - On success: `store.update_outbox_delivery(row["id"], "delivered", json.dumps(result))`
   - On `McpToolError` with routing `REJECT`: `store.update_outbox_delivery(row["id"], "failed", error_json)` — permanent failure, do not retry regardless of remaining attempts
   - On any other `McpToolError`: determine final status — if `row["delivery_attempts"] + 1 >= self._max_attempts`, pass `"failed"`; otherwise pass `"pending"`. Call `store.update_outbox_delivery(row["id"], status, error_json)`
   - On unexpected `Exception`: log with `exc_info=True`, call `update_outbox_delivery(row["id"], "pending", ...)` so attempts increments and we don't lose the row
3. Sleep `drain_interval_secs` or until `shutdown_event.is_set()` — use `asyncio.wait_for(self._shutdown_event.wait(), timeout=self._drain_interval_secs)` with `except asyncio.TimeoutError: pass`, matching the Phase 1 heartbeat pattern

**Delivery status progression:**
- `pending` → `delivered`: successful `send_dm`
- `pending` → `pending` (with incremented attempts): transient `McpToolError`
- `pending` → `failed`: `REJECT` routing, or attempts exhausted

Once `delivery_status = 'failed'`, the row is permanently excluded from `get_pending_outbox_items`. No further action is taken by the drainer; the operator must inspect and decide.

**Note:** outbox processing is sequential within each drain cycle — rows are attempted one at a time. This is intentional for Phase 2: it keeps retry accounting simple and avoids concurrent session contention. If throughput becomes a bottleneck in later phases, batching can be added then.

---

### Supervisor changes (`deskbridge/supervisor.py`)

Tasks are spawned **after** `await broker.unlock_all()` so the first poll iteration is likely to find a live session:

```python
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
        ).run(),
        name=f"dm_watcher_{identity.label}",
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

try:
    interval = self._config.supervisor.heartbeat_interval_secs
    while not self._shutdown_event.is_set():
        await broker.refresh_if_needed()
        try:
            await asyncio.wait_for(
                self._shutdown_event.wait(), timeout=float(interval)
            )
        except asyncio.TimeoutError:
            pass
    log.info("supervisor_stopped")
finally:
    for task in watcher_tasks + [drainer_task]:
        task.cancel()
    await asyncio.gather(*watcher_tasks, drainer_task, return_exceptions=True)
    # signal handler cleanup (existing)
```

Tasks that exit early (e.g., watcher stopped by `REJECT`) are already done when `cancel()` is called — `cancel()` on a completed task is a no-op, so this is safe.

---

## Data Flow

```
nostrdesk-mcp
    │  wait_for_new_dms (per identity, long-poll)
    ▼
DmWatcher
    ├──▶ cursors table (upsert after each non-empty batch)
    └──▶ work_items table (INSERT OR IGNORE, source_type=dm)
                          │
                          └── Phase 3 reads and acts on these rows

outbox table ◀──── Phase 3/4 writes rows here
    │  get_pending_outbox_items
    ▼
OutboxDrainer
    │  send_dm (with idempotency_key)
    ▼
nostrdesk-mcp
```

---

## Error Handling

| Scenario | Component | Behavior |
|---|---|---|
| `wait_for_new_dms` timeout — no new DMs | DmWatcher | Normal — `messages` is empty, cursor not updated, loop continues |
| `McpToolError` REAUTH | DmWatcher | Log warning, sleep 5s — heartbeat re-unlocks the session |
| `McpToolError` REJECT | DmWatcher | Log error, exit `run()` — permanent refusal, operator must intervene |
| `McpToolError` other | DmWatcher | Log error, sleep 5s, retry |
| Unexpected exception | DmWatcher | Log with `exc_info=True`, sleep 5s, continue — single bad message must not kill watcher |
| `send_dm` success | OutboxDrainer | Status → `delivered` |
| `send_dm` REJECT | OutboxDrainer | Status → `failed` immediately, regardless of attempts remaining |
| `send_dm` transient failure, attempts remaining | OutboxDrainer | Status stays `pending`, `delivery_attempts` incremented |
| `send_dm` transient failure, attempts exhausted | OutboxDrainer | Status → `failed` |
| Row with no `dest_pubkey` | OutboxDrainer | Skip silently (debug log) |
| Unknown `identity_id` on outbox row | OutboxDrainer | Log error, skip row |
| Session not yet unlocked | Both | DmWatcher sleeps 5s; OutboxDrainer skips the row — retry next cycle |
| Crash between writing work items and saving cursor | DmWatcher | Idempotency key prevents duplicate `work_items` rows on replay |

---

## Testing Strategy

**Store tests** (`tests/test_store.py`) — real aiosqlite DB via existing `db_conn` fixture:
- `upsert_work_item` inserts a row; second call with same `idempotency_key` is a no-op (row count stays 1)
- `get_pending_outbox_items` returns pending rows with attempts < max; excludes rows at max attempts; excludes `'failed'` rows
- `update_outbox_delivery`: sets status to `'delivered'`, increments `delivery_attempts`, sets `delivered_at`
- `update_outbox_delivery`: sets status to `'pending'`, increments `delivery_attempts`, leaves `delivered_at` null
- `update_outbox_delivery`: sets status to `'failed'`, increments `delivery_attempts`

**DmWatcher tests** (`tests/test_dm_watcher.py`) — `AsyncMock` for `McpClient.call_tool`, real `Store`, real aiosqlite:
- New DMs returned → `work_items` row inserted, cursor updated in DB
- Empty response (timeout) → no `work_items` row inserted, cursor unchanged in DB
- Same message id seen twice (replayed cursor) → `work_items` row count stays 1
- `McpToolError` REAUTH → no crash, loop resumes (run one iteration, assert it didn't raise)
- `McpToolError` REJECT → `run()` exits cleanly (task completes without error)
- No session (`broker.get_session_id` returns None) → no MCP call made, loop continues

**OutboxDrainer tests** (`tests/test_outbox_drainer.py`) — `AsyncMock` for `McpClient.call_tool`, real `Store`:
- Pending row + successful `send_dm` → `delivery_status = 'delivered'`, `delivery_attempts = 1`
- Pending row + transient `McpToolError` (attempts < max) → `delivery_status = 'pending'`, attempts incremented
- Pending row + transient `McpToolError` (attempts = max - 1) → `delivery_status = 'failed'`, attempts incremented
- Pending row + `REJECT` `McpToolError` → `delivery_status = 'failed'` immediately (regardless of attempts)
- Row at max attempts → not fetched by `get_pending_outbox_items`, no MCP call made
- Row with no `dest_pubkey` → skipped, no MCP call
- Unknown identity on row → skipped, no MCP call
- No session for identity → row skipped, no MCP call

**Supervisor tests** (`tests/test_supervisor.py`) — extend existing tests with patched `DmWatcher` and `OutboxDrainer`:
- After `unlock_all()`, watcher tasks and drainer task are created
- On shutdown, all tasks are cancelled and awaited without hanging
- A watcher task that exits early (simulated `REJECT`) does not prevent clean supervisor shutdown

---

## Invariants and Constraints

- Cursor is written **after** all work items for the batch — never before
- Cursor is loaded once before the loop and held in memory — DB is not re-queried on every iteration
- `upsert_work_item` uses `INSERT OR IGNORE` on `idempotency_key` — replay-safe by design
- `delivery_attempts` is incremented by `update_outbox_delivery` via SQL (`delivery_attempts + 1`), not Python read-modify-write — safe if multiple drainer instances ever exist
- `delivery_status = 'failed'` is set explicitly by the drainer (on `REJECT` or exhausted retries) — operators can distinguish "queued" from "permanently stuck" without counting attempts
- Outbox rows with no `dest_pubkey` are skipped and left in place — they are not the drainer's responsibility and must not be deleted
- Tasks are spawned after `unlock_all()` returns — first poll iteration is expected to find a live session
- Watcher and drainer are fully independent — drainer does not know the watcher exists; any Phase 3/4 component may write to `outbox` without knowing about the drainer

## Outbox Idempotency Key Format

Callers inserting into `outbox` must supply a stable `idempotency_key`. Recommended format:

```
deskbridge:<identity_id>:<operation_type>:<operation_id>
```

Examples:
- `deskbridge:acc-alice:task_update:run-abc123` — agent posting a task status update
- `deskbridge:acc-alice:approval_notify:approval-xyz` — approval notification DM

The key must be deterministic for the logical operation so that retries after a crash do not send duplicate messages. It must be unique across different operations for the same identity.
