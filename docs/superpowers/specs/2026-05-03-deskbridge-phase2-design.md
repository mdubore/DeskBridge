# DeskBridge Phase 2: DM Automation — Design Spec

**Goal:** Add inbound DM watching (per-identity, cursor-persisted), outbound DM delivery (outbox drain loop), and the Store methods both need — so the supervisor can receive operator commands as `work_items` rows and send autonomous notifications via `outbox`.

**Architecture:** Three new components run as asyncio tasks inside the supervisor alongside the existing heartbeat loop. `DmWatcher` (one per identity) long-polls `wait_for_new_dms` and writes inbound DMs to `work_items`. `OutboxDrainer` (one shared instance) polls the `outbox` table every 5 seconds and calls `send_dm`. Both components depend on new `Store` methods; neither knows about each other.

**Tech stack:** Same as Phase 1 — aiosqlite, structlog, mcp ClientSession via `McpClient.call_tool`, pytest-asyncio (`asyncio_mode = auto`).

---

## Scope

**In scope:**
- `DmWatcher`: per-identity asyncio task, `wait_for_new_dms` long-poll, cursor persistence, `work_items` row creation
- `OutboxDrainer`: shared asyncio task, 5-second drain loop, `send_dm` with idempotency key, delivery status tracking, max 3 attempts before marking failed
- `Store` additions: `upsert_work_item`, `get_pending_outbox_items`, `update_outbox_delivery`
- Supervisor wiring: spawn watcher tasks and drainer task, cancel cleanly on shutdown

**Out of scope:**
- Parsing or routing inbound DM content (Phase 4)
- Group message watching (Phase 4)
- Agent acting on `work_items` rows (Phase 3)
- `dest_group_id` outbox rows — outbox drainer handles `dest_pubkey` only; group sends come in Phase 4

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
SQL: `INSERT OR IGNORE INTO work_items (...) VALUES (...)` keyed on `idempotency_key UNIQUE`.

**`get_pending_outbox_items`** — returns rows with `delivery_status = 'pending'` and `delivery_attempts < max_attempts`:
```python
async def get_pending_outbox_items(self, max_attempts: int = 3) -> list[aiosqlite.Row]:
```

**`update_outbox_delivery`** — sets `delivery_status`, increments `delivery_attempts`, sets `delivered_at` when status is `'delivered'`:
```python
async def update_outbox_delivery(
    self,
    id: str,
    delivery_status: str,
    delivery_result_json: str,
) -> None:
```

---

### DmWatcher (`deskbridge/dm/watcher.py`)

One instance per identity. Runs as an asyncio task spawned by the supervisor.

```python
class DmWatcher:
    def __init__(
        self,
        identity_label: str,
        identity_npub: str,
        store: Store,
        client: McpClient,
        broker: SessionBroker,
        shutdown_event: asyncio.Event,
        poll_timeout_secs: int = 30,
    ) -> None:
```

**Loop behavior:**
1. Load cursor from `store.get_cursor(cursor_type="dm_watcher", identity_id=account_id)`
2. Get `session_id` from `broker.get_session_id(identity_label)` — if `None`, sleep 5s and retry (session not yet unlocked or degraded)
3. Call `client.call_tool("wait_for_new_dms", {"session_id": ..., "after_message_id": ..., "after_created_at": ..., "timeout_seconds": poll_timeout_secs})`
   - **Contract assumption:** response is `{"messages": [...], "last_message_id": str|null, "last_created_at": str|null}`. Each message has at minimum `"id"` and `"content"` fields. Verify against actual nostrdesk-mcp response shape when writing tests.
4. For each message in result `messages` list:
   - Call `store.upsert_work_item(...)` with `source_type="dm"`, `source_id=message["id"]`, `idempotency_key=message["id"]`, `summary=message["content"][:200]`, `payload_json=json.dumps(message)`
5. If result contains cursor fields (`last_message_id`, `last_created_at`), call `store.upsert_cursor(...)` to persist them
6. On `McpToolError` with routing `REAUTH`: log warning, sleep 5s (session broker's heartbeat will re-unlock)
7. On `McpToolError` with other routing: log error, sleep 5s
8. Loop until `shutdown_event.is_set()`

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
    ) -> None:
```

**Loop behavior:**
1. Fetch `rows = await store.get_pending_outbox_items(max_attempts=3)`
2. For each row:
   - Look up `session_id` from `broker.get_session_id(identity_label_for(row["identity_id"]))` — if `None`, skip row (will retry next drain)
   - Call `client.call_tool("send_dm", {"session_id": ..., "recipient_pubkey": row["dest_pubkey"], "content": row["message_text"], "idempotency_key": row["idempotency_key"]})`
   - On success: `store.update_outbox_delivery(id, "delivered", result_json)`
   - On `McpToolError`: `store.update_outbox_delivery(id, "pending", error_json)` (increments attempts; becomes `"failed"` once attempts reach max — enforced by the query filter)
3. Sleep `drain_interval_secs` or until `shutdown_event.is_set()` (use `asyncio.wait_for` on `shutdown_event.wait()` like the heartbeat loop)

**Identity label lookup:** `OutboxDrainer` holds a dict `{account_id: label}` built from `identities` at init time, e.g. `{"acc-alice": "alice"}`. Used to resolve `row["identity_id"]` → label for `broker.get_session_id`.

**Delivery status progression:** `pending` → `delivered` (success) or stays `pending` with incremented `delivery_attempts` (transient failure). Once `delivery_attempts >= 3`, `get_pending_outbox_items` no longer returns the row (it uses `delivery_attempts < max_attempts` filter). A separate operator view can show permanently-stuck rows.

**Note on `dest_group_id` rows:** rows with `dest_group_id` set and no `dest_pubkey` are skipped silently (logged at debug level). Group sends are Phase 4.

---

### Supervisor changes (`deskbridge/supervisor.py`)

`run()` gains a task-spawning block after creating `SessionBroker`:

```python
watcher_tasks = [
    asyncio.create_task(
        DmWatcher(
            identity_label=identity.label,
            identity_npub=identity.npub,
            store=store,
            client=client,
            broker=broker,
            shutdown_event=self._shutdown_event,
        ).run()
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
    ).run()
)

try:
    await broker.unlock_all()
    # ... existing heartbeat loop ...
finally:
    for task in watcher_tasks + [drainer_task]:
        task.cancel()
    await asyncio.gather(*watcher_tasks, drainer_task, return_exceptions=True)
    # existing signal handler cleanup
```

---

## Data Flow

```
nostrdesk-mcp
    │  wait_for_new_dms (per identity)
    ▼
DmWatcher
    │  upsert_work_item (source_type=dm)
    ▼
work_items table ──── (Phase 3 reads this)

outbox table ──── (Phase 3/4 writes here)
    │  get_pending_outbox_items
    ▼
OutboxDrainer
    │  send_dm (with idempotency_key)
    ▼
nostrdesk-mcp
```

---

## Error Handling

| Scenario | Behavior |
|---|---|
| `wait_for_new_dms` timeout (no new DMs) | Normal — result has empty `messages`, loop continues |
| `McpToolError` REAUTH on DM poll | Log warning, sleep 5s, retry — heartbeat will re-unlock |
| `McpToolError` other on DM poll | Log error, sleep 5s, retry |
| `send_dm` transient failure | Increment `delivery_attempts`, retry on next drain |
| `send_dm` after 3 failures | Row excluded from future drain queries, logged as stuck |
| Session not yet unlocked | Skip (watcher sleeps 5s, drainer skips the row) |
| Crash between writing work items and saving cursor | Idempotency key prevents duplicate `work_items` rows on replay |

---

## Testing Strategy

**Store tests** — real aiosqlite DB via existing `db_conn` fixture:
- `upsert_work_item` inserts; second call with same `idempotency_key` is a no-op
- `get_pending_outbox_items` returns pending rows, excludes rows at max attempts
- `update_outbox_delivery` sets status, increments attempts, sets `delivered_at` on success

**DmWatcher tests** — mock `McpClient.call_tool`, real `Store`, real aiosqlite:
- Returns new DMs → `work_items` row created, cursor saved
- Empty response → no rows inserted, cursor saved
- Duplicate DM id (idempotency) → second call is a no-op in `work_items`
- `McpToolError` REAUTH → logs warning, does not crash, loop continues
- No session → watcher skips poll (sleeps), does not crash

**OutboxDrainer tests** — mock `McpClient.call_tool`, real `Store`:
- Pending row + successful `send_dm` → status becomes `delivered`
- Pending row + `McpToolError` → `delivery_attempts` increments, status stays `pending`
- Row at max attempts → not fetched, not attempted
- Row with `dest_group_id` only → skipped silently
- No session for identity → row skipped, no MCP call

**Supervisor tests** — extend existing `test_supervisor.py`:
- Watcher and drainer tasks are spawned
- Tasks are cancelled and awaited on shutdown (no hanging)

---

## Invariants and Constraints

- Cursor is written **after** all work items for the batch — never before
- `upsert_work_item` uses `INSERT OR IGNORE` on `idempotency_key` — replay-safe
- `send_dm` always uses `idempotency_key = row["idempotency_key"]` from the outbox row — callers are responsible for generating a stable key when inserting
- Watcher and drainer are independent — drainer does not know the watcher exists
- Outbox rows with no `dest_pubkey` (group-only) are skipped, not deleted
- `delivery_attempts` is incremented by `update_outbox_delivery` via SQL (`delivery_attempts + 1`), not by Python read-modify-write, to avoid races if multiple drainer instances ever exist
