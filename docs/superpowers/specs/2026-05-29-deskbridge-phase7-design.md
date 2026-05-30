# DeskBridge Phase 7: Kanban Task Coordination — Design Spec

**Goal:** Poll NostrDesk kanban boards for cards assigned to each identity, dispatch them as agent work items, and write card status back to the board as work progresses.

**Architecture:** One new component (`KanbanWatcher`) plus two new `update_board_card` calls in `WorkItemPoller`. No schema changes. No new MCP tools beyond what Phase 6 already uses.

**Tech stack:** Same as prior phases — aiosqlite, structlog, asyncio, McpClient, pytest-asyncio (`asyncio_mode = auto`).

---

## Background

DeskBridge currently accepts work via operator DMs and group @mentions. Kanban boards are a natural third intake path: an operator assigns a card to an identity's npub, DeskBridge picks it up, dispatches an agent, and writes the result back to the board. This closes the loop without requiring the operator to send a DM.

The relevant MCP tools:
- `list_assigned_board_cards` — returns cards assigned to the session identity on a given board channel
- `update_board_card` — writes column, title, or description changes back to a card

Board data is locally cached (not relay-fresh), so polling at 30-second intervals is appropriate.

---

## Config Changes

`ProjectConfig` gains three new fields:

```toml
[[projects]]
id = "proj-1"
name = "My Project"
repo_path = "/home/user/myproject"
identity = "alice"
escalation_dm_target = "npub1..."
boards = ["channel-id-abc", "channel-id-def"]
kanban_column_in_progress = "in_progress"
kanban_column_done = "done"
```

`boards` is a list of Nostr channel IDs for boards DeskBridge should poll. Defaults to `[]`. If empty, no `KanbanWatcher` is spawned for the project's identity.

`kanban_column_in_progress` and `kanban_column_done` are the column names written to the board when a work item is claimed and completed respectively. They default to `"in_progress"` and `"done"` but must be configurable because Nostr kanban boards use user-defined column names. Both are passed through to `WorkItemPoller` at startup.

---

## File Map

| File | Action | What changes |
|---|---|---|
| `deskbridge/config.py` | Modify | Add `boards`, `kanban_column_in_progress`, `kanban_column_done` to `ProjectConfig` |
| `deskbridge/db/store.py` | Modify | `upsert_work_item` returns `bool` — `True` if newly inserted, `False` if already existed |
| `deskbridge/dm/kanban_watcher.py` | Create | New interval-poll watcher for assigned board cards |
| `deskbridge/agent/poller.py` | Modify | Call `update_board_card` on claim and completion for kanban-sourced work items |
| `deskbridge/supervisor.py` | Modify | Spawn `KanbanWatcher` per identity with boards configured |
| `tests/test_kanban_watcher.py` | Create | Unit tests for `KanbanWatcher` |
| `tests/test_work_item_poller.py` | Modify | Add writeback tests for kanban-sourced work items |

No changes to `schema.py`. The existing `work_items` table columns — `source_type`, `source_id`, `idempotency_key`, `summary`, `payload_json` — cover everything needed.

---

## KanbanWatcher

One instance per identity with boards configured. Polls every 30 seconds.

### Constructor

```python
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
```

`account_id` is passed directly from the Supervisor (which already holds the bootstrapped account data) rather than being derived internally from `identity_label`. This removes the hidden coupling to the `f"acc-{label}"` naming convention.

### Poll loop

```
while not shutdown:
    session_id = await broker.get_session_id(identity_label)
    if session_id is None:
        sleep; continue

    for board_channel_id in boards:
        cards = await client.call_tool(
            "list_assigned_board_cards",
            {"session_id": session_id, "channel_id": board_channel_id, "limit": 50},
        )
        for card in cards:
            await _process_card(card)

    sleep(poll_interval_secs)
```

### `_process_card`

For each card returned:

1. Extract `card_id`, `title`, `description` (fallback to `""` if absent).
2. Build `idempotency_key = f"kanban-{card_id}"`.
3. Call `store.upsert_work_item(id=str(uuid.uuid4()), source_type="kanban", source_id=card_id, identity_id=self._account_id, idempotency_key=..., summary=title, payload_json=description)`.
4. If the return value is `True` (newly inserted):
   - Log `kanban_watcher_new_card` at INFO.
   - If `operator_npub` is set, call `store.insert_outbox_item(...)` — a local DB write, not a network call. The `OutboxDrainer` delivers the message asynchronously. Processing 50 cards in a poll cycle is 50 DB inserts, not 50 network requests. The DM text is `"New task assigned: {title}\n\nCard ID: {card_id}"` with idempotency key `f"kanban-notify-{card_id}"`.

Deduplication is entirely DB-side via the `UNIQUE` constraint on `idempotency_key`. A card already in the DB causes `upsert_work_item` to return `False` — no log, no DM.

### `store.upsert_work_item` return value

The existing method uses `INSERT OR IGNORE`. It is extended to return `bool`: `True` if the row was inserted (rowcount == 1), `False` if it already existed (rowcount == 0). All existing callers that ignore the return value are unaffected.

### Error handling

| Failure | Behaviour |
|---|---|
| No session | Log DEBUG, sleep, retry |
| `McpToolError` on poll | Log WARNING, sleep, continue outer loop |
| Unexpected exception | Log ERROR with traceback, sleep, continue |
| Card missing `id` field | Log WARNING, skip card |
| Card missing `title` | Use `"(untitled)"` as summary |
| Empty card list | Normal — no work to do |

### Sleep

Uses `asyncio.wait_for` on the shutdown event, same pattern as `ApprovalRequestWatcher`:

```python
async def _sleep(self) -> None:
    try:
        await asyncio.wait_for(
            self._shutdown_event.wait(), timeout=self._poll_interval_secs
        )
    except asyncio.TimeoutError:
        pass
```

---

## WorkItemPoller Changes

`WorkItemPoller` already claims work items and runs agents. Two new `update_board_card` calls are added based on `source_type`.

### On claim (dispatch start)

After `store.claim_work_item(work_item_id)` succeeds:

```python
if work_item["source_type"] == "kanban":
    await self._sync_card_column(
        work_item["source_id"],
        column=self._kanban_column_in_progress,
        idempotency_key=f"deskbridge-{work_item_id}-in-progress",
    )
```

### On completion

After `store.complete_work_item(work_item_id, status)`:

```python
if work_item["source_type"] == "kanban":
    await self._sync_card_column(
        work_item["source_id"],
        column=self._kanban_column_done,
        idempotency_key=f"deskbridge-{work_item_id}-done",
    )
```

`_kanban_column_in_progress` and `_kanban_column_done` are set from the project config at `WorkItemPoller` construction time.

### `_sync_card_column`

```python
async def _sync_card_column(self, card_id: str, column: str, idempotency_key: str) -> None:
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

A failed writeback logs a warning but never blocks dispatch or completion. A card left in the wrong column is a cosmetic issue; the agent work must proceed.

---

## Supervisor Changes

In the per-identity task-spawning block, after spawning `ApprovalRequestWatcher`:

```python
project = await store.get_project_for_identity(account_id)
if project and project["boards_json"]:
    boards = json.loads(project["boards_json"])
    if boards:
        tasks.append(asyncio.create_task(
            KanbanWatcher(
                account_id=account_id,
                identity_label=identity.label,
                store=store,
                client=client,
                broker=broker,
                shutdown_event=shutdown_event,
                boards=boards,
                operator_npub=identity.operator_npub,
            ).run(),
            name=f"kanban_watcher_{identity.label}",
        ))
```

---

## Data Flow

```
Operator assigns card on NostrDesk board
  ↓
KanbanWatcher polls list_assigned_board_cards (30s interval)
  ↓
New card → upsert_work_item (source_type="kanban", source_id=card_id)
  ↓
Operator DM sent: "New task assigned: {title}"
  ↓
WorkItemPoller claims work item
  ↓
update_board_card(column="in_progress")
  ↓
Agent subprocess runs
  ↓
WorkItemPoller completes work item
  ↓
update_board_card(column="done")
```

---

## Column Values

Configurable per project via `kanban_column_in_progress` and `kanban_column_done` in `ProjectConfig`. Defaults:

| Event | Config field | Default value |
|---|---|---|
| Work item claimed (dispatch start) | `kanban_column_in_progress` | `"in_progress"` |
| Work item completed | `kanban_column_done` | `"done"` |

---

## Testing

### `tests/test_kanban_watcher.py` (new)

- New card → `work_items` row inserted with `source_type="kanban"`, `source_id=card_id`; outbox DM sent with title and card ID.
- Second poll returns same card → no new `work_items` row; no new outbox DM (idempotent).
- Multiple boards configured → cards from all boards processed in one poll cycle.
- Card missing `id` field → skipped; log warning; other cards in batch processed.
- Card missing `title` → `"(untitled)"` used as summary.
- No session → poll skipped; `list_assigned_board_cards` not called.
- `McpToolError` on poll → logged as WARNING; watcher sleeps and continues.
- `operator_npub=None` → no outbox DM inserted for new cards.
- Shutdown event set mid-poll → watcher exits cleanly.

### `tests/test_work_item_poller.py` (extend)

- Kanban work item claimed → `update_board_card` called with `column="in_progress"` and correct `card_id`.
- Kanban work item completed → `update_board_card` called with `column="done"`.
- DM-sourced work item claimed → `update_board_card` not called.
- `update_board_card` raises exception on claim → warning logged; dispatch proceeds; work item is not left unclaimed.
- No session when syncing → warning logged; dispatch proceeds.

---

## Non-Goals

- Watching boards for any card in a specific column (column-based trigger).
- Creating or deleting board cards from DeskBridge.
- Syncing agent run output back to card description.
- Cursor-based incremental polling (dedup is DB-side via `idempotency_key`).
- Guaranteed eventual consistency of kanban column state. The `update_board_card` writeback on completion is fire-and-forget. If the MCP server or Nostr relay is unavailable precisely when a work item completes, the board card will remain in its previous column permanently — `WorkItemPoller` fires the writeback once and does not retry. This is an accepted tradeoff; the local DB remains authoritative.
