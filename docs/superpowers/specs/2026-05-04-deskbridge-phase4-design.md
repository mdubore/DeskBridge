# DeskBridge Phase 4: DM Intent Parsing & Group Message Routing — Design Spec

## Goal

Parse incoming DMs and group messages for operator intent, route each message to the correct handler (task creation, status query, cancel, approve, reject), and restrict command processing to a single configured operator npub per identity.

## Architecture

Phase 4 adds a shared `IntentParser` module and a new `GroupWatcher` component, modifies `DmWatcher` to route by intent instead of blindly creating work items, and makes a surgical addition to `WorkItemPoller` to propagate cancel signals from the store to running agent tasks.

All routing is keyword-based — DeskBridge itself makes no LLM calls. When a message cannot be classified as a known command it defaults to `TASK`, and the agent's own LLM handles the natural language instruction.

### Message flow (DM)

```
inbound DM
  → check sender == operator_npub (drop if mismatch)
  → IntentParser.parse(content)
  → _handle_task / _handle_status / _handle_cancel / _handle_approve / _handle_reject
  → store write + optional outbox reply
```

### Message flow (group)

```
inbound group message
  → check sender == operator_npub (drop if mismatch)
  → check message contains @mention of identity npub (drop if absent)
  → strip mention prefix
  → IntentParser.parse(remaining text)
  → same _handle_* methods as DM path
  → outbox reply with dest_group_id
```

---

## File Map

| File | Action | Responsibility |
|---|---|---|
| `deskbridge/dm/intent.py` | Create | `Intent` enum + `parse()` — pure keyword classifier |
| `deskbridge/dm/watcher.py` | Modify | Add operator_npub check; route by intent instead of always creating work items |
| `deskbridge/dm/group_watcher.py` | Create | `GroupWatcher` — polls group MCP tool, filters @mention, routes via IntentParser |
| `deskbridge/agent/poller.py` | Modify | Track active work item ID; check `cancel_requested` status on each poll cycle |
| `deskbridge/db/store.py` | Modify | Add 7 new Store methods |
| `deskbridge/config.py` | Modify | Add `operator_npub: str \| None = None` to `IdentityConfig` |
| `deskbridge/supervisor.py` | Modify | Spawn `GroupWatcher` tasks for identities with configured groups |
| `tests/test_intent.py` | Create | Unit tests for `IntentParser.parse()` |
| `tests/test_dm_watcher.py` | Create | Intent routing tests + operator_npub filter tests |
| `tests/test_group_watcher.py` | Create | @mention filter tests + intent routing tests |
| `tests/test_work_item_poller.py` | Modify | Add cancel-check test |
| `tests/test_store.py` | Modify | Append tests for 7 new Store methods |

---

## Component Design

### IntentParser (`deskbridge/dm/intent.py`)

Pure module — no async, no I/O, no dependencies beyond stdlib.

```python
import enum
import re

class Intent(enum.Enum):
    TASK = "task"
    STATUS = "status"
    CANCEL = "cancel"
    APPROVE = "approve"
    REJECT = "reject"

_RULES: list[tuple[Intent, re.Pattern]] = [
    (Intent.STATUS,  re.compile(r"\b(status|update|progress|what.?s happening)\b", re.I)),
    (Intent.CANCEL,  re.compile(r"\b(cancel|abort|halt)\b", re.I)),
    (Intent.APPROVE, re.compile(r"\b(approve|yes|go ahead|confirmed|proceed)\b", re.I)),
    (Intent.REJECT,  re.compile(r"\b(reject|no|deny|don.?t)\b", re.I)),
]

def parse(text: str) -> Intent:
    for intent, pattern in _RULES:
        if pattern.search(text):
            return intent
    return Intent.TASK
```

Rules are checked in order; first match wins. `TASK` is the default.

`stop` is intentionally excluded from both CANCEL and REJECT keywords — it is ambiguous and defaulting to TASK is safer than a false cancel or reject.

### Config change (`deskbridge/config.py`)

```python
class IdentityConfig(BaseModel):
    label: str
    npub: str
    passphrase_ref: str
    operator_npub: str | None = None
```

If `operator_npub` is `None`, all inbound DMs and group messages for that identity are silently dropped at the authorization check. No config validation error — opting out of command routing is valid.

Example TOML:

```toml
[[identities]]
label = "alice"
npub = "npub1alice..."
passphrase_ref = "env:ALICE"
operator_npub = "npub1operator..."
```

### Modified DmWatcher (`deskbridge/dm/watcher.py`)

The message loop gains an authorization check before intent parsing:

```python
for msg in messages:
    if self._operator_npub is None or msg["sender_pubkey"] != self._operator_npub:
        log.debug("dm_watcher_unauthorized", identity=self._identity_label,
                  sender=msg.get("sender_pubkey"))
        continue

    intent = parse(msg["content"])
    await self._dispatch(intent, msg)
```

`_dispatch` delegates to one of five private methods:

```python
async def _handle_task(self, msg: dict) -> None:
    await self._store.upsert_work_item(
        id=str(uuid.uuid4()),
        source_type="dm",
        source_id=msg["id"],
        identity_id=self._account_id,
        summary=msg["content"][:200],
        payload_json=json.dumps(msg),
        idempotency_key=msg["id"],
    )

async def _handle_status(self, msg: dict) -> None:
    row = await self._store.get_latest_work_item(self._account_id)
    if row is None:
        reply = "No tasks found."
    else:
        reply = f"Task [{row['summary']}] is {row['status']}."
    await self._store.insert_outbox_item(
        id=str(uuid.uuid4()),
        identity_id=self._account_id,
        dest_pubkey=msg["sender_pubkey"],
        message_text=reply,
        idempotency_key=f"status-reply-{msg['id']}",
    )

async def _handle_cancel(self, msg: dict) -> None:
    row = await self._store.get_latest_dispatched_work_item(self._account_id)
    if row is None:
        reply = "No active task to cancel."
    else:
        await self._store.mark_work_item_cancel_requested(row["id"])
        reply = f"Cancel requested for task [{row['summary']}]."
    await self._store.insert_outbox_item(
        id=str(uuid.uuid4()),
        identity_id=self._account_id,
        dest_pubkey=msg["sender_pubkey"],
        message_text=reply,
        idempotency_key=f"cancel-reply-{msg['id']}",
    )

async def _handle_approve(self, msg: dict) -> None:
    row = await self._store.get_pending_approval(self._account_id)
    if row is None:
        reply = "No pending approval to approve."
    else:
        await self._store.resolve_approval(row["id"], "approved")
        reply = "Approved."
    await self._store.insert_outbox_item(
        id=str(uuid.uuid4()),
        identity_id=self._account_id,
        dest_pubkey=msg["sender_pubkey"],
        message_text=reply,
        idempotency_key=f"approve-reply-{msg['id']}",
    )

async def _handle_reject(self, msg: dict) -> None:
    row = await self._store.get_pending_approval(self._account_id)
    if row is None:
        reply = "No pending approval to reject."
    else:
        await self._store.resolve_approval(row["id"], "rejected")
        reply = "Rejected."
    await self._store.insert_outbox_item(
        id=str(uuid.uuid4()),
        identity_id=self._account_id,
        dest_pubkey=msg["sender_pubkey"],
        message_text=reply,
        idempotency_key=f"reject-reply-{msg['id']}",
    )
```

`_operator_npub` is set in `__init__` from `identity.operator_npub` (passed by supervisor via `IdentityConfig`).

Each `_handle_*` method catches its own exceptions so one bad handler does not block the others.

### GroupWatcher (`deskbridge/dm/group_watcher.py`)

Same structure as `DmWatcher`. Constructor:

```python
class GroupWatcher:
    def __init__(
        self,
        identity_label: str,
        identity_npub: str,
        operator_npub: str | None,
        group_ids: list[str],
        store: Store,
        client: McpClient,
        broker: SessionBroker,
        shutdown_event: asyncio.Event,
        poll_timeout_secs: int = 30,
    ) -> None:
```

Poll loop calls MCP tool `wait_for_new_group_messages` (exact tool name confirmed against NostrDesk MCP docs during implementation) with `group_ids` and `timeout_seconds`.

For each message:

1. Check `msg["sender_pubkey"] == operator_npub` — drop if mismatch
2. Check `identity_npub` appears in `msg["content"]` — drop if absent
3. Strip the mention (remove `nostr:<identity_npub>` or `@<identity_npub>` prefix/substring)
4. Call `parse(stripped_text)` → route to `_handle_*`

Replies use `store.insert_outbox_item(dest_group_id=msg["group_id"], ...)`.

Cursor tracking uses `cursor_type="group_watcher"` with `identity_id` as the key. If multiple groups are watched by one identity, a single cursor tracks the latest message across all of them (the MCP tool returns a unified `last_message_id`). If the tool returns per-group cursors, the cursor key becomes `f"group_watcher_{group_id}"`.

The `_handle_*` methods are identical to `DmWatcher`'s except that `insert_outbox_item` uses `dest_group_id` instead of `dest_pubkey`.

### WorkItemPoller cancel-checking (`deskbridge/agent/poller.py`)

New instance variable:

```python
self._active_work_item_id: str | None = None
```

Set alongside `self._active_run_task` when a runner is spawned:

```python
self._active_run_task = asyncio.create_task(runner.run(), name=f"agent_run_{row['id']}")
self._active_work_item_id = row["id"]
```

Cleared in `_poll_once` after the task finishes and in the cancel-check path:

```python
if self._active_run_task is not None and self._active_run_task.done():
    self._active_run_task = None
    self._active_work_item_id = None
```

Cancel-check at the top of `_poll_once` (before scanning for new items):

```python
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
```

The confirmation DM is queued by `DmWatcher._handle_cancel` immediately — the poller does not send a DM, it only updates the work item status.

### Store additions (`deskbridge/db/store.py`)

Seven new methods appended before `bootstrap_accounts_from_config`:

```python
async def get_work_item(self, id: str) -> aiosqlite.Row | None:
    async with self._conn.execute(
        "SELECT * FROM work_items WHERE id = ?", (id,)
    ) as cur:
        return await cur.fetchone()

async def get_latest_work_item(self, identity_id: str) -> aiosqlite.Row | None:
    async with self._conn.execute(
        "SELECT * FROM work_items WHERE identity_id = ? ORDER BY created_at DESC LIMIT 1",
        (identity_id,),
    ) as cur:
        return await cur.fetchone()

async def get_latest_dispatched_work_item(self, identity_id: str) -> aiosqlite.Row | None:
    async with self._conn.execute(
        """
        SELECT * FROM work_items
        WHERE identity_id = ? AND status IN ('dispatched', 'cancel_requested')
        ORDER BY created_at DESC LIMIT 1
        """,
        (identity_id,),
    ) as cur:
        return await cur.fetchone()

async def mark_work_item_cancel_requested(self, id: str) -> None:
    await self._conn.execute(
        "UPDATE work_items SET status = 'cancel_requested', "
        "updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now') WHERE id = ?",
        (id,),
    )
    await self._conn.commit()

async def get_pending_approval(self, identity_id: str) -> aiosqlite.Row | None:
    async with self._conn.execute(
        """
        SELECT a.* FROM approvals a
        JOIN work_items w ON a.work_item_id = w.id
        WHERE w.identity_id = ? AND a.status = 'pending'
        ORDER BY a.created_at DESC LIMIT 1
        """,
        (identity_id,),
    ) as cur:
        return await cur.fetchone()

async def resolve_approval(self, id: str, status: str) -> None:
    await self._conn.execute(
        "UPDATE approvals SET status = ?, resolved_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now') "
        "WHERE id = ?",
        (status, id),
    )
    await self._conn.commit()

async def get_project_groups(self, identity_id: str) -> list[str]:
    async with self._conn.execute(
        "SELECT groups_json FROM projects WHERE identity_id = ? LIMIT 1",
        (identity_id,),
    ) as cur:
        row = await cur.fetchone()
    if row is None:
        return []
    import json
    return json.loads(row["groups_json"])
```

### Supervisor wiring (`deskbridge/supervisor.py`)

```python
from deskbridge.dm.group_watcher import GroupWatcher

# before try block:
group_watcher_tasks: list[asyncio.Task] = []

# inside try, after watcher_tasks:
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

# in finally:
tasks_to_cancel = watcher_tasks + group_watcher_tasks + poller_tasks + (
    [drainer_task] if drainer_task is not None else []
)
```

---

## Error Handling

- **Unauthorized sender**: silent drop, `log.debug`
- **No @mention** (group): silent drop, `log.debug`
- **Command with no applicable record** (cancel with no active task, approve with no pending approval): log warning, queue a human-readable "nothing to cancel/approve" reply
- **Store error in handler**: `log.exception`, do not re-raise — allows the next message to be processed
- **MCP error in GroupWatcher**: same routing as `DmWatcher` (`REJECT` → stop loop, `REAUTH`/transient → back off 5 s and retry)

---

## Out of Scope

- Approval request *creation* from the agent side (Phase 5)
- Multi-operator support (one `operator_npub` per identity only)
- Per-project operator override
- Group message threading or quoting
- LLM-based intent classification (keyword fallback is sufficient; agents handle natural language task descriptions)
