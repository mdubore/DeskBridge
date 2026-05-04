# DeskBridge Phase 4: DM Intent Parsing & Group Message Routing — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Parse incoming DMs and group messages for operator intent, route each message to the correct handler (task creation, status query, cancel, approve, reject), and restrict command processing to a configured `operator_npub` per identity.

**Architecture:** A pure `IntentParser` module does keyword classification; `DmWatcher` gains an operator auth check and dispatches by intent; a new `GroupWatcher` mirrors `DmWatcher` for Nostr groups (filters to @mention only); `WorkItemPoller` gains cancel-signal propagation; and the supervisor wires `GroupWatcher` tasks alongside the existing watchers.

**Tech Stack:** Python asyncio, aiosqlite, structlog, pydantic — no new dependencies.

---

## Context for every task

- Run tests with: `uv run pytest --tb=short -q`
- All tests use `asyncio_mode = "auto"` — no `@pytest.mark.asyncio` decorator needed
- Fixtures `store` and `db_conn` come from `tests/conftest.py` (real SQLite DB, not mocked)
- `account_id` convention: `f"acc-{identity_label}"` (e.g., `"acc-alice"`)
- Existing `insert_outbox_item` signature (store.py:305): `(id, identity_id, dest_pubkey, message_text, idempotency_key)` — Task 2 updates this
- 118 tests currently passing — do not break them

---

## Task 1: IntentParser + `operator_npub` config field

**Files:**
- Create: `deskbridge/dm/intent.py`
- Modify: `deskbridge/config.py`
- Create: `tests/test_intent.py`

- [ ] **Step 1: Write failing tests for IntentParser**

Create `tests/test_intent.py`:

```python
from deskbridge.dm.intent import Intent, parse


def test_parse_status_keywords():
    assert parse("what's the status?") == Intent.STATUS
    assert parse("give me an update") == Intent.STATUS
    assert parse("progress report") == Intent.STATUS
    assert parse("what's happening") == Intent.STATUS


def test_parse_cancel_keywords():
    assert parse("please cancel that") == Intent.CANCEL
    assert parse("abort the run") == Intent.CANCEL
    assert parse("halt everything") == Intent.CANCEL


def test_parse_approve_keywords():
    assert parse("yes go ahead") == Intent.APPROVE
    assert parse("approve it") == Intent.APPROVE
    assert parse("confirmed, proceed") == Intent.APPROVE


def test_parse_reject_keywords():
    assert parse("no don't") == Intent.REJECT
    assert parse("reject that") == Intent.REJECT
    assert parse("deny the request") == Intent.REJECT


def test_parse_default_task():
    assert parse("fix the auth bug in service X") == Intent.TASK
    assert parse("add tests for the login flow") == Intent.TASK
    assert parse("hello") == Intent.TASK


def test_parse_status_takes_priority_over_later_rules():
    # "status update" matches STATUS rule first
    assert parse("status update please") == Intent.STATUS


def test_parse_stop_is_not_cancel_or_reject():
    # "stop" is intentionally excluded — ambiguous, safe to treat as TASK
    assert parse("stop that") == Intent.TASK


def test_parse_case_insensitive():
    assert parse("STATUS") == Intent.STATUS
    assert parse("CANCEL") == Intent.CANCEL
    assert parse("YES") == Intent.APPROVE
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
uv run pytest tests/test_intent.py -v
```

Expected: `ModuleNotFoundError: No module named 'deskbridge.dm.intent'`

- [ ] **Step 3: Create `deskbridge/dm/intent.py`**

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

- [ ] **Step 4: Run intent tests to verify they pass**

```bash
uv run pytest tests/test_intent.py -v
```

Expected: 8 tests PASS.

- [ ] **Step 5: Write failing test for `operator_npub` config field**

Append to `tests/test_config.py` (open it, find the end, add after existing tests):

```python
def test_identity_config_operator_npub_defaults_to_none():
    from deskbridge.config import IdentityConfig
    identity = IdentityConfig(label="alice", npub="npub1alice", passphrase_ref="env:X")
    assert identity.operator_npub is None


def test_identity_config_operator_npub_can_be_set():
    from deskbridge.config import IdentityConfig
    identity = IdentityConfig(
        label="alice", npub="npub1alice", passphrase_ref="env:X",
        operator_npub="npub1op"
    )
    assert identity.operator_npub == "npub1op"
```

- [ ] **Step 6: Run config tests to verify they fail**

```bash
uv run pytest tests/test_config.py -v -k "operator_npub"
```

Expected: FAIL — `IdentityConfig` has no field `operator_npub`.

- [ ] **Step 7: Add `operator_npub` to `IdentityConfig` in `deskbridge/config.py`**

Find `IdentityConfig` (around line 29) and add the field:

```python
class IdentityConfig(BaseModel):
    label: str
    npub: str
    passphrase_ref: str
    operator_npub: str | None = None
```

- [ ] **Step 8: Run all tests to verify nothing is broken**

```bash
uv run pytest --tb=short -q
```

Expected: All previously passing tests still pass + 10 new tests pass (8 intent + 2 config).

- [ ] **Step 9: Commit**

```bash
git add deskbridge/dm/intent.py deskbridge/config.py tests/test_intent.py tests/test_config.py
git commit -m "feat: add IntentParser and operator_npub identity config field"
```

---

## Task 2: Store additions (7 new methods + `insert_outbox_item` update)

**Files:**
- Modify: `deskbridge/db/store.py` (append 7 methods before `bootstrap_accounts_from_config`; update `insert_outbox_item` signature)
- Modify: `tests/test_store.py` (append tests for new methods; fix 2 positional callers of `insert_outbox_item`)

**Background:** `insert_outbox_item` currently requires `dest_pubkey: str`. GroupWatcher replies go to groups, not pubkeys, so it needs an optional `dest_group_id`. We extend the signature to `dest_pubkey: str | None` (required positional, now nullable) plus `dest_group_id: str | None = None` (new optional). The SQL inserts both columns. Two existing positional callers in `test_store.py` (lines 469-470) use `"npub1op"` as the 3rd arg — they still work since the positional order is unchanged.

- [ ] **Step 1: Write failing tests for the 7 new Store methods**

Append to `tests/test_store.py` (after the last test in the file):

```python
# ===========================================================================
# Phase 4 Store methods
# ===========================================================================

async def _seed_work_item_phase4(conn, *, id, identity_id, status="pending"):
    await conn.execute(
        """
        INSERT INTO work_items (id, source_type, source_id, identity_id, status, idempotency_key)
        VALUES (?, 'dm', ?, ?, ?, ?)
        """,
        (id, id, identity_id, status, f"idem-{id}"),
    )
    await conn.commit()


async def _seed_approval(conn, *, id, work_item_id, status="pending"):
    await conn.execute(
        """
        INSERT INTO approvals (id, work_item_id, action_description, status)
        VALUES (?, ?, 'do something', ?)
        """,
        (id, work_item_id, status),
    )
    await conn.commit()


# get_work_item
# ---------------------------------------------------------------------------

async def test_get_work_item_returns_row(store: Store, db_conn):
    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    await _seed_work_item_phase4(db_conn, id="wi-1", identity_id="acc-alice")
    row = await store.get_work_item("wi-1")
    assert row is not None
    assert row["id"] == "wi-1"


async def test_get_work_item_returns_none_for_missing(store: Store):
    result = await store.get_work_item("nonexistent")
    assert result is None


# get_latest_work_item
# ---------------------------------------------------------------------------

async def test_get_latest_work_item_returns_most_recent(store: Store, db_conn):
    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    await _seed_work_item_phase4(db_conn, id="wi-1", identity_id="acc-alice")
    await _seed_work_item_phase4(db_conn, id="wi-2", identity_id="acc-alice")
    row = await store.get_latest_work_item("acc-alice")
    assert row is not None
    assert row["id"] == "wi-2"


async def test_get_latest_work_item_returns_none_when_empty(store: Store):
    result = await store.get_latest_work_item("acc-nobody")
    assert result is None


# get_latest_dispatched_work_item
# ---------------------------------------------------------------------------

async def test_get_latest_dispatched_work_item_finds_dispatched(store: Store, db_conn):
    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    await _seed_work_item_phase4(db_conn, id="wi-1", identity_id="acc-alice", status="pending")
    await _seed_work_item_phase4(db_conn, id="wi-2", identity_id="acc-alice", status="dispatched")
    row = await store.get_latest_dispatched_work_item("acc-alice")
    assert row is not None
    assert row["id"] == "wi-2"


async def test_get_latest_dispatched_work_item_finds_cancel_requested(store: Store, db_conn):
    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    await _seed_work_item_phase4(db_conn, id="wi-1", identity_id="acc-alice", status="cancel_requested")
    row = await store.get_latest_dispatched_work_item("acc-alice")
    assert row is not None
    assert row["id"] == "wi-1"


async def test_get_latest_dispatched_work_item_ignores_pending(store: Store, db_conn):
    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    await _seed_work_item_phase4(db_conn, id="wi-1", identity_id="acc-alice", status="pending")
    result = await store.get_latest_dispatched_work_item("acc-alice")
    assert result is None


# mark_work_item_cancel_requested
# ---------------------------------------------------------------------------

async def test_mark_work_item_cancel_requested_updates_status(store: Store, db_conn):
    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    await _seed_work_item_phase4(db_conn, id="wi-1", identity_id="acc-alice", status="dispatched")
    await store.mark_work_item_cancel_requested("wi-1")
    row = await store.get_work_item("wi-1")
    assert row["status"] == "cancel_requested"


# get_pending_approval
# ---------------------------------------------------------------------------

async def test_get_pending_approval_returns_most_recent(store: Store, db_conn):
    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    await _seed_work_item_phase4(db_conn, id="wi-1", identity_id="acc-alice")
    await _seed_approval(db_conn, id="appr-1", work_item_id="wi-1")
    row = await store.get_pending_approval("acc-alice")
    assert row is not None
    assert row["id"] == "appr-1"


async def test_get_pending_approval_ignores_resolved(store: Store, db_conn):
    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    await _seed_work_item_phase4(db_conn, id="wi-1", identity_id="acc-alice")
    await _seed_approval(db_conn, id="appr-1", work_item_id="wi-1", status="approved")
    result = await store.get_pending_approval("acc-alice")
    assert result is None


async def test_get_pending_approval_returns_none_when_empty(store: Store):
    result = await store.get_pending_approval("acc-nobody")
    assert result is None


# resolve_approval
# ---------------------------------------------------------------------------

async def test_resolve_approval_approved(store: Store, db_conn):
    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    await _seed_work_item_phase4(db_conn, id="wi-1", identity_id="acc-alice")
    await _seed_approval(db_conn, id="appr-1", work_item_id="wi-1")
    await store.resolve_approval("appr-1", "approved")
    async with db_conn.execute("SELECT status, resolved_at FROM approvals WHERE id='appr-1'") as cur:
        row = await cur.fetchone()
    assert row["status"] == "approved"
    assert row["resolved_at"] is not None


async def test_resolve_approval_rejected(store: Store, db_conn):
    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    await _seed_work_item_phase4(db_conn, id="wi-1", identity_id="acc-alice")
    await _seed_approval(db_conn, id="appr-1", work_item_id="wi-1")
    await store.resolve_approval("appr-1", "rejected")
    async with db_conn.execute("SELECT status FROM approvals WHERE id='appr-1'") as cur:
        row = await cur.fetchone()
    assert row["status"] == "rejected"


# get_project_groups
# ---------------------------------------------------------------------------

async def test_get_project_groups_returns_list(store: Store, db_conn):
    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    await db_conn.execute(
        "INSERT INTO projects (id, name, repo_path, identity_id, groups_json) "
        "VALUES ('proj-1', 'P', '/repo', 'acc-alice', '[\"grp-1\", \"grp-2\"]')"
    )
    await db_conn.commit()
    groups = await store.get_project_groups("acc-alice")
    assert groups == ["grp-1", "grp-2"]


async def test_get_project_groups_returns_empty_when_no_project(store: Store):
    result = await store.get_project_groups("acc-nobody")
    assert result == []


async def test_get_project_groups_returns_empty_list_for_empty_json(store: Store, db_conn):
    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    await db_conn.execute(
        "INSERT INTO projects (id, name, repo_path, identity_id, groups_json) "
        "VALUES ('proj-1', 'P', '/repo', 'acc-alice', '[]')"
    )
    await db_conn.commit()
    groups = await store.get_project_groups("acc-alice")
    assert groups == []


# insert_outbox_item with dest_group_id
# ---------------------------------------------------------------------------

async def test_insert_outbox_item_with_group_id(store: Store, db_conn):
    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    await store.insert_outbox_item(
        "ob-grp-1", "acc-alice", None, "done", "idem-grp-1",
        dest_group_id="grp-1",
    )
    async with db_conn.execute("SELECT dest_pubkey, dest_group_id FROM outbox WHERE id='ob-grp-1'") as cur:
        row = await cur.fetchone()
    assert row["dest_pubkey"] is None
    assert row["dest_group_id"] == "grp-1"
```

- [ ] **Step 2: Run failing tests**

```bash
uv run pytest tests/test_store.py -v -k "phase4 or get_work_item or get_latest or dispatched or cancel_requested or pending_approval or resolve_approval or get_project_groups or group_id"
```

Expected: All new tests FAIL — methods not yet defined.

- [ ] **Step 3: Add 7 new methods and update `insert_outbox_item` in `deskbridge/db/store.py`**

First, add `import json` at the top of the file (after the existing imports if not already present):

```python
import json
```

Next, find `insert_outbox_item` (line ~305) and update its signature and SQL. Replace the existing method:

```python
    async def insert_outbox_item(
        self,
        id: str,
        identity_id: str,
        dest_pubkey: str | None,
        message_text: str,
        idempotency_key: str,
        dest_group_id: str | None = None,
    ) -> None:
        async with self._conn.execute(
            """
            INSERT OR IGNORE INTO outbox
                (id, identity_id, dest_pubkey, dest_group_id, message_text, idempotency_key)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (id, identity_id, dest_pubkey, dest_group_id, message_text, idempotency_key),
        ):
            pass
        await self._conn.commit()
```

Then append the 7 new methods immediately before `bootstrap_accounts_from_config` (after the closing of `insert_outbox_item`):

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
        async with self._conn.execute(
            "UPDATE work_items SET status = 'cancel_requested', "
            "updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now') WHERE id = ?",
            (id,),
        ):
            pass
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
        async with self._conn.execute(
            "UPDATE approvals SET status = ?, "
            "resolved_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now') WHERE id = ?",
            (status, id),
        ):
            pass
        await self._conn.commit()

    async def get_project_groups(self, identity_id: str) -> list[str]:
        async with self._conn.execute(
            "SELECT groups_json FROM projects WHERE identity_id = ? LIMIT 1",
            (identity_id,),
        ) as cur:
            row = await cur.fetchone()
        if row is None:
            return []
        return json.loads(row["groups_json"])
```

- [ ] **Step 4: Run all tests**

```bash
uv run pytest --tb=short -q
```

Expected: All previously passing tests pass + all new store tests pass.

- [ ] **Step 5: Commit**

```bash
git add deskbridge/db/store.py tests/test_store.py
git commit -m "feat: add 7 Phase 4 Store methods and extend insert_outbox_item for group replies"
```

---

## Task 3: Modified DmWatcher (operator auth + intent routing)

**Files:**
- Modify: `deskbridge/dm/watcher.py`
- Modify: `tests/test_dm_watcher.py`

**Background:** `DmWatcher` currently calls `upsert_work_item` unconditionally for every DM. After this task it:
1. Drops messages not from `operator_npub`
2. Parses intent and routes to one of five `_handle_*` methods
3. `_handle_task` does the existing work item creation
4. Other handlers query/write the store and queue outbox replies

The existing 6 tests need two changes:
- `make_watcher` needs to accept `operator_npub` (default `"npub1op"` in tests)
- Two tests that receive messages need `sender_pubkey` in the mock message dict

- [ ] **Step 1: Update existing tests and add new intent routing tests**

Replace the entire `tests/test_dm_watcher.py` with:

```python
import asyncio
import uuid
from unittest.mock import AsyncMock, MagicMock
from deskbridge.dm.watcher import DmWatcher
from deskbridge.mcp.client import McpToolError
from deskbridge.mcp.errors import RoutingDecision
from deskbridge.models import McpError, McpErrorCategory


OPERATOR_NPUB = "npub1op"


def make_watcher(store, shutdown_event, *, call_tool_mock, session_id="sess-123",
                 operator_npub=OPERATOR_NPUB):
    mock_client = MagicMock()
    mock_client.call_tool = call_tool_mock
    mock_broker = MagicMock()
    mock_broker.get_session_id = AsyncMock(return_value=session_id)
    return DmWatcher(
        identity_label="alice",
        store=store,
        client=mock_client,
        broker=mock_broker,
        shutdown_event=shutdown_event,
        operator_npub=operator_npub,
        poll_timeout_secs=1,
    )


def _dm_response(*messages, last_id="msg-1"):
    return {
        "messages": list(messages),
        "last_message_id": last_id,
        "last_created_at": "2026-01-01T00:00:00Z",
    }


def _msg(id="msg-1", content="fix the auth bug", sender_pubkey=OPERATOR_NPUB):
    return {"id": id, "content": content, "sender_pubkey": sender_pubkey}


# ---------------------------------------------------------------------------
# Existing MCP-level tests (updated to include sender_pubkey in messages)
# ---------------------------------------------------------------------------

async def test_dm_watcher_creates_work_item_and_cursor(store):
    shutdown_event = asyncio.Event()

    async def one_shot(tool_name, arguments):
        shutdown_event.set()
        return _dm_response(_msg(content="hello operator"))

    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    watcher = make_watcher(store, shutdown_event, call_tool_mock=AsyncMock(side_effect=one_shot))
    await watcher.run()

    async with store._conn.execute("SELECT * FROM work_items") as cur:
        rows = await cur.fetchall()
    assert len(rows) == 1
    assert rows[0]["source_type"] == "dm"
    assert rows[0]["source_id"] == "msg-1"
    assert rows[0]["idempotency_key"] == "msg-1"
    assert rows[0]["identity_id"] == "acc-alice"
    assert rows[0]["summary"] == "hello operator"

    cursor_row = await store.get_cursor("dm_watcher", "acc-alice")
    assert cursor_row is not None
    assert cursor_row["last_entity_id"] == "msg-1"


async def test_dm_watcher_empty_response_no_row_no_cursor(store):
    shutdown_event = asyncio.Event()

    async def one_shot(tool_name, arguments):
        shutdown_event.set()
        return {"messages": [], "last_message_id": None, "last_created_at": None}

    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    watcher = make_watcher(store, shutdown_event, call_tool_mock=AsyncMock(side_effect=one_shot))
    await watcher.run()

    async with store._conn.execute("SELECT COUNT(*) FROM work_items") as cur:
        row = await cur.fetchone()
    assert row[0] == 0
    assert await store.get_cursor("dm_watcher", "acc-alice") is None


async def test_dm_watcher_idempotent_on_replay(store):
    shutdown_event = asyncio.Event()
    call_count = 0

    async def two_shot(tool_name, arguments):
        nonlocal call_count
        call_count += 1
        if call_count >= 2:
            shutdown_event.set()
        return _dm_response(_msg(content="hello"))

    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    watcher = make_watcher(store, shutdown_event, call_tool_mock=AsyncMock(side_effect=two_shot))
    await watcher.run()

    async with store._conn.execute("SELECT COUNT(*) FROM work_items") as cur:
        row = await cur.fetchone()
    assert row[0] == 1


async def test_dm_watcher_reauth_does_not_crash(store):
    shutdown_event = asyncio.Event()
    call_count = 0

    async def reauth_then_stop(tool_name, arguments):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise McpToolError(
                mcp_error=McpError(category=McpErrorCategory.INVALID_SESSION, message="reauth"),
                routing=RoutingDecision.REAUTH,
            )
        shutdown_event.set()
        return {"messages": [], "last_message_id": None, "last_created_at": None}

    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    watcher = make_watcher(store, shutdown_event, call_tool_mock=AsyncMock(side_effect=reauth_then_stop))
    await watcher.run()
    assert call_count == 2


async def test_dm_watcher_reject_exits_cleanly(store):
    shutdown_event = asyncio.Event()

    async def reject(tool_name, arguments):
        raise McpToolError(
            mcp_error=McpError(category=McpErrorCategory.UNSUPPORTED_STATE, message="rejected"),
            routing=RoutingDecision.REJECT,
        )

    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    watcher = make_watcher(store, shutdown_event, call_tool_mock=AsyncMock(side_effect=reject))
    await watcher.run()
    assert not shutdown_event.is_set()


async def test_dm_watcher_no_session_skips_poll(store):
    shutdown_event = asyncio.Event()
    mock_client = MagicMock()
    mock_client.call_tool = AsyncMock()
    mock_broker = MagicMock()
    mock_broker.get_session_id = AsyncMock(return_value=None)

    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    watcher = DmWatcher(
        identity_label="alice",
        store=store,
        client=mock_client,
        broker=mock_broker,
        shutdown_event=shutdown_event,
        operator_npub=OPERATOR_NPUB,
        poll_timeout_secs=1,
    )

    async def stop():
        await asyncio.sleep(0.05)
        shutdown_event.set()

    await asyncio.gather(watcher.run(), stop())
    mock_client.call_tool.assert_not_awaited()


# ---------------------------------------------------------------------------
# Authorization tests
# ---------------------------------------------------------------------------

async def test_dm_watcher_unauthorized_sender_ignored(store):
    shutdown_event = asyncio.Event()

    async def one_shot(tool_name, arguments):
        shutdown_event.set()
        return _dm_response(_msg(sender_pubkey="npub1stranger"))

    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    watcher = make_watcher(store, shutdown_event, call_tool_mock=AsyncMock(side_effect=one_shot))
    await watcher.run()

    async with store._conn.execute("SELECT COUNT(*) FROM work_items") as cur:
        row = await cur.fetchone()
    assert row[0] == 0


async def test_dm_watcher_no_operator_npub_ignores_all(store):
    shutdown_event = asyncio.Event()

    async def one_shot(tool_name, arguments):
        shutdown_event.set()
        return _dm_response(_msg())

    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    watcher = make_watcher(store, shutdown_event,
                            call_tool_mock=AsyncMock(side_effect=one_shot),
                            operator_npub=None)
    await watcher.run()

    async with store._conn.execute("SELECT COUNT(*) FROM work_items") as cur:
        row = await cur.fetchone()
    assert row[0] == 0


# ---------------------------------------------------------------------------
# Intent routing tests
# ---------------------------------------------------------------------------

async def test_dm_watcher_status_intent_queues_reply_no_tasks(store):
    shutdown_event = asyncio.Event()

    async def one_shot(tool_name, arguments):
        shutdown_event.set()
        return _dm_response(_msg(content="status please"))

    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    watcher = make_watcher(store, shutdown_event, call_tool_mock=AsyncMock(side_effect=one_shot))
    await watcher.run()

    async with store._conn.execute("SELECT COUNT(*) FROM work_items") as cur:
        row = await cur.fetchone()
    assert row[0] == 0

    async with store._conn.execute("SELECT message_text, dest_pubkey FROM outbox") as cur:
        rows = await cur.fetchall()
    assert len(rows) == 1
    assert "No tasks" in rows[0]["message_text"]
    assert rows[0]["dest_pubkey"] == OPERATOR_NPUB


async def test_dm_watcher_status_intent_reports_latest_task(store, db_conn):
    shutdown_event = asyncio.Event()

    async def one_shot(tool_name, arguments):
        shutdown_event.set()
        return _dm_response(_msg(id="msg-2", content="status please"))

    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    await db_conn.execute(
        "INSERT INTO work_items (id, source_type, source_id, identity_id, status, summary, idempotency_key) "
        "VALUES ('wi-1', 'dm', 'wi-1', 'acc-alice', 'done', 'fix auth bug', 'idem-wi-1')"
    )
    await db_conn.commit()

    watcher = make_watcher(store, shutdown_event, call_tool_mock=AsyncMock(side_effect=one_shot))
    await watcher.run()

    async with store._conn.execute("SELECT message_text FROM outbox") as cur:
        rows = await cur.fetchall()
    assert len(rows) == 1
    assert "fix auth bug" in rows[0]["message_text"]
    assert "done" in rows[0]["message_text"]


async def test_dm_watcher_cancel_intent_marks_cancel_requested(store, db_conn):
    shutdown_event = asyncio.Event()

    async def one_shot(tool_name, arguments):
        shutdown_event.set()
        return _dm_response(_msg(id="msg-2", content="cancel that task"))

    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    await db_conn.execute(
        "INSERT INTO work_items (id, source_type, source_id, identity_id, status, summary, idempotency_key) "
        "VALUES ('wi-1', 'dm', 'wi-1', 'acc-alice', 'dispatched', 'fix auth bug', 'idem-wi-1')"
    )
    await db_conn.commit()

    watcher = make_watcher(store, shutdown_event, call_tool_mock=AsyncMock(side_effect=one_shot))
    await watcher.run()

    async with store._conn.execute("SELECT status FROM work_items WHERE id='wi-1'") as cur:
        row = await cur.fetchone()
    assert row["status"] == "cancel_requested"

    async with store._conn.execute("SELECT message_text FROM outbox") as cur:
        rows = await cur.fetchall()
    assert len(rows) == 1
    assert "Cancel requested" in rows[0]["message_text"]


async def test_dm_watcher_cancel_no_active_task_sends_reply(store):
    shutdown_event = asyncio.Event()

    async def one_shot(tool_name, arguments):
        shutdown_event.set()
        return _dm_response(_msg(content="abort the run"))

    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    watcher = make_watcher(store, shutdown_event, call_tool_mock=AsyncMock(side_effect=one_shot))
    await watcher.run()

    async with store._conn.execute("SELECT message_text FROM outbox") as cur:
        rows = await cur.fetchall()
    assert len(rows) == 1
    assert "No active task" in rows[0]["message_text"]


async def test_dm_watcher_approve_intent_resolves_approval(store, db_conn):
    shutdown_event = asyncio.Event()

    async def one_shot(tool_name, arguments):
        shutdown_event.set()
        return _dm_response(_msg(id="msg-2", content="yes go ahead"))

    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    await db_conn.execute(
        "INSERT INTO work_items (id, source_type, source_id, identity_id, status, idempotency_key) "
        "VALUES ('wi-1', 'dm', 'wi-1', 'acc-alice', 'pending', 'idem-wi-1')"
    )
    await db_conn.execute(
        "INSERT INTO approvals (id, work_item_id, action_description, status) "
        "VALUES ('appr-1', 'wi-1', 'do something', 'pending')"
    )
    await db_conn.commit()

    watcher = make_watcher(store, shutdown_event, call_tool_mock=AsyncMock(side_effect=one_shot))
    await watcher.run()

    async with store._conn.execute("SELECT status, resolved_at FROM approvals WHERE id='appr-1'") as cur:
        row = await cur.fetchone()
    assert row["status"] == "approved"
    assert row["resolved_at"] is not None


async def test_dm_watcher_reject_intent_resolves_approval(store, db_conn):
    shutdown_event = asyncio.Event()

    async def one_shot(tool_name, arguments):
        shutdown_event.set()
        return _dm_response(_msg(id="msg-2", content="no, reject that"))

    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    await db_conn.execute(
        "INSERT INTO work_items (id, source_type, source_id, identity_id, status, idempotency_key) "
        "VALUES ('wi-1', 'dm', 'wi-1', 'acc-alice', 'pending', 'idem-wi-1')"
    )
    await db_conn.execute(
        "INSERT INTO approvals (id, work_item_id, action_description, status) "
        "VALUES ('appr-1', 'wi-1', 'do something', 'pending')"
    )
    await db_conn.commit()

    watcher = make_watcher(store, shutdown_event, call_tool_mock=AsyncMock(side_effect=one_shot))
    await watcher.run()

    async with store._conn.execute("SELECT status FROM approvals WHERE id='appr-1'") as cur:
        row = await cur.fetchone()
    assert row["status"] == "rejected"


async def test_dm_watcher_approve_no_pending_sends_reply(store):
    shutdown_event = asyncio.Event()

    async def one_shot(tool_name, arguments):
        shutdown_event.set()
        return _dm_response(_msg(content="approve"))

    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    watcher = make_watcher(store, shutdown_event, call_tool_mock=AsyncMock(side_effect=one_shot))
    await watcher.run()

    async with store._conn.execute("SELECT message_text FROM outbox") as cur:
        rows = await cur.fetchall()
    assert len(rows) == 1
    assert "No pending approval" in rows[0]["message_text"]
```

- [ ] **Step 2: Run failing tests**

```bash
uv run pytest tests/test_dm_watcher.py -v
```

Expected: Most tests FAIL — `DmWatcher.__init__` does not accept `operator_npub`, and intent routing methods don't exist.

- [ ] **Step 3: Rewrite `deskbridge/dm/watcher.py`**

Replace the entire file with:

```python
import asyncio
import json
import uuid
import structlog

from deskbridge.db.store import Store
from deskbridge.dm.intent import Intent, parse
from deskbridge.mcp.client import McpClient, McpToolError
from deskbridge.mcp.errors import RoutingDecision
from deskbridge.mcp.session import SessionBroker

log = structlog.get_logger()


class DmWatcher:
    def __init__(
        self,
        identity_label: str,
        store: Store,
        client: McpClient,
        broker: SessionBroker,
        shutdown_event: asyncio.Event,
        operator_npub: str | None = None,
        poll_timeout_secs: int = 30,
    ) -> None:
        self._identity_label = identity_label
        self._account_id = f"acc-{identity_label}"
        self._store = store
        self._client = client
        self._broker = broker
        self._shutdown_event = shutdown_event
        self._operator_npub = operator_npub
        self._poll_timeout_secs = poll_timeout_secs

    async def run(self) -> None:
        cursor_row = await self._store.get_cursor(
            cursor_type="dm_watcher", identity_id=self._account_id
        )
        after_message_id: str | None = cursor_row["last_entity_id"] if cursor_row else None
        after_created_at: str | None = cursor_row["last_created_at"] if cursor_row else None

        log.info("dm_watcher_started", identity=self._identity_label)

        while not self._shutdown_event.is_set():
            session_id = await self._broker.get_session_id(self._identity_label)
            if session_id is None:
                log.debug("dm_watcher_no_session", identity=self._identity_label)
                try:
                    await asyncio.wait_for(self._shutdown_event.wait(), timeout=5.0)
                except asyncio.TimeoutError:
                    pass
                continue

            try:
                result = await self._client.call_tool(
                    "wait_for_new_dms",
                    {
                        "session_id": session_id,
                        "after_message_id": after_message_id,
                        "after_created_at": after_created_at,
                        "timeout_seconds": self._poll_timeout_secs,
                    },
                )
                messages = result.get("messages", [])
                for msg in messages:
                    if (
                        self._operator_npub is None
                        or msg.get("sender_pubkey") != self._operator_npub
                    ):
                        log.debug(
                            "dm_watcher_unauthorized",
                            identity=self._identity_label,
                            sender=msg.get("sender_pubkey"),
                        )
                        continue
                    intent = parse(msg["content"])
                    await self._dispatch(intent, msg)

                if messages:
                    new_cursor_id = result.get("last_message_id")
                    if new_cursor_id is None:
                        log.warning(
                            "dm_watcher_missing_cursor_in_response",
                            identity=self._identity_label,
                            message_count=len(messages),
                        )
                    else:
                        after_message_id = new_cursor_id
                        after_created_at = result.get("last_created_at")
                        await self._store.upsert_cursor(
                            cursor_type="dm_watcher",
                            identity_id=self._account_id,
                            last_entity_id=after_message_id,
                            last_created_at=after_created_at,
                            last_imported_at=None,
                            raw_json=json.dumps(result),
                        )

            except McpToolError as e:
                if e.routing == RoutingDecision.REJECT:
                    log.error(
                        "dm_watcher_rejected",
                        identity=self._identity_label,
                        message=e.mcp_error.message,
                    )
                    return
                elif e.routing == RoutingDecision.REAUTH:
                    log.warning("dm_watcher_reauth", identity=self._identity_label)
                elif e.routing == RoutingDecision.RESET_CURSOR:
                    log.warning(
                        "dm_watcher_reset_cursor",
                        identity=self._identity_label,
                        message=e.mcp_error.message,
                    )
                    after_message_id = None
                    after_created_at = None
                else:
                    log.error(
                        "dm_watcher_error",
                        identity=self._identity_label,
                        routing=e.routing,
                        message=e.mcp_error.message,
                    )
                try:
                    await asyncio.wait_for(self._shutdown_event.wait(), timeout=5.0)
                except asyncio.TimeoutError:
                    pass

            except Exception:
                log.exception("dm_watcher_unexpected_error", identity=self._identity_label)
                try:
                    await asyncio.wait_for(self._shutdown_event.wait(), timeout=5.0)
                except asyncio.TimeoutError:
                    pass

        log.info("dm_watcher_stopped", identity=self._identity_label)

    async def _dispatch(self, intent: Intent, msg: dict) -> None:
        handlers = {
            Intent.TASK: self._handle_task,
            Intent.STATUS: self._handle_status,
            Intent.CANCEL: self._handle_cancel,
            Intent.APPROVE: self._handle_approve,
            Intent.REJECT: self._handle_reject,
        }
        await handlers[intent](msg)

    async def _handle_task(self, msg: dict) -> None:
        try:
            await self._store.upsert_work_item(
                id=str(uuid.uuid4()),
                source_type="dm",
                source_id=msg["id"],
                identity_id=self._account_id,
                summary=msg["content"][:200],
                payload_json=json.dumps(msg),
                idempotency_key=msg["id"],
            )
        except Exception:
            log.exception("dm_watcher_handle_task_error", identity=self._identity_label)

    async def _handle_status(self, msg: dict) -> None:
        try:
            row = await self._store.get_latest_work_item(self._account_id)
            if row is None:
                reply = "No tasks found."
            else:
                reply = f"Task [{row['summary']}] is {row['status']}."
            await self._store.insert_outbox_item(
                str(uuid.uuid4()),
                self._account_id,
                msg["sender_pubkey"],
                reply,
                f"status-reply-{msg['id']}",
            )
        except Exception:
            log.exception("dm_watcher_handle_status_error", identity=self._identity_label)

    async def _handle_cancel(self, msg: dict) -> None:
        try:
            row = await self._store.get_latest_dispatched_work_item(self._account_id)
            if row is None:
                reply = "No active task to cancel."
            else:
                await self._store.mark_work_item_cancel_requested(row["id"])
                reply = f"Cancel requested for task [{row['summary']}]."
            await self._store.insert_outbox_item(
                str(uuid.uuid4()),
                self._account_id,
                msg["sender_pubkey"],
                reply,
                f"cancel-reply-{msg['id']}",
            )
        except Exception:
            log.exception("dm_watcher_handle_cancel_error", identity=self._identity_label)

    async def _handle_approve(self, msg: dict) -> None:
        try:
            row = await self._store.get_pending_approval(self._account_id)
            if row is None:
                reply = "No pending approval to approve."
            else:
                await self._store.resolve_approval(row["id"], "approved")
                reply = "Approved."
            await self._store.insert_outbox_item(
                str(uuid.uuid4()),
                self._account_id,
                msg["sender_pubkey"],
                reply,
                f"approve-reply-{msg['id']}",
            )
        except Exception:
            log.exception("dm_watcher_handle_approve_error", identity=self._identity_label)

    async def _handle_reject(self, msg: dict) -> None:
        try:
            row = await self._store.get_pending_approval(self._account_id)
            if row is None:
                reply = "No pending approval to reject."
            else:
                await self._store.resolve_approval(row["id"], "rejected")
                reply = "Rejected."
            await self._store.insert_outbox_item(
                str(uuid.uuid4()),
                self._account_id,
                msg["sender_pubkey"],
                reply,
                f"reject-reply-{msg['id']}",
            )
        except Exception:
            log.exception("dm_watcher_handle_reject_error", identity=self._identity_label)
```

- [ ] **Step 4: Run all tests**

```bash
uv run pytest --tb=short -q
```

Expected: All 118 previously passing tests still pass + all new DmWatcher tests pass.

- [ ] **Step 5: Commit**

```bash
git add deskbridge/dm/watcher.py tests/test_dm_watcher.py
git commit -m "feat: add operator auth check and intent routing to DmWatcher"
```

---

## Task 4: GroupWatcher

**Files:**
- Create: `deskbridge/dm/group_watcher.py`
- Create: `tests/test_group_watcher.py`

**Background:** `GroupWatcher` mirrors `DmWatcher` structurally. It polls the MCP tool `wait_for_new_group_messages` (verify exact tool name against NostrDesk MCP tool list during implementation — if the name differs, update the string in `_poll_once`). For each message, it:
1. Checks sender == `operator_npub`
2. Checks identity's npub appears in the message content
3. Strips the mention, parses intent, routes to `_handle_*` methods
4. Replies go to the group (`dest_group_id`) not to a pubkey

The `_handle_*` methods are identical to `DmWatcher`'s except `insert_outbox_item` passes `dest_pubkey=None` and `dest_group_id=msg["group_id"]`.

- [ ] **Step 1: Write failing tests**

Create `tests/test_group_watcher.py`:

```python
import asyncio
from unittest.mock import AsyncMock, MagicMock
from deskbridge.dm.group_watcher import GroupWatcher
from deskbridge.mcp.client import McpToolError
from deskbridge.mcp.errors import RoutingDecision
from deskbridge.models import McpError, McpErrorCategory


IDENTITY_NPUB = "npub1alice"
OPERATOR_NPUB = "npub1op"
GROUP_ID = "grp-1"


def make_group_watcher(store, shutdown_event, *, call_tool_mock, session_id="sess-123",
                        operator_npub=OPERATOR_NPUB, group_ids=None):
    mock_client = MagicMock()
    mock_client.call_tool = call_tool_mock
    mock_broker = MagicMock()
    mock_broker.get_session_id = AsyncMock(return_value=session_id)
    return GroupWatcher(
        identity_label="alice",
        identity_npub=IDENTITY_NPUB,
        operator_npub=operator_npub,
        group_ids=group_ids or [GROUP_ID],
        store=store,
        client=mock_client,
        broker=mock_broker,
        shutdown_event=shutdown_event,
        poll_timeout_secs=1,
    )


def _msg(id="gmsg-1", content=f"fix the auth bug", sender_pubkey=OPERATOR_NPUB, group_id=GROUP_ID,
         identity_npub=IDENTITY_NPUB):
    return {
        "id": id,
        "content": f"nostr:{identity_npub} {content}",
        "sender_pubkey": sender_pubkey,
        "group_id": group_id,
    }


def _group_response(*messages, last_id="gmsg-1"):
    return {
        "messages": list(messages),
        "last_message_id": last_id,
        "last_created_at": "2026-01-01T00:00:00Z",
    }


# ---------------------------------------------------------------------------
# Authorization and mention filtering
# ---------------------------------------------------------------------------

async def test_group_watcher_task_creates_work_item(store):
    shutdown_event = asyncio.Event()

    async def one_shot(tool_name, arguments):
        shutdown_event.set()
        return _group_response(_msg(content="fix the auth bug"))

    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    watcher = make_group_watcher(store, shutdown_event, call_tool_mock=AsyncMock(side_effect=one_shot))
    await watcher.run()

    async with store._conn.execute("SELECT COUNT(*) FROM work_items") as cur:
        row = await cur.fetchone()
    assert row[0] == 1


async def test_group_watcher_unauthorized_sender_ignored(store):
    shutdown_event = asyncio.Event()

    async def one_shot(tool_name, arguments):
        shutdown_event.set()
        return _group_response(_msg(sender_pubkey="npub1stranger"))

    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    watcher = make_group_watcher(store, shutdown_event, call_tool_mock=AsyncMock(side_effect=one_shot))
    await watcher.run()

    async with store._conn.execute("SELECT COUNT(*) FROM work_items") as cur:
        row = await cur.fetchone()
    assert row[0] == 0


async def test_group_watcher_no_mention_ignored(store):
    shutdown_event = asyncio.Event()

    async def one_shot(tool_name, arguments):
        shutdown_event.set()
        # Message does NOT contain identity_npub
        return _group_response({
            "id": "gmsg-1",
            "content": "hey everyone, fix the auth bug",
            "sender_pubkey": OPERATOR_NPUB,
            "group_id": GROUP_ID,
        })

    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    watcher = make_group_watcher(store, shutdown_event, call_tool_mock=AsyncMock(side_effect=one_shot))
    await watcher.run()

    async with store._conn.execute("SELECT COUNT(*) FROM work_items") as cur:
        row = await cur.fetchone()
    assert row[0] == 0


async def test_group_watcher_no_operator_npub_ignores_all(store):
    shutdown_event = asyncio.Event()

    async def one_shot(tool_name, arguments):
        shutdown_event.set()
        return _group_response(_msg())

    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    watcher = make_group_watcher(store, shutdown_event,
                                  call_tool_mock=AsyncMock(side_effect=one_shot),
                                  operator_npub=None)
    await watcher.run()

    async with store._conn.execute("SELECT COUNT(*) FROM work_items") as cur:
        row = await cur.fetchone()
    assert row[0] == 0


# ---------------------------------------------------------------------------
# Intent routing
# ---------------------------------------------------------------------------

async def test_group_watcher_status_intent_replies_to_group(store):
    shutdown_event = asyncio.Event()

    async def one_shot(tool_name, arguments):
        shutdown_event.set()
        return _group_response(_msg(content="status"))

    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    watcher = make_group_watcher(store, shutdown_event, call_tool_mock=AsyncMock(side_effect=one_shot))
    await watcher.run()

    async with store._conn.execute("SELECT dest_group_id, dest_pubkey, message_text FROM outbox") as cur:
        rows = await cur.fetchall()
    assert len(rows) == 1
    assert rows[0]["dest_group_id"] == GROUP_ID
    assert rows[0]["dest_pubkey"] is None
    assert "No tasks" in rows[0]["message_text"]


async def test_group_watcher_cancel_intent_marks_cancel_requested(store, db_conn):
    shutdown_event = asyncio.Event()

    async def one_shot(tool_name, arguments):
        shutdown_event.set()
        return _group_response(_msg(id="gmsg-2", content="cancel"))

    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    await db_conn.execute(
        "INSERT INTO work_items (id, source_type, source_id, identity_id, status, summary, idempotency_key) "
        "VALUES ('wi-1', 'dm', 'wi-1', 'acc-alice', 'dispatched', 'fix auth', 'idem-wi-1')"
    )
    await db_conn.commit()

    watcher = make_group_watcher(store, shutdown_event, call_tool_mock=AsyncMock(side_effect=one_shot))
    await watcher.run()

    async with store._conn.execute("SELECT status FROM work_items WHERE id='wi-1'") as cur:
        row = await cur.fetchone()
    assert row["status"] == "cancel_requested"


async def test_group_watcher_no_session_skips_poll(store):
    shutdown_event = asyncio.Event()
    mock_client = MagicMock()
    mock_client.call_tool = AsyncMock()
    mock_broker = MagicMock()
    mock_broker.get_session_id = AsyncMock(return_value=None)

    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    watcher = GroupWatcher(
        identity_label="alice",
        identity_npub=IDENTITY_NPUB,
        operator_npub=OPERATOR_NPUB,
        group_ids=[GROUP_ID],
        store=store,
        client=mock_client,
        broker=mock_broker,
        shutdown_event=shutdown_event,
        poll_timeout_secs=1,
    )

    async def stop():
        await asyncio.sleep(0.05)
        shutdown_event.set()

    await asyncio.gather(watcher.run(), stop())
    mock_client.call_tool.assert_not_awaited()


async def test_group_watcher_reject_exits_cleanly(store):
    shutdown_event = asyncio.Event()

    async def reject(tool_name, arguments):
        raise McpToolError(
            mcp_error=McpError(category=McpErrorCategory.UNSUPPORTED_STATE, message="rejected"),
            routing=RoutingDecision.REJECT,
        )

    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    watcher = make_group_watcher(store, shutdown_event, call_tool_mock=AsyncMock(side_effect=reject))
    await watcher.run()
    assert not shutdown_event.is_set()
```

- [ ] **Step 2: Run failing tests**

```bash
uv run pytest tests/test_group_watcher.py -v
```

Expected: `ModuleNotFoundError: No module named 'deskbridge.dm.group_watcher'`

- [ ] **Step 3: Create `deskbridge/dm/group_watcher.py`**

```python
import asyncio
import json
import re
import uuid
import structlog

from deskbridge.db.store import Store
from deskbridge.dm.intent import Intent, parse
from deskbridge.mcp.client import McpClient, McpToolError
from deskbridge.mcp.errors import RoutingDecision
from deskbridge.mcp.session import SessionBroker

log = structlog.get_logger()

# Matches "nostr:<npub>" or "@<npub>" anywhere in the message content.
_MENTION_RE_TEMPLATE = r"(?:nostr:|@){npub}"


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
        self._identity_label = identity_label
        self._account_id = f"acc-{identity_label}"
        self._identity_npub = identity_npub
        self._operator_npub = operator_npub
        self._group_ids = group_ids
        self._store = store
        self._client = client
        self._broker = broker
        self._shutdown_event = shutdown_event
        self._poll_timeout_secs = poll_timeout_secs
        self._mention_re = re.compile(
            _MENTION_RE_TEMPLATE.format(npub=re.escape(identity_npub)), re.I
        )

    async def run(self) -> None:
        cursor_row = await self._store.get_cursor(
            cursor_type="group_watcher", identity_id=self._account_id
        )
        after_message_id: str | None = cursor_row["last_entity_id"] if cursor_row else None
        after_created_at: str | None = cursor_row["last_created_at"] if cursor_row else None

        log.info("group_watcher_started", identity=self._identity_label, groups=self._group_ids)

        while not self._shutdown_event.is_set():
            session_id = await self._broker.get_session_id(self._identity_label)
            if session_id is None:
                log.debug("group_watcher_no_session", identity=self._identity_label)
                try:
                    await asyncio.wait_for(self._shutdown_event.wait(), timeout=5.0)
                except asyncio.TimeoutError:
                    pass
                continue

            try:
                # NOTE: Verify "wait_for_new_group_messages" against NostrDesk MCP tool list.
                result = await self._client.call_tool(
                    "wait_for_new_group_messages",
                    {
                        "session_id": session_id,
                        "group_ids": self._group_ids,
                        "after_message_id": after_message_id,
                        "after_created_at": after_created_at,
                        "timeout_seconds": self._poll_timeout_secs,
                    },
                )
                messages = result.get("messages", [])
                for msg in messages:
                    if (
                        self._operator_npub is None
                        or msg.get("sender_pubkey") != self._operator_npub
                    ):
                        log.debug(
                            "group_watcher_unauthorized",
                            identity=self._identity_label,
                            sender=msg.get("sender_pubkey"),
                        )
                        continue
                    content = msg.get("content", "")
                    if not self._mention_re.search(content):
                        log.debug(
                            "group_watcher_no_mention",
                            identity=self._identity_label,
                            group_id=msg.get("group_id"),
                        )
                        continue
                    stripped = self._mention_re.sub("", content).strip()
                    intent = parse(stripped)
                    await self._dispatch(intent, msg)

                if messages:
                    new_cursor_id = result.get("last_message_id")
                    if new_cursor_id is None:
                        log.warning(
                            "group_watcher_missing_cursor_in_response",
                            identity=self._identity_label,
                        )
                    else:
                        after_message_id = new_cursor_id
                        after_created_at = result.get("last_created_at")
                        await self._store.upsert_cursor(
                            cursor_type="group_watcher",
                            identity_id=self._account_id,
                            last_entity_id=after_message_id,
                            last_created_at=after_created_at,
                            last_imported_at=None,
                            raw_json=json.dumps(result),
                        )

            except McpToolError as e:
                if e.routing == RoutingDecision.REJECT:
                    log.error(
                        "group_watcher_rejected",
                        identity=self._identity_label,
                        message=e.mcp_error.message,
                    )
                    return
                elif e.routing == RoutingDecision.REAUTH:
                    log.warning("group_watcher_reauth", identity=self._identity_label)
                else:
                    log.error(
                        "group_watcher_error",
                        identity=self._identity_label,
                        routing=e.routing,
                        message=e.mcp_error.message,
                    )
                try:
                    await asyncio.wait_for(self._shutdown_event.wait(), timeout=5.0)
                except asyncio.TimeoutError:
                    pass

            except Exception:
                log.exception("group_watcher_unexpected_error", identity=self._identity_label)
                try:
                    await asyncio.wait_for(self._shutdown_event.wait(), timeout=5.0)
                except asyncio.TimeoutError:
                    pass

        log.info("group_watcher_stopped", identity=self._identity_label)

    async def _dispatch(self, intent: Intent, msg: dict) -> None:
        handlers = {
            Intent.TASK: self._handle_task,
            Intent.STATUS: self._handle_status,
            Intent.CANCEL: self._handle_cancel,
            Intent.APPROVE: self._handle_approve,
            Intent.REJECT: self._handle_reject,
        }
        await handlers[intent](msg)

    async def _handle_task(self, msg: dict) -> None:
        try:
            stripped = self._mention_re.sub("", msg["content"]).strip()
            await self._store.upsert_work_item(
                id=str(uuid.uuid4()),
                source_type="group",
                source_id=msg["id"],
                identity_id=self._account_id,
                summary=stripped[:200],
                payload_json=json.dumps(msg),
                idempotency_key=msg["id"],
            )
        except Exception:
            log.exception("group_watcher_handle_task_error", identity=self._identity_label)

    async def _handle_status(self, msg: dict) -> None:
        try:
            row = await self._store.get_latest_work_item(self._account_id)
            if row is None:
                reply = "No tasks found."
            else:
                reply = f"Task [{row['summary']}] is {row['status']}."
            await self._store.insert_outbox_item(
                str(uuid.uuid4()),
                self._account_id,
                None,
                reply,
                f"status-reply-{msg['id']}",
                dest_group_id=msg["group_id"],
            )
        except Exception:
            log.exception("group_watcher_handle_status_error", identity=self._identity_label)

    async def _handle_cancel(self, msg: dict) -> None:
        try:
            row = await self._store.get_latest_dispatched_work_item(self._account_id)
            if row is None:
                reply = "No active task to cancel."
            else:
                await self._store.mark_work_item_cancel_requested(row["id"])
                reply = f"Cancel requested for task [{row['summary']}]."
            await self._store.insert_outbox_item(
                str(uuid.uuid4()),
                self._account_id,
                None,
                reply,
                f"cancel-reply-{msg['id']}",
                dest_group_id=msg["group_id"],
            )
        except Exception:
            log.exception("group_watcher_handle_cancel_error", identity=self._identity_label)

    async def _handle_approve(self, msg: dict) -> None:
        try:
            row = await self._store.get_pending_approval(self._account_id)
            if row is None:
                reply = "No pending approval to approve."
            else:
                await self._store.resolve_approval(row["id"], "approved")
                reply = "Approved."
            await self._store.insert_outbox_item(
                str(uuid.uuid4()),
                self._account_id,
                None,
                reply,
                f"approve-reply-{msg['id']}",
                dest_group_id=msg["group_id"],
            )
        except Exception:
            log.exception("group_watcher_handle_approve_error", identity=self._identity_label)

    async def _handle_reject(self, msg: dict) -> None:
        try:
            row = await self._store.get_pending_approval(self._account_id)
            if row is None:
                reply = "No pending approval to reject."
            else:
                await self._store.resolve_approval(row["id"], "rejected")
                reply = "Rejected."
            await self._store.insert_outbox_item(
                str(uuid.uuid4()),
                self._account_id,
                None,
                reply,
                f"reject-reply-{msg['id']}",
                dest_group_id=msg["group_id"],
            )
        except Exception:
            log.exception("group_watcher_handle_reject_error", identity=self._identity_label)
```

- [ ] **Step 4: Run all tests**

```bash
uv run pytest --tb=short -q
```

Expected: All previously passing tests still pass + all new GroupWatcher tests pass.

- [ ] **Step 5: Commit**

```bash
git add deskbridge/dm/group_watcher.py tests/test_group_watcher.py
git commit -m "feat: add GroupWatcher for @mention-filtered group message routing"
```

---

## Task 5: WorkItemPoller cancel-checking

**Files:**
- Modify: `deskbridge/agent/poller.py`
- Modify: `tests/test_work_item_poller.py`

**Background:** When `DmWatcher._handle_cancel` marks a work item `cancel_requested`, the running `AgentRunner` task won't notice until `WorkItemPoller._poll_once` checks it. This task adds:
1. `self._active_work_item_id: str | None = None` — tracks the ID of the running work item
2. At the top of `_poll_once`: if active task is running and its work item is `cancel_requested`, cancel the task and mark it `cancelled`
3. When a runner is spawned, set `_active_work_item_id = row["id"]`
4. When active task finishes naturally, clear `_active_work_item_id`

- [ ] **Step 1: Write failing test**

Append to `tests/test_work_item_poller.py` (after the last test):

```python
async def test_poller_cancels_runner_when_work_item_cancel_requested():
    store = MagicMock()
    project_row = _row(id="proj-1")
    cancel_work_item_row = _row(
        id="wi-1", status="cancel_requested", summary="fix auth bug"
    )

    call_count = 0

    async def fake_get_work_item(id):
        nonlocal call_count
        call_count += 1
        return cancel_work_item_row

    store.get_project_for_identity = AsyncMock(return_value=project_row)
    store.get_pending_work_items = AsyncMock(return_value=[])
    store.get_work_item = AsyncMock(side_effect=fake_get_work_item)
    store.complete_work_item = AsyncMock()

    config = make_config()
    mock_client = MagicMock()
    mock_broker = MagicMock()
    mock_broker.get_session_id = AsyncMock(return_value="sess-1")
    shutdown_event = asyncio.Event()

    poller = WorkItemPoller(
        identity_label="alice",
        store=store,
        client=mock_client,
        broker=mock_broker,
        config=config,
        shutdown_event=shutdown_event,
        poll_interval_secs=0.01,
    )

    # Inject an active (never-finishing) task and its work item ID
    async def never_finishes():
        await asyncio.Event().wait()

    poller._active_run_task = asyncio.create_task(never_finishes())
    poller._active_work_item_id = "wi-1"

    async def stop():
        await asyncio.sleep(0.05)
        shutdown_event.set()

    await asyncio.gather(poller.run(), stop())

    store.complete_work_item.assert_awaited_with("wi-1", "cancelled")
    assert poller._active_run_task is None
    assert poller._active_work_item_id is None
```

- [ ] **Step 2: Run failing test**

```bash
uv run pytest tests/test_work_item_poller.py::test_poller_cancels_runner_when_work_item_cancel_requested -v
```

Expected: FAIL — `WorkItemPoller` has no `_active_work_item_id` attribute and no cancel-check logic.

- [ ] **Step 3: Update `deskbridge/agent/poller.py`**

Add `self._active_work_item_id: str | None = None` to `__init__` after `self._active_run_task`:

```python
        self._active_run_task: asyncio.Task | None = None
        self._active_work_item_id: str | None = None
```

Replace the entire `_poll_once` method with:

```python
    async def _poll_once(self) -> None:
        # Clear finished runner
        if self._active_run_task is not None and self._active_run_task.done():
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
```

- [ ] **Step 4: Run all tests**

```bash
uv run pytest --tb=short -q
```

Expected: All previously passing tests still pass + the new cancel-check test passes.

- [ ] **Step 5: Commit**

```bash
git add deskbridge/agent/poller.py tests/test_work_item_poller.py
git commit -m "feat: add cancel-signal propagation to WorkItemPoller"
```

---

## Task 6: Supervisor wiring

**Files:**
- Modify: `deskbridge/supervisor.py`
- Modify: `tests/test_supervisor.py`

**Background:** The supervisor needs two changes:
1. Pass `operator_npub=identity.operator_npub` when constructing each `DmWatcher`
2. After DmWatcher tasks, spawn `GroupWatcher` tasks for any identity whose project has non-empty `groups_json`

`group_watcher_tasks` is initialized before the `try` block (same pattern as `poller_tasks` and `watcher_tasks`) so `finally` can always reference it.

- [ ] **Step 1: Write failing test for GroupWatcher spawning**

Append to `tests/test_supervisor.py`:

```python
async def test_supervisor_spawns_group_watcher_when_groups_configured(tmp_path, monkeypatch, mock_broker, mock_client_ctx):
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
         patch("deskbridge.supervisor.Store") as MockStore:

        mock_instance = MockMcpClient.return_value
        mock_instance.connect.return_value = mock_client_ctx

        async def never_finishes():
            await asyncio.Event().wait()

        MockDmWatcher.return_value.run = Mock(side_effect=never_finishes)
        MockOutboxDrainer.return_value.run = Mock(side_effect=never_finishes)
        MockWorkItemPoller.return_value.run = Mock(side_effect=never_finishes)
        MockGroupWatcher.return_value.run = Mock(side_effect=never_finishes)

        # get_project_groups returns a non-empty list so GroupWatcher is spawned
        mock_store_instance = AsyncMock()
        mock_store_instance.get_project_groups = AsyncMock(return_value=["grp-1"])
        MockStore.return_value = mock_store_instance

        supervisor = Supervisor(config=config)

        async def stop():
            await asyncio.sleep(0.1)
            supervisor.request_shutdown()

        await asyncio.gather(supervisor.run(), stop())

    MockGroupWatcher.assert_called_once_with(
        identity_label="alice",
        identity_npub="npub1alice",
        operator_npub=None,
        group_ids=["grp-1"],
        store=ANY,
        client=ANY,
        broker=mock_broker,
        shutdown_event=ANY,
    )
    MockGroupWatcher.return_value.run.assert_called_once()


async def test_supervisor_no_group_watcher_when_no_groups(tmp_path, monkeypatch, mock_broker, mock_client_ctx):
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
         patch("deskbridge.supervisor.Store") as MockStore:

        mock_instance = MockMcpClient.return_value
        mock_instance.connect.return_value = mock_client_ctx

        async def never_finishes():
            await asyncio.Event().wait()

        MockDmWatcher.return_value.run = Mock(side_effect=never_finishes)
        MockOutboxDrainer.return_value.run = Mock(side_effect=never_finishes)
        MockWorkItemPoller.return_value.run = Mock(side_effect=never_finishes)

        # get_project_groups returns empty — no GroupWatcher should be spawned
        mock_store_instance = AsyncMock()
        mock_store_instance.get_project_groups = AsyncMock(return_value=[])
        MockStore.return_value = mock_store_instance

        supervisor = Supervisor(config=config)

        async def stop():
            await asyncio.sleep(0.1)
            supervisor.request_shutdown()

        await asyncio.gather(supervisor.run(), stop())

    MockGroupWatcher.assert_not_called()
```

Also update the existing `test_supervisor_spawns_and_cancels_dm_tasks` test to add `GroupWatcher` to its patch list (to prevent a real GroupWatcher running against the test DB). Find that test and add `patch("deskbridge.supervisor.GroupWatcher") as MockGroupWatcher` to its `with` block.

- [ ] **Step 2: Run failing tests**

```bash
uv run pytest tests/test_supervisor.py -v -k "group_watcher"
```

Expected: FAIL — `deskbridge.supervisor` does not import `GroupWatcher`.

- [ ] **Step 3: Update `deskbridge/supervisor.py`**

Replace the entire file with:

```python
import asyncio
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
                drainer_task: asyncio.Task | None = None
                poller_tasks: list[asyncio.Task] = []

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
                        + poller_tasks
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

- [ ] **Step 4: Update the existing `test_supervisor_spawns_and_cancels_dm_tasks` in `tests/test_supervisor.py`**

Find that test and add `patch("deskbridge.supervisor.GroupWatcher") as MockGroupWatcher` alongside the other patches. The test body doesn't need further changes — it just needs GroupWatcher patched so it doesn't try to spawn a real watcher.

Also add `patch("deskbridge.supervisor.Store") as MockStore` and configure `MockStore.return_value.get_project_groups = AsyncMock(return_value=[])` so the supervisor doesn't try to spawn GroupWatcher tasks.

The updated test `with` block (add the two new patches):

```python
    with patch("deskbridge.supervisor.McpClient") as MockMcpClient, \
         patch("deskbridge.supervisor.SessionBroker", return_value=mock_broker), \
         patch("deskbridge.supervisor.apply_schema", new=AsyncMock()), \
         patch("deskbridge.supervisor.bootstrap_accounts_from_config", new=AsyncMock()), \
         patch("deskbridge.supervisor.DmWatcher") as MockDmWatcher, \
         patch("deskbridge.supervisor.OutboxDrainer") as MockOutboxDrainer, \
         patch("deskbridge.supervisor.WorkItemPoller") as MockWorkItemPoller, \
         patch("deskbridge.supervisor.GroupWatcher") as MockGroupWatcher, \
         patch("deskbridge.supervisor.Store") as MockStore:
        ...
        mock_store_instance = AsyncMock()
        mock_store_instance.get_project_groups = AsyncMock(return_value=[])
        MockStore.return_value = mock_store_instance
```

Apply the same `GroupWatcher` and `Store` mock pattern to `test_supervisor_spawns_and_cancels_poller_tasks` as well.

Also fix `test_supervisor_calls_refresh_on_heartbeat`: that test mocks `apply_schema` but does NOT mock `DmWatcher`/`OutboxDrainer`/`WorkItemPoller`. After this task, the supervisor calls `store.get_project_groups()` directly in the try block — which will raise `OperationalError: no such table: projects` when `apply_schema` is mocked (no schema applied). Fix: remove the `apply_schema` mock from that test so the real schema runs and `get_project_groups` returns `[]` on an empty DB:

```python
# Remove this line from test_supervisor_calls_refresh_on_heartbeat:
#   patch("deskbridge.supervisor.apply_schema", new=AsyncMock()),
# The test then uses a real schema on the tmp_path DB.
with patch("deskbridge.supervisor.McpClient") as MockMcpClient, \
     patch("deskbridge.supervisor.SessionBroker", return_value=mock_broker), \
     patch("deskbridge.supervisor.bootstrap_accounts_from_config", new=AsyncMock()):
    ...
```

- [ ] **Step 5: Run all tests**

```bash
uv run pytest --tb=short -q
```

Expected: All previously passing tests pass + new supervisor GroupWatcher tests pass.

- [ ] **Step 6: Commit**

```bash
git add deskbridge/supervisor.py tests/test_supervisor.py
git commit -m "feat: wire GroupWatcher into supervisor and pass operator_npub to DmWatcher"
```

---

## Final verification

- [ ] **Run full test suite**

```bash
uv run pytest --tb=short -q
```

Expected: All tests pass (previously 118 + new Phase 4 tests).
