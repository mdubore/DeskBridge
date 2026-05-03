# DeskBridge Phase 2: DM Automation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add per-identity DM watching with cursor persistence, an outbox drain loop for outbound DMs, and the Store methods both components need — so the supervisor can receive operator DMs as `work_items` rows and deliver autonomous notifications via `send_dm`.

**Architecture:** Four tasks build bottom-up: Store additions (foundation), DmWatcher (per-identity asyncio task long-polling `wait_for_new_dms`), OutboxDrainer (shared asyncio task draining the `outbox` table via `send_dm`), then Supervisor wiring to spawn and cancel both. All new components integrate with the existing `Store`, `McpClient`, and `SessionBroker` from Phase 1 without modifying their interfaces.

**Tech Stack:** Python 3.12, aiosqlite, structlog, asyncio, unittest.mock (`AsyncMock`, `patch`, `ANY`), pytest-asyncio (`asyncio_mode = auto`).

---

## Codebase Context

Before starting, read these Phase 1 files to understand existing patterns:

- `deskbridge/db/store.py` — `Store` class; all methods use `async with self._conn.execute(...): pass` for writes, `async with ... as cur: return await cur.fetchone()` for reads; always calls `await self._conn.commit()` after writes
- `deskbridge/db/schema.py` — `work_items` table has `idempotency_key TEXT UNIQUE`; `outbox` table has `identity_id NOT NULL REFERENCES accounts(id)`, `dest_pubkey TEXT`, `delivery_attempts INTEGER DEFAULT 0`, `delivery_status TEXT DEFAULT 'pending'`; FK enforcement is ON
- `deskbridge/mcp/client.py` — `McpClient.call_tool(tool_name, arguments)` raises `McpToolError` on MCP errors; `McpToolError` has `.mcp_error: McpError` and `.routing: RoutingDecision`
- `deskbridge/mcp/errors.py` — `RoutingDecision(StrEnum)`: `RETRY`, `REAUTH`, `RESET_CURSOR`, `ESCALATE`, `REJECT`
- `deskbridge/mcp/session.py` — `SessionBroker.get_session_id(label: str) -> str | None` (async)
- `deskbridge/supervisor.py` — heartbeat loop uses `asyncio.wait_for(self._shutdown_event.wait(), timeout=float(interval))` with `except asyncio.TimeoutError: pass` for responsive shutdown
- `tests/conftest.py` — `db_conn` fixture: real aiosqlite DB with schema applied, `row_factory = aiosqlite.Row`; `store` fixture: `Store(db_conn)`

The `account_id` convention throughout this codebase is `f"acc-{label}"` (e.g. label `"alice"` → id `"acc-alice"`).

All sleeps in loop-based components must use `asyncio.wait_for(shutdown_event.wait(), timeout=N)` with `except asyncio.TimeoutError: pass` — never `asyncio.sleep(N)` — so the shutdown event always terminates the sleep immediately.

---

## File Map

| File | Action | Responsibility |
|---|---|---|
| `deskbridge/db/store.py` | Modify | Add `upsert_work_item`, `get_pending_outbox_items`, `update_outbox_delivery` |
| `deskbridge/dm/__init__.py` | Create | Re-export `DmWatcher`, `OutboxDrainer` |
| `deskbridge/dm/watcher.py` | Create | `DmWatcher` — per-identity long-poll loop |
| `deskbridge/dm/outbox.py` | Create | `OutboxDrainer` — shared drain loop |
| `deskbridge/supervisor.py` | Modify | Import and spawn watcher + drainer tasks after `unlock_all()` |
| `tests/test_store.py` | Modify | Append tests for the three new Store methods |
| `tests/test_dm_watcher.py` | Create | `DmWatcher` tests |
| `tests/test_outbox_drainer.py` | Create | `OutboxDrainer` tests |
| `tests/test_supervisor.py` | Modify | Append test: watcher/drainer tasks spawned and cancelled |

---

## Task 1: Store Additions

**Files:**
- Modify: `deskbridge/db/store.py` (add three methods before `bootstrap_accounts_from_config`)
- Modify: `tests/test_store.py` (append five new tests at the bottom)

### Step 1a: Write failing tests for `upsert_work_item`

- [ ] Append to `tests/test_store.py`:

```python
async def test_upsert_work_item_inserts(store: Store):
    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    await store.upsert_work_item(
        id="wi-1",
        source_type="dm",
        source_id="msg-1",
        identity_id="acc-alice",
        summary="hello world",
        payload_json='{"id":"msg-1","content":"hello world"}',
        idempotency_key="msg-1",
    )
    async with store._conn.execute("SELECT * FROM work_items WHERE id = 'wi-1'") as cur:
        row = await cur.fetchone()
    assert row is not None
    assert row["source_type"] == "dm"
    assert row["source_id"] == "msg-1"
    assert row["idempotency_key"] == "msg-1"
    assert row["summary"] == "hello world"


async def test_upsert_work_item_idempotent(store: Store):
    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    for _ in range(2):
        await store.upsert_work_item(
            id="wi-1",
            source_type="dm",
            source_id="msg-1",
            identity_id="acc-alice",
            summary="hello",
            payload_json="{}",
            idempotency_key="msg-1",
        )
    async with store._conn.execute("SELECT COUNT(*) FROM work_items") as cur:
        row = await cur.fetchone()
    assert row[0] == 1
```

- [ ] Run tests to confirm they fail:

```
pytest tests/test_store.py::test_upsert_work_item_inserts tests/test_store.py::test_upsert_work_item_idempotent -v
```

Expected: `AttributeError: 'Store' object has no attribute 'upsert_work_item'`

### Step 1b: Implement `upsert_work_item`

- [ ] In `deskbridge/db/store.py`, add this method immediately before `bootstrap_accounts_from_config`:

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
    async with self._conn.execute(
        """
        INSERT OR IGNORE INTO work_items
            (id, source_type, source_id, identity_id, summary, payload_json, idempotency_key)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (id, source_type, source_id, identity_id, summary, payload_json, idempotency_key),
    ):
        pass
    await self._conn.commit()
```

- [ ] Run tests to confirm they pass:

```
pytest tests/test_store.py::test_upsert_work_item_inserts tests/test_store.py::test_upsert_work_item_idempotent -v
```

Expected: `2 passed`

### Step 1c: Write failing tests for `get_pending_outbox_items` and `update_outbox_delivery`

The `outbox` table has a FK on `identity_id → accounts(id)`, so an account must exist first. Insert outbox rows directly via SQL since no `insert_outbox_item` Store method exists yet.

- [ ] Append to `tests/test_store.py`:

```python
async def _seed_outbox_row(
    conn,
    *,
    id: str,
    identity_id: str = "acc-alice",
    dest_pubkey: str = "npub1dest",
    message_text: str = "hello",
    idempotency_key: str,
    delivery_status: str = "pending",
    delivery_attempts: int = 0,
) -> None:
    await conn.execute(
        """
        INSERT INTO outbox
            (id, identity_id, dest_pubkey, message_text, idempotency_key,
             delivery_status, delivery_attempts)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (id, identity_id, dest_pubkey, message_text, idempotency_key,
         delivery_status, delivery_attempts),
    )
    await conn.commit()


async def test_get_pending_outbox_items_returns_pending(store: Store, db_conn):
    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    await _seed_outbox_row(db_conn, id="ob-1", idempotency_key="k1")
    rows = await store.get_pending_outbox_items(max_attempts=3)
    assert len(rows) == 1
    assert rows[0]["id"] == "ob-1"


async def test_get_pending_outbox_items_excludes_exhausted(store: Store, db_conn):
    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    await _seed_outbox_row(db_conn, id="ob-1", idempotency_key="k1", delivery_attempts=3)
    rows = await store.get_pending_outbox_items(max_attempts=3)
    assert rows == []


async def test_get_pending_outbox_items_excludes_failed(store: Store, db_conn):
    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    await _seed_outbox_row(db_conn, id="ob-1", idempotency_key="k1", delivery_status="failed")
    rows = await store.get_pending_outbox_items(max_attempts=3)
    assert rows == []


async def test_update_outbox_delivery_delivered(store: Store, db_conn):
    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    await _seed_outbox_row(db_conn, id="ob-1", idempotency_key="k1")
    await store.update_outbox_delivery("ob-1", "delivered", '{"ok":true}')
    async with db_conn.execute("SELECT * FROM outbox WHERE id = 'ob-1'") as cur:
        row = await cur.fetchone()
    assert row["delivery_status"] == "delivered"
    assert row["delivery_attempts"] == 1
    assert row["delivered_at"] is not None
    assert row["delivery_result_json"] == '{"ok":true}'


async def test_update_outbox_delivery_pending_increments(store: Store, db_conn):
    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    await _seed_outbox_row(db_conn, id="ob-1", idempotency_key="k1")
    await store.update_outbox_delivery("ob-1", "pending", '{"error":"transient"}')
    async with db_conn.execute("SELECT * FROM outbox WHERE id = 'ob-1'") as cur:
        row = await cur.fetchone()
    assert row["delivery_status"] == "pending"
    assert row["delivery_attempts"] == 1
    assert row["delivered_at"] is None


async def test_update_outbox_delivery_failed(store: Store, db_conn):
    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    await _seed_outbox_row(db_conn, id="ob-1", idempotency_key="k1")
    await store.update_outbox_delivery("ob-1", "failed", '{"error":"reject"}')
    async with db_conn.execute("SELECT * FROM outbox WHERE id = 'ob-1'") as cur:
        row = await cur.fetchone()
    assert row["delivery_status"] == "failed"
    assert row["delivery_attempts"] == 1
    assert row["delivered_at"] is None
```

- [ ] Run to confirm they fail:

```
pytest tests/test_store.py::test_get_pending_outbox_items_returns_pending tests/test_store.py::test_update_outbox_delivery_delivered -v
```

Expected: `AttributeError: 'Store' object has no attribute 'get_pending_outbox_items'`

### Step 1d: Implement `get_pending_outbox_items` and `update_outbox_delivery`

- [ ] In `deskbridge/db/store.py`, add these two methods immediately after `upsert_work_item`:

```python
async def get_pending_outbox_items(self, max_attempts: int = 3) -> list[aiosqlite.Row]:
    async with self._conn.execute(
        """
        SELECT * FROM outbox
        WHERE delivery_status = 'pending'
          AND delivery_attempts < ?
        ORDER BY created_at
        """,
        (max_attempts,),
    ) as cur:
        return await cur.fetchall()

async def update_outbox_delivery(
    self,
    id: str,
    delivery_status: str,
    delivery_result_json: str,
) -> None:
    async with self._conn.execute(
        """
        UPDATE outbox
        SET delivery_status = ?,
            delivery_attempts = delivery_attempts + 1,
            delivery_result_json = ?,
            delivered_at = CASE WHEN ? = 'delivered'
                           THEN strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
                           ELSE NULL END
        WHERE id = ?
        """,
        (delivery_status, delivery_result_json, delivery_status, id),
    ):
        pass
    await self._conn.commit()
```

- [ ] Run all new store tests:

```
pytest tests/test_store.py -v -k "work_item or outbox or pending or delivery"
```

Expected: `8 passed`

### Step 1e: Run full suite and commit

- [ ] Run full suite:

```
pytest --tb=short -q
```

Expected: all existing tests plus 8 new ones pass.

- [ ] Commit:

```bash
git add deskbridge/db/store.py tests/test_store.py
git commit -m "feat: Store — upsert_work_item, get_pending_outbox_items, update_outbox_delivery"
```

---

## Task 2: DmWatcher

**Files:**
- Create: `deskbridge/dm/__init__.py`
- Create: `deskbridge/dm/watcher.py`
- Create: `tests/test_dm_watcher.py`

### Step 2a: Create empty package and write failing tests

- [ ] Create `deskbridge/dm/__init__.py` (empty for now):

```python
```

- [ ] Create `tests/test_dm_watcher.py`:

```python
import asyncio
import json
import pytest
from unittest.mock import AsyncMock, MagicMock
from deskbridge.dm.watcher import DmWatcher
from deskbridge.mcp.client import McpToolError
from deskbridge.mcp.errors import RoutingDecision
from deskbridge.models import McpError, McpErrorCategory


def make_watcher(store, shutdown_event, *, call_tool_mock, session_id="sess-123"):
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
        poll_timeout_secs=1,
    )


async def test_dm_watcher_creates_work_item_and_cursor(store):
    shutdown_event = asyncio.Event()

    async def one_shot(tool_name, arguments):
        shutdown_event.set()
        return {
            "messages": [{"id": "msg-1", "content": "hello operator"}],
            "last_message_id": "msg-1",
            "last_created_at": "2026-01-01T00:00:00Z",
        }

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
    assert cursor_row["last_created_at"] == "2026-01-01T00:00:00Z"


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

    cursor_row = await store.get_cursor("dm_watcher", "acc-alice")
    assert cursor_row is None


async def test_dm_watcher_idempotent_on_replay(store):
    shutdown_event = asyncio.Event()
    call_count = 0

    async def two_shot(tool_name, arguments):
        nonlocal call_count
        call_count += 1
        if call_count >= 2:
            shutdown_event.set()
        return {
            "messages": [{"id": "msg-1", "content": "hello"}],
            "last_message_id": "msg-1",
            "last_created_at": "2026-01-01T00:00:00Z",
        }

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
    mock_client = MagicMock()
    mock_client.call_tool = AsyncMock(side_effect=reject)
    mock_broker = MagicMock()
    mock_broker.get_session_id = AsyncMock(return_value="sess-123")
    watcher = DmWatcher(
        identity_label="alice",
        store=store,
        client=mock_client,
        broker=mock_broker,
        shutdown_event=shutdown_event,
        poll_timeout_secs=1,
    )
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
        poll_timeout_secs=1,
    )

    async def stop():
        await asyncio.sleep(0.05)
        shutdown_event.set()

    await asyncio.gather(watcher.run(), stop())
    mock_client.call_tool.assert_not_awaited()
```

- [ ] Run to confirm they fail:

```
pytest tests/test_dm_watcher.py -v
```

Expected: `ImportError: cannot import name 'DmWatcher' from 'deskbridge.dm.watcher'`

### Step 2b: Implement `DmWatcher`

- [ ] Create `deskbridge/dm/watcher.py` with the full implementation:

```python
import asyncio
import json
import uuid
import structlog

from deskbridge.db.store import Store
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
        poll_timeout_secs: int = 30,
    ) -> None:
        self._identity_label = identity_label
        self._account_id = f"acc-{identity_label}"
        self._store = store
        self._client = client
        self._broker = broker
        self._shutdown_event = shutdown_event
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
                    await self._store.upsert_work_item(
                        id=str(uuid.uuid4()),
                        source_type="dm",
                        source_id=msg["id"],
                        identity_id=self._account_id,
                        summary=msg["content"][:200],
                        payload_json=json.dumps(msg),
                        idempotency_key=msg["id"],
                    )
                if messages and result.get("last_message_id"):
                    after_message_id = result["last_message_id"]
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
                if e.routing == RoutingDecision.REAUTH:
                    log.warning("dm_watcher_reauth", identity=self._identity_label)
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
```

- [ ] Run DmWatcher tests:

```
pytest tests/test_dm_watcher.py -v
```

Expected: `6 passed`

### Step 2c: Run full suite and commit

- [ ] Run full suite:

```
pytest --tb=short -q
```

Expected: all previous tests plus 6 new ones pass.

- [ ] Commit:

```bash
git add deskbridge/dm/__init__.py deskbridge/dm/watcher.py tests/test_dm_watcher.py
git commit -m "feat: DmWatcher — per-identity long-poll with cursor persistence and work_items"
```

---

## Task 3: OutboxDrainer

**Files:**
- Create: `deskbridge/dm/outbox.py`
- Create: `tests/test_outbox_drainer.py`

### Step 3a: Write failing tests

- [ ] Create `tests/test_outbox_drainer.py`:

```python
import asyncio
import json
import pytest
from unittest.mock import AsyncMock, MagicMock
from deskbridge.dm.outbox import OutboxDrainer
from deskbridge.mcp.client import McpToolError
from deskbridge.mcp.errors import RoutingDecision
from deskbridge.models import McpError, McpErrorCategory
from deskbridge.config import IdentityConfig


ALICE = IdentityConfig(label="alice", npub="npub1alice", passphrase_ref="env:X")


async def _seed_outbox(conn, *, id, idempotency_key, dest_pubkey="npub1dest",
                       delivery_attempts=0, delivery_status="pending", identity_id="acc-alice"):
    await conn.execute(
        """
        INSERT INTO outbox
            (id, identity_id, dest_pubkey, message_text, idempotency_key,
             delivery_status, delivery_attempts)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (id, identity_id, dest_pubkey, "hello", idempotency_key,
         delivery_status, delivery_attempts),
    )
    await conn.commit()


def make_drainer(store, db_conn, *, send_dm_mock, session_id="sess-123",
                 drain_interval=0.01, max_attempts=3):
    mock_client = MagicMock()
    mock_client.call_tool = send_dm_mock
    mock_broker = MagicMock()
    mock_broker.get_session_id = AsyncMock(return_value=session_id)
    shutdown_event = asyncio.Event()
    drainer = OutboxDrainer(
        store=store,
        client=mock_client,
        broker=mock_broker,
        identities=[ALICE],
        shutdown_event=shutdown_event,
        drain_interval_secs=drain_interval,
        max_attempts=max_attempts,
    )
    return drainer, shutdown_event, db_conn


async def test_outbox_drainer_delivers_message(store, db_conn):
    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    await _seed_outbox(db_conn, id="ob-1", idempotency_key="k1")

    async def send_and_stop(tool_name, arguments):
        drainer._shutdown_event.set()
        return {"delivered": True}

    drainer, shutdown_event, _ = make_drainer(
        store, db_conn, send_dm_mock=AsyncMock(side_effect=send_and_stop)
    )
    await drainer.run()

    async with db_conn.execute("SELECT delivery_status, delivery_attempts FROM outbox WHERE id='ob-1'") as cur:
        row = await cur.fetchone()
    assert row["delivery_status"] == "delivered"
    assert row["delivery_attempts"] == 1


async def test_outbox_drainer_transient_failure_increments(store, db_conn):
    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    await _seed_outbox(db_conn, id="ob-1", idempotency_key="k1")

    async def fail_and_stop(tool_name, arguments):
        drainer._shutdown_event.set()
        raise McpToolError(
            mcp_error=McpError(category=McpErrorCategory.TRANSIENT_TRANSPORT, message="timeout"),
            routing=RoutingDecision.RETRY,
        )

    drainer, shutdown_event, _ = make_drainer(
        store, db_conn, send_dm_mock=AsyncMock(side_effect=fail_and_stop), max_attempts=3
    )
    await drainer.run()

    async with db_conn.execute("SELECT delivery_status, delivery_attempts FROM outbox WHERE id='ob-1'") as cur:
        row = await cur.fetchone()
    assert row["delivery_status"] == "pending"
    assert row["delivery_attempts"] == 1


async def test_outbox_drainer_last_attempt_marks_failed(store, db_conn):
    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    await _seed_outbox(db_conn, id="ob-1", idempotency_key="k1", delivery_attempts=2)

    async def fail_and_stop(tool_name, arguments):
        drainer._shutdown_event.set()
        raise McpToolError(
            mcp_error=McpError(category=McpErrorCategory.TRANSIENT_TRANSPORT, message="timeout"),
            routing=RoutingDecision.RETRY,
        )

    drainer, shutdown_event, _ = make_drainer(
        store, db_conn, send_dm_mock=AsyncMock(side_effect=fail_and_stop), max_attempts=3
    )
    await drainer.run()

    async with db_conn.execute("SELECT delivery_status, delivery_attempts FROM outbox WHERE id='ob-1'") as cur:
        row = await cur.fetchone()
    assert row["delivery_status"] == "failed"
    assert row["delivery_attempts"] == 3


async def test_outbox_drainer_reject_marks_failed_immediately(store, db_conn):
    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    await _seed_outbox(db_conn, id="ob-1", idempotency_key="k1", delivery_attempts=0)

    async def reject_and_stop(tool_name, arguments):
        drainer._shutdown_event.set()
        raise McpToolError(
            mcp_error=McpError(category=McpErrorCategory.UNSUPPORTED_STATE, message="nope"),
            routing=RoutingDecision.REJECT,
        )

    drainer, shutdown_event, _ = make_drainer(
        store, db_conn, send_dm_mock=AsyncMock(side_effect=reject_and_stop), max_attempts=3
    )
    await drainer.run()

    async with db_conn.execute("SELECT delivery_status, delivery_attempts FROM outbox WHERE id='ob-1'") as cur:
        row = await cur.fetchone()
    assert row["delivery_status"] == "failed"


async def test_outbox_drainer_exhausted_row_not_fetched(store, db_conn):
    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    await _seed_outbox(db_conn, id="ob-1", idempotency_key="k1", delivery_attempts=3)

    mock_client = MagicMock()
    mock_client.call_tool = AsyncMock()
    mock_broker = MagicMock()
    mock_broker.get_session_id = AsyncMock(return_value="sess-123")
    shutdown_event = asyncio.Event()
    shutdown_event.set()
    drainer = OutboxDrainer(
        store=store,
        client=mock_client,
        broker=mock_broker,
        identities=[ALICE],
        shutdown_event=shutdown_event,
        drain_interval_secs=0.01,
        max_attempts=3,
    )
    await drainer.run()
    mock_client.call_tool.assert_not_awaited()


async def test_outbox_drainer_skips_no_dest_pubkey(store, db_conn):
    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    await db_conn.execute(
        "INSERT INTO outbox (id, identity_id, dest_group_id, message_text, idempotency_key) "
        "VALUES ('ob-1', 'acc-alice', 'grp-1', 'hello', 'k1')"
    )
    await db_conn.commit()

    mock_client = MagicMock()
    mock_client.call_tool = AsyncMock()
    mock_broker = MagicMock()
    mock_broker.get_session_id = AsyncMock(return_value="sess-123")
    shutdown_event = asyncio.Event()
    shutdown_event.set()
    drainer = OutboxDrainer(
        store=store,
        client=mock_client,
        broker=mock_broker,
        identities=[ALICE],
        shutdown_event=shutdown_event,
        drain_interval_secs=0.01,
    )
    await drainer.run()
    mock_client.call_tool.assert_not_awaited()


async def test_outbox_drainer_skips_unknown_identity(store, db_conn):
    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    await store.upsert_account(id="acc-bob", npub="npub1bob", label="bob", passphrase_ref="env:Y")
    await _seed_outbox(db_conn, id="ob-1", idempotency_key="k1", identity_id="acc-bob")

    mock_client = MagicMock()
    mock_client.call_tool = AsyncMock()
    mock_broker = MagicMock()
    mock_broker.get_session_id = AsyncMock(return_value="sess-123")
    shutdown_event = asyncio.Event()
    shutdown_event.set()
    drainer = OutboxDrainer(
        store=store,
        client=mock_client,
        broker=mock_broker,
        identities=[ALICE],  # bob not in identities list
        shutdown_event=shutdown_event,
        drain_interval_secs=0.01,
    )
    await drainer.run()
    mock_client.call_tool.assert_not_awaited()


async def test_outbox_drainer_skips_no_session(store, db_conn):
    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    await _seed_outbox(db_conn, id="ob-1", idempotency_key="k1")

    mock_client = MagicMock()
    mock_client.call_tool = AsyncMock()
    mock_broker = MagicMock()
    mock_broker.get_session_id = AsyncMock(return_value=None)
    shutdown_event = asyncio.Event()
    shutdown_event.set()
    drainer = OutboxDrainer(
        store=store,
        client=mock_client,
        broker=mock_broker,
        identities=[ALICE],
        shutdown_event=shutdown_event,
        drain_interval_secs=0.01,
    )
    await drainer.run()
    mock_client.call_tool.assert_not_awaited()
```

- [ ] Run to confirm they fail:

```
pytest tests/test_outbox_drainer.py -v
```

Expected: `ImportError: cannot import name 'OutboxDrainer' from 'deskbridge.dm.outbox'`

### Step 3b: Implement `OutboxDrainer`

- [ ] Create `deskbridge/dm/outbox.py`:

```python
import asyncio
import json
import structlog

from deskbridge.config import IdentityConfig
from deskbridge.db.store import Store
from deskbridge.mcp.client import McpClient, McpToolError
from deskbridge.mcp.errors import RoutingDecision
from deskbridge.mcp.session import SessionBroker

log = structlog.get_logger()


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
        self._store = store
        self._client = client
        self._broker = broker
        self._account_to_label = {f"acc-{i.label}": i.label for i in identities}
        self._shutdown_event = shutdown_event
        self._drain_interval_secs = drain_interval_secs
        self._max_attempts = max_attempts

    async def run(self) -> None:
        log.info("outbox_drainer_started")

        while not self._shutdown_event.is_set():
            rows = await self._store.get_pending_outbox_items(max_attempts=self._max_attempts)
            for row in rows:
                await self._drain_row(row)
            try:
                await asyncio.wait_for(
                    self._shutdown_event.wait(), timeout=self._drain_interval_secs
                )
            except asyncio.TimeoutError:
                pass

        log.info("outbox_drainer_stopped")

    async def _drain_row(self, row) -> None:
        if not row["dest_pubkey"]:
            log.debug("outbox_drainer_skip_no_dest_pubkey", id=row["id"])
            return

        label = self._account_to_label.get(row["identity_id"])
        if label is None:
            log.error(
                "outbox_drainer_unknown_identity",
                id=row["id"],
                identity_id=row["identity_id"],
            )
            return

        session_id = await self._broker.get_session_id(label)
        if session_id is None:
            log.debug("outbox_drainer_no_session", id=row["id"], identity=label)
            return

        try:
            result = await self._client.call_tool(
                "send_dm",
                {
                    "session_id": session_id,
                    "recipient_pubkey": row["dest_pubkey"],
                    "content": row["message_text"],
                    "idempotency_key": row["idempotency_key"],
                },
            )
            await self._store.update_outbox_delivery(
                row["id"], "delivered", json.dumps(result)
            )
            log.info("outbox_drainer_delivered", id=row["id"])

        except McpToolError as e:
            is_permanent = (
                e.routing == RoutingDecision.REJECT
                or row["delivery_attempts"] + 1 >= self._max_attempts
            )
            final_status = "failed" if is_permanent else "pending"
            error_json = json.dumps(
                {"error": e.mcp_error.message, "routing": str(e.routing)}
            )
            await self._store.update_outbox_delivery(row["id"], final_status, error_json)
            log.error(
                "outbox_drainer_send_failed",
                id=row["id"],
                status=final_status,
                routing=e.routing,
            )

        except Exception:
            log.exception("outbox_drainer_unexpected_error", id=row["id"])
            await self._store.update_outbox_delivery(
                row["id"], "pending", json.dumps({"error": "unexpected_error"})
            )
```

- [ ] Run OutboxDrainer tests:

```
pytest tests/test_outbox_drainer.py -v
```

Expected: `8 passed`

### Step 3c: Update dm `__init__.py` and run full suite

- [ ] Update `deskbridge/dm/__init__.py`:

```python
from .watcher import DmWatcher
from .outbox import OutboxDrainer
```

- [ ] Run full suite:

```
pytest --tb=short -q
```

Expected: all previous tests plus 8 new ones pass.

- [ ] Commit:

```bash
git add deskbridge/dm/__init__.py deskbridge/dm/outbox.py tests/test_outbox_drainer.py
git commit -m "feat: OutboxDrainer — shared drain loop with idempotent send_dm delivery"
```

---

## Task 4: Supervisor Wiring

**Files:**
- Modify: `deskbridge/supervisor.py`
- Modify: `tests/test_supervisor.py` (append one new test at the bottom)

### Step 4a: Write failing supervisor test

- [ ] Append to `tests/test_supervisor.py`:

```python
async def test_supervisor_spawns_and_cancels_dm_tasks(tmp_path, monkeypatch, mock_broker, mock_client_ctx):
    from unittest.mock import ANY
    monkeypatch.setenv("ALICE", "pass")
    config = make_config(tmp_path)

    with patch("deskbridge.supervisor.McpClient") as MockMcpClient, \
         patch("deskbridge.supervisor.SessionBroker", return_value=mock_broker), \
         patch("deskbridge.supervisor.apply_schema", new=AsyncMock()), \
         patch("deskbridge.supervisor.bootstrap_accounts_from_config", new=AsyncMock()), \
         patch("deskbridge.supervisor.DmWatcher") as MockDmWatcher, \
         patch("deskbridge.supervisor.OutboxDrainer") as MockOutboxDrainer:

        mock_instance = MockMcpClient.return_value
        mock_instance.connect.return_value = mock_client_ctx
        MockDmWatcher.return_value.run = AsyncMock()
        MockOutboxDrainer.return_value.run = AsyncMock()

        supervisor = Supervisor(config=config)

        async def stop():
            await asyncio.sleep(0.1)
            supervisor.request_shutdown()

        await asyncio.gather(supervisor.run(), stop())

    MockDmWatcher.assert_called_once_with(
        identity_label="alice",
        store=ANY,
        client=ANY,
        broker=mock_broker,
        shutdown_event=ANY,
    )
    MockOutboxDrainer.assert_called_once_with(
        store=ANY,
        client=ANY,
        broker=mock_broker,
        identities=config.identities,
        shutdown_event=ANY,
    )
    MockDmWatcher.return_value.run.assert_called_once()
    MockOutboxDrainer.return_value.run.assert_called_once()
```

- [ ] Run to confirm it fails:

```
pytest tests/test_supervisor.py::test_supervisor_spawns_and_cancels_dm_tasks -v
```

Expected: `ImportError` or `AssertionError` — `DmWatcher` not yet imported in supervisor.

### Step 4b: Wire DmWatcher and OutboxDrainer into supervisor

- [ ] Replace the full contents of `deskbridge/supervisor.py` with:

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
from deskbridge.dm.outbox import OutboxDrainer
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
                drainer_task: asyncio.Task | None = None

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
                    tasks_to_cancel = watcher_tasks + (
                        [drainer_task] if drainer_task is not None else []
                    )
                    for task in tasks_to_cancel:
                        task.cancel()
                    if tasks_to_cancel:
                        await asyncio.gather(*tasks_to_cancel, return_exceptions=True)
                    if is_main:
                        for sig in (signal.SIGTERM, signal.SIGINT):
                            loop.remove_signal_handler(sig)
```

- [ ] Run new supervisor test:

```
pytest tests/test_supervisor.py::test_supervisor_spawns_and_cancels_dm_tasks -v
```

Expected: `1 passed`

### Step 4c: Run full suite and commit

- [ ] Run full suite:

```
pytest --tb=short -q
```

Expected: all tests pass (previous count + 1 new supervisor test).

- [ ] Commit:

```bash
git add deskbridge/supervisor.py tests/test_supervisor.py
git commit -m "feat: supervisor — spawn DmWatcher and OutboxDrainer tasks after unlock_all"
```

---

## Self-Review

**Spec coverage:**

| Spec requirement | Task |
|---|---|
| `upsert_work_item` with INSERT OR IGNORE | Task 1 |
| `get_pending_outbox_items` filters by status and attempts | Task 1 |
| `update_outbox_delivery` increments via SQL, sets delivered_at | Task 1 |
| DmWatcher per-identity loop | Task 2 |
| Cursor loaded once before loop | Task 2 |
| Cursor persisted after each non-empty batch | Task 2 |
| REJECT routing exits run() | Task 2 |
| REAUTH routing sleeps and continues | Task 2 |
| No session skips poll, sleeps 5s responsively | Task 2 |
| Unexpected exception logged, loop continues | Task 2 |
| OutboxDrainer drain loop | Task 3 |
| REJECT marks failed immediately | Task 3 |
| Exhausted attempts marks failed | Task 3 |
| Transient failure increments attempts | Task 3 |
| No dest_pubkey skipped | Task 3 |
| Unknown identity_id skipped | Task 3 |
| No session skips row | Task 3 |
| Supervisor spawns tasks after unlock_all | Task 4 |
| Supervisor cancels and gathers tasks on shutdown | Task 4 |
| Tasks safe to cancel if already exited (REJECT) | Task 4 — `cancel()` on completed task is no-op |

**Placeholder scan:** No TBD, "implement later", or vague steps. All code blocks are complete.

**Type consistency:**
- `DmWatcher.__init__` signature matches usage in supervisor and tests ✓
- `OutboxDrainer.__init__` signature matches usage in supervisor and tests ✓
- `store.upsert_work_item` param names match calls in watcher ✓
- `store.get_pending_outbox_items` / `update_outbox_delivery` match calls in drainer ✓
- `account_id = f"acc-{label}"` used consistently in watcher and drainer ✓
