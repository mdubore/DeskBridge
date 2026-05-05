# DeskBridge Phase 5: Agent Approval Requests — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Allow an agent subprocess to pause and request human approval mid-run; DeskBridge notifies the operator via DM, waits for their response, and signals the decision back to NostrDesk so the agent can continue or abort.

**Architecture:** A new `ApprovalRequestWatcher` polls a new NostrDesk MCP tool for pending approval requests, writes the details to the DB and a notification to the operator's outbox. When the operator replies, the existing `DmWatcher` `_handle_approve`/`_handle_reject` handlers—after updating the DB—call a second new MCP tool to signal the decision back so the blocked agent can unblock.

**Tech Stack:** Python asyncio, aiosqlite, structlog, pytest-asyncio, unittest.mock. The three new MCP tools (`wait_for_pending_approval_requests`, `wait_for_approval_resolution`, `resolve_approval_request`) are defined in the Phase 5 spec and will be implemented by the NostrDesk team; for now they are mocked in tests.

---

## File Map

| File | Action | Purpose |
|---|---|---|
| `deskbridge/dm/approval_watcher.py` | Create | New `ApprovalRequestWatcher` class |
| `deskbridge/dm/watcher.py` | Modify | Store session ID; call `resolve_approval_request` in approve/reject handlers |
| `deskbridge/supervisor.py` | Modify | Spawn `ApprovalRequestWatcher` per identity |
| `tests/test_approval_watcher.py` | Create | Tests for `ApprovalRequestWatcher` |
| `tests/test_dm_watcher.py` | Modify | Tests for new approve/reject MCP call behaviour |
| `tests/test_supervisor.py` | Modify | Test that supervisor spawns `ApprovalRequestWatcher` |

No DB schema changes are needed — `approvals.mcp_approval_id` already exists.

---

## Task 1: ApprovalRequestWatcher

**Files:**
- Create: `deskbridge/dm/approval_watcher.py`
- Create: `tests/test_approval_watcher.py`

### Background

`ApprovalRequestWatcher` mirrors `DmWatcher` in structure. It polls `wait_for_pending_approval_requests` (long-blocking, like `wait_for_new_dms`). For each new request it:
1. Inserts an `approvals` row (using the existing `store.insert_approval`)
2. Writes a DM to the outbox notifying the operator
3. Advances a cursor so it won't re-process the same requests

The existing `store.insert_approval` signature (from `deskbridge/db/store.py:103`):
```python
async def insert_approval(
    self,
    id: str,
    mcp_approval_id: str | None,
    work_item_id: str | None,
    action_description: str,
    scope: str | None,
    request_text: str | None,
    expires_at: str | None,
) -> None:
```

The existing `store.insert_outbox_item` signature (from `deskbridge/db/store.py`):
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
```

The existing `store.get_latest_dispatched_work_item(identity_id)` returns an `aiosqlite.Row | None`.

The existing `store.upsert_cursor` signature:
```python
async def upsert_cursor(
    self,
    cursor_type: str,
    identity_id: str,
    last_entity_id: str | None,
    last_created_at: str | None,
    last_imported_at: str | None,
    raw_json: str,
) -> None:
```

Error routing uses `deskbridge.mcp.errors.RoutingDecision` (REJECT, REAUTH, RESET_CURSOR) and `deskbridge.mcp.client.McpToolError`.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_approval_watcher.py`:

```python
import asyncio
import uuid
from unittest.mock import AsyncMock, MagicMock
from deskbridge.dm.approval_watcher import ApprovalRequestWatcher
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
    return ApprovalRequestWatcher(
        identity_label="alice",
        operator_npub=operator_npub,
        store=store,
        client=mock_client,
        broker=mock_broker,
        shutdown_event=shutdown_event,
        poll_timeout_secs=1,
    )


def _approval_response(*requests, last_id="req-1"):
    return {
        "requests": list(requests),
        "last_request_id": last_id,
    }


def _req(id="req-1", description="Force push to main", triggered_by_tool="git_push",
         created_at="2026-01-01T00:00:00Z"):
    return {"id": id, "description": description,
            "triggered_by_tool": triggered_by_tool, "created_at": created_at}


async def test_approval_watcher_new_request_inserts_approval(store):
    shutdown_event = asyncio.Event()

    async def one_shot(tool_name, arguments):
        shutdown_event.set()
        return _approval_response(_req())

    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    watcher = make_watcher(store, shutdown_event, call_tool_mock=AsyncMock(side_effect=one_shot))
    await watcher.run()

    async with store._conn.execute("SELECT * FROM approvals WHERE mcp_approval_id='req-1'") as cur:
        row = await cur.fetchone()
    assert row is not None
    assert row["mcp_approval_id"] == "req-1"
    assert row["action_description"] == "Force push to main"
    assert row["status"] == "pending"


async def test_approval_watcher_new_request_notifies_operator(store):
    shutdown_event = asyncio.Event()

    async def one_shot(tool_name, arguments):
        shutdown_event.set()
        return _approval_response(_req(id="req-1", description="Delete production DB"))

    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    watcher = make_watcher(store, shutdown_event, call_tool_mock=AsyncMock(side_effect=one_shot))
    await watcher.run()

    async with store._conn.execute("SELECT message_text, dest_pubkey FROM outbox") as cur:
        rows = await cur.fetchall()
    assert len(rows) == 1
    assert "Delete production DB" in rows[0]["message_text"]
    assert rows[0]["dest_pubkey"] == OPERATOR_NPUB


async def test_approval_watcher_cursor_advances(store):
    shutdown_event = asyncio.Event()

    async def one_shot(tool_name, arguments):
        shutdown_event.set()
        return _approval_response(_req(id="req-42"), last_id="req-42")

    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    watcher = make_watcher(store, shutdown_event, call_tool_mock=AsyncMock(side_effect=one_shot))
    await watcher.run()

    cursor_row = await store.get_cursor("approval_watcher", "acc-alice")
    assert cursor_row is not None
    assert cursor_row["last_entity_id"] == "req-42"


async def test_approval_watcher_empty_response_no_insert_no_cursor(store):
    shutdown_event = asyncio.Event()

    async def one_shot(tool_name, arguments):
        shutdown_event.set()
        return {"requests": [], "last_request_id": None}

    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    watcher = make_watcher(store, shutdown_event, call_tool_mock=AsyncMock(side_effect=one_shot))
    await watcher.run()

    async with store._conn.execute("SELECT COUNT(*) FROM approvals") as cur:
        row = await cur.fetchone()
    assert row[0] == 0
    assert await store.get_cursor("approval_watcher", "acc-alice") is None


async def test_approval_watcher_no_session_skips_poll(store):
    shutdown_event = asyncio.Event()
    mock_client = MagicMock()
    mock_client.call_tool = AsyncMock()
    mock_broker = MagicMock()
    mock_broker.get_session_id = AsyncMock(return_value=None)

    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    watcher = ApprovalRequestWatcher(
        identity_label="alice",
        operator_npub=OPERATOR_NPUB,
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


async def test_approval_watcher_reject_exits_cleanly(store):
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
```

- [ ] **Step 2: Run to confirm all tests fail**

```bash
cd /home/missydog/Desktop/Learnding/Tools/DeskBridge
python -m pytest tests/test_approval_watcher.py -v 2>&1 | tail -20
```

Expected: ImportError or collection error — `ApprovalRequestWatcher` does not exist yet.

- [ ] **Step 3: Implement `deskbridge/dm/approval_watcher.py`**

Create the file with this content:

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


class ApprovalRequestWatcher:
    def __init__(
        self,
        identity_label: str,
        operator_npub: str | None,
        store: Store,
        client: McpClient,
        broker: SessionBroker,
        shutdown_event: asyncio.Event,
        poll_timeout_secs: int = 30,
    ) -> None:
        self._identity_label = identity_label
        self._account_id = f"acc-{identity_label}"
        self._operator_npub = operator_npub
        self._store = store
        self._client = client
        self._broker = broker
        self._shutdown_event = shutdown_event
        self._poll_timeout_secs = poll_timeout_secs

    async def run(self) -> None:
        cursor_row = await self._store.get_cursor(
            cursor_type="approval_watcher", identity_id=self._account_id
        )
        after_request_id: str | None = cursor_row["last_entity_id"] if cursor_row else None

        log.info("approval_watcher_started", identity=self._identity_label)

        while not self._shutdown_event.is_set():
            session_id = await self._broker.get_session_id(self._identity_label)
            if session_id is None:
                log.debug("approval_watcher_no_session", identity=self._identity_label)
                try:
                    await asyncio.wait_for(self._shutdown_event.wait(), timeout=5.0)
                except asyncio.TimeoutError:
                    pass
                continue

            try:
                result = await self._client.call_tool(
                    "wait_for_pending_approval_requests",
                    {
                        "session_id": session_id,
                        "after_request_id": after_request_id,
                        "timeout_seconds": self._poll_timeout_secs,
                    },
                )
                requests = result.get("requests", [])
                for req in requests:
                    dispatched = await self._store.get_latest_dispatched_work_item(
                        self._account_id
                    )
                    work_item_id = dispatched["id"] if dispatched else None
                    await self._store.insert_approval(
                        id=str(uuid.uuid4()),
                        mcp_approval_id=req["id"],
                        work_item_id=work_item_id,
                        action_description=req["description"],
                        scope=None,
                        request_text=None,
                        expires_at=None,
                    )
                    if self._operator_npub:
                        message = (
                            f"Approval required: {req['description']}\n\n"
                            f"Request ID: {req['id']}\n\n"
                            f"Reply 'approve' or 'reject'."
                        )
                        await self._store.insert_outbox_item(
                            str(uuid.uuid4()),
                            self._account_id,
                            self._operator_npub,
                            message,
                            f"approval-notify-{req['id']}",
                        )

                if requests:
                    new_cursor_id = result.get("last_request_id")
                    if new_cursor_id:
                        after_request_id = new_cursor_id
                        await self._store.upsert_cursor(
                            cursor_type="approval_watcher",
                            identity_id=self._account_id,
                            last_entity_id=after_request_id,
                            last_created_at=None,
                            last_imported_at=None,
                            raw_json=json.dumps(result),
                        )

            except McpToolError as e:
                if e.routing == RoutingDecision.REJECT:
                    log.error(
                        "approval_watcher_rejected",
                        identity=self._identity_label,
                        message=e.mcp_error.message,
                    )
                    return
                elif e.routing == RoutingDecision.REAUTH:
                    log.warning("approval_watcher_reauth", identity=self._identity_label)
                elif e.routing == RoutingDecision.RESET_CURSOR:
                    log.warning(
                        "approval_watcher_reset_cursor",
                        identity=self._identity_label,
                        message=e.mcp_error.message,
                    )
                    after_request_id = None
                else:
                    log.error(
                        "approval_watcher_error",
                        identity=self._identity_label,
                        routing=e.routing,
                        message=e.mcp_error.message,
                    )
                try:
                    await asyncio.wait_for(self._shutdown_event.wait(), timeout=5.0)
                except asyncio.TimeoutError:
                    pass

            except Exception:
                log.exception("approval_watcher_unexpected_error", identity=self._identity_label)
                try:
                    await asyncio.wait_for(self._shutdown_event.wait(), timeout=5.0)
                except asyncio.TimeoutError:
                    pass

        log.info("approval_watcher_stopped", identity=self._identity_label)
```

- [ ] **Step 4: Run tests to confirm they pass**

```bash
python -m pytest tests/test_approval_watcher.py -v 2>&1 | tail -20
```

Expected: 6 PASSED

- [ ] **Step 5: Run full suite to check for regressions**

```bash
python -m pytest --tb=short -q 2>&1 | tail -10
```

Expected: all tests pass (165+6 = 171 passed)

- [ ] **Step 6: Commit**

```bash
git add deskbridge/dm/approval_watcher.py tests/test_approval_watcher.py
git commit -m "feat: add ApprovalRequestWatcher — polls MCP for pending approvals, notifies operator via DM"
```

---

## Task 2: DmWatcher — call `resolve_approval_request` on approve/reject

**Files:**
- Modify: `deskbridge/dm/watcher.py`
- Modify: `tests/test_dm_watcher.py`

### Background

Two changes to `deskbridge/dm/watcher.py`:

1. Track `session_id` as an instance variable so `_handle_approve`/`_handle_reject` can use it when calling `resolve_approval_request`.

2. After resolving an approval in the DB, check `row["mcp_approval_id"]`; if set, call `resolve_approval_request` on the MCP server. Any exception from that call is logged and swallowed — the DB update already recorded the operator's intent.

The existing `_handle_approve` and `_handle_reject` methods start at lines 197 and 215 respectively.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_dm_watcher.py`:

```python
async def test_dm_watcher_approve_calls_resolve_when_mcp_approval_id_set(store, db_conn):
    shutdown_event = asyncio.Event()
    resolve_calls = []

    async def call_tool_side_effect(tool_name, arguments):
        if tool_name == "wait_for_new_dms":
            shutdown_event.set()
            return _dm_response(_msg(id="msg-2", content="yes go ahead"))
        elif tool_name == "resolve_approval_request":
            resolve_calls.append(arguments)
            return {"ok": True}

    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    await db_conn.execute(
        "INSERT INTO work_items (id, source_type, source_id, identity_id, status, idempotency_key) "
        "VALUES ('wi-1', 'dm', 'wi-1', 'acc-alice', 'pending', 'idem-wi-1')"
    )
    await db_conn.execute(
        "INSERT INTO approvals (id, work_item_id, action_description, status, mcp_approval_id) "
        "VALUES ('appr-1', 'wi-1', 'force push', 'pending', 'req-ext-1')"
    )
    await db_conn.commit()

    watcher = make_watcher(store, shutdown_event,
                           call_tool_mock=AsyncMock(side_effect=call_tool_side_effect))
    await watcher.run()

    assert len(resolve_calls) == 1
    assert resolve_calls[0]["request_id"] == "req-ext-1"
    assert resolve_calls[0]["decision"] == "approved"
    assert resolve_calls[0]["session_id"] == "sess-123"


async def test_dm_watcher_approve_skips_resolve_when_no_mcp_approval_id(store, db_conn):
    shutdown_event = asyncio.Event()
    resolve_calls = []

    async def call_tool_side_effect(tool_name, arguments):
        if tool_name == "wait_for_new_dms":
            shutdown_event.set()
            return _dm_response(_msg(id="msg-2", content="yes go ahead"))
        elif tool_name == "resolve_approval_request":
            resolve_calls.append(arguments)
            return {"ok": True}

    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    await db_conn.execute(
        "INSERT INTO work_items (id, source_type, source_id, identity_id, status, idempotency_key) "
        "VALUES ('wi-1', 'dm', 'wi-1', 'acc-alice', 'pending', 'idem-wi-1')"
    )
    await db_conn.execute(
        "INSERT INTO approvals (id, work_item_id, action_description, status) "
        "VALUES ('appr-1', 'wi-1', 'force push', 'pending')"
    )
    await db_conn.commit()

    watcher = make_watcher(store, shutdown_event,
                           call_tool_mock=AsyncMock(side_effect=call_tool_side_effect))
    await watcher.run()

    assert len(resolve_calls) == 0


async def test_dm_watcher_reject_calls_resolve_when_mcp_approval_id_set(store, db_conn):
    shutdown_event = asyncio.Event()
    resolve_calls = []

    async def call_tool_side_effect(tool_name, arguments):
        if tool_name == "wait_for_new_dms":
            shutdown_event.set()
            return _dm_response(_msg(id="msg-2", content="no, reject that"))
        elif tool_name == "resolve_approval_request":
            resolve_calls.append(arguments)
            return {"ok": True}

    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    await db_conn.execute(
        "INSERT INTO work_items (id, source_type, source_id, identity_id, status, idempotency_key) "
        "VALUES ('wi-1', 'dm', 'wi-1', 'acc-alice', 'pending', 'idem-wi-1')"
    )
    await db_conn.execute(
        "INSERT INTO approvals (id, work_item_id, action_description, status, mcp_approval_id) "
        "VALUES ('appr-1', 'wi-1', 'force push', 'pending', 'req-ext-2')"
    )
    await db_conn.commit()

    watcher = make_watcher(store, shutdown_event,
                           call_tool_mock=AsyncMock(side_effect=call_tool_side_effect))
    await watcher.run()

    assert len(resolve_calls) == 1
    assert resolve_calls[0]["request_id"] == "req-ext-2"
    assert resolve_calls[0]["decision"] == "rejected"
```

- [ ] **Step 2: Run to confirm all new tests fail**

```bash
python -m pytest tests/test_dm_watcher.py::test_dm_watcher_approve_calls_resolve_when_mcp_approval_id_set tests/test_dm_watcher.py::test_dm_watcher_approve_skips_resolve_when_no_mcp_approval_id tests/test_dm_watcher.py::test_dm_watcher_reject_calls_resolve_when_mcp_approval_id_set -v 2>&1 | tail -15
```

Expected: 3 FAILED (assert len(resolve_calls) == 1 fails, resolve_calls is empty)

- [ ] **Step 3: Update `deskbridge/dm/watcher.py`**

**Change 1** — add `self._session_id` to `__init__`. The constructor currently ends at line 33 (`self._poll_timeout_secs = poll_timeout_secs`). Add one line after it:

```python
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
        self._session_id: str | None = None
```

**Change 2** — in `run()`, after getting `session_id` from the broker (line 45), store it:

Replace:
```python
            session_id = await self._broker.get_session_id(self._identity_label)
            if session_id is None:
```

With:
```python
            session_id = await self._broker.get_session_id(self._identity_label)
            self._session_id = session_id
            if session_id is None:
```

**Change 3** — replace `_handle_approve` entirely:

```python
    async def _handle_approve(self, msg: dict) -> None:
        try:
            row = await self._store.get_pending_approval(self._account_id)
            if row is None:
                reply = "No pending approval to approve."
            else:
                await self._store.resolve_approval(row["id"], "approved")
                reply = "Approved."
                mcp_approval_id = row["mcp_approval_id"]
                if mcp_approval_id and self._session_id:
                    try:
                        await self._client.call_tool(
                            "resolve_approval_request",
                            {
                                "session_id": self._session_id,
                                "request_id": mcp_approval_id,
                                "decision": "approved",
                            },
                        )
                    except Exception:
                        log.exception(
                            "dm_watcher_resolve_approval_error",
                            identity=self._identity_label,
                        )
            await self._store.insert_outbox_item(
                str(uuid.uuid4()),
                self._account_id,
                msg["sender_pubkey"],
                reply,
                f"approve-reply-{msg['id']}",
            )
        except Exception:
            log.exception("dm_watcher_handle_approve_error", identity=self._identity_label)
```

**Change 4** — replace `_handle_reject` entirely:

```python
    async def _handle_reject(self, msg: dict) -> None:
        try:
            row = await self._store.get_pending_approval(self._account_id)
            if row is None:
                reply = "No pending approval to reject."
            else:
                await self._store.resolve_approval(row["id"], "rejected")
                reply = "Rejected."
                mcp_approval_id = row["mcp_approval_id"]
                if mcp_approval_id and self._session_id:
                    try:
                        await self._client.call_tool(
                            "resolve_approval_request",
                            {
                                "session_id": self._session_id,
                                "request_id": mcp_approval_id,
                                "decision": "rejected",
                            },
                        )
                    except Exception:
                        log.exception(
                            "dm_watcher_resolve_approval_error",
                            identity=self._identity_label,
                        )
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

- [ ] **Step 4: Run the new tests to confirm they pass**

```bash
python -m pytest tests/test_dm_watcher.py -v 2>&1 | tail -25
```

Expected: all 18 tests pass (15 existing + 3 new)

- [ ] **Step 5: Run full suite to check for regressions**

```bash
python -m pytest --tb=short -q 2>&1 | tail -10
```

Expected: all 174 tests pass

- [ ] **Step 6: Commit**

```bash
git add deskbridge/dm/watcher.py tests/test_dm_watcher.py
git commit -m "feat: DmWatcher calls resolve_approval_request MCP tool after approve/reject"
```

---

## Task 3: Supervisor — spawn ApprovalRequestWatcher per identity

**Files:**
- Modify: `deskbridge/supervisor.py`
- Modify: `tests/test_supervisor.py`

### Background

`supervisor.py` currently spawns `DmWatcher`, `GroupWatcher` (conditional), `OutboxDrainer`, and `WorkItemPoller` per identity. We add `ApprovalRequestWatcher` alongside `DmWatcher` — one per identity, unconditional.

The existing pattern is to keep a `list[asyncio.Task]`, create tasks in the `try` block, and cancel them all in `finally`. Follow this exact pattern.

- [ ] **Step 1: Write the failing test**

Append to `tests/test_supervisor.py`:

```python
async def test_supervisor_spawns_approval_watcher(tmp_path, monkeypatch, mock_broker, mock_client_ctx):
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
         patch("deskbridge.supervisor.ApprovalRequestWatcher") as MockApprovalWatcher, \
         patch("deskbridge.supervisor.Store") as MockStore:

        mock_instance = MockMcpClient.return_value
        mock_instance.connect.return_value = mock_client_ctx

        async def never_finishes():
            await asyncio.Event().wait()

        MockDmWatcher.return_value.run = Mock(side_effect=never_finishes)
        MockOutboxDrainer.return_value.run = Mock(side_effect=never_finishes)
        MockWorkItemPoller.return_value.run = Mock(side_effect=never_finishes)
        MockApprovalWatcher.return_value.run = Mock(side_effect=never_finishes)

        mock_store_instance = AsyncMock()
        mock_store_instance.get_project_groups = AsyncMock(return_value=[])
        MockStore.return_value = mock_store_instance

        supervisor = Supervisor(config=config)

        async def stop():
            await asyncio.sleep(0.1)
            supervisor.request_shutdown()

        await asyncio.gather(supervisor.run(), stop())

    MockApprovalWatcher.assert_called_once_with(
        identity_label="alice",
        operator_npub=None,
        store=ANY,
        client=ANY,
        broker=mock_broker,
        shutdown_event=ANY,
    )
    MockApprovalWatcher.return_value.run.assert_called_once()
```

- [ ] **Step 2: Run to confirm the new test fails**

```bash
python -m pytest tests/test_supervisor.py::test_supervisor_spawns_approval_watcher -v 2>&1 | tail -15
```

Expected: FAILED — `MockApprovalWatcher.assert_called_once_with` raises `AssertionError` because `ApprovalRequestWatcher` is not imported or called yet.

- [ ] **Step 3: Update `deskbridge/supervisor.py`**

**Change 1** — add import after the `GroupWatcher` import line:

```python
from deskbridge.dm.approval_watcher import ApprovalRequestWatcher
```

**Change 2** — add a task list variable alongside the existing ones (after `group_watcher_tasks`):

```python
                watcher_tasks: list[asyncio.Task] = []
                group_watcher_tasks: list[asyncio.Task] = []
                approval_watcher_tasks: list[asyncio.Task] = []
                drainer_task: asyncio.Task | None = None
                poller_tasks: list[asyncio.Task] = []
```

**Change 3** — spawn `ApprovalRequestWatcher` tasks right after spawning `watcher_tasks` (the DmWatcher tasks). Insert after the `watcher_tasks = [...]` list comprehension:

```python
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
```

**Change 4** — add `approval_watcher_tasks` to the cancellation list in `finally`:

```python
                    tasks_to_cancel = (
                        watcher_tasks
                        + group_watcher_tasks
                        + approval_watcher_tasks
                        + poller_tasks
                        + ([drainer_task] if drainer_task is not None else [])
                    )
```

- [ ] **Step 4: Run the new test to confirm it passes**

```bash
python -m pytest tests/test_supervisor.py::test_supervisor_spawns_approval_watcher -v 2>&1 | tail -10
```

Expected: 1 PASSED

- [ ] **Step 5: Run full supervisor tests**

```bash
python -m pytest tests/test_supervisor.py -v 2>&1 | tail -15
```

Expected: all 7 tests pass

- [ ] **Step 6: Run full suite**

```bash
python -m pytest --tb=short -q 2>&1 | tail -10
```

Expected: all 175 tests pass

- [ ] **Step 7: Commit**

```bash
git add deskbridge/supervisor.py tests/test_supervisor.py
git commit -m "feat: supervisor spawns ApprovalRequestWatcher per identity"
```
