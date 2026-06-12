# DeskBridge Phase 12: Group Bootstrap + Operator CLI Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `GroupWatcher` actually start (config-driven group ids) and give the operator terminal commands — `cancel`, `retry`, `approve`, `reject` — plus IDs in `deskbridge status` so those commands are usable.

**Architecture:** `ProjectConfig` gains a `groups` list that bootstrap writes into `projects.groups_json` (config is authoritative; the never-built "relay sync" expectation is dropped). The CLI never talks to MCP: `cancel`/`retry` are pure SQLite state transitions the existing poller already honors, while `approve`/`reject` queue a decision by flipping the approval row to `approve_requested`/`reject_requested`; a new supervisor-side `ApprovalDecisionPoller` forwards queued decisions to MCP through a shared resolution helper extracted from `DmWatcher._call_respond_to_approval`.

**Tech Stack:** Python 3.12, aiosqlite, pydantic v2, click, structlog, pytest-asyncio (asyncio_mode=auto), uv.

---

## Design Decisions (locked in)

1. **Groups are config-authoritative.** Nothing has ever written `projects.groups_json` (the "populated by relay sync" comment in `store.py` is aspirational dead weight). Config becomes the single source: bootstrap overwrites `groups_json` on every start, so removing a group from config stops watching it. The existing test `test_bootstrap_project_upsert_preserves_groups_json` is **replaced**, not kept.
2. **The CLI process never talks to MCP** (preserves the supervisor-only-MCP invariant). `approve`/`reject` queue decisions in SQLite; the supervisor forwards them. If the supervisor is down, decisions sit queued — the CLI says so.
3. **Cancel semantics:** `pending` → CLI sets `cancelled` directly (it isn't running). `dispatched` → CLI sets `cancel_requested`; the existing `WorkItemPoller` cancels the run and emits the terminal audit. `mark_work_item_cancel_requested` gains a `status = 'dispatched'` guard and a bool return.
4. **Retry semantics:** operator retry is a fresh start — `status='pending'`, `attempt_count=0`, `next_retry_at=NULL`, allowed only from `failed`/`cancelled`/`interrupted`. (This is distinct from the poller's internal `retry_work_item`, which increments `attempt_count`.)
5. **Approval statuses gain two transient values:** `approve_requested` and `reject_requested`. `get_pending_approval` (used by DM approve/reject) already filters `status = 'pending'`, so queued rows are invisible to the DM flow — no double-resolution.
6. **New audit event types:** `work_item_cancel_requested`, `approval_decision_requested`. Reused: `work_item_terminal`, `work_item_retry_queued`, `approval_resolved` (all CLI-originated payloads carry `"via": "cli"`).

## File Map

| File | Change |
|---|---|
| `deskbridge/config.py` | `groups: list[str]` field on `ProjectConfig` |
| `deskbridge/db/store.py` | `groups_json` in `upsert_project`/bootstrap; guard+bool on `mark_work_item_cancel_requested`; add `cancel_pending_work_item`, `reset_work_item_for_retry`, `request_approval_decision`, `get_requested_approval_decisions` |
| `deskbridge/dm/approval_resolution.py` | **New** — `resolve_approval_via_mcp` shared helper (moved from `DmWatcher`) |
| `deskbridge/dm/watcher.py` | `_call_respond_to_approval` delegates to the helper |
| `deskbridge/dm/approval_decision_poller.py` | **New** — `ApprovalDecisionPoller` forwards queued CLI decisions |
| `deskbridge/supervisor.py` | Spawn `ApprovalDecisionPoller` per identity |
| `deskbridge/cli.py` | Shared helpers; `cancel`/`retry`/`approve`/`reject` commands; status shows IDs |
| `deskbridge.example.toml` | `groups = []` example |
| `CLAUDE.md` | Commands, architecture list, invariants |
| `tests/test_config.py` | groups field tests |
| `tests/test_store.py` | groups-authoritative tests (replaces preservation test); new method tests |
| `tests/test_approval_decision_poller.py` | **New** — poller tests |
| `tests/test_supervisor.py` | poller spawn test |
| `tests/test_cli.py` | command tests + status-IDs test |

Note: the full suite currently takes ~11 minutes. Steps run targeted test files; the full suite runs once, in the final task.

---

### Task 1: Config — `groups` field on `ProjectConfig`

**Files:**
- Modify: `deskbridge/config.py`
- Test: `tests/test_config.py`

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_config.py` after the `max_agent_attempts` tests:

```python
def test_project_groups_default_empty(tmp_path):
    cfg_file = tmp_path / "config.toml"
    cfg_file.write_text(MINIMAL_CONFIG)
    config = load_config(cfg_file)
    assert config.projects[0].groups == []


def test_project_groups_parsed_from_toml(tmp_path):
    custom = MINIMAL_CONFIG.replace(
        'escalation_dm_target = "npub1human"',
        'escalation_dm_target = "npub1human"\ngroups = ["grp-1", "grp-2"]',
    )
    cfg_file = tmp_path / "config.toml"
    cfg_file.write_text(custom)
    config = load_config(cfg_file)
    assert config.projects[0].groups == ["grp-1", "grp-2"]
```

- [ ] **Step 2: Run tests to verify they fail**

```
uv run pytest tests/test_config.py::test_project_groups_default_empty tests/test_config.py::test_project_groups_parsed_from_toml -v
```

Expected: 2 failures — `ProjectConfig` has no attribute `groups`.

- [ ] **Step 3: Add the field to `ProjectConfig` in `deskbridge/config.py`**

After the `boards` field (line 80), add:

```python
    groups: list[str] = Field(default_factory=list)
```

- [ ] **Step 4: Run tests to verify they pass**

```
uv run pytest tests/test_config.py -v
```

Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add deskbridge/config.py tests/test_config.py
git commit -m "feat: add groups list to ProjectConfig"
```

---

### Task 2: Store — config-authoritative `groups_json`

**Files:**
- Modify: `deskbridge/db/store.py`
- Test: `tests/test_store.py`

Context: `upsert_project` currently excludes `groups_json` with the comment "populated by relay sync, must survive restarts". Relay sync was never built; config becomes authoritative. The signature gains a required `groups_json` param, so the two existing `upsert_project` tests must be updated in the same step as the new tests (they'd otherwise fail with a `TypeError` once the implementation lands).

- [ ] **Step 1: Write the failing tests and update call sites in `tests/test_store.py`**

**(a)** Replace `test_bootstrap_project_upsert_preserves_groups_json` (line 794) entirely with:

```python
async def test_bootstrap_project_groups_config_is_authoritative(tmp_path):
    config = DeskBridgeConfig(
        supervisor=SupervisorConfig(db_path=str(tmp_path / "test.db")),
        mcp=McpConfig(command="nostrdesk-mcp"),
        identities=[IdentityConfig(label="alice", npub="npub1alice", passphrase_ref="env:X")],
        projects=[ProjectConfig(
            id="proj-1", name="MyProj", repo_path="/repo",
            identity="alice", escalation_dm_target="npub1op",
            groups=["grp-1"],
        )],
    )
    db_path = tmp_path / "test.db"
    async with aiosqlite.connect(db_path) as conn:
        conn.row_factory = aiosqlite.Row
        await apply_schema(conn)
        store = Store(conn)
        await bootstrap_accounts_from_config(store=store, config=config)
        row = await store.get_project_for_identity("acc-alice")
        assert json.loads(row["groups_json"]) == ["grp-1"]
        # Manual/stale db edits are overwritten on restart — config wins
        await conn.execute(
            "UPDATE projects SET groups_json = ? WHERE id = ?",
            ('["grp-stale"]', "proj-1"),
        )
        await conn.commit()
        await bootstrap_accounts_from_config(store=store, config=config)
        row = await store.get_project_for_identity("acc-alice")
    assert json.loads(row["groups_json"]) == ["grp-1"], (
        "config groups must overwrite stale db state on restart"
    )
```

**(b)** Add a direct store test after `test_upsert_project_writes_openclaw_agent_id` (line 878):

```python
async def test_upsert_project_writes_groups_json(store: Store, db_conn):
    await store.upsert_account(
        id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:A"
    )
    await store.upsert_project(
        id="proj-1",
        name="MyProject",
        repo_path="/repo",
        identity_id="acc-alice",
        adapter="claude-code",
        openclaw_agent_id=None,
        boards_json="[]",
        groups_json='["grp-1", "grp-2"]',
        allowed_actions_json="[]",
        escalation_dm_target=None,
    )
    assert await store.get_project_groups("acc-alice") == ["grp-1", "grp-2"]
```

**(c)** Update the two existing `upsert_project` tests to pass the new required kwarg. In `test_upsert_project_writes_adapter` (line 858) and `test_upsert_project_writes_openclaw_agent_id` (line 878), add this line directly after `boards_json="[]",`:

```python
        groups_json="[]",
```

- [ ] **Step 2: Run tests to verify they fail**

```
uv run pytest tests/test_store.py::test_bootstrap_project_groups_config_is_authoritative tests/test_store.py::test_upsert_project_writes_groups_json -v
```

Expected: 2 failures — `upsert_project` got an unexpected keyword argument `groups_json` / stale groups survive.

- [ ] **Step 3: Update `upsert_project` and `bootstrap_accounts_from_config` in `deskbridge/db/store.py`**

Replace the whole `upsert_project` method (lines 34–67) with:

```python
    async def upsert_project(
        self,
        id: str,
        name: str,
        repo_path: str,
        identity_id: str,
        adapter: str,
        openclaw_agent_id: str | None,
        boards_json: str,
        groups_json: str,
        allowed_actions_json: str,
        escalation_dm_target: str | None,
    ) -> None:
        async with self._conn.execute(
            """
            INSERT INTO projects
                (id, name, repo_path, identity_id, adapter, openclaw_agent_id,
                 boards_json, groups_json, allowed_actions_json, escalation_dm_target)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                name                 = excluded.name,
                repo_path            = excluded.repo_path,
                identity_id          = excluded.identity_id,
                adapter              = excluded.adapter,
                openclaw_agent_id    = excluded.openclaw_agent_id,
                boards_json          = excluded.boards_json,
                groups_json          = excluded.groups_json,
                allowed_actions_json = excluded.allowed_actions_json,
                escalation_dm_target = excluded.escalation_dm_target
            """,
            (id, name, repo_path, identity_id, adapter, openclaw_agent_id,
             boards_json, groups_json, allowed_actions_json, escalation_dm_target),
        ):
            pass
        await self._conn.commit()
```

(The `-- groups_json excluded: populated by relay sync` comment is gone.)

In `bootstrap_accounts_from_config` (line 453), add one kwarg to the `upsert_project` call, directly after `boards_json=json.dumps(project.boards),`:

```python
            groups_json=json.dumps(project.groups),
```

- [ ] **Step 4: Run the store suite**

```
uv run pytest tests/test_store.py -v
```

Expected: all PASS (including the two updated `upsert_project` tests).

- [ ] **Step 5: Run the supervisor suite (bootstrap behavior changed)**

```
uv run pytest tests/test_supervisor.py -v
```

Expected: all PASS — supervisor reads `get_project_groups` after bootstrap, so configured groups now reach `GroupWatcher` with no supervisor change.

- [ ] **Step 6: Commit**

```bash
git add deskbridge/db/store.py tests/test_store.py
git commit -m "feat: make project groups config-authoritative via groups_json bootstrap"
```

---

### Task 3: Store — work-item cancel/retry operations

**Files:**
- Modify: `deskbridge/db/store.py`
- Test: `tests/test_store.py`

Context: `mark_work_item_cancel_requested` (store.py line 403) currently updates unconditionally and returns `None`. It gains a `status = 'dispatched'` guard and a bool return — its existing test seeds a dispatched item, so it stays green. Two new guarded methods support the CLI. Callers in `dm/watcher.py:185` and `dm/group_watcher.py:206` ignore the return value and only call it on dispatched/cancel_requested rows, so no watcher change is needed.

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_store.py` after `test_mark_work_item_cancel_requested_updates_status` (line 625):

```python
async def test_mark_work_item_cancel_requested_returns_true_from_dispatched(store: Store, db_conn):
    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    await _seed_work_item(store, status="dispatched")
    assert await store.mark_work_item_cancel_requested("wi-1") is True


async def test_mark_work_item_cancel_requested_refuses_pending(store: Store, db_conn):
    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    await _seed_work_item(store)
    assert await store.mark_work_item_cancel_requested("wi-1") is False
    row = await store.get_work_item("wi-1")
    assert row["status"] == "pending"


async def test_cancel_pending_work_item_sets_cancelled(store: Store, db_conn):
    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    await _seed_work_item(store)
    assert await store.cancel_pending_work_item("wi-1") is True
    row = await store.get_work_item("wi-1")
    assert row["status"] == "cancelled"


async def test_cancel_pending_work_item_refuses_dispatched(store: Store, db_conn):
    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    await _seed_work_item(store, status="dispatched")
    assert await store.cancel_pending_work_item("wi-1") is False
    row = await store.get_work_item("wi-1")
    assert row["status"] == "dispatched"


async def test_reset_work_item_for_retry_resets_all_fields(store: Store, db_conn):
    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    await _seed_work_item(store, status="failed")
    async with db_conn.execute(
        "UPDATE work_items SET attempt_count = 2, next_retry_at = '2099-01-01T00:00:00Z' WHERE id = 'wi-1'"
    ):
        pass
    await db_conn.commit()
    assert await store.reset_work_item_for_retry("wi-1") is True
    row = await store.get_work_item("wi-1")
    assert row["status"] == "pending"
    assert row["attempt_count"] == 0
    assert row["next_retry_at"] is None


async def test_reset_work_item_for_retry_refuses_dispatched(store: Store, db_conn):
    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    await _seed_work_item(store, status="dispatched")
    assert await store.reset_work_item_for_retry("wi-1") is False


async def test_reset_work_item_for_retry_allows_interrupted(store: Store, db_conn):
    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    await _seed_work_item(store, status="interrupted")
    assert await store.reset_work_item_for_retry("wi-1") is True
    row = await store.get_work_item("wi-1")
    assert row["status"] == "pending"
```

- [ ] **Step 2: Run tests to verify they fail**

```
uv run pytest tests/test_store.py -k "cancel_requested_returns_true or cancel_requested_refuses or cancel_pending or reset_work_item" -v
```

Expected: 7 failures — bool returns missing, methods missing.

- [ ] **Step 3: Implement in `deskbridge/db/store.py`**

Replace `mark_work_item_cancel_requested` (lines 403–414) with:

```python
    async def mark_work_item_cancel_requested(self, id: str) -> bool:
        async with self._conn.execute(
            """
            UPDATE work_items
            SET status = 'cancel_requested',
                updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
            WHERE id = ? AND status = 'dispatched'
            """,
            (id,),
        ) as cur:
            updated = cur.rowcount > 0
        await self._conn.commit()
        return updated
```

Add the two new methods immediately after it:

```python
    async def cancel_pending_work_item(self, id: str) -> bool:
        async with self._conn.execute(
            """
            UPDATE work_items
            SET status = 'cancelled',
                updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
            WHERE id = ? AND status = 'pending'
            """,
            (id,),
        ) as cur:
            updated = cur.rowcount > 0
        await self._conn.commit()
        return updated

    async def reset_work_item_for_retry(self, id: str) -> bool:
        async with self._conn.execute(
            """
            UPDATE work_items
            SET status = 'pending',
                attempt_count = 0,
                next_retry_at = NULL,
                updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
            WHERE id = ? AND status IN ('failed', 'cancelled', 'interrupted')
            """,
            (id,),
        ) as cur:
            updated = cur.rowcount > 0
        await self._conn.commit()
        return updated
```

- [ ] **Step 4: Run store + dm-watcher suites (callers of the guarded method)**

```
uv run pytest tests/test_store.py tests/test_dm_watcher.py tests/test_group_watcher.py -v
```

Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add deskbridge/db/store.py tests/test_store.py
git commit -m "feat: add guarded cancel/retry work-item store operations"
```

---

### Task 4: Store — approval decision queue operations

**Files:**
- Modify: `deskbridge/db/store.py`
- Test: `tests/test_store.py`

Context: identity scoping mirrors `get_pending_approval` (store.py line 416) — match via the work item's `identity_id`, or via `approvals.identity_id` when there is no work item.

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_store.py` after the `get_pending_approval` tests:

```python
async def test_request_approval_decision_from_pending(store: Store, db_conn):
    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    await db_conn.execute(
        "INSERT INTO approvals (id, identity_id, action_description, status) "
        "VALUES ('appr-1', 'acc-alice', 'pay invoice', 'pending')"
    )
    await db_conn.commit()
    assert await store.request_approval_decision("appr-1", "approve_requested") is True
    row = await store.get_approval("appr-1")
    assert row["status"] == "approve_requested"


async def test_request_approval_decision_refuses_resolved(store: Store, db_conn):
    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    await db_conn.execute(
        "INSERT INTO approvals (id, identity_id, action_description, status) "
        "VALUES ('appr-1', 'acc-alice', 'pay invoice', 'approved')"
    )
    await db_conn.commit()
    assert await store.request_approval_decision("appr-1", "reject_requested") is False
    row = await store.get_approval("appr-1")
    assert row["status"] == "approved"


async def test_request_approval_decision_rejects_unknown_decision(store: Store):
    with pytest.raises(ValueError):
        await store.request_approval_decision("appr-1", "approved")


async def test_get_requested_approval_decisions_scopes_by_identity(store: Store, db_conn):
    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    await store.upsert_account(id="acc-bob", npub="npub1bob", label="bob", passphrase_ref="env:Y")
    await db_conn.execute(
        "INSERT INTO approvals (id, identity_id, action_description, status) "
        "VALUES ('appr-1', 'acc-alice', 'a', 'approve_requested')"
    )
    await db_conn.execute(
        "INSERT INTO approvals (id, identity_id, action_description, status) "
        "VALUES ('appr-2', 'acc-bob', 'b', 'reject_requested')"
    )
    await db_conn.commit()
    rows = await store.get_requested_approval_decisions("acc-alice")
    assert [r["id"] for r in rows] == ["appr-1"]


async def test_get_requested_approval_decisions_via_work_item_identity(store: Store, db_conn):
    await store.upsert_account(id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X")
    await _seed_work_item_phase4(db_conn, id="wi-1", identity_id="acc-alice")
    await db_conn.execute(
        "INSERT INTO approvals (id, work_item_id, action_description, status) "
        "VALUES ('appr-1', 'wi-1', 'a', 'approve_requested')"
    )
    await db_conn.commit()
    rows = await store.get_requested_approval_decisions("acc-alice")
    assert [r["id"] for r in rows] == ["appr-1"]
```

- [ ] **Step 2: Run tests to verify they fail**

```
uv run pytest tests/test_store.py -k "request_approval_decision or get_requested_approval_decisions" -v
```

Expected: 5 failures — methods don't exist.

- [ ] **Step 3: Implement in `deskbridge/db/store.py`**

Add immediately after `resolve_approval` (which ends around line 440):

```python
    async def request_approval_decision(self, id: str, decision: str) -> bool:
        if decision not in ("approve_requested", "reject_requested"):
            raise ValueError(f"invalid approval decision {decision!r}")
        async with self._conn.execute(
            """
            UPDATE approvals
            SET status = ?
            WHERE id = ? AND status = 'pending'
            """,
            (decision, id),
        ) as cur:
            updated = cur.rowcount > 0
        await self._conn.commit()
        return updated

    async def get_requested_approval_decisions(self, identity_id: str) -> list[aiosqlite.Row]:
        async with self._conn.execute(
            """
            SELECT a.* FROM approvals a
            LEFT JOIN work_items w ON a.work_item_id = w.id
            WHERE a.status IN ('approve_requested', 'reject_requested')
              AND (w.identity_id = ? OR (a.work_item_id IS NULL AND a.identity_id = ?))
            ORDER BY a.created_at, a.rowid
            """,
            (identity_id, identity_id),
        ) as cur:
            return await cur.fetchall()
```

- [ ] **Step 4: Run the store suite**

```
uv run pytest tests/test_store.py -v
```

Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add deskbridge/db/store.py tests/test_store.py
git commit -m "feat: add approval decision queue store operations"
```

---

### Task 5: Extract shared approval resolution helper

**Files:**
- Create: `deskbridge/dm/approval_resolution.py`
- Modify: `deskbridge/dm/watcher.py`

Context: this is a behavior-preserving refactor — `DmWatcher._call_respond_to_approval` (watcher.py lines 267–421) moves into a module function so the new poller can reuse it. No new tests; the existing DM-watcher approval tests (which mock `client.call_tool` against a real store) are the safety net. Return contract changes from `str` to `tuple[bool, str]` — `resolved` is True whenever the local approval row reached a terminal status (success **and** the already-resolved/expired/not-found sync paths); the watcher wrapper keeps returning just the string. The six identical resolve+audit blocks collapse into one `_resolve_and_audit` helper (payload unchanged). Log event names change prefix from `dm_watcher_*` to `approval_resolution_*` (no test asserts log events).

- [ ] **Step 1: Run the existing suites to confirm a green baseline**

```
uv run pytest tests/test_dm_watcher.py tests/test_approval_integration.py -v
```

Expected: all PASS.

- [ ] **Step 2: Create `deskbridge/dm/approval_resolution.py`**

```python
import json
import uuid
import structlog

from deskbridge.db.store import Store
from deskbridge.mcp.client import McpClient, McpToolError

log = structlog.get_logger()

_FAILURE_REPLY = "Failed to record decision — please check logs."
_STALE_REPLY = "Decision received, but the approval was already resolved or has expired."


async def _resolve_and_audit(
    store: Store, account_id: str, row: dict, local_status: str
) -> None:
    await store.resolve_approval(row["id"], local_status)
    try:
        await store.log_audit(
            id=str(uuid.uuid4()),
            event_type="approval_resolved",
            identity_id=account_id,
            work_item_id=row["work_item_id"],
            payload_json=json.dumps({"approval_id": row["id"], "resolution": local_status}),
        )
    except Exception:
        log.warning("audit_log_failed", event_type="approval_resolved")


async def resolve_approval_via_mcp(
    *,
    store: Store,
    client: McpClient,
    identity_label: str,
    account_id: str,
    row: dict,
    mcp_approval_id: str,
    session_id: str,
    approved: bool,
) -> tuple[bool, str]:
    """Forward an operator decision to MCP and sync the local approval row.

    Returns (resolved, reply): resolved is True when the local approval row
    reached a terminal status; reply is the operator-facing message.
    """
    try:
        result = await client.call_tool(
            "respond_to_approval",
            {
                "session_id": session_id,
                "approval_request_id": mcp_approval_id,
                "approved": approved,
                "note": None,
            },
        )
        if not (isinstance(result, dict) and result.get("ok") is True):
            log.error(
                "approval_resolution_unexpected_response",
                identity=identity_label,
                mcp_approval_id=mcp_approval_id,
            )
            return False, _FAILURE_REPLY
        returned_id = result.get("approval_request_id")
        if returned_id != mcp_approval_id:
            log.error(
                "approval_resolution_id_mismatch",
                identity=identity_label,
                sent=mcp_approval_id,
                returned=returned_id,
            )
            return False, _FAILURE_REPLY
        response_status = result.get("status", "")
        if response_status == "approved":
            local_status = "approved"
        elif response_status == "denied":
            local_status = "rejected"
        else:
            log.error(
                "approval_resolution_unknown_response_status",
                identity=identity_label,
                status=response_status,
                mcp_approval_id=mcp_approval_id,
            )
            return False, _FAILURE_REPLY
        expected_response_status = "approved" if approved else "denied"
        if response_status != expected_response_status:
            log.error(
                "approval_resolution_status_mismatch",
                identity=identity_label,
                sent_approved=approved,
                response_status=response_status,
                mcp_approval_id=mcp_approval_id,
            )
            return False, _FAILURE_REPLY
        await _resolve_and_audit(store, account_id, row, local_status)
        log.info(
            "approval_resolution_resolved",
            identity=identity_label,
            mcp_approval_id=mcp_approval_id,
        )
        return True, ("Approved." if approved else "Denied.")
    except McpToolError as e:
        cat = e.category
        if cat == "approval_already_resolved":
            data_status = (e.data or {}).get("status", "")
            if data_status == "approved":
                local_status = "approved"
            elif data_status == "denied":
                local_status = "rejected"
            else:
                log.error(
                    "approval_resolution_already_resolved_unknown_status",
                    identity=identity_label,
                    data_status=data_status,
                    mcp_approval_id=mcp_approval_id,
                )
                return False, _FAILURE_REPLY
            await _resolve_and_audit(store, account_id, row, local_status)
            log.warning(
                "approval_resolution_already_resolved",
                identity=identity_label,
                mcp_approval_id=mcp_approval_id,
            )
            return True, _STALE_REPLY
        elif cat == "approval_expired":
            await _resolve_and_audit(store, account_id, row, "rejected")
            log.warning(
                "approval_resolution_expired",
                identity=identity_label,
                mcp_approval_id=mcp_approval_id,
            )
            return True, _STALE_REPLY
        elif cat == "approval_not_found":
            await _resolve_and_audit(store, account_id, row, "rejected")
            log.error(
                "approval_resolution_not_found",
                identity=identity_label,
                mcp_approval_id=mcp_approval_id,
            )
            return True, _STALE_REPLY
        else:
            log.error(
                "approval_resolution_error",
                identity=identity_label,
                message=e.mcp_error.message,
            )
            return False, _FAILURE_REPLY
    except Exception:
        log.exception("approval_resolution_error", identity=identity_label)
        return False, _FAILURE_REPLY
```

- [ ] **Step 3: Replace `_call_respond_to_approval` in `deskbridge/dm/watcher.py`**

Add to the imports at the top of the file:

```python
from deskbridge.dm.approval_resolution import resolve_approval_via_mcp
```

Replace the entire `_call_respond_to_approval` method (lines 267–421, everything from `async def _call_respond_to_approval(` to the end of the file) with:

```python
    async def _call_respond_to_approval(
        self,
        row: dict,
        mcp_approval_id: str,
        session_id: str,
        approved: bool,
    ) -> str:
        _resolved, reply = await resolve_approval_via_mcp(
            store=self._store,
            client=self._client,
            identity_label=self._identity_label,
            account_id=self._account_id,
            row=row,
            mcp_approval_id=mcp_approval_id,
            session_id=session_id,
            approved=approved,
        )
        return reply
```

(`McpToolError` and `RoutingDecision` imports in watcher.py are still used by `run()` — leave them.)

- [ ] **Step 4: Run the suites to verify behavior is preserved**

```
uv run pytest tests/test_dm_watcher.py tests/test_approval_integration.py -v
```

Expected: all PASS, unchanged.

- [ ] **Step 5: Commit**

```bash
git add deskbridge/dm/approval_resolution.py deskbridge/dm/watcher.py
git commit -m "refactor: extract shared MCP approval resolution helper from DmWatcher"
```

---

### Task 6: `ApprovalDecisionPoller`

**Files:**
- Create: `deskbridge/dm/approval_decision_poller.py`
- Test: `tests/test_approval_decision_poller.py`

Context: per-identity poll loop (same shape as `ApprovalRequestWatcher`). For each queued decision: MCP-correlated rows are forwarded via `resolve_approval_via_mcp` (skipped this cycle if no session; left in `*_requested` for retry if forwarding fails); local-only rows (no `mcp_approval_id`) are resolved directly with an `approval_resolved` audit carrying `"via": "cli"`.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_approval_decision_poller.py`:

```python
import asyncio
from unittest.mock import AsyncMock, MagicMock

from deskbridge.dm.approval_decision_poller import ApprovalDecisionPoller
from deskbridge.mcp.client import McpToolError
from deskbridge.mcp.errors import RoutingDecision
from deskbridge.models import McpError, McpErrorCategory


def make_poller(store, shutdown_event, *, call_tool_mock=None, session_id="sess-123"):
    mock_client = MagicMock()
    mock_client.call_tool = call_tool_mock or AsyncMock()
    mock_broker = MagicMock()
    mock_broker.get_session_id = AsyncMock(return_value=session_id)
    poller = ApprovalDecisionPoller(
        identity_label="alice",
        store=store,
        client=mock_client,
        broker=mock_broker,
        shutdown_event=shutdown_event,
        poll_interval_secs=0.05,
    )
    return poller, mock_client


async def _seed_account(store):
    await store.upsert_account(
        id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X"
    )


async def _seed_requested_approval(db_conn, *, id="appr-1", identity_id="acc-alice",
                                   status="approve_requested", mcp_approval_id=None):
    await db_conn.execute(
        "INSERT INTO approvals (id, identity_id, mcp_approval_id, action_description, status) "
        "VALUES (?, ?, ?, 'pay invoice', ?)",
        (id, identity_id, mcp_approval_id, status),
    )
    await db_conn.commit()


async def test_local_approve_request_resolves_approved(store, db_conn):
    await _seed_account(store)
    await _seed_requested_approval(db_conn)
    poller, client = make_poller(store, asyncio.Event())
    await poller._poll_once()
    row = await store.get_approval("appr-1")
    assert row["status"] == "approved"
    client.call_tool.assert_not_awaited()
    audits = await store.get_audit_events("approval_resolved")
    assert len(audits) == 1


async def test_local_reject_request_resolves_rejected(store, db_conn):
    await _seed_account(store)
    await _seed_requested_approval(db_conn, status="reject_requested")
    poller, _client = make_poller(store, asyncio.Event())
    await poller._poll_once()
    row = await store.get_approval("appr-1")
    assert row["status"] == "rejected"


async def test_mcp_correlated_decision_forwards_to_mcp(store, db_conn):
    await _seed_account(store)
    await _seed_requested_approval(db_conn, mcp_approval_id="req-9")
    call_tool = AsyncMock(return_value={
        "ok": True, "approval_request_id": "req-9", "status": "approved",
    })
    poller, client = make_poller(store, asyncio.Event(), call_tool_mock=call_tool)
    await poller._poll_once()
    client.call_tool.assert_awaited_once()
    tool_name, args = client.call_tool.await_args.args
    assert tool_name == "respond_to_approval"
    assert args["approval_request_id"] == "req-9"
    assert args["approved"] is True
    row = await store.get_approval("appr-1")
    assert row["status"] == "approved"


async def test_mcp_failure_leaves_row_requested_for_retry(store, db_conn):
    await _seed_account(store)
    await _seed_requested_approval(db_conn, mcp_approval_id="req-9")
    err = McpToolError(
        mcp_error=McpError(category=McpErrorCategory.TRANSIENT_TRANSPORT, message="boom"),
        routing=RoutingDecision.RETRY,
    )
    poller, _client = make_poller(
        store, asyncio.Event(), call_tool_mock=AsyncMock(side_effect=err)
    )
    await poller._poll_once()
    row = await store.get_approval("appr-1")
    assert row["status"] == "approve_requested"


async def test_no_session_skips_mcp_correlated_row(store, db_conn):
    await _seed_account(store)
    await _seed_requested_approval(db_conn, mcp_approval_id="req-9")
    poller, client = make_poller(store, asyncio.Event(), session_id=None)
    await poller._poll_once()
    client.call_tool.assert_not_awaited()
    row = await store.get_approval("appr-1")
    assert row["status"] == "approve_requested"


async def test_ignores_other_identity_rows(store, db_conn):
    await _seed_account(store)
    await store.upsert_account(
        id="acc-bob", npub="npub1bob", label="bob", passphrase_ref="env:Y"
    )
    await _seed_requested_approval(db_conn, identity_id="acc-bob")
    poller, _client = make_poller(store, asyncio.Event())
    await poller._poll_once()
    row = await store.get_approval("appr-1")
    assert row["status"] == "approve_requested"


async def test_run_loop_processes_and_stops_on_shutdown(store, db_conn):
    await _seed_account(store)
    await _seed_requested_approval(db_conn)
    shutdown = asyncio.Event()
    poller, _client = make_poller(store, shutdown)

    async def stop():
        await asyncio.sleep(0.12)
        shutdown.set()

    await asyncio.gather(poller.run(), stop())
    row = await store.get_approval("appr-1")
    assert row["status"] == "approved"
```

- [ ] **Step 2: Run tests to verify they fail**

```
uv run pytest tests/test_approval_decision_poller.py -v
```

Expected: collection error — module `deskbridge.dm.approval_decision_poller` does not exist.

- [ ] **Step 3: Create `deskbridge/dm/approval_decision_poller.py`**

```python
import asyncio
import json
import uuid
import structlog

from deskbridge.db.store import Store
from deskbridge.dm.approval_resolution import resolve_approval_via_mcp
from deskbridge.mcp.client import McpClient
from deskbridge.mcp.session import SessionBroker

log = structlog.get_logger()


class ApprovalDecisionPoller:
    """Forwards operator decisions queued by the CLI (approve_requested /
    reject_requested approval rows) to MCP. The CLI itself never talks to MCP."""

    def __init__(
        self,
        identity_label: str,
        store: Store,
        client: McpClient,
        broker: SessionBroker,
        shutdown_event: asyncio.Event,
        poll_interval_secs: float = 2.0,
    ) -> None:
        self._identity_label = identity_label
        self._account_id = f"acc-{identity_label}"
        self._store = store
        self._client = client
        self._broker = broker
        self._shutdown_event = shutdown_event
        self._poll_interval_secs = poll_interval_secs

    async def run(self) -> None:
        log.info("approval_decision_poller_started", identity=self._identity_label)
        while not self._shutdown_event.is_set():
            try:
                await self._poll_once()
            except Exception:
                log.exception(
                    "approval_decision_poller_unexpected_error",
                    identity=self._identity_label,
                )
            await self._sleep()
        log.info("approval_decision_poller_stopped", identity=self._identity_label)

    async def _poll_once(self) -> None:
        rows = await self._store.get_requested_approval_decisions(self._account_id)
        for row in rows:
            approved = row["status"] == "approve_requested"
            local_status = "approved" if approved else "rejected"
            mcp_approval_id = row["mcp_approval_id"]

            if mcp_approval_id:
                session_id = await self._broker.get_session_id(self._identity_label)
                if session_id is None:
                    log.debug(
                        "approval_decision_poller_no_session",
                        identity=self._identity_label,
                        approval_id=row["id"],
                    )
                    continue  # retry next cycle once a session exists
                resolved, _reply = await resolve_approval_via_mcp(
                    store=self._store,
                    client=self._client,
                    identity_label=self._identity_label,
                    account_id=self._account_id,
                    row=row,
                    mcp_approval_id=mcp_approval_id,
                    session_id=session_id,
                    approved=approved,
                )
                if not resolved:
                    # Row stays in *_requested status; retried next cycle.
                    log.warning(
                        "approval_decision_poller_forward_failed",
                        identity=self._identity_label,
                        approval_id=row["id"],
                    )
            else:
                await self._store.resolve_approval(row["id"], local_status)
                try:
                    await self._store.log_audit(
                        id=str(uuid.uuid4()),
                        event_type="approval_resolved",
                        identity_id=self._account_id,
                        work_item_id=row["work_item_id"],
                        payload_json=json.dumps({
                            "approval_id": row["id"],
                            "resolution": local_status,
                            "via": "cli",
                        }),
                    )
                except Exception:
                    log.warning("audit_log_failed", event_type="approval_resolved")
                log.info(
                    "approval_decision_poller_resolved_locally",
                    identity=self._identity_label,
                    approval_id=row["id"],
                    resolution=local_status,
                )

    async def _sleep(self) -> None:
        try:
            await asyncio.wait_for(
                self._shutdown_event.wait(), timeout=self._poll_interval_secs
            )
        except asyncio.TimeoutError:
            pass
```

- [ ] **Step 4: Run tests to verify they pass**

```
uv run pytest tests/test_approval_decision_poller.py -v
```

Expected: 7 PASS.

- [ ] **Step 5: Commit**

```bash
git add deskbridge/dm/approval_decision_poller.py tests/test_approval_decision_poller.py
git commit -m "feat: add ApprovalDecisionPoller forwarding queued CLI decisions to MCP"
```

---

### Task 7: Supervisor wiring

**Files:**
- Modify: `deskbridge/supervisor.py`
- Test: `tests/test_supervisor.py`

- [ ] **Step 1: Write the failing test**

Add to `tests/test_supervisor.py` after `test_supervisor_spawns_approval_watcher`:

```python
async def test_supervisor_spawns_approval_decision_poller(tmp_path, monkeypatch, mock_broker, mock_client_ctx):
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
         patch("deskbridge.supervisor.ApprovalDecisionPoller") as MockDecisionPoller, \
         patch("deskbridge.supervisor.Store") as MockStore:

        mock_instance = MockMcpClient.return_value
        mock_instance.connect.return_value = mock_client_ctx

        async def never_finishes():
            await asyncio.Event().wait()

        MockDmWatcher.return_value.run = Mock(side_effect=never_finishes)
        MockOutboxDrainer.return_value.run = Mock(side_effect=never_finishes)
        MockWorkItemPoller.return_value.run = Mock(side_effect=never_finishes)
        MockDecisionPoller.return_value.run = Mock(side_effect=never_finishes)

        mock_store_instance = AsyncMock()
        mock_store_instance.get_project_groups = AsyncMock(return_value=[])
        mock_store_instance.get_project_for_identity = AsyncMock(return_value=None)
        MockStore.return_value = mock_store_instance

        supervisor = Supervisor(config=config)

        async def stop():
            await asyncio.sleep(0.1)
            supervisor.request_shutdown()

        await asyncio.gather(supervisor.run(), stop())

    MockDecisionPoller.assert_called_once_with(
        identity_label="alice",
        store=ANY,
        client=ANY,
        broker=mock_broker,
        shutdown_event=ANY,
    )
    MockDecisionPoller.return_value.run.assert_called_once()
```

- [ ] **Step 2: Run the test to verify it fails**

```
uv run pytest tests/test_supervisor.py::test_supervisor_spawns_approval_decision_poller -v
```

Expected: FAIL — `deskbridge.supervisor` has no attribute `ApprovalDecisionPoller`.

- [ ] **Step 3: Wire the poller into `deskbridge/supervisor.py`**

Add to the imports (after the `ApprovalRequestWatcher` import on line 15):

```python
from deskbridge.dm.approval_decision_poller import ApprovalDecisionPoller
```

Add to the task-list declarations (after `approval_watcher_tasks: list[asyncio.Task] = []` on line 66):

```python
                approval_decision_poller_tasks: list[asyncio.Task] = []
```

Spawn the pollers right after the `approval_watcher_tasks = [...]` block (which ends on line 103):

```python
                    approval_decision_poller_tasks = [
                        asyncio.create_task(
                            ApprovalDecisionPoller(
                                identity_label=identity.label,
                                store=store,
                                client=client,
                                broker=broker,
                                shutdown_event=self._shutdown_event,
                            ).run(),
                            name=f"approval_decision_poller_{identity.label}",
                        )
                        for identity in self._config.identities
                    ]
```

Add to `tasks_to_cancel` in the `finally` block (after `+ approval_watcher_tasks`):

```python
                        + approval_decision_poller_tasks
```

- [ ] **Step 4: Run the supervisor suite**

```
uv run pytest tests/test_supervisor.py -v
```

Expected: all PASS. (The pre-existing supervisor tests that don't patch `ApprovalDecisionPoller` run a real instance against a mocked store — `get_requested_approval_decisions` returns a `MagicMock` whose default `__iter__` is empty, so the loop is a harmless no-op, same as the unpatched `ApprovalRequestWatcher` precedent.)

- [ ] **Step 5: Commit**

```bash
git add deskbridge/supervisor.py tests/test_supervisor.py
git commit -m "feat: spawn ApprovalDecisionPoller per identity in supervisor"
```

---

### Task 8: CLI — shared helpers, `cancel`, `retry`

**Files:**
- Modify: `deskbridge/cli.py` (full-file replacement)
- Test: `tests/test_cli.py`

Context: cli.py grows shared helpers (`_CONFIG_OPTION`, `_load_config_or_exit`, `_require_db`, `_run_async`, `_log_audit_safe`) and two commands. `status` keeps its exit-0 "No database found" behavior (a test depends on it); the new write commands exit 1 in that case. Write commands run `apply_schema` (idempotent) so they work even if the daemon hasn't restarted since a migration.

- [ ] **Step 1: Write the failing tests**

In `tests/test_cli.py`, add these imports/helpers after the existing imports:

```python
async def _seed(db_path, *sql_statements):
    async with aiosqlite.connect(db_path) as conn:
        conn.row_factory = aiosqlite.Row
        await apply_schema(conn)
        await conn.execute(
            "INSERT INTO accounts (id, npub, label, passphrase_ref) "
            "VALUES ('acc-alice', 'npub1alice', 'alice', 'env:X')"
        )
        for stmt in sql_statements:
            await conn.execute(stmt)
        await conn.commit()


async def _fetch_one(db_path, sql):
    async with aiosqlite.connect(db_path) as conn:
        conn.row_factory = aiosqlite.Row
        async with conn.execute(sql) as cur:
            return await cur.fetchone()
```

Then add the command tests at the end of the file:

```python
async def test_cancel_pending_work_item_sets_cancelled(config_file, tmp_path):
    db_path = tmp_path / "test.db"
    await _seed(
        db_path,
        "INSERT INTO work_items (id, source_type, source_id, identity_id, status, idempotency_key, summary) "
        "VALUES ('wi-1', 'dm', 'msg-1', 'acc-alice', 'pending', 'k1', 'fix the bug')",
    )
    runner = CliRunner()
    result = runner.invoke(cli, ["cancel", "wi-1", "--config", str(config_file)])
    assert result.exit_code == 0
    assert "cancelled" in result.output.lower()
    row = await _fetch_one(db_path, "SELECT status FROM work_items WHERE id='wi-1'")
    assert row["status"] == "cancelled"
    audit = await _fetch_one(
        db_path, "SELECT id FROM audit_log WHERE event_type='work_item_terminal'"
    )
    assert audit is not None


async def test_cancel_dispatched_work_item_requests_cancel(config_file, tmp_path):
    db_path = tmp_path / "test.db"
    await _seed(
        db_path,
        "INSERT INTO work_items (id, source_type, source_id, identity_id, status, idempotency_key, summary) "
        "VALUES ('wi-1', 'dm', 'msg-1', 'acc-alice', 'dispatched', 'k1', 'fix the bug')",
    )
    runner = CliRunner()
    result = runner.invoke(cli, ["cancel", "wi-1", "--config", str(config_file)])
    assert result.exit_code == 0
    assert "cancel requested" in result.output.lower()
    row = await _fetch_one(db_path, "SELECT status FROM work_items WHERE id='wi-1'")
    assert row["status"] == "cancel_requested"
    audit = await _fetch_one(
        db_path, "SELECT id FROM audit_log WHERE event_type='work_item_cancel_requested'"
    )
    assert audit is not None


async def test_cancel_done_work_item_fails(config_file, tmp_path):
    db_path = tmp_path / "test.db"
    await _seed(
        db_path,
        "INSERT INTO work_items (id, source_type, source_id, identity_id, status, idempotency_key, summary) "
        "VALUES ('wi-1', 'dm', 'msg-1', 'acc-alice', 'done', 'k1', 'fix the bug')",
    )
    runner = CliRunner()
    result = runner.invoke(cli, ["cancel", "wi-1", "--config", str(config_file)])
    assert result.exit_code == 1
    row = await _fetch_one(db_path, "SELECT status FROM work_items WHERE id='wi-1'")
    assert row["status"] == "done"


async def test_cancel_missing_work_item_fails(config_file, tmp_path):
    db_path = tmp_path / "test.db"
    await _seed(db_path)
    runner = CliRunner()
    result = runner.invoke(cli, ["cancel", "wi-missing", "--config", str(config_file)])
    assert result.exit_code == 1
    assert "not found" in result.output.lower()


async def test_retry_failed_work_item_requeues(config_file, tmp_path):
    db_path = tmp_path / "test.db"
    await _seed(
        db_path,
        "INSERT INTO work_items (id, source_type, source_id, identity_id, status, idempotency_key, summary, attempt_count, next_retry_at) "
        "VALUES ('wi-1', 'dm', 'msg-1', 'acc-alice', 'failed', 'k1', 'fix the bug', 2, '2099-01-01T00:00:00Z')",
    )
    runner = CliRunner()
    result = runner.invoke(cli, ["retry", "wi-1", "--config", str(config_file)])
    assert result.exit_code == 0
    assert "re-queued" in result.output.lower()
    row = await _fetch_one(
        db_path, "SELECT status, attempt_count, next_retry_at FROM work_items WHERE id='wi-1'"
    )
    assert row["status"] == "pending"
    assert row["attempt_count"] == 0
    assert row["next_retry_at"] is None
    audit = await _fetch_one(
        db_path, "SELECT id FROM audit_log WHERE event_type='work_item_retry_queued'"
    )
    assert audit is not None


async def test_retry_pending_work_item_fails(config_file, tmp_path):
    db_path = tmp_path / "test.db"
    await _seed(
        db_path,
        "INSERT INTO work_items (id, source_type, source_id, identity_id, status, idempotency_key, summary) "
        "VALUES ('wi-1', 'dm', 'msg-1', 'acc-alice', 'pending', 'k1', 'fix the bug')",
    )
    runner = CliRunner()
    result = runner.invoke(cli, ["retry", "wi-1", "--config", str(config_file)])
    assert result.exit_code == 1
    row = await _fetch_one(db_path, "SELECT status FROM work_items WHERE id='wi-1'")
    assert row["status"] == "pending"


async def test_retry_missing_work_item_fails(config_file, tmp_path):
    db_path = tmp_path / "test.db"
    await _seed(db_path)
    runner = CliRunner()
    result = runner.invoke(cli, ["retry", "wi-missing", "--config", str(config_file)])
    assert result.exit_code == 1
    assert "not found" in result.output.lower()
```

- [ ] **Step 2: Run tests to verify they fail**

```
uv run pytest tests/test_cli.py -k "cancel or retry" -v
```

Expected: 7 failures — `Error: No such command 'cancel'` / `'retry'`.

- [ ] **Step 3: Replace `deskbridge/cli.py` entirely**

```python
import asyncio
import concurrent.futures
import json
from pathlib import Path
from uuid import uuid4

import click
import aiosqlite
import structlog

from deskbridge.config import load_config, ConfigError, DeskBridgeConfig
from deskbridge.db.schema import apply_schema
from deskbridge.db.store import Store
from deskbridge.supervisor import Supervisor

log = structlog.get_logger()

DEFAULT_CONFIG = Path.home() / ".deskbridge" / "config.toml"

_CONFIG_OPTION = click.option(
    "--config",
    "config_path",
    type=click.Path(),
    default=str(DEFAULT_CONFIG),
    show_default=True,
    help="Path to config TOML file",
)


def _load_config_or_exit(config_path: str) -> DeskBridgeConfig:
    try:
        return load_config(Path(config_path))
    except ConfigError as e:
        click.echo(f"Config error: {e}", err=True)
        raise SystemExit(1)


def _require_db(config_path: str) -> Path:
    config = _load_config_or_exit(config_path)
    db_path = Path(config.supervisor.db_path).expanduser()
    if not db_path.exists():
        click.echo("No database found — DeskBridge has not been started yet.", err=True)
        raise SystemExit(1)
    return db_path


def _run_async(coro):
    # Run in a dedicated thread so asyncio.run() always gets a fresh event loop.
    # This avoids RuntimeError when the command is invoked from within an already-
    # running loop (e.g. during tests).
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        return pool.submit(asyncio.run, coro).result()


async def _log_audit_safe(
    store: Store,
    event_type: str,
    *,
    identity_id: str | None = None,
    work_item_id: str | None = None,
    payload: dict,
) -> None:
    try:
        await store.log_audit(
            id=str(uuid4()),
            event_type=event_type,
            identity_id=identity_id,
            work_item_id=work_item_id,
            payload_json=json.dumps(payload),
        )
    except Exception:
        log.warning("audit_log_failed", event_type=event_type)


@click.group()
def cli():
    pass


@cli.command()
@_CONFIG_OPTION
def start(config_path: str):
    """Start the DeskBridge supervisor daemon (foreground)."""
    config = _load_config_or_exit(config_path)

    click.echo(f"Starting DeskBridge (config: {config_path})")

    supervisor = Supervisor(config=config)
    try:
        asyncio.run(supervisor.run())
    except KeyboardInterrupt:
        pass
    click.echo("DeskBridge stopped.")


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


@cli.command()
@_CONFIG_OPTION
def status(config_path: str):
    """Show current session health from the local SQLite database."""
    config = _load_config_or_exit(config_path)

    db_path = Path(config.supervisor.db_path).expanduser()
    if not db_path.exists():
        click.echo("No database found — DeskBridge has not been started yet.")
        return

    _run_async(_show_status(db_path))


async def _cancel_work_item(db_path: Path, work_item_id: str) -> tuple[bool, str]:
    async with aiosqlite.connect(db_path) as conn:
        conn.row_factory = aiosqlite.Row
        await apply_schema(conn)
        store = Store(conn)
        row = await store.get_work_item(work_item_id)
        if row is None:
            return False, f"Work item {work_item_id} not found."
        status = row["status"]
        if status == "pending":
            if not await store.cancel_pending_work_item(work_item_id):
                return False, (
                    f"Work item {work_item_id} is no longer pending — "
                    "run 'deskbridge status' and try again."
                )
            await _log_audit_safe(
                store, "work_item_terminal",
                identity_id=row["identity_id"], work_item_id=work_item_id,
                payload={"status": "cancelled", "via": "cli"},
            )
            return True, f"Work item {work_item_id} cancelled."
        if status == "dispatched":
            if not await store.mark_work_item_cancel_requested(work_item_id):
                return False, (
                    f"Work item {work_item_id} is no longer running — "
                    "run 'deskbridge status' and try again."
                )
            await _log_audit_safe(
                store, "work_item_cancel_requested",
                identity_id=row["identity_id"], work_item_id=work_item_id,
                payload={"via": "cli"},
            )
            return True, (
                f"Cancel requested for running work item {work_item_id} — "
                "the supervisor will stop the agent shortly."
            )
        if status == "cancel_requested":
            return False, f"Work item {work_item_id} already has a pending cancel request."
        return False, f"Work item {work_item_id} is {status} — nothing to cancel."


async def _retry_work_item(db_path: Path, work_item_id: str) -> tuple[bool, str]:
    async with aiosqlite.connect(db_path) as conn:
        conn.row_factory = aiosqlite.Row
        await apply_schema(conn)
        store = Store(conn)
        row = await store.get_work_item(work_item_id)
        if row is None:
            return False, f"Work item {work_item_id} not found."
        if row["status"] not in ("failed", "cancelled", "interrupted"):
            return False, (
                f"Work item {work_item_id} is {row['status']} — only failed, "
                "cancelled, or interrupted items can be retried."
            )
        if not await store.reset_work_item_for_retry(work_item_id):
            return False, (
                f"Work item {work_item_id} changed state — "
                "run 'deskbridge status' and try again."
            )
        await _log_audit_safe(
            store, "work_item_retry_queued",
            identity_id=row["identity_id"], work_item_id=work_item_id,
            payload={"via": "cli", "attempt_count": 0},
        )
        return True, f"Work item {work_item_id} re-queued (attempt counter reset)."


@cli.command()
@click.argument("work_item_id")
@_CONFIG_OPTION
def cancel(work_item_id: str, config_path: str):
    """Cancel a pending or running work item."""
    db_path = _require_db(config_path)
    ok, message = _run_async(_cancel_work_item(db_path, work_item_id))
    click.echo(message)
    if not ok:
        raise SystemExit(1)


@cli.command()
@click.argument("work_item_id")
@_CONFIG_OPTION
def retry(work_item_id: str, config_path: str):
    """Re-queue a failed, cancelled, or interrupted work item (resets attempt count)."""
    db_path = _require_db(config_path)
    ok, message = _run_async(_retry_work_item(db_path, work_item_id))
    click.echo(message)
    if not ok:
        raise SystemExit(1)
```

- [ ] **Step 4: Run the CLI suite**

```
uv run pytest tests/test_cli.py -v
```

Expected: all PASS (existing start/status tests plus the 7 new ones).

- [ ] **Step 5: Commit**

```bash
git add deskbridge/cli.py tests/test_cli.py
git commit -m "feat: add deskbridge cancel and retry operator commands"
```

---

### Task 9: CLI — `approve`, `reject`

**Files:**
- Modify: `deskbridge/cli.py`
- Test: `tests/test_cli.py`

- [ ] **Step 1: Write the failing tests**

Add at the end of `tests/test_cli.py`:

```python
async def test_approve_pending_approval_queues_decision(config_file, tmp_path):
    db_path = tmp_path / "test.db"
    await _seed(
        db_path,
        "INSERT INTO approvals (id, identity_id, action_description, status) "
        "VALUES ('appr-1', 'acc-alice', 'pay invoice', 'pending')",
    )
    runner = CliRunner()
    result = runner.invoke(cli, ["approve", "appr-1", "--config", str(config_file)])
    assert result.exit_code == 0
    assert "queued" in result.output.lower()
    row = await _fetch_one(db_path, "SELECT status FROM approvals WHERE id='appr-1'")
    assert row["status"] == "approve_requested"
    audit = await _fetch_one(
        db_path, "SELECT id FROM audit_log WHERE event_type='approval_decision_requested'"
    )
    assert audit is not None


async def test_reject_pending_approval_queues_decision(config_file, tmp_path):
    db_path = tmp_path / "test.db"
    await _seed(
        db_path,
        "INSERT INTO approvals (id, identity_id, action_description, status) "
        "VALUES ('appr-1', 'acc-alice', 'pay invoice', 'pending')",
    )
    runner = CliRunner()
    result = runner.invoke(cli, ["reject", "appr-1", "--config", str(config_file)])
    assert result.exit_code == 0
    row = await _fetch_one(db_path, "SELECT status FROM approvals WHERE id='appr-1'")
    assert row["status"] == "reject_requested"


async def test_approve_already_resolved_fails(config_file, tmp_path):
    db_path = tmp_path / "test.db"
    await _seed(
        db_path,
        "INSERT INTO approvals (id, identity_id, action_description, status) "
        "VALUES ('appr-1', 'acc-alice', 'pay invoice', 'approved')",
    )
    runner = CliRunner()
    result = runner.invoke(cli, ["approve", "appr-1", "--config", str(config_file)])
    assert result.exit_code == 1
    row = await _fetch_one(db_path, "SELECT status FROM approvals WHERE id='appr-1'")
    assert row["status"] == "approved"


async def test_approve_missing_approval_fails(config_file, tmp_path):
    db_path = tmp_path / "test.db"
    await _seed(db_path)
    runner = CliRunner()
    result = runner.invoke(cli, ["approve", "appr-missing", "--config", str(config_file)])
    assert result.exit_code == 1
    assert "not found" in result.output.lower()
```

- [ ] **Step 2: Run tests to verify they fail**

```
uv run pytest tests/test_cli.py -k "approve or reject" -v
```

Expected: 4 failures — `Error: No such command 'approve'` / `'reject'`.

- [ ] **Step 3: Add the commands to `deskbridge/cli.py`**

Append at the end of the file:

```python
async def _request_approval_decision(
    db_path: Path, approval_id: str, approved: bool
) -> tuple[bool, str]:
    async with aiosqlite.connect(db_path) as conn:
        conn.row_factory = aiosqlite.Row
        await apply_schema(conn)
        store = Store(conn)
        row = await store.get_approval(approval_id)
        if row is None:
            return False, f"Approval {approval_id} not found."
        if row["status"] != "pending":
            return False, (
                f"Approval {approval_id} is {row['status']} — "
                "only pending approvals can be decided."
            )
        decision = "approve_requested" if approved else "reject_requested"
        if not await store.request_approval_decision(approval_id, decision):
            return False, (
                f"Approval {approval_id} changed state — "
                "run 'deskbridge status' and try again."
            )
        await _log_audit_safe(
            store, "approval_decision_requested",
            identity_id=row["identity_id"], work_item_id=row["work_item_id"],
            payload={
                "approval_id": approval_id,
                "decision": "approved" if approved else "rejected",
                "via": "cli",
            },
        )
        verb = "Approval" if approved else "Rejection"
        return True, (
            f"{verb} queued for {approval_id} — "
            "the supervisor will forward the decision to MCP."
        )


@cli.command()
@click.argument("approval_id")
@_CONFIG_OPTION
def approve(approval_id: str, config_path: str):
    """Approve a pending approval request (the supervisor forwards it to MCP)."""
    db_path = _require_db(config_path)
    ok, message = _run_async(_request_approval_decision(db_path, approval_id, approved=True))
    click.echo(message)
    if not ok:
        raise SystemExit(1)


@cli.command()
@click.argument("approval_id")
@_CONFIG_OPTION
def reject(approval_id: str, config_path: str):
    """Reject a pending approval request (the supervisor forwards it to MCP)."""
    db_path = _require_db(config_path)
    ok, message = _run_async(_request_approval_decision(db_path, approval_id, approved=False))
    click.echo(message)
    if not ok:
        raise SystemExit(1)
```

- [ ] **Step 4: Run the CLI suite**

```
uv run pytest tests/test_cli.py -v
```

Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add deskbridge/cli.py tests/test_cli.py
git commit -m "feat: add deskbridge approve and reject operator commands"
```

---

### Task 10: `status` shows actionable IDs

**Files:**
- Modify: `deskbridge/cli.py`
- Test: `tests/test_cli.py`

Context: `cancel wi-…` is unusable if the operator can't see IDs. The Work Queue section lists up to 10 non-terminal items with IDs; the Approvals section lists up to 10 open approvals (pending + queued decisions) with IDs. Existing count lines stay, so `test_status_shows_all_sections` keeps passing.

- [ ] **Step 1: Write the failing test**

Add to `tests/test_cli.py` after `test_status_shows_all_sections`:

```python
async def test_status_lists_active_ids(config_file, tmp_path):
    db_path = tmp_path / "test.db"
    await _seed(
        db_path,
        "INSERT INTO work_items (id, source_type, source_id, identity_id, status, idempotency_key, summary) "
        "VALUES ('wi-1', 'dm', 'msg-1', 'acc-alice', 'pending', 'k1', 'fix the bug')",
        "INSERT INTO approvals (id, identity_id, action_description, status) "
        "VALUES ('appr-1', 'acc-alice', 'pay invoice', 'pending')",
    )
    runner = CliRunner()
    result = runner.invoke(cli, ["status", "--config", str(config_file)])
    assert result.exit_code == 0
    assert "wi-1" in result.output
    assert "appr-1" in result.output
```

- [ ] **Step 2: Run the test to verify it fails**

```
uv run pytest tests/test_cli.py::test_status_lists_active_ids -v
```

Expected: FAIL — `wi-1` not in output.

- [ ] **Step 3: Extend `_show_status` in `deskbridge/cli.py`**

In `_show_status`, directly after the Work Queue counts `click.echo(...)` call, add:

```python
        async with conn.execute(
            """
            SELECT id, status, attempt_count, summary FROM work_items
            WHERE status IN ('pending', 'dispatched', 'cancel_requested')
            ORDER BY created_at LIMIT 10
            """
        ) as cursor:
            active_items = await cursor.fetchall()
        for item in active_items:
            summary = (item["summary"] or "")[:40]
            click.echo(
                f"  {item['id']}  {item['status']:<17}"
                f" attempts={item['attempt_count']}  \"{summary}\""
            )
```

Directly after the Approvals `click.echo(f"\nApprovals\n  pending=...")` call, add:

```python
        async with conn.execute(
            """
            SELECT id, status, action_description FROM approvals
            WHERE status IN ('pending', 'approve_requested', 'reject_requested')
            ORDER BY created_at LIMIT 10
            """
        ) as cursor:
            open_approvals = await cursor.fetchall()
        for appr in open_approvals:
            action = (appr["action_description"] or "")[:50]
            click.echo(f"  {appr['id']}  {appr['status']:<17} \"{action}\"")
```

- [ ] **Step 4: Run the CLI suite**

```
uv run pytest tests/test_cli.py -v
```

Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add deskbridge/cli.py tests/test_cli.py
git commit -m "feat: list actionable work-item and approval ids in status output"
```

---

### Task 11: Docs and full-suite verification

**Files:**
- Modify: `deskbridge.example.toml`
- Modify: `CLAUDE.md`

- [ ] **Step 1: Add `groups` to `deskbridge.example.toml`**

In the `[[projects]]` section, after the `allowed_autonomous_actions` line, add:

```toml
# NIP-29 group ids this identity should watch for @mentions (optional).
# Config is authoritative: groups removed here stop being watched on restart.
groups = []
```

- [ ] **Step 2: Update `CLAUDE.md`**

**(a)** In the Commands block, after the `deskbridge status` line, add:

```bash
uv run deskbridge cancel <work-item-id>  # cancel a pending/running work item
uv run deskbridge retry <work-item-id>   # re-queue a failed/cancelled/interrupted item
uv run deskbridge approve <approval-id>  # queue an approval decision for the supervisor
uv run deskbridge reject <approval-id>   # queue a rejection decision for the supervisor
```

**(b)** In Architecture → Process model, change the per-identity list to:

```markdown
- **Per identity:** `DmWatcher`, `ApprovalRequestWatcher`, `ApprovalDecisionPoller`, `WorkItemPoller`, and (when configured) `GroupWatcher`, `KanbanWatcher`, `ScheduledCheckInWatcher`
```

**(c)** In Key invariants, extend the approvals bullet with one sentence:

```markdown
Operator decisions can also be queued from the CLI (`approve`/`reject` set `approve_requested`/`reject_requested` statuses); the supervisor's `ApprovalDecisionPoller` forwards them to MCP — the CLI process never talks to MCP.
```

**(d)** In the Config section, add one sentence:

```markdown
Group ids are configured per project (`groups = [...]`) and are config-authoritative — bootstrap overwrites `projects.groups_json` on every start.
```

- [ ] **Step 3: Run the full test suite**

```
uv run pytest
```

Expected: all PASS (~306 pre-existing + ~31 new). Takes ~11 minutes.

- [ ] **Step 4: Commit**

```bash
git add deskbridge.example.toml CLAUDE.md
git commit -m "docs: document groups config and operator CLI commands"
```

---

## Self-Review

**Spec coverage:**
- ✅ `groups` config field — Task 1
- ✅ Config-authoritative `groups_json` bootstrap (GroupWatcher now starts; supervisor already wired) — Task 2
- ✅ Guarded `mark_work_item_cancel_requested` + `cancel_pending_work_item` + `reset_work_item_for_retry` — Task 3
- ✅ `request_approval_decision` + `get_requested_approval_decisions` — Task 4
- ✅ Shared `resolve_approval_via_mcp` (DmWatcher behavior preserved) — Task 5
- ✅ `ApprovalDecisionPoller` forwarding queued decisions — Task 6
- ✅ Supervisor spawns the poller per identity — Task 7
- ✅ `deskbridge cancel` / `retry` — Task 8
- ✅ `deskbridge approve` / `reject` — Task 9
- ✅ Status shows actionable IDs — Task 10
- ✅ Example config + CLAUDE.md — Task 11

**Type consistency:** `resolve_approval_via_mcp(... ) -> tuple[bool, str]` matches its three call sites (helper definition Task 5, watcher delegation Task 5, poller Task 6). Store bool returns (`mark_work_item_cancel_requested`, `cancel_pending_work_item`, `reset_work_item_for_retry`, `request_approval_decision`) match CLI usage in Tasks 8–9. `upsert_project(groups_json: str)` matches bootstrap and all four test call sites.

**Invariant check:** the DM flow can't double-resolve queued decisions (`get_pending_approval` filters `status='pending'`); the CLI never opens an MCP connection; all audit writes are wrapped best-effort; outbound DMs are untouched (no new direct sends).

**Race handling:** every CLI write is a guarded UPDATE re-checked by rowcount, so a concurrent supervisor transition produces a clean "changed state — re-run status" error instead of a clobber.
