# DeskBridge Phase 13 — Backlog Polish Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the five deferred polish items from Phase 12's final review plus add a status-query index — six small, independent tasks that eliminate the last UX gaps and code-quality issues flagged by the Phase 12 opus reviewer.

**Architecture:** Six independent changes, each touching 1–3 files, committed separately. Task ordering matters for one dependency: Task 2 (guard `resolve_approval`, return `bool`) must precede Task 3 (unify `resolve_and_audit`) so the helper can check the return value and skip duplicate audit writes.

**Tech Stack:** Python 3.12, aiosqlite, click >= 8.0, pytest-asyncio (asyncio_mode=auto), structlog, uv

---

## File Structure

| File | Tasks | Changes |
|------|-------|---------|
| `deskbridge/cli.py` | 1, 4 | Add retry target listing; branch approve/reject copy on mcp_approval_id |
| `deskbridge/db/store.py` | 2 | Add terminal-status guard + return `bool` to `resolve_approval` |
| `deskbridge/dm/approval_resolution.py` | 3 | Rename `_resolve_and_audit` → `resolve_and_audit`, add optional `via` param, skip audit on no-op guard |
| `deskbridge/dm/watcher.py` | 3 | Replace inline resolve+audit blocks with helper call in both local paths |
| `deskbridge/dm/approval_decision_poller.py` | 3 | Replace inline resolve+audit block with helper call; drop unused `uuid` import |
| `deskbridge/db/schema.py` | 6 | Append `CREATE INDEX` migration for `work_items(status, created_at)` |
| `tests/test_cli.py` | 1, 4, 5 | Retry targets test; local-only / MCP-correlated copy tests; stderr routing tests |
| `tests/test_approval_resolution.py` | 3 | NEW — unit tests for `resolve_and_audit` with and without `via` |
| `tests/test_store.py` | 2 | Guard tests for `resolve_approval` |
| `tests/test_schema.py` | 6 | Index existence test |

---

### Task 1: List retry targets in `deskbridge status`

The Work Queue section of `deskbridge status` shows `pending`, `dispatched`, and `cancel_requested` items (things the operator can `cancel`). It does **not** show `failed`, `cancelled`, or `interrupted` items — the things the operator can `retry`. The operator has no way to discover retryable IDs without a raw SQL query.

Fix: add a "retry targets" subsection inside Work Queue showing up to 10 retryable items in newest-first order.

**Files:**
- Modify: `deskbridge/cli.py` (function `_show_status`, around lines 129–142)
- Test: `tests/test_cli.py`

- [ ] **Step 1: Write the failing test**

Add to `tests/test_cli.py`:

```python
async def test_status_lists_retryable_ids(config_file, tmp_path):
    db_path = tmp_path / "test.db"
    await _seed(
        db_path,
        "INSERT INTO work_items (id, source_type, source_id, identity_id, status, "
        "idempotency_key, summary, attempt_count) "
        "VALUES ('wi-1', 'dm', 'msg-1', 'acc-alice', 'failed', 'k1', 'fix the bug', 3)",
        "INSERT INTO work_items (id, source_type, source_id, identity_id, status, "
        "idempotency_key, summary, attempt_count) "
        "VALUES ('wi-2', 'dm', 'msg-2', 'acc-alice', 'cancelled', 'k2', 'write tests', 1)",
    )
    runner = CliRunner()
    result = runner.invoke(cli, ["status", "--config", str(config_file)])
    assert result.exit_code == 0
    assert "wi-1" in result.output
    assert "wi-2" in result.output
    assert "retry" in result.output.lower()
```

- [ ] **Step 2: Run test to verify it fails**

```bash
uv run pytest tests/test_cli.py::test_status_lists_retryable_ids -v
```

Expected: FAIL — `wi-1` and `wi-2` not in output.

- [ ] **Step 3: Implement the change in `deskbridge/cli.py`**

In `_show_status`, add the following block immediately after the `for item in active_items:` loop (before the `# Approvals` comment):

```python
        async with conn.execute(
            """
            SELECT id, status, attempt_count, summary FROM work_items
            WHERE status IN ('failed', 'cancelled', 'interrupted')
            ORDER BY created_at DESC LIMIT 10
            """
        ) as cursor:
            retryable_items = await cursor.fetchall()
        if retryable_items:
            click.echo("  --- retry targets ---")
            for item in retryable_items:
                summary = (item["summary"] or "")[:40]
                click.echo(
                    f"  {item['id']}  {item['status']:<17}"
                    f" attempts={item['attempt_count']}  \"{summary}\""
                )
```

The full Work Queue block (replace lines 117–142) now reads:

```python
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
        async with conn.execute(
            """
            SELECT id, status, attempt_count, summary FROM work_items
            WHERE status IN ('failed', 'cancelled', 'interrupted')
            ORDER BY created_at DESC LIMIT 10
            """
        ) as cursor:
            retryable_items = await cursor.fetchall()
        if retryable_items:
            click.echo("  --- retry targets ---")
            for item in retryable_items:
                summary = (item["summary"] or "")[:40]
                click.echo(
                    f"  {item['id']}  {item['status']:<17}"
                    f" attempts={item['attempt_count']}  \"{summary}\""
                )
```

- [ ] **Step 4: Run test to verify it passes**

```bash
uv run pytest tests/test_cli.py::test_status_lists_retryable_ids -v
```

Expected: PASS

- [ ] **Step 5: Run the full CLI test suite to catch regressions**

```bash
uv run pytest tests/test_cli.py -v
```

Expected: all tests pass

- [ ] **Step 6: Commit**

```bash
git add deskbridge/cli.py tests/test_cli.py
git commit -m "feat: list retry targets (failed/cancelled/interrupted) in status output"
```

---

### Task 2: Guard `store.resolve_approval` against double-resolution

`store.resolve_approval` currently runs an unguarded `UPDATE approvals SET status = ? WHERE id = ?`. Callers are safe today through disjoint status ownership, but as defense-in-depth this task adds `AND status NOT IN ('approved', 'rejected')` to the WHERE clause and changes the return type to `bool` (True = row was updated; False = guard blocked it).

**This task must be done before Task 3** so that `resolve_and_audit` (Task 3) can check the return value and skip the audit write when the guard blocked the update.

All current callers do `await store.resolve_approval(...)` without capturing the return — the type change is backward-compatible until Task 3 adds the check.

**Files:**
- Modify: `deskbridge/db/store.py` (function `resolve_approval`, lines 461–472)
- Test: `tests/test_store.py`

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_store.py`:

```python
async def test_resolve_approval_returns_true_for_pending(store, db_conn):
    await store.upsert_account(
        id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X"
    )
    await db_conn.execute(
        "INSERT INTO work_items (id, source_type, source_id, identity_id, "
        "status, idempotency_key) VALUES ('wi-1', 'dm', 's', 'acc-alice', 'pending', 'k1')"
    )
    await db_conn.execute(
        "INSERT INTO approvals (id, work_item_id, action_description, status) "
        "VALUES ('appr-1', 'wi-1', 'do stuff', 'pending')"
    )
    await db_conn.commit()
    result = await store.resolve_approval("appr-1", "approved")
    assert result is True
    row = await store.get_approval("appr-1")
    assert row["status"] == "approved"


async def test_resolve_approval_returns_false_if_already_resolved(store, db_conn):
    await store.upsert_account(
        id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X"
    )
    await db_conn.execute(
        "INSERT INTO work_items (id, source_type, source_id, identity_id, "
        "status, idempotency_key) VALUES ('wi-2', 'dm', 's', 'acc-alice', 'pending', 'k2')"
    )
    await db_conn.execute(
        "INSERT INTO approvals (id, work_item_id, action_description, status, resolved_at) "
        "VALUES ('appr-2', 'wi-2', 'do stuff', 'approved', strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))"
    )
    await db_conn.commit()
    result = await store.resolve_approval("appr-2", "rejected")
    assert result is False
    row = await store.get_approval("appr-2")
    assert row["status"] == "approved"
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
uv run pytest tests/test_store.py::test_resolve_approval_returns_true_for_pending tests/test_store.py::test_resolve_approval_returns_false_if_already_resolved -v
```

Expected: both FAIL — `resolve_approval` currently returns `None` (not `True`/`False`).

- [ ] **Step 3: Update `resolve_approval` in `deskbridge/db/store.py`**

Replace lines 461–472:
```python
    async def resolve_approval(self, id: str, status: str) -> None:
        async with self._conn.execute(
            """
            UPDATE approvals
            SET status = ?,
                resolved_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
            WHERE id = ?
            """,
            (status, id),
        ):
            pass
        await self._conn.commit()
```
with:
```python
    async def resolve_approval(self, id: str, status: str) -> bool:
        async with self._conn.execute(
            """
            UPDATE approvals
            SET status = ?,
                resolved_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
            WHERE id = ? AND status NOT IN ('approved', 'rejected')
            """,
            (status, id),
        ) as cur:
            affected = cur.rowcount
        await self._conn.commit()
        return affected > 0
```

- [ ] **Step 4: Run new tests to verify they pass**

```bash
uv run pytest tests/test_store.py::test_resolve_approval_returns_true_for_pending tests/test_store.py::test_resolve_approval_returns_false_if_already_resolved -v
```

Expected: both PASS

- [ ] **Step 5: Run the broader store and approval suites to confirm no callers break**

```bash
uv run pytest tests/test_store.py tests/test_dm_watcher.py tests/test_approval_decision_poller.py -v
```

Expected: all tests PASS (callers currently ignore the return value)

- [ ] **Step 6: Commit**

```bash
git add deskbridge/db/store.py tests/test_store.py
git commit -m "feat: guard resolve_approval against double-resolution, return bool"
```

---

### Task 3: Unify local resolve+audit paths

Three copies of the same inline "call `resolve_approval` then `log_audit`" pattern exist:
1. `DmWatcher._handle_approve` — local path (when no `mcp_approval_id` or no session)
2. `DmWatcher._handle_reject` — local path (same conditions)
3. `ApprovalDecisionPoller._poll_once` — local path (when no `mcp_approval_id`)

A private `_resolve_and_audit` helper already lives in `approval_resolution.py` but is not exported and does not support a `via` field. This task:
- Renames it `resolve_and_audit` (public export)
- Adds an optional keyword-only `via` param that includes `"via"` in the audit payload when set
- Skips the audit write if `resolve_approval` returned `False` (guard blocked it — audit already written by the first caller)
- Routes all three inline copies through the helper

**Requires Task 2** to be committed first so `resolve_approval` already returns `bool`.

All external behaviour is preserved: the poller's local path continues to produce `"via": "cli"`; the watcher's local paths continue to produce no `via`.

**Files:**
- Modify: `deskbridge/dm/approval_resolution.py`
- Modify: `deskbridge/dm/watcher.py`
- Modify: `deskbridge/dm/approval_decision_poller.py`
- Create: `tests/test_approval_resolution.py`

- [ ] **Step 1: Write failing tests for the new public function**

Create `tests/test_approval_resolution.py`:

```python
import json
import pytest
from deskbridge.dm.approval_resolution import resolve_and_audit


async def test_resolve_and_audit_includes_via_when_provided(store, db_conn):
    await store.upsert_account(
        id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X"
    )
    await db_conn.execute(
        "INSERT INTO work_items (id, source_type, source_id, identity_id, "
        "status, idempotency_key) "
        "VALUES ('wi-1', 'dm', 'src-1', 'acc-alice', 'pending', 'k1')"
    )
    await db_conn.execute(
        "INSERT INTO approvals (id, work_item_id, action_description, status) "
        "VALUES ('appr-1', 'wi-1', 'do stuff', 'pending')"
    )
    await db_conn.commit()
    row = await store.get_approval("appr-1")
    await resolve_and_audit(store, "acc-alice", row, "approved", via="cli")
    audits = await store.get_audit_events("approval_resolved")
    assert len(audits) == 1
    payload = json.loads(audits[0]["payload_json"])
    assert payload["via"] == "cli"
    assert payload["resolution"] == "approved"
    assert payload["approval_id"] == "appr-1"


async def test_resolve_and_audit_omits_via_when_not_provided(store, db_conn):
    await store.upsert_account(
        id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X"
    )
    await db_conn.execute(
        "INSERT INTO work_items (id, source_type, source_id, identity_id, "
        "status, idempotency_key) "
        "VALUES ('wi-2', 'dm', 'src-2', 'acc-alice', 'pending', 'k2')"
    )
    await db_conn.execute(
        "INSERT INTO approvals (id, work_item_id, action_description, status) "
        "VALUES ('appr-2', 'wi-2', 'other stuff', 'pending')"
    )
    await db_conn.commit()
    row = await store.get_approval("appr-2")
    await resolve_and_audit(store, "acc-alice", row, "rejected")
    audits = await store.get_audit_events("approval_resolved")
    assert len(audits) == 1
    payload = json.loads(audits[0]["payload_json"])
    assert "via" not in payload
    assert payload["resolution"] == "rejected"
    assert payload["approval_id"] == "appr-2"


async def test_resolve_and_audit_skips_audit_when_guard_blocks(store, db_conn):
    """If resolve_approval returns False (already resolved), no audit is written."""
    await store.upsert_account(
        id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:X"
    )
    await db_conn.execute(
        "INSERT INTO work_items (id, source_type, source_id, identity_id, "
        "status, idempotency_key) "
        "VALUES ('wi-3', 'dm', 'src-3', 'acc-alice', 'pending', 'k3')"
    )
    await db_conn.execute(
        "INSERT INTO approvals (id, work_item_id, action_description, status) "
        "VALUES ('appr-3', 'wi-3', 'already done', 'approved')"
    )
    await db_conn.commit()
    row = await store.get_approval("appr-3")
    await resolve_and_audit(store, "acc-alice", row, "rejected")
    audits = await store.get_audit_events("approval_resolved")
    assert len(audits) == 0
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
uv run pytest tests/test_approval_resolution.py -v
```

Expected: FAIL — `ImportError: cannot import name 'resolve_and_audit'` (only `_resolve_and_audit` exists).

- [ ] **Step 3: Update `deskbridge/dm/approval_resolution.py`**

Replace the entire file:

```python
import json
import uuid
import structlog

from deskbridge.db.store import Store
from deskbridge.mcp.client import McpClient, McpToolError

log = structlog.get_logger()

_FAILURE_REPLY = "Failed to record decision — please check logs."
_STALE_REPLY = "Decision received, but the approval was already resolved or has expired."


async def resolve_and_audit(
    store: Store, account_id: str, row: dict, local_status: str,
    *, via: str | None = None,
) -> None:
    updated = await store.resolve_approval(row["id"], local_status)
    if not updated:
        log.debug("approval_already_resolved_skipping_audit", approval_id=row["id"])
        return
    payload: dict = {"approval_id": row["id"], "resolution": local_status}
    if via is not None:
        payload["via"] = via
    try:
        await store.log_audit(
            id=str(uuid.uuid4()),
            event_type="approval_resolved",
            identity_id=account_id,
            work_item_id=row["work_item_id"],
            payload_json=json.dumps(payload),
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
        await resolve_and_audit(store, account_id, row, local_status)
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
            await resolve_and_audit(store, account_id, row, local_status)
            log.warning(
                "approval_resolution_already_resolved",
                identity=identity_label,
                mcp_approval_id=mcp_approval_id,
            )
            return True, _STALE_REPLY
        elif cat == "approval_expired":
            await resolve_and_audit(store, account_id, row, "rejected")
            log.warning(
                "approval_resolution_expired",
                identity=identity_label,
                mcp_approval_id=mcp_approval_id,
            )
            return True, _STALE_REPLY
        elif cat == "approval_not_found":
            await resolve_and_audit(store, account_id, row, "rejected")
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

- [ ] **Step 4: Update `deskbridge/dm/watcher.py`**

Change the import at line 7 from:
```python
from deskbridge.dm.approval_resolution import resolve_approval_via_mcp
```
to:
```python
from deskbridge.dm.approval_resolution import resolve_and_audit, resolve_approval_via_mcp
```

In `_handle_approve`, replace the `else:` block (lines 211–222):
```python
                else:
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
with:
```python
                else:
                    await resolve_and_audit(self._store, self._account_id, row, "approved")
                    reply = "Approved."
```

In `_handle_reject`, replace the `else:` block (lines 246–257):
```python
                else:
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
with:
```python
                else:
                    await resolve_and_audit(self._store, self._account_id, row, "rejected")
                    reply = "Rejected."
```

Do **not** remove `import uuid` from `watcher.py` — it is still used by `_handle_task`, `_handle_status`, and `_handle_cancel`.

- [ ] **Step 5: Update `deskbridge/dm/approval_decision_poller.py`**

Remove `import uuid` (line 3 — no longer used after this change).

Change the import at line 6 from:
```python
from deskbridge.dm.approval_resolution import resolve_approval_via_mcp
```
to:
```python
from deskbridge.dm.approval_resolution import resolve_and_audit, resolve_approval_via_mcp
```

In `_poll_once`, replace the `else:` block (lines 85–99):
```python
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
```
with:
```python
            else:
                await resolve_and_audit(
                    self._store, self._account_id, row, local_status, via="cli"
                )
                log.info(
                    "approval_decision_poller_resolved_locally",
                    identity=self._identity_label,
                    approval_id=row["id"],
                    resolution=local_status,
                )
```

- [ ] **Step 6: Run new tests to verify they pass**

```bash
uv run pytest tests/test_approval_resolution.py -v
```

Expected: all three tests PASS

- [ ] **Step 7: Run the watcher and poller suites to catch regressions**

```bash
uv run pytest tests/test_dm_watcher.py tests/test_approval_decision_poller.py -v
```

Expected: all existing tests PASS

- [ ] **Step 8: Commit**

```bash
git add deskbridge/dm/approval_resolution.py deskbridge/dm/watcher.py deskbridge/dm/approval_decision_poller.py tests/test_approval_resolution.py
git commit -m "refactor: unify local resolve+audit paths through shared resolve_and_audit helper"
```

---

### Task 4: Fix approve/reject success copy for local-only approvals

`deskbridge approve <id>` and `deskbridge reject <id>` always reply:

> "Approval queued for appr-xxx — the supervisor will forward the decision to MCP."

This is wrong when the approval has no `mcp_approval_id`. In that case `ApprovalDecisionPoller` resolves locally and never calls MCP. Fix: branch the suffix on `row["mcp_approval_id"]`.

**Files:**
- Modify: `deskbridge/cli.py` (function `_request_approval_decision`, lines 337–340)
- Test: `tests/test_cli.py`

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_cli.py`:

```python
async def test_approve_local_only_approval_copy_mentions_locally(config_file, tmp_path):
    """Approvals without mcp_approval_id are resolved locally — copy should say so."""
    db_path = tmp_path / "test.db"
    await _seed(
        db_path,
        "INSERT INTO approvals (id, identity_id, action_description, status) "
        "VALUES ('appr-1', 'acc-alice', 'pay invoice', 'pending')",
    )
    runner = CliRunner()
    result = runner.invoke(cli, ["approve", "appr-1", "--config", str(config_file)])
    assert result.exit_code == 0
    assert "locally" in result.output.lower()
    assert "mcp" not in result.output.lower()


async def test_approve_mcp_correlated_approval_copy_mentions_mcp(config_file, tmp_path):
    """Approvals with mcp_approval_id are forwarded to MCP — copy should say so."""
    db_path = tmp_path / "test.db"
    await _seed(
        db_path,
        "INSERT INTO approvals (id, identity_id, action_description, status, mcp_approval_id) "
        "VALUES ('appr-1', 'acc-alice', 'pay invoice', 'pending', 'req-ext-1')",
    )
    runner = CliRunner()
    result = runner.invoke(cli, ["approve", "appr-1", "--config", str(config_file)])
    assert result.exit_code == 0
    assert "mcp" in result.output.lower()
    assert "locally" not in result.output.lower()
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
uv run pytest tests/test_cli.py::test_approve_local_only_approval_copy_mentions_locally tests/test_cli.py::test_approve_mcp_correlated_approval_copy_mentions_mcp -v
```

Expected: `test_approve_local_only_approval_copy_mentions_locally` FAILS (output currently says "MCP"); `test_approve_mcp_correlated_approval_copy_mentions_mcp` PASSES (already correct by accident since the hardcoded copy mentions MCP).

- [ ] **Step 3: Update `_request_approval_decision` in `deskbridge/cli.py`**

Replace lines 337–340:
```python
        verb = "Approval" if approved else "Rejection"
        return True, (
            f"{verb} queued for {approval_id} — "
            "the supervisor will forward the decision to MCP."
        )
```
with:
```python
        verb = "Approval" if approved else "Rejection"
        if row["mcp_approval_id"]:
            suffix = "the supervisor will forward the decision to MCP."
        else:
            suffix = "the supervisor will resolve this locally (no MCP approval associated)."
        return True, f"{verb} queued for {approval_id} — {suffix}"
```

- [ ] **Step 4: Run both new tests to verify they pass**

```bash
uv run pytest tests/test_cli.py::test_approve_local_only_approval_copy_mentions_locally tests/test_cli.py::test_approve_mcp_correlated_approval_copy_mentions_mcp -v
```

Expected: both PASS

- [ ] **Step 5: Verify existing approve/reject tests still pass**

The existing tests (`test_approve_pending_approval_queues_decision`, `test_reject_pending_approval_queues_decision`) seed approvals without `mcp_approval_id` and assert `"queued" in result.output.lower()`. The new local-only message still contains "queued", so those tests remain green.

```bash
uv run pytest tests/test_cli.py -v
```

Expected: all tests PASS

- [ ] **Step 6: Commit**

```bash
git add deskbridge/cli.py tests/test_cli.py
git commit -m "fix: branch approve/reject copy on mcp_approval_id for local-only approvals"
```

---

### Task 5: Test stderr routing for CLI failure messages

`cancel`, `retry`, `approve`, and `reject` emit failure messages via `click.echo(message, err=not ok)` — success goes to stdout, failures go to stderr. The existing tests use `CliRunner()` which merges both streams into `result.output`, so the routing has never been verified.

Add three tests using `CliRunner(mix_stderr=False)` that assert failure messages land in stderr and stdout is empty. With `mix_stderr=False` in Click 8.0+: `result.output` is stdout only; `result.stderr` is stderr.

> These tests verify existing behaviour. If any test fails, the `err=not ok` wiring in `cli.py` is broken and needs to be fixed there — not in the test.

**Files:**
- Test: `tests/test_cli.py`

- [ ] **Step 1: Write the tests**

Add to `tests/test_cli.py`:

```python
async def test_cancel_missing_item_failure_goes_to_stderr(config_file, tmp_path):
    db_path = tmp_path / "test.db"
    await _seed(db_path)
    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(cli, ["cancel", "wi-missing", "--config", str(config_file)])
    assert result.exit_code == 1
    assert "not found" in result.stderr.lower()
    assert result.output == ""


async def test_retry_wrong_status_failure_goes_to_stderr(config_file, tmp_path):
    db_path = tmp_path / "test.db"
    await _seed(
        db_path,
        "INSERT INTO work_items (id, source_type, source_id, identity_id, status, "
        "idempotency_key, summary) "
        "VALUES ('wi-1', 'dm', 'msg-1', 'acc-alice', 'pending', 'k1', 'fix the bug')",
    )
    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(cli, ["retry", "wi-1", "--config", str(config_file)])
    assert result.exit_code == 1
    assert "only failed" in result.stderr.lower()
    assert result.output == ""


async def test_approve_missing_approval_failure_goes_to_stderr(config_file, tmp_path):
    db_path = tmp_path / "test.db"
    await _seed(db_path)
    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(cli, ["approve", "appr-missing", "--config", str(config_file)])
    assert result.exit_code == 1
    assert "not found" in result.stderr.lower()
    assert result.output == ""
```

- [ ] **Step 2: Run tests to verify they pass**

```bash
uv run pytest tests/test_cli.py::test_cancel_missing_item_failure_goes_to_stderr tests/test_cli.py::test_retry_wrong_status_failure_goes_to_stderr tests/test_cli.py::test_approve_missing_approval_failure_goes_to_stderr -v
```

Expected: all three PASS (verifying existing wiring)

- [ ] **Step 3: Commit**

```bash
git add tests/test_cli.py
git commit -m "test: verify cancel/retry/approve failure messages route to stderr"
```

---

### Task 6: Add index for `work_items` status queries

`deskbridge status` (Task 1) queries `work_items` twice by `status`: once for active items and once for retry targets, both ordered by `created_at`. With no index on `(status, created_at)` these are full table scans. Add a migration to create the index.

The `_MIGRATIONS` list in `schema.py` uses `CREATE INDEX IF NOT EXISTS`, which is natively idempotent (unlike `ALTER TABLE`), so no error-swallowing is needed.

**Files:**
- Modify: `deskbridge/db/schema.py` (`_MIGRATIONS` list, currently ending at line 144)
- Test: `tests/test_schema.py`

- [ ] **Step 1: Write the failing test**

Add to `tests/test_schema.py`:

```python
async def test_work_items_status_index_exists(db):
    async with db.execute(
        "SELECT name FROM sqlite_master "
        "WHERE type='index' AND name='work_items_status_created_at'"
    ) as cur:
        row = await cur.fetchone()
    assert row is not None, "index work_items_status_created_at not found"
```

(The `db` fixture in `test_schema.py` already calls `apply_schema`, so the migration runs before the test.)

- [ ] **Step 2: Run test to verify it fails**

```bash
uv run pytest tests/test_schema.py::test_work_items_status_index_exists -v
```

Expected: FAIL — index does not yet exist.

- [ ] **Step 3: Add the migration to `deskbridge/db/schema.py`**

Append to `_MIGRATIONS` (lines 140–145):
```python
_MIGRATIONS = [
    "ALTER TABLE projects ADD COLUMN adapter TEXT NOT NULL DEFAULT 'claude-code'",
    "ALTER TABLE projects ADD COLUMN openclaw_agent_id TEXT",
    "ALTER TABLE work_items ADD COLUMN attempt_count INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE work_items ADD COLUMN next_retry_at TEXT",
    "CREATE INDEX IF NOT EXISTS work_items_status_created_at ON work_items (status, created_at)",
]
```

- [ ] **Step 4: Run test to verify it passes**

```bash
uv run pytest tests/test_schema.py::test_work_items_status_index_exists -v
```

Expected: PASS

- [ ] **Step 5: Run the full schema test suite**

```bash
uv run pytest tests/test_schema.py -v
```

Expected: all tests PASS

- [ ] **Step 6: Commit**

```bash
git add deskbridge/db/schema.py tests/test_schema.py
git commit -m "feat: add index on work_items(status, created_at) for status query performance"
```

---

## Self-Review

**1. Spec coverage:**

| Item | Task |
|---|---|
| `status` doesn't list retry targets | Task 1 ✅ |
| Guard `resolve_approval` (enables Task 3) | Task 2 ✅ |
| Unify resolve+audit duplication | Task 3 ✅ |
| Approve/reject copy overstates for local-only | Task 4 ✅ |
| stderr routing untested | Task 5 ✅ |
| No index on `work_items(status, created_at)` | Task 6 ✅ |

**2. Placeholder scan:** No TBDs, "similar to above", or missing code blocks found.

**3. Type consistency:**
- Task 3 depends on Task 2: `resolve_approval` returns `bool` before `resolve_and_audit` is rewritten — the `if not updated: return` check is valid at the time it executes.
- `resolve_and_audit(store, account_id, row, local_status, *, via=None)` — `row` from `get_pending_approval` and `get_requested_approval_decisions` both return `aiosqlite.Row` with `id`, `work_item_id` columns. Confirmed in schema.
- Task 4: `row["mcp_approval_id"]` — `get_approval` does `SELECT * FROM approvals`; column exists in schema, nullable. Falsy when NULL.
- Task 6: `CREATE INDEX IF NOT EXISTS` is idempotent. The `apply_schema` loop's duplicate-column guard is not needed (and not triggered) for this migration type.

**4. Task ordering constraint:** Task 2 must commit before Task 3 begins. Tasks 1, 4, 5, 6 are independent of each other and of the Task 2/3 pair.
