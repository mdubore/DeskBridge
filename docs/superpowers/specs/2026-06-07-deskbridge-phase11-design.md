# DeskBridge Phase 11: Operability Triad — Design

## Overview

Three tightly related improvements that turn DeskBridge from a demo daemon into something you can leave running for a week and trust:

1. **Work item retry with bounded attempts** — failed runs are automatically re-queued with a 60-second cooldown, up to a configurable per-project maximum.
2. **Comprehensive audit logging** — call sites added at every meaningful operational seam; the existing table and `Store.log_audit` method are already in place.
3. **Enhanced `deskbridge status` CLI** — five sections covering accounts, queue counts, pending approvals, recent agent runs, and watcher cursor freshness.

---

## 1. Work Item Retry

### Schema

Two new columns on `work_items`, added via `_MIGRATIONS` in `deskbridge/db/schema.py`:

```sql
ALTER TABLE work_items ADD COLUMN attempt_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE work_items ADD COLUMN next_retry_at TEXT;
```

- `attempt_count` — incremented each time the item is dispatched. Starts at 0; becomes 1 on first dispatch.
- `next_retry_at` — ISO timestamp. `NULL` means eligible immediately. Set to `now + 60s` when a failed item is re-queued for retry.

### Config

New optional field on `ProjectConfig` in `deskbridge/config.py`:

```python
max_agent_attempts: int = 3
```

- Default: 3 (one initial attempt + two retries).
- Validated: must be ≥ 1.
- Example TOML:

```toml
[projects.my-project]
max_agent_attempts = 5
```

### Store changes

**`get_pending_work_items`** gains a `next_retry_at` filter so cooling-down items are skipped:

```sql
SELECT * FROM work_items
WHERE status = 'pending'
  AND identity_id = ?
  AND (next_retry_at IS NULL OR next_retry_at <= strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
ORDER BY priority, created_at
LIMIT ?
```

**New method: `retry_work_item`** — resets a failed item to `pending` with incremented attempt count and a cooldown:

```python
async def retry_work_item(self, id: str, next_retry_at: str) -> None:
    async with self._conn.execute(
        """
        UPDATE work_items
        SET status = 'pending',
            attempt_count = attempt_count + 1,
            next_retry_at = ?,
            updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
        WHERE id = ?
        """,
        (next_retry_at, id),
    ):
        pass
    await self._conn.commit()
```

**`claim_work_item`** is unchanged — it only transitions `status` from `pending` to `dispatched`. `attempt_count` is incremented by `retry_work_item` when a failed item is re-queued, not at claim time. This means `attempt_count` counts completed failed attempts: 0 on first dispatch, 1 after the first retry is queued, etc. The poller compares `attempt_count` against `max_agent_attempts` after the run finishes.

### Poller changes (`deskbridge/agent/poller.py`)

After the runner task completes with `status == 'failed'`, the poller checks whether the item is retry-eligible:

```python
if completed_item["status"] == "failed":
    if completed_item["attempt_count"] < project_cfg.max_agent_attempts:
        next_retry_at = _iso_offset(60)  # now + 60s
        await self._store.retry_work_item(completed_item["id"], next_retry_at)
        await self._store.log_audit(
            id=str(uuid4()),
            event_type="work_item_retry_queued",
            work_item_id=completed_item["id"],
            payload_json=json.dumps({
                "attempt_count": completed_item["attempt_count"],
                "next_retry_at": next_retry_at,
            }),
        )
    else:
        # Terminal failure — leave as failed, emit audit event
        await self._store.log_audit(
            id=str(uuid4()),
            event_type="work_item_terminal",
            work_item_id=completed_item["id"],
            payload_json=json.dumps({
                "status": "failed",
                "attempt_count": completed_item["attempt_count"],
            }),
        )
```

`_iso_offset(seconds)` is a module-level helper:

```python
def _iso_offset(seconds: int) -> str:
    from datetime import datetime, timezone, timedelta
    return (datetime.now(timezone.utc) + timedelta(seconds=seconds)).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
```

The `done`, `interrupted`, and `cancelled` terminal statuses also emit `work_item_terminal` audit events. The `attempt_count` field is read from the row returned by `get_work_item` after the run completes.

---

## 2. Audit Log

The `audit_log` table and `Store.log_audit` method already exist. Phase 11 adds call sites.

All `log_audit` calls are wrapped in `try/except Exception` — a failed audit write must never crash a watcher, runner, or outbox drainer.

### Event types

| Event type | File | Key payload fields |
|---|---|---|
| `work_item_dispatched` | `agent/poller.py` | `work_item_id`, `adapter`, `attempt_count` |
| `work_item_retry_queued` | `agent/poller.py` | `work_item_id`, `attempt_count`, `next_retry_at` |
| `work_item_terminal` | `agent/poller.py` | `work_item_id`, `status`, `attempt_count` |
| `agent_run_started` | `agent/runner.py` | `run_id`, `adapter`, `work_item_id` |
| `agent_run_finished` | `agent/runner.py` | `run_id`, `status`, `returncode` |
| `approval_requested` | `dm/approval_watcher.py` | `approval_id`, `mcp_approval_id`, `scope` |
| `approval_resolved` | `dm/approval_watcher.py` | `approval_id`, `resolution` |
| `outbox_delivered` | `dm/outbox.py` | `outbox_id`, `dest_pubkey` |
| `outbox_delivery_failed` | `dm/outbox.py` | `outbox_id`, `attempts` |

`session_unlocked` and `session_unlock_failed` already exist in `mcp/session.py` and are unchanged.

### Placement in `runner.py`

`agent_run_started` fires immediately before the subprocess is spawned (step 3 of `_do_run`):

```python
try:
    await self._store.log_audit(
        id=str(uuid4()),
        event_type="agent_run_started",
        identity_id=f"acc-{project.identity}",
        project_id=project.id,
        work_item_id=work_item["id"],
        payload_json=json.dumps({
            "run_id": run_id,
            "adapter": project.adapter,
        }),
    )
except Exception:
    log.warning("audit_log_failed", event_type="agent_run_started", run_id=run_id)
```

`agent_run_finished` fires in step 6, after `update_agent_run` writes the final status:

```python
try:
    await self._store.log_audit(
        id=str(uuid4()),
        event_type="agent_run_finished",
        identity_id=f"acc-{project.identity}",
        project_id=project.id,
        work_item_id=work_item["id"],
        payload_json=json.dumps({
            "run_id": run_id,
            "status": final_status,
            "returncode": proc.returncode,
        }),
    )
except Exception:
    log.warning("audit_log_failed", event_type="agent_run_finished", run_id=run_id)
```

### Placement in `approval_watcher.py`

`approval_requested` fires after a new approval row is inserted. `approval_resolved` fires after the approval status is updated to `approved` or `rejected`.

### Placement in `outbox.py`

`outbox_delivered` fires after a successful `send_dm` call. `outbox_delivery_failed` fires when `delivery_attempts` reaches the outbox max and the item is marked permanently failed.

---

## 3. Enhanced `deskbridge status`

`cli.py` `_show_status` grows from one query to five. All reads are direct SQLite — no running daemon required.

### Output format

```
Accounts
  [ok] alice  session=abc123  last_unlocked=2026-06-07T09:00:00Z
  [degraded] bob  session=none  last_unlocked=never

Work Queue
  pending=2  dispatched=1  done=47  failed=3  cancelled=0

Approvals
  pending=1

Recent Runs (last 5)
  done     claude-code  2026-06-07T10:12:00Z  "Fix login bug"
  failed   codex        2026-06-07T09:55:00Z  "Add tests for auth"
  done     gemini       2026-06-07T09:30:00Z  "Summarize open issues"
  done     claude-code  2026-06-07T08:44:00Z  "Update README"
  done     hermes       2026-06-07T08:01:00Z  "Check-in: status report"

Watchers (last cursor update)
  dm      alice  2026-06-07T10:14:22Z
  group   alice  2026-06-07T10:14:01Z
  kanban  alice  2026-06-07T10:13:45Z
```

### Queries

**Work Queue** — single aggregation query:
```sql
SELECT status, COUNT(*) as n FROM work_items GROUP BY status
```

**Approvals** — count of `status = 'pending'` rows:
```sql
SELECT COUNT(*) FROM approvals WHERE status = 'pending'
```

**Recent Runs** — join `agent_runs` with `work_items` for the summary:
```sql
SELECT ar.status, ar.adapter_type, ar.updated_at, wi.summary
FROM agent_runs ar
LEFT JOIN work_items wi ON wi.id = ar.work_item_id
ORDER BY ar.updated_at DESC
LIMIT 5
```

**Watchers** — all cursor rows ordered by type:
```sql
SELECT cursor_type, identity_id, updated_at FROM cursors ORDER BY cursor_type
```

The `_show_status` function receives the `db_path` only. The five queries run sequentially inside a single `aiosqlite.connect` context. No config parsing is required beyond resolving `db_path`.

---

## Files Touched

| File | Change |
|---|---|
| `deskbridge/db/schema.py` | Add two `_MIGRATIONS` entries for `attempt_count` and `next_retry_at` |
| `deskbridge/config.py` | Add `max_agent_attempts: int = 3` with `ge=1` constraint |
| `deskbridge/db/store.py` | Add `retry_work_item`; update `get_pending_work_items` SQL (`next_retry_at` filter); `claim_work_item` unchanged |
| `deskbridge/agent/poller.py` | Add retry/terminal logic after runner completes; add `work_item_dispatched`, `work_item_retry_queued`, `work_item_terminal` audit calls |
| `deskbridge/agent/runner.py` | Add `agent_run_started`, `agent_run_finished` audit calls |
| `deskbridge/dm/approval_watcher.py` | Add `approval_requested`, `approval_resolved` audit calls |
| `deskbridge/dm/outbox.py` | Add `outbox_delivered`, `outbox_delivery_failed` audit calls |
| `deskbridge/cli.py` | Expand `_show_status` to five-section output |
| `tests/test_store.py` | Add tests for `retry_work_item`, updated `get_pending_work_items` cooldown filter, `claim_work_item` attempt increment |
| `tests/test_work_item_poller.py` | Add retry and terminal-failure path tests |
| `tests/test_config.py` | Add `max_agent_attempts` validation test |
| `tests/test_cli.py` | Add tests for five-section status output |

`tests/test_agent_runner.py`, `tests/test_approval_watcher.py`, and `tests/test_outbox_drainer.py` require no structural changes — audit call sites are fire-and-forget and do not alter observable return values.

---

## Testing

**`test_store.py`:**
- `test_retry_work_item_resets_to_pending` — inserts a failed item, calls `retry_work_item`, asserts `status='pending'`, `attempt_count` incremented, `next_retry_at` set
- `test_get_pending_work_items_skips_cooling_down` — inserts a pending item with `next_retry_at` in the future, asserts it is not returned
- `test_get_pending_work_items_includes_elapsed_cooldown` — inserts a pending item with `next_retry_at` in the past, asserts it is returned
- `test_claim_work_item_does_not_change_attempt_count` — asserts `attempt_count` remains 0 after `claim_work_item`

**`test_work_item_poller.py`:**
- `test_failed_run_below_max_attempts_requeues` — runner returns `failed`, `attempt_count < max_agent_attempts`: item resets to `pending` with `next_retry_at` set
- `test_failed_run_at_max_attempts_stays_failed` — runner returns `failed`, `attempt_count == max_agent_attempts`: item stays `failed`

**`test_config.py`:**
- `test_max_agent_attempts_default_is_3` — `ProjectConfig(...)` without the field gives `max_agent_attempts=3`
- `test_max_agent_attempts_zero_raises` — `max_agent_attempts=0` raises `ConfigError`

**`test_cli.py`:**
- `test_status_shows_all_sections` — seeds accounts, work items, approvals, agent runs, cursors; asserts all five section headers and representative values appear in output
