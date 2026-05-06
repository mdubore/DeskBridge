# DeskBridge Phase 6: Approval Flow Alignment — Design Spec

**Goal:** Replace the Phase 5 approval flow (built against a hypothetical NostrDesk interface) with one that matches the real NostrDesk MCP contract: snapshot-polling `list_pending_approvals`, resolving via `respond_to_approval`, and injecting an `approval_required` retry instruction into agent subprocesses.

**Architecture:** Three surgical changes to existing files — no new files, no schema changes, no supervisor changes. `ApprovalRequestWatcher` switches from long-poll to interval-poll. `DmWatcher._handle_approve/_handle_reject` swap the MCP tool call. `AgentRunner` prepends a retry instruction to the agent prompt.

**Tech stack:** Same as prior phases — aiosqlite, structlog, asyncio, McpClient, pytest-asyncio (`asyncio_mode = auto`).

---

## Background

Phase 5 was built against three hypothetical NostrDesk MCP tools (`wait_for_pending_approval_requests`, `wait_for_approval_resolution`, `resolve_approval_request`) that do not exist in the real NostrDesk implementation. The real interface is:

- `list_pending_approvals` — snapshot poll, returns all pending approval rows for the active session
- `get_approval_status` — fetch one known approval row
- `respond_to_approval` — write the operator's decision back to NostrDesk

When an agent calls a protected MCP tool, NostrDesk creates a `permission_requests` row, waits internally for a decision, and — if the wait times out — returns an MCP error with `category: "approval_required"` and `approval_request_id` in the error payload. DeskBridge detects this via polling, notifies the operator, and calls `respond_to_approval`. The agent is instructed (via system prompt) to wait 60 seconds and retry if it receives this error.

---

## NostrDesk MCP Contract

### `list_pending_approvals`

**Input:**
```json
{
  "session_id": "string",
  "limit": 100
}
```

**Output:** JSON array of `PermissionRequest` records. Relevant fields per item:

| Field | Type | Notes |
|---|---|---|
| `id` | string | The `approval_request_id` — required for `respond_to_approval` |
| `tool_name` | string | Name of the approval-gated tool |
| `display_payload_json` | string | Operator-facing description of the request |
| `request_payload_json` | string | Operational details — handle carefully, do not include in DMs |
| `status` | string | `pending` only (expired rows are filtered out) |
| `expires_at` | string (ISO 8601) | Stored in `approvals.expires_at` |

### `respond_to_approval`

**Input:**
```json
{
  "session_id": "string",
  "approval_request_id": "string",
  "approved": true,
  "note": "string | null"
}
```

**Output (success):**
```json
{
  "ok": true,
  "approval_request_id": "string",
  "status": "approved | denied"
}
```

**Error categories:**
- `already_resolved` — approval was already approved or denied
- `expired` — approval window has passed
- `not_found` — no approval with that ID exists for the session (wrong session, stale local DB, or malformed state)
- Other MCP errors — internal failure

---

## File Map

| File | Action | Change |
|---|---|---|
| `deskbridge/dm/approval_watcher.py` | Modify | Replace long-poll with interval-poll; remove cursor logic; add row validation |
| `deskbridge/dm/watcher.py` | Modify | Swap `resolve_approval_request` → `respond_to_approval`; update operator reply per MCP result |
| `deskbridge/agent/runner.py` | Modify | Prepend `_APPROVAL_INSTRUCTION` constant to agent prompt |
| `tests/test_approval_watcher.py` | Modify | Update mocks; remove cursor tests; add dedup + malformed-row tests |
| `tests/test_dm_watcher.py` | Modify | Update tool assertions; add per-error-category reply tests |
| `tests/test_agent_runner.py` | Modify | Add prompt structure and length assertions |

No changes to `schema.py`, `store.py`, `supervisor.py`, or any `mcp/` module.

---

## Component Design

### `deskbridge/dm/approval_watcher.py`

**Constructor change:** rename `poll_timeout_secs: int = 30` → `poll_interval_secs: float = 2.0`. The poll interval is intentionally short (2 seconds) so approvals are discovered well within NostrDesk's internal wait window.

**`run()` loop:**

1. Get `session_id` from broker. If `None`, sleep `poll_interval_secs` and retry.
2. Call `list_pending_approvals` with `{"session_id": session_id, "limit": 100}`.
3. For each item in the result, validate before processing:
   - Missing `id` → log `approval_watcher_malformed_row` with raw item, skip
   - Missing `tool_name` → use `"unknown tool"`
   - Missing `display_payload_json` → fall back to `request_payload_json`; if also absent, use `"{}"`
   - `display_payload_json` present but not valid JSON → store as `{"raw_display_payload": "<raw string>"}`
4. For each valid item, call `store.insert_approval(mcp_approval_id=req["id"], action_description=f"{tool_name}: {display_payload}", expires_at=req.get("expires_at"), identity_id=self._account_id, ...)`. `INSERT OR IGNORE` on the `mcp_approval_id` unique index makes this idempotent — repeated polls produce no duplicate rows.
5. If `operator_npub` is set, call `store.insert_outbox_item(..., idempotency_key=f"approval-notify-{req['id']}")`. `INSERT OR IGNORE` on `idempotency_key` ensures the operator receives exactly one DM per approval regardless of poll frequency.
6. Sleep `poll_interval_secs` using `asyncio.wait_for(self._shutdown_event.wait(), timeout=self._poll_interval_secs)` with `except asyncio.TimeoutError: pass`.

**Operator DM format:**
```
Approval required: <tool_name>

<display_payload (formatted or raw)>

Request ID: <id>

Reply 'approve' or 'reject'.
```

**Removed:** all cursor load/save/warning logic. `list_pending_approvals` is a snapshot; there is no cursor.

**Error handling:** REJECT exits the loop. REAUTH logs a warning and sleeps. RESET_CURSOR is treated identically to REAUTH (log warning + sleep) — there is no cursor to reset, so it is a no-op beyond the retry. Unknown MCP errors and unexpected exceptions log and sleep. Per-item validation failures log and skip — the loop continues.

---

### `deskbridge/dm/watcher.py`

**`_handle_approve`** change — MCP call:

```python
# Before (Phase 5)
await self._client.call_tool(
    "resolve_approval_request",
    {
        "session_id": session_id,
        "request_id": mcp_approval_id,
        "decision": "approved",
    },
)

# After (Phase 6)
await self._client.call_tool(
    "respond_to_approval",
    {
        "session_id": session_id,
        "approval_request_id": mcp_approval_id,
        "approved": True,
        "note": None,
    },
)
```

**`_handle_reject`** — same swap with `"approved": False`.

**Operator reply per MCP result:**

| Outcome | Log event | Operator DM reply |
|---|---|---|
| Success (`ok: true`) | `dm_watcher_approval_resolved` | `"Approved."` / `"Denied."` |
| `already_resolved` / `expired` error | `dm_watcher_approval_already_resolved` at `warning` | `"Decision received, but the approval was already resolved or has expired."` |
| `not_found` error | `dm_watcher_approval_not_found` at `error` (wrong session, stale DB, or malformed state) | `"Decision received, but the approval was already resolved or has expired."` |
| Other MCP error | `dm_watcher_resolve_approval_error` (with full error detail in log, not in DM) | `"Failed to record decision — please check logs."` |

`not_found` is logged at a higher severity than `already_resolved`/`expired` — it can indicate wrong `session_id`, stale local DB state, or malformed approval data, making it more diagnostic. The operator-facing reply is the same for both.

Full error detail is logged but never included verbatim in the operator DM — internal traces and protocol details stay out of the operator's DM channel.

The `store.resolve_approval` local DB call and the outbox reply to the operator happen regardless of MCP outcome. The outbox reply message reflects the actual outcome category as above.

---

### `deskbridge/agent/runner.py`

**Module-level constant:**

```python
_APPROVAL_INSTRUCTION = (
    "If any MCP tool returns an error with category \"approval_required\", "
    "do not stop. Wait 60 seconds and retry the same tool call — a human "
    "operator has been notified and will decide shortly. "
    "If retrying still fails, report the error clearly and stop.\n\n"
)
```

**Prompt construction in `_do_run()`:**

```python
# Before
prompt = f"{work_item['summary']}\n\n{work_item['payload_json']}"[:4000]

# After
task_text = f"{work_item['summary']}\n\n{work_item['payload_json']}"
prompt = _APPROVAL_INSTRUCTION + task_text[:4000 - len(_APPROVAL_INSTRUCTION)]
```

The approval instruction is never truncated. The `[:4000 - len(_APPROVAL_INSTRUCTION)]` guard ensures the total prompt stays within 4000 characters with the instruction intact.

---

## End-to-End Flow

```
Agent calls protected MCP tool
  → NostrDesk creates permission_requests row and waits internally

DeskBridge ApprovalRequestWatcher polls list_pending_approvals (every 2s)
  → Sees pending request
  → INSERT OR IGNORE into approvals table (idempotent)
  → INSERT OR IGNORE into outbox (idempotent, idempotency_key=approval-notify-{id})
  → OutboxDrainer sends operator DM once: "Approval required: <tool_name>..."

Operator replies "approve" or "reject"
  → DmWatcher parses intent as APPROVE/REJECT
  → store.resolve_approval updates local DB
  → DmWatcher calls respond_to_approval(session_id, approval_request_id, approved=True/False)
  → NostrDesk updates permission_requests.status
  → NostrDesk's internal wait unblocks → original tool call proceeds or aborts

Agent (if still waiting) retries the tool call and succeeds/fails based on decision
```

**Recovery path (internal wait timed out before operator responded):**
- Agent received `approval_required` error, waited 60s, retried — still got the error if operator hadn't responded yet
- Agent reports the error and stops the run
- DeskBridge still calls `respond_to_approval` when the operator eventually responds
- Operator re-triggers the task knowing the next run will succeed

---

## Testing

### `tests/test_approval_watcher.py`

Updates:
- Replace `wait_for_pending_approval_requests` mock with `list_pending_approvals` returning a list
- Remove: cursor advance test, cursor warning test

New tests:
- Second poll with same `id` → no new `approvals` row, no new `outbox` row (INSERT OR IGNORE dedup)
- Row missing `id` → skipped entirely, watcher loop continues
- Row missing `tool_name` → inserted with `"unknown tool"` in `action_description`
- Row with invalid `display_payload_json` → `action_description` uses `{"raw_display_payload": "..."}` form
- Row missing `display_payload_json` → falls back to `request_payload_json`

Keep: no session skips poll; REJECT exits; operator DM written when `operator_npub` set; unexpected exception logs and continues.

### `tests/test_dm_watcher.py`

Updates:
- `approve calls resolve` → assert `respond_to_approval` called with `{"session_id": <active session from broker>, "approval_request_id": ..., "approved": True, "note": None}` — `session_id` must match the actual session returned by the broker mock, not a placeholder
- `reject calls resolve` → same with `approved: False`

New tests:
- `respond_to_approval` returns `already_resolved`/`expired` → operator DM says "already resolved or expired"; `dm_watcher_approval_already_resolved` logged; loop continues
- `respond_to_approval` returns `not_found` → operator DM says "already resolved or expired"; `dm_watcher_approval_not_found` logged at higher severity; loop continues
- `respond_to_approval` returns other MCP error → operator DM says "Failed to record decision"; full error in logs, not in DM; loop continues

Keep: MCP failure still sends outbox reply.

### `tests/test_agent_runner.py`

New test: prompt structure and length:
- Assert `prompt.startswith(_APPROVAL_INSTRUCTION)` — instruction is present and not truncated
- Assert `len(prompt) <= 4000` — total prompt within limit
- With a task content longer than `4000 - len(_APPROVAL_INSTRUCTION)`: assert instruction is intact and task content is truncated (not the instruction)

---

## Error Handling Summary

| Scenario | Behavior |
|---|---|
| `list_pending_approvals` MCP error | Log + sleep; watcher continues |
| Approval row missing `id` | Log `approval_watcher_malformed_row`; skip row |
| Approval row missing `tool_name` | Use `"unknown tool"`; continue |
| Invalid `display_payload_json` | Store as `{"raw_display_payload": "..."}`; continue |
| `INSERT OR IGNORE` on duplicate approval | Silently ignored; no duplicate row or DM |
| `respond_to_approval` — success | Reply `"Approved."` / `"Denied."` |
| `respond_to_approval` — already resolved/expired | Log `warning`; reply "already resolved or expired"; non-fatal |
| `respond_to_approval` — not found | Log `error`; same operator reply; non-fatal |
| `respond_to_approval` — other error | Log full error detail; reply "Failed to record decision"; non-fatal |
| Agent subprocess gets `approval_required` error | Wait 60s; retry; report and stop if still failing |
