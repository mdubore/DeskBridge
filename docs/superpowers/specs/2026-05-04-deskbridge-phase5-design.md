# DeskBridge Phase 5: Agent Approval Requests — Design Spec

## Overview

Agents running under DeskBridge sometimes need human authorization before taking a sensitive action (e.g., force-pushing, deleting data). Phase 5 delivers the full approval flow: an agent signals it needs approval, DeskBridge notifies the operator via DM, the operator responds, and DeskBridge signals the decision back so the agent can continue or abort.

---

## Architecture

When a tool call requires human approval, NostrDesk returns `APPROVAL_REQUIRED` instead of a normal result. The agent immediately calls `wait_for_approval_resolution` and blocks.

Meanwhile, DeskBridge's `ApprovalRequestWatcher` (one per identity, running alongside `DmWatcher`) polls `wait_for_pending_approval_requests`. When it sees the new request it writes a DM to the outbox notifying the operator, and records the approval in the DB with `external_request_id` set.

When the operator replies, `DmWatcher` parses the intent as `APPROVE` or `REJECT`. The handler looks up the pending approval row, calls `store.resolve_approval`, then calls `resolve_approval_request` on the MCP server using the stored `external_request_id`. NostrDesk unblocks the agent's `wait_for_approval_resolution` with the decision. The agent proceeds or fails accordingly.

The work item stays `dispatched` throughout. If `wait_for_approval_resolution` times out, the agent receives a timeout error and should treat it as a rejection.

---

## NostrDesk Feature Spec

> **This section is a feature request for NostrDesk to implement.** DeskBridge cannot deliver Phase 5 without these MCP server changes.

### Trigger behavior change

When a tool call requires human authorization, instead of raising an error, NostrDesk returns:

```json
{
  "status": "APPROVAL_REQUIRED",
  "request_id": "<uuid>",
  "description": "<human-readable description of what needs approval and why>"
}
```

The calling agent is expected to immediately follow up with a `wait_for_approval_resolution` call using the returned `request_id`. NostrDesk must hold the request internally until it receives a `resolve_approval_request` call or the wait times out.

### New MCP Tool 1: `wait_for_pending_approval_requests`

**Purpose:** DeskBridge polls this to discover new approval requests from agents. Long-blocking like `wait_for_new_dms`.

**Input:**
```json
{
  "session_id": "string",
  "after_request_id": "string | null",
  "timeout_seconds": "integer"
}
```

**Output:**
```json
{
  "requests": [
    {
      "id": "string",
      "description": "string",
      "triggered_by_tool": "string",
      "created_at": "string (ISO 8601)"
    }
  ],
  "last_request_id": "string | null"
}
```

- Blocks until at least one pending request arrives or `timeout_seconds` elapses.
- `after_request_id` is a cursor: only return requests created after this ID.
- Returns an empty `requests` array (not an error) on timeout.

### New MCP Tool 2: `wait_for_approval_resolution`

**Purpose:** Called by the agent subprocess immediately after receiving `APPROVAL_REQUIRED`. Blocks until the approval is resolved or the timeout fires.

**Input:**
```json
{
  "session_id": "string",
  "request_id": "string",
  "timeout_seconds": "integer"
}
```

**Output (resolved):**
```json
{
  "decision": "approved | rejected"
}
```

**Output (timeout):** Return a tool error indicating the request timed out. The agent should treat this as a rejection.

### New MCP Tool 3: `resolve_approval_request`

**Purpose:** Called by DeskBridge after the operator sends an approve or reject DM. Signals the decision to NostrDesk so it can unblock the waiting agent.

**Input:**
```json
{
  "session_id": "string",
  "request_id": "string",
  "decision": "approved | rejected"
}
```

**Output:**
```json
{
  "ok": true
}
```

- If the request has already been resolved (double-resolve), return an error. DeskBridge will log and ignore it.
- If the request does not exist, return an error.

---

## DeskBridge Implementation

### Data model

No schema changes needed. The `approvals` table already has `mcp_approval_id TEXT` (the NostrDesk-issued request ID) and `work_item_id TEXT`. Both are nullable, so non-agent approvals continue to work as before.

### New file: `deskbridge/dm/approval_watcher.py`

`ApprovalRequestWatcher` — mirrors `DmWatcher` structure, one instance per identity.

- Constructor: `identity_label`, `store`, `client`, `broker`, `shutdown_event`, `poll_timeout_secs`
- `run()`:
  - Loads cursor from `store.get_cursor("approval_watcher", account_id)`
  - Loop: get session → call `wait_for_pending_approval_requests` with cursor → for each request: look up active work item via `store.get_latest_dispatched_work_item(account_id)`, call `store.insert_approval(id=uuid4(), mcp_approval_id=request["id"], work_item_id=dispatched_row["id"] if dispatched_row else None, action_description=request["description"], scope=None, request_text=None, expires_at=None)`, then write outbox DM → advance cursor
  - Outbox DM format: `"Approval required: <description>\n\nRequest ID: <id>\n\nReply 'approve' or 'reject'."`
  - Error handling: REJECT exits, REAUTH/RESET_CURSOR retry after sleep, unknown errors log + sleep + retry

### `deskbridge/dm/watcher.py` changes

- Add `self._session_id: str | None = None` to `__init__`
- In `run()` loop, set `self._session_id = session_id` after getting it from the broker
- `_handle_approve`: after `store.resolve_approval`, check `approval["mcp_approval_id"]`; if non-null, call `client.call_tool("resolve_approval_request", {"session_id": self._session_id, "request_id": approval["mcp_approval_id"], "decision": "approved"})`; log and ignore MCP errors
- `_handle_reject`: same pattern with `"rejected"`

### `deskbridge/db/store.py` changes

No new methods needed. `insert_approval` and `get_pending_approval` already exist. `get_pending_approval` uses `SELECT a.*` so it already returns `mcp_approval_id`.

### `deskbridge/supervisor.py` changes

- Import `ApprovalRequestWatcher`
- Spawn one `ApprovalRequestWatcher` task per identity alongside `DmWatcher`
- Add to `finally` cleanup list

---

## Error Handling

| Scenario | Behavior |
|---|---|
| `resolve_approval_request` MCP call fails | Log error; approval recorded in DB; agent times out on `wait_for_approval_resolution` and treats as rejection |
| Operator approves when no pending approval exists | Existing "No pending approval" reply; no MCP call |
| Approval request arrives but no session | `ApprovalRequestWatcher` holds cursor, retries next poll |
| Operator double-resolves | `resolve_approval_request` returns error from NostrDesk; DeskBridge logs and ignores |

---

## Testing

- **`tests/test_approval_watcher.py`** — mock store/client: new request triggers outbox write and DB upsert; no session skips poll; REJECT exits; cursor advances after batch
- **`tests/test_dm_watcher.py`** additions — `_handle_approve` calls `resolve_approval_request` when `external_request_id` present; skips MCP call when `external_request_id` is null
- **`tests/test_store.py`** additions — `insert_approval` with `mcp_approval_id` set; `get_pending_approval` returns `mcp_approval_id`
- **`tests/test_supervisor.py`** additions — `ApprovalRequestWatcher` task spawned per identity
