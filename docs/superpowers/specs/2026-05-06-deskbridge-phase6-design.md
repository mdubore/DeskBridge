# DeskBridge Phase 6: Approval Flow Alignment — Design Spec

**Goal:** Replace the Phase 5 approval flow (built against a hypothetical NostrDesk interface) with one that matches the real NostrDesk MCP contract: snapshot-polling `list_pending_approvals`, resolving via `respond_to_approval`, and injecting an `approval_required` retry instruction into agent subprocesses.

**Architecture:** Surgical changes to three existing files plus MCP error category parsing. No schema changes, no supervisor changes, no new long-running tasks.

**Tech stack:** Same as prior phases — aiosqlite, structlog, asyncio, McpClient, pytest-asyncio (`asyncio_mode = auto`).

---

## Background

Phase 5 was built against three hypothetical NostrDesk MCP tools (`wait_for_pending_approval_requests`, `wait_for_approval_resolution`, `resolve_approval_request`) that do not exist in the real NostrDesk implementation. The real interface is:

- `list_pending_approvals` — snapshot poll, returns all pending approval rows for the active session
- `get_approval_status` — fetch one known approval row by ID
- `respond_to_approval` — write the operator's decision back to NostrDesk

When an agent calls a protected MCP tool, NostrDesk creates a `permission_requests` row and waits internally for a decision. If the wait times out, it returns an MCP error with `category: "approval_required"` and `approval_request_id` in the error payload. DeskBridge detects new approvals via polling, notifies the operator, and calls `respond_to_approval` when the operator responds. The agent is instructed via system prompt to wait 60 seconds and retry if it receives this error.

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
| `display_payload_json` | string | Operator-facing description — safe to include in DMs |
| `request_payload_json` | string | Operational details — do not include in operator DMs |
| `status` | string | `pending` only (expired rows filtered out by NostrDesk) |
| `expires_at` | integer | Unix timestamp (seconds since epoch) |

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

**Error categories (in `error.data.category`):**
- `approval_already_resolved` — approval was already approved or denied
- `approval_expired` — approval window has passed
- `approval_not_found` — no approval with that ID exists for this session (wrong session, stale local DB, or malformed approval state)
- Other MCP errors — internal or infrastructure failure

---

## File Map

| File | Action | Change |
|---|---|---|
| `deskbridge/dm/approval_watcher.py` | Modify | Replace long-poll with interval-poll; remove cursor logic; add row validation; Unix timestamp conversion |
| `deskbridge/dm/watcher.py` | Modify | Swap `resolve_approval_request` → `respond_to_approval`; conditional local DB resolution; per-category operator reply |
| `deskbridge/agent/runner.py` | Modify | Prepend `_APPROVAL_INSTRUCTION` constant to agent prompt |
| `deskbridge/models.py` | Modify | Add `data: dict | None` field to `McpError`; preserve raw category string alongside enum |
| `deskbridge/mcp/errors.py` | Modify | Expose `category` and `data` from structured MCP error payload |
| `deskbridge/mcp/client.py` | Modify | Catch SDK-raised exceptions with `.data`; convert to `McpToolError` |
| `tests/test_approval_watcher.py` | Modify | Update mocks; remove cursor tests; add dedup, malformed-row, timestamp, and payload-safety tests |
| `tests/test_dm_watcher.py` | Modify | Update tool assertions; add per-error-category reply and local-resolution tests |
| `tests/test_mcp_errors.py` | Create | MCP error parser tests using NostrDesk-style structured error objects |
| `tests/test_agent_runner.py` | Modify | Add prompt structure and length assertions |
| `tests/test_approval_integration.py` | Create | End-to-end fake-MCP test: watcher → DB → operator DM → handler → respond_to_approval |

No changes to `schema.py`, `store.py`, or `supervisor.py`.

---

## Component Design

### `deskbridge/models.py`

`McpError` gains a `raw_category: str | None` field to preserve the exact category string from NostrDesk even when it is not a known `McpErrorCategory` enum value:

```python
class McpError(BaseModel):
    category: McpErrorCategory       # enum; unknown strings map to INTERNAL_ERROR
    raw_category: str | None = None  # exact string from error payload, e.g. "approval_expired"
    message: str
    approval_request_id: str | None = None
    data: dict | None = None         # full error.data dict
```

In `from_tool_result_text`, after extracting `raw_category = err.get("category", "internal_error")`:
- Set `raw_category` on the model unconditionally
- Map to `category` enum via `McpErrorCategory(raw_category)` with fallback to `INTERNAL_ERROR`
- Set `data = err.get("data") if isinstance(err.get("data"), dict) else None`

Callers in `DmWatcher` must use `e.mcp_error.raw_category` (not `e.mcp_error.category`) to check for NostrDesk-specific categories like `"approval_expired"`, since those are not in `McpErrorCategory` and would otherwise appear as `INTERNAL_ERROR`.

---

### `deskbridge/mcp/errors.py` and `deskbridge/mcp/client.py`

MCP tool errors surface through two distinct paths. Both must produce a `McpToolError` with `category` and `data` populated from NostrDesk's structured error payload.

**Path 1: `CallToolResult.isError`**

The MCP session returns normally but `result.isError = True`. `McpClient.call_tool` already handles this path. The text content is parsed via `McpError.from_tool_result_text`. That parser must also extract `error.data` when present:

```python
# In McpError.from_tool_result_text — existing error dict shape:
# {"error": {"category": "...", "message": "...", "data": {"category": "...", "status": "..."}}}
# Extract err["data"] as the structured payload dict.
```

`McpToolError` gains two new attributes:

```python
class McpToolError(Exception):
    def __init__(self, mcp_error: McpError, routing: RoutingDecision,
                 category: str | None = None, data: dict | None = None) -> None:
        ...
        self.category = category   # NostrDesk-specific category string, e.g. "approval_expired"
        self.data = data           # full error.data dict; None if absent or not a dict
```

`category` is populated from `data["category"]` when `data` is a dict. If `data` is absent or not a dict, both remain `None`.

**Path 2: SDK-raised exception with `.data`**

The MCP SDK may raise its own exception (e.g. before returning a result) that carries a `.data` attribute — a dict containing `category`, `approval_request_id`, `status`, etc. This path is currently uncaught.

`McpClient.call_tool` must wrap the `session.call_tool` call to catch these:

```python
try:
    result = await self._session.call_tool(tool_name, arguments)
except Exception as e:
    sdk_data = getattr(e, "data", None)
    if isinstance(sdk_data, dict):
        category = sdk_data.get("category")
        raw_cat = McpErrorCategory(category) if category in McpErrorCategory._value2member_map_ else McpErrorCategory.INTERNAL_ERROR
        mcp_error = McpError(category=raw_cat, message=str(e))
        routing = route_mcp_error(mcp_error)
        raise McpToolError(mcp_error=mcp_error, routing=routing, category=category, data=sdk_data) from e
    raise  # re-raise non-structured SDK exceptions unchanged
```

The catch must only intercept exceptions that carry a structured `.data` dict. Plain SDK errors without `.data` (network failures, timeouts) are re-raised unchanged so existing error handling upstream continues to work.

---

### `deskbridge/dm/approval_watcher.py`

**Constructor change:** rename `poll_timeout_secs: int = 30` → `poll_interval_secs: float = 2.0`. Two seconds is intentionally short so approvals are discovered well within NostrDesk's internal wait window.

**`run()` loop:**

1. Get `session_id` from broker. If `None`, sleep `poll_interval_secs` and retry.
2. Call `list_pending_approvals` with `{"session_id": session_id, "limit": 100}`.
3. For each item in the result, validate before processing:
   - Missing `id` → log `approval_watcher_malformed_row` with raw item (no sensitive fields), skip
   - Missing `tool_name` → use `"unknown tool"`
   - `expires_at` present → convert from Unix timestamp to ISO 8601: `datetime.fromtimestamp(expires_at, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")`; if absent or not a number, store `None`
   - `display_payload_json` valid JSON string → use as DM display content
   - `display_payload_json` present but not valid JSON → DM display is `{"raw_display_payload": "<raw string>"}`; log `approval_watcher_invalid_display_payload`
   - `display_payload_json` absent → DM display is `"(details unavailable)"`; `request_payload_json` is stored in `action_description` for internal reference only, never included in the operator DM
4. For each valid item, call:
   ```python
   store.insert_approval(
       id=str(uuid4()),
       mcp_approval_id=req["id"],
       work_item_id=work_item_id,
       action_description=f"{tool_name}: {dm_display}",
       expires_at=expires_at_iso,
       identity_id=self._account_id,
       scope=None,
       request_text=None,
   )
   ```
   `INSERT OR IGNORE` on the `mcp_approval_id` unique index makes this idempotent — repeated polls produce no duplicate rows.
5. If `operator_npub` is set:
   ```
   Approval required: <tool_name>

   <dm_display>

   Request ID: <id>

   Reply 'approve' or 'reject'.
   ```
   Call `store.insert_outbox_item(..., idempotency_key=f"approval-notify-{req['id']}")`. `INSERT OR IGNORE` on `idempotency_key` ensures exactly one operator DM per approval regardless of poll frequency.
6. Sleep `poll_interval_secs` using `asyncio.wait_for(self._shutdown_event.wait(), timeout=self._poll_interval_secs)` with `except asyncio.TimeoutError: pass`.

**`request_payload_json` storage rules:**
- MAY be stored in `approvals.request_text` for internal reference
- MUST NOT appear in `action_description` — this field feeds the operator DM format
- MUST NOT appear in any outbox `message_text`

When `display_payload_json` is absent, the operator DM shows `"(details unavailable)"`. `request_payload_json` is stored in `insert_approval`'s `request_text` parameter only.

**Removed:** all cursor load/save/warning logic. `list_pending_approvals` is a snapshot; there is no cursor.

**Error handling:** REJECT exits the loop. REAUTH logs a warning and sleeps. RESET_CURSOR is treated identically to REAUTH (log warning + sleep) — there is no cursor to reset, making it a no-op beyond the retry sleep. Unknown MCP errors and unexpected exceptions log and sleep. Per-item validation failures log and skip — the loop continues.

---

### `deskbridge/dm/watcher.py`

**Approval lookup — explicit latest-only semantics:**

`_handle_approve` and `_handle_reject` act on the most recently created pending approval for the identity via `store.get_pending_approval(identity_id)`. This is safe under the current one-agent-at-a-time constraint, which guarantees at most one pending approval per session at any time. This semantics must be documented and tested explicitly — callers must not assume ID-based lookup without updating the store method.

**`_handle_approve`** — updated MCP call:

```python
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

**Local DB resolution — conditional on MCP outcome:**

`store.resolve_approval` is only called to a terminal state when the MCP result is definitive:

| MCP outcome | `store.resolve_approval` | Operator DM reply |
|---|---|---|
| Success (`ok: true`) | Call → `approved` / `rejected` (from response `status`) | `"Approved."` / `"Denied."` |
| `approval_already_resolved` | Call → status from `error.data.status` (`"approved"` if `"approved"`, `"rejected"` otherwise); default `"rejected"` if absent | `"Decision received, but the approval was already resolved or has expired."` |
| `approval_expired` | Call → `"rejected"` (treat as terminal) | `"Decision received, but the approval was already resolved or has expired."` |
| `approval_not_found` | Call → `"rejected"` (treat as terminal) | `"Decision received, but the approval was already resolved or has expired."` |
| Unknown / internal MCP error | **Do not call** — leave local approval `pending` | `"Failed to record decision — please check logs."` |

For `approval_already_resolved`, NostrDesk includes the actual final decision in `error.data.status` (`"approved"` or `"denied"`). DeskBridge must read this field to record the correct local state — hardcoding `"approved"` would misrepresent denials. Default to `"rejected"` if `error.data.status` is absent or unrecognised.

For unknown/internal MCP errors, the local approval stays `pending` so the operator can retry the approve/reject command after the MCP issue is resolved. Leaving it terminal on an infrastructure error would silently lose the approval without NostrDesk ever receiving the decision.

**Log events:**

| Outcome | Log event | Level |
|---|---|---|
| Success | `dm_watcher_approval_resolved` | `info` |
| `approval_already_resolved` | `dm_watcher_approval_already_resolved` | `warning` |
| `approval_expired` | `dm_watcher_approval_expired` | `warning` |
| `approval_not_found` | `dm_watcher_approval_not_found` | `error` |
| Unknown/internal error | `dm_watcher_resolve_approval_error` | `error` |

`approval_not_found` is `error` because it can indicate wrong `session_id`, stale local DB, or malformed approval state — more diagnostic than a normal double-response or expiry.

Full error detail is always logged but never included verbatim in the operator DM.

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

The approval instruction is never truncated. The truncation guard applies only to `task_text`.

---

### `deskbridge/dm/group_watcher.py` — no change

Approval commands (`approve` / `reject`) are DM-only in Phase 6. `GroupWatcher` handles @mention-routed messages and does not parse approval intents. An operator who sends "approve" as a group @mention will receive no response from DeskBridge; the pending approval will remain open until resolved via DM. This constraint is intentional: approval decisions are sensitive and should go through the authenticated operator DM channel, not a group chat where other participants may be present. No changes to `GroupWatcher` in this phase.

---

## End-to-End Flow

```
Agent calls protected MCP tool
  → NostrDesk creates permission_requests row and waits internally

DeskBridge ApprovalRequestWatcher polls list_pending_approvals (every 2s)
  → Sees pending request {id: "req-abc", tool_name: "pay_invoice", ...}
  → INSERT OR IGNORE into approvals (mcp_approval_id="req-abc") — idempotent
  → INSERT OR IGNORE into outbox (idempotency_key="approval-notify-req-abc") — idempotent
  → OutboxDrainer sends operator DM once

Operator replies "approve"
  → DmWatcher parses intent APPROVE
  → store.get_pending_approval → row with mcp_approval_id="req-abc"
  → call respond_to_approval(session_id="session-123", approval_request_id="req-abc", approved=True, note=None)
  → On success: store.resolve_approval → "approved"; reply "Approved."
  → NostrDesk unblocks internal wait → original tool proceeds

Agent (if still waiting) retries and succeeds
```

**Recovery path (NostrDesk internal wait timed out before operator responded):**
- Agent received `approval_required` error, waited 60s, retried — still failed if operator hadn't responded
- Agent reports the failure and stops the run
- The specific `permission_requests` row in NostrDesk may be expired by the time the operator responds; if so, DeskBridge's `respond_to_approval` call returns `approval_expired` and local approval is marked terminal
- Late approval does not grant persistent or future authorization — it only resolves the specific request row it was created for
- To proceed, the operator re-triggers the task; the agent calls the protected tool again, which creates a new approval request, and DeskBridge handles it from the start

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
- Row with invalid `display_payload_json` → DM content uses `{"raw_display_payload": "..."}` form
- Row missing `display_payload_json` → operator DM shows `"(details unavailable)"`; `request_payload_json` not present in outbox message
- `expires_at` as Unix integer → stored as ISO 8601 string in `approvals.expires_at`
- `expires_at` absent → stored as `None`
- Regression: when `display_payload_json` is absent and `request_payload_json` is `'{"amount": 1000, "dest": "abc"}'`:
  - Assert `approvals.request_text` contains the `request_payload_json` value (the one permitted storage location)
  - Assert `approvals.action_description` does not contain `"amount"`, `"dest"`, or `"abc"`
  - Assert outbox `message_text` does not contain `"amount"`, `"dest"`, or `"abc"`

Keep: no session skips poll; REJECT exits; operator DM written when `operator_npub` set; unexpected exception logs and continues.

### `tests/test_dm_watcher.py`

Updates:
- `approve calls resolve` → assert `respond_to_approval` called with `{"session_id": <exact session from broker mock>, "approval_request_id": <exact id from DB row>, "approved": True, "note": None}`
- `reject calls resolve` → same with `approved: False`

New tests — MCP error category routing (using NostrDesk-style structured error data):
- `approval_already_resolved` with `{"data": {"category": "approval_already_resolved", "status": "approved"}}` → `store.resolve_approval` called with `"approved"`; `dm_watcher_approval_already_resolved` logged at `warning`; operator DM says "already resolved or expired"
- `approval_already_resolved` with `{"data": {"category": "approval_already_resolved", "status": "denied"}}` → `store.resolve_approval` called with `"rejected"`, not `"approved"`; same operator DM
- `approval_expired` with `{"data": {"category": "approval_expired"}}` → `store.resolve_approval` called with `"rejected"`; `dm_watcher_approval_expired` logged at `warning`; operator DM says "already resolved or expired"
- `approval_not_found` with `{"data": {"category": "approval_not_found"}}` → `store.resolve_approval` called with `"rejected"`; `dm_watcher_approval_not_found` logged at `error`; operator DM says "already resolved or expired"
- Unknown/internal MCP error → `store.resolve_approval` **not** called; `dm_watcher_resolve_approval_error` logged at `error`; operator DM says "Failed to record decision"; local approval stays `pending`

Keep: MCP failure still sends outbox reply in all cases.

### `tests/test_mcp_errors.py` (new file)

MCP error parser tests covering both error paths.

**Path 1: `CallToolResult.isError` text content**

- Text `'{"error": {"category": "approval_required", "message": "...", "data": {"category": "approval_expired", "approval_request_id": "req-abc", "status": "pending"}}}'` → `McpToolError.category == "approval_expired"`, `McpToolError.data["approval_request_id"] == "req-abc"`, `McpToolError.data["status"] == "pending"`
- Text with `error.data` containing `{"category": "approval_already_resolved", "status": "denied"}` → `category == "approval_already_resolved"`, `data["status"] == "denied"`
- Text with `error.data` as a non-dict string → `category is None`, `data is None`
- Text with no `error.data` field → `category is None`, `data is None`
- Text with `error.data` as `{}` (empty dict) → `category is None`, `data == {}`

**`McpError` model — category preservation**

- `from_tool_result_text` with `error.category = "approval_expired"` (not in `McpErrorCategory`): `mcp_error.category == McpErrorCategory.INTERNAL_ERROR`, `mcp_error.raw_category == "approval_expired"` — raw string preserved even though enum falls back
- `from_tool_result_text` with `error.category = "invalid_session"` (known enum value): `mcp_error.category == McpErrorCategory.INVALID_SESSION`, `mcp_error.raw_category == "invalid_session"`
- `from_tool_result_text` with no `error.category` key: `mcp_error.category == McpErrorCategory.INTERNAL_ERROR`, `mcp_error.raw_category == "internal_error"` (the default)
- `from_tool_result_text` with `error.data` as a dict: `mcp_error.data == {"category": "...", ...}`
- `from_tool_result_text` with `error.data` as a non-dict: `mcp_error.data is None`

**Path 2: SDK-raised exception with `.data`**

- SDK exception where `e.data = {"category": "approval_expired", "approval_request_id": "req-abc", "status": "pending"}` → `McpClient.call_tool` raises `McpToolError` with `category == "approval_expired"`, `data["approval_request_id"] == "req-abc"`
- SDK exception where `e.data = {"category": "approval_already_resolved", "status": "denied"}` → `McpToolError.category == "approval_already_resolved"`, `data["status"] == "denied"`
- SDK exception where `e.data` is absent → exception is re-raised as-is (not converted to `McpToolError`)
- SDK exception where `e.data` is a non-dict → exception is re-raised as-is

### `tests/test_agent_runner.py`

New test — prompt structure and length:
- Assert `prompt.startswith(_APPROVAL_INSTRUCTION)` — instruction present and not truncated
- Assert `len(prompt) <= 4000`
- With `task_text` longer than `4000 - len(_APPROVAL_INSTRUCTION)`: assert instruction is intact and only `task_text` is truncated

### `tests/test_approval_integration.py` (new file)

End-to-end fake-MCP test verifying the `approval_request_id` flows correctly from watcher through DB to `respond_to_approval`:

```python
async def test_approval_round_trip():
    # Setup: fake store backed by real aiosqlite in-memory DB (or full mock)
    # Fake broker returns session_id = "session-123"
    # Fake MCP client:
    #   list_pending_approvals → [{"id": "req-abc", "tool_name": "pay_invoice",
    #                              "display_payload_json": '{"amount": 100}',
    #                              "expires_at": 1770000000}]
    #   respond_to_approval → {"ok": True, "approval_request_id": "req-abc", "status": "approved"}

    # Step 1: Run ApprovalRequestWatcher for one cycle
    # Assert: approvals table has one row with mcp_approval_id="req-abc"
    # Assert: outbox has one row with idempotency_key="approval-notify-req-abc"

    # Step 2: Run ApprovalRequestWatcher for a second cycle (same list returned)
    # Assert: no new approvals row (INSERT OR IGNORE)
    # Assert: no new outbox row (INSERT OR IGNORE)

    # Step 3: Simulate operator "approve" DM → DmWatcher._handle_approve
    # Assert: respond_to_approval called with exactly:
    #   session_id="session-123", approval_request_id="req-abc", approved=True, note=None
    # Assert: store.resolve_approval called with "approved"
    # Assert: outbox reply says "Approved."
```

**SDK exception terminal path test:**

```python
async def test_approval_sdk_exception_terminal():
    # Fake MCP client: respond_to_approval raises an SDK exception where
    #   e.data = {"category": "approval_expired", "approval_request_id": "req-abc"}
    # (not a CallToolResult.isError — a genuine SDK exception with .data)

    # Simulate operator "approve" DM → DmWatcher._handle_approve
    # Assert: McpToolError raised with category="approval_expired"
    # Assert: DeskBridge handles it as approval_expired (terminal):
    #   - store.resolve_approval called with "rejected"
    #   - dm_watcher_approval_expired logged at warning
    #   - outbox reply says "already resolved or expired"
    # Assert: local approval is NOT left pending (i.e. not treated as unknown/internal error)
```

---

## Error Handling Summary

| Scenario | Behavior |
|---|---|
| `list_pending_approvals` MCP error | Log + sleep; watcher continues |
| Approval row missing `id` | Log `approval_watcher_malformed_row`; skip row |
| Approval row missing `tool_name` | Use `"unknown tool"`; continue |
| Invalid `display_payload_json` | DM shows `{"raw_display_payload": "..."}`; log warning; continue |
| `display_payload_json` absent | DM shows `"(details unavailable)"`; `request_payload_json` not in DM |
| `expires_at` not a number | Store `None`; continue |
| `INSERT OR IGNORE` on duplicate approval | Silently ignored; no duplicate row or DM |
| `respond_to_approval` — success | `store.resolve_approval` called; reply `"Approved."` / `"Denied."` |
| `respond_to_approval` — `approval_already_resolved` | `store.resolve_approval` called with status from `error.data.status` (default `"rejected"`); `warning` log; reply "already resolved or expired" |
| `respond_to_approval` — `approval_expired` | `store.resolve_approval` called (terminal); `warning` log; reply "already resolved or expired" |
| `respond_to_approval` — `approval_not_found` | `store.resolve_approval` called (terminal); `error` log; reply "already resolved or expired" |
| `respond_to_approval` — unknown/internal error | Local approval stays `pending`; `error` log (detail in log only); reply "Failed to record decision" |
| Agent subprocess gets `approval_required` error | Wait 60s; retry; report and stop if still failing |
