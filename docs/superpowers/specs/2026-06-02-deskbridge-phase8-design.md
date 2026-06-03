# DeskBridge Phase 8: Scheduled Project Check-ins — Design

## Overview

Add autonomous periodic check-ins to DeskBridge projects. Instead of waiting for an operator DM to trigger agent work, the daemon fires a configurable check-in on a time interval, inserting a work item that the existing agent runner dispatches. Results are DMed to `operator_npub` via the outbox, same as any other completed work item.

---

## Config

Two new optional fields on `ProjectConfig` in `deskbridge/config.py`:

```toml
[projects.my-project]
# ... existing fields ...
check_in_interval_hours = 24.0
check_in_prompt = "Review open issues and summarize any blockers or progress."
```

- `check_in_interval_hours` — float, optional. If absent, no check-in watcher is spawned for this project.
- `check_in_prompt` — string, optional. If absent, defaults to `"Perform a project status check-in and report any blockers or progress."`

Both fields are TOML-only. No runtime reconfiguration via DM.

---

## Data Model

No new DB tables. Scheduled check-ins reuse the `work_items` table with a new `source_type`:

| Field | Value |
|---|---|
| `source_type` | `"scheduled"` |
| `source_id` | `"scheduled"` |
| `summary` | First 200 chars of the prompt |
| `payload_json` | `{"prompt": "...", "operator_npub": "npub1..."}` |
| `idempotency_key` | `f"checkin-{identity_id}-{int(time.time() // interval_secs)}"` |

The idempotency key uses an interval bucket so a daemon restart mid-interval does not fire a duplicate. `operator_npub` is stored in `payload_json` (same pattern as DM-sourced work items) so `WorkItemPoller` can route the completion DM without a new constructor param.

---

## New Component: `ScheduledCheckInWatcher`

**File:** `deskbridge/agent/checkin_scheduler.py`

```python
class ScheduledCheckInWatcher:
    def __init__(
        self,
        identity_label: str,
        identity_id: str,
        operator_npub: str,
        interval_hours: float,
        prompt: str,
        store: Store,
        client: McpClient,
        broker: SessionBroker,
        shutdown_event: asyncio.Event,
    ) -> None:
        # interval_hours is validated > 0 at config parse time (see ProjectConfig)
        self._interval_secs = interval_hours * 3600
        ...
```

**`run()` behavior:**

Each iteration:
1. Compute the current bucket: `current_bucket = int(time.time() // interval_secs)`.
2. Call `store.get_latest_dispatched_work_item(identity_id)`. If an active run is found, log `checkin_skipped_agent_busy`.
3. Otherwise, call `store.upsert_work_item(...)` with `idempotency_key=f"checkin-{identity_id}-{current_bucket}"`. If it returns `False` (key already exists — daemon restart mid-interval), log `checkin_already_queued`. If it returns `True`, log `checkin_work_item_created`.
4. Sleep until the next bucket boundary:
   ```python
   next_bucket_time = (current_bucket + 1) * self._interval_secs
   sleep_duration = next_bucket_time - time.time()
   log.debug(
       "checkin_sleeping",
       next_checkin_utc=datetime.utcfromtimestamp(next_bucket_time).isoformat() + "Z",
       sleep_secs=round(sleep_duration, 1),
   )
   try:
       await asyncio.wait_for(shutdown_event.wait(), timeout=sleep_duration)
   except asyncio.TimeoutError:
       pass
   ```
5. Exceptions caught and logged, loop continues.

**On startup:** the watcher checks the current bucket immediately (no initial sleep). Because `upsert_work_item` enforces the idempotency key, a restart mid-interval is a safe no-op — operators get immediate confirmation their config is working if a new bucket is open, or a silent no-op if the check-in already ran.

**Note:** If the daemon starts within seconds of a bucket boundary, two check-ins may fire in rapid succession (one for the ending bucket, one for the new bucket). This is correct behavior — both have distinct idempotency keys — and is visible to the operator via two DMs.

---

## Modified Component: `WorkItemPoller`

**File:** `deskbridge/agent/poller.py`

Add a completion branch for `source_type == "scheduled"`:

```python
if source_type == "scheduled":
    operator_npub = payload.get("operator_npub")
    if operator_npub:
        await store.insert_outbox_item(
            id=str(uuid4()),
            identity_id=identity_id,
            dest_pubkey=operator_npub,
            message_text=result_summary,
            idempotency_key=f"checkin-result-{work_item_id}",
        )
```

No new constructor params. `operator_npub` is read from `payload_json`.

---

## Modified Component: `Supervisor`

**File:** `deskbridge/supervisor.py`

After the existing `kanban_watcher_tasks` loop, add:

```python
checkin_watcher_tasks: list[asyncio.Task] = []

for identity in self._config.identities:
    project_cfg = next(
        (p for p in self._config.projects if p.identity == identity.label), None
    )
    if project_cfg and project_cfg.check_in_interval_hours:
        checkin_watcher_tasks.append(asyncio.create_task(
            ScheduledCheckInWatcher(
                identity_label=identity.label,
                identity_id=f"acc-{identity.label}",
                operator_npub=identity.operator_npub,
                interval_hours=project_cfg.check_in_interval_hours,
                prompt=project_cfg.check_in_prompt,
                store=store,
                client=client,
                broker=broker,
                shutdown_event=self._shutdown_event,
            ).run(),
            name=f"checkin_watcher_{identity.label}",
        ))
```

`checkin_watcher_tasks` added to the `finally` cancel list.

---

## Modified Component: `ProjectConfig`

**File:** `deskbridge/config.py`

```python
check_in_interval_hours: float | None = Field(default=None, gt=0)
check_in_prompt: str = "Perform a project status check-in and report any blockers or progress."
```

`Field(gt=0)` causes Pydantic to reject `0.0` or negative values at config parse time, before any watcher is spawned.

---

## Files Touched

| File | Change |
|---|---|
| `deskbridge/config.py` | Add `check_in_interval_hours`, `check_in_prompt` to `ProjectConfig` |
| `deskbridge/agent/checkin_scheduler.py` | New — `ScheduledCheckInWatcher` |
| `deskbridge/agent/poller.py` | Add `source_type == "scheduled"` completion branch |
| `deskbridge/supervisor.py` | Spawn `ScheduledCheckInWatcher` per identity, add to cancel list |
| `tests/test_checkin_scheduler.py` | New — unit tests for `ScheduledCheckInWatcher` |
| `tests/test_work_item_poller.py` | Add test for scheduled completion DM |
| `tests/test_supervisor.py` | Add tests for conditional watcher spawn |

---

## Testing

**`ScheduledCheckInWatcher` tests:**
- Inserts work item immediately on first iteration when bucket is new
- No-ops on first iteration when idempotency key already exists (daemon restart mid-interval)
- Skips (no insert) when active run exists
- Sleeps to next bucket boundary, not a fixed interval
- Shutdown event stops the loop cleanly
- Exception in upsert is caught and logged; loop continues

**`WorkItemPoller` tests:**
- Scheduled completion DMes `operator_npub` from payload
- DM-sourced completion is unaffected (regression)

**`Supervisor` tests:**
- Spawns watcher when `check_in_interval_hours` is set
- Does not spawn watcher when field is absent
