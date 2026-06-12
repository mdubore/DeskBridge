# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What DeskBridge Is

A standalone local-first bridge daemon that sits between one machine's `nostrdesk-mcp` server (Nostr DMs/groups/kanban over MCP) and local autonomous coding agents (claude-code, codex, gemini, hermes, openclaw). It watches for inbound DMs, kanban assignments, group mentions, and scheduled check-ins, turns them into work items, dispatches them to agent CLIs, and DMs results back to the human operator. Full design rationale: `docs/plans/2026-05-03-deskbridge-design.md`. Work is planned in phases under `docs/superpowers/plans/`.

## Commands

```bash
uv sync                                  # install deps (Python >= 3.11, uses uv.lock)
uv run pytest                            # run all tests
uv run pytest tests/test_agent_runner.py # run one test file
uv run pytest -k test_name               # run one test by name
uv run deskbridge start --config <toml>  # run the supervisor daemon (foreground)
uv run deskbridge status                 # operational dashboard from the SQLite db
uv run deskbridge cancel <work-item-id>  # cancel a pending/running work item
uv run deskbridge retry <work-item-id>   # re-queue a failed/cancelled/interrupted item
uv run deskbridge approve <approval-id>  # queue an approval decision for the supervisor
uv run deskbridge reject <approval-id>   # queue a rejection decision for the supervisor
```

There is no linter or formatter configured. Tests use `asyncio_mode = "auto"` — async test functions need no `@pytest.mark.asyncio` decorator. `tests/conftest.py` provides `db_conn` and `store` fixtures backed by a temp SQLite database with the real schema applied.

## Architecture

### Process model

`Supervisor.run()` (`deskbridge/supervisor.py`) is the whole daemon: it opens the SQLite db, applies the schema, spawns the single `nostrdesk-mcp` subprocess via `McpClient.connect()`, unlocks all identities through `SessionBroker`, then runs everything else as concurrent asyncio tasks sharing one `shutdown_event`:

- **Per identity:** `DmWatcher`, `ApprovalRequestWatcher`, `ApprovalDecisionPoller`, `WorkItemPoller`, and (when configured) `GroupWatcher`, `KanbanWatcher`, `ScheduledCheckInWatcher`
- **Singleton:** `OutboxDrainer`

### Event flow (the main pipeline)

1. Watchers (`deskbridge/dm/*.py`, `deskbridge/agent/checkin_scheduler.py`) call MCP watcher tools (`wait_for_new_dms`, etc.), classify DM text via `dm/intent.py`, and insert rows into `work_items` with idempotency keys (UNIQUE constraint provides dedupe).
2. `WorkItemPoller` (`agent/poller.py`) claims pending items — **one active agent run at a time per identity** — and handles retry-on-failure (`attempt_count` vs `max_agent_attempts`, 60s backoff), operator cancellation (`cancel_requested` status), and kanban column sync.
3. `AgentRunner` (`agent/runner.py`) builds an agent CLI command via `agent/adapters.py:build_command`, spawns it as a subprocess in the project's repo, heartbeats into `agent_runs`, and on completion queues the result as an outbox row.
4. `OutboxDrainer` (`dm/outbox.py`) delivers queued DMs through MCP `send_dm`.

### Key invariants

- **Only the supervisor process talks to MCP**, and all calls go through `McpClient.call_tool` (`mcp/client.py`). It parses machine-readable error categories into `McpToolError` carrying a `RoutingDecision` (`mcp/errors.py`: retry / reauth / reset_cursor / escalate / reject). Callers branch on `e.routing` — never string-match error messages.
- **SQLite is the source of truth.** All DB access goes through `Store` (`db/store.py`). Schema lives in `db/schema.py` as base DDL plus an additive `_MIGRATIONS` list of idempotent `ALTER TABLE`s (duplicate-column errors are swallowed) — add new columns there, don't edit existing DDL.
- **Account id convention:** `acc-{identity_label}` links config identities to `accounts` rows; used everywhere (sessions, cursors, work items).
- **Cursors are persisted verbatim** as returned by MCP (shared shape: `last_entity_id` / `last_created_at` / `last_imported_at`), keyed by `(cursor_type, identity_id)`. On `invalid_cursor` errors, watchers reset to `None` and re-poll.
- **Outbound messages always go through the outbox table** with idempotency keys (convention: `deskbridge:{run_id}:{purpose}`), never sent directly.
- **Audit events** (`audit_log` table, via `store.log_audit`) are written best-effort — wrapped in try/except that logs a warning, never failing the operation.
- **Approvals are never auto-resolved.** `approval_required` MCP errors are captured with their correlation id into `approvals` and escalated to the operator via DM; agents are instructed (see `_APPROVAL_INSTRUCTION` in `runner.py`) to wait and retry. Operator decisions can also be queued from the CLI (`approve`/`reject` set `approve_requested`/`reject_requested` statuses); the supervisor's `ApprovalDecisionPoller` forwards them to MCP — the CLI process never talks to MCP.
- **Secrets stay out of config:** `passphrase_ref` is an indirection (`env:VAR` or `keyring:service:key`) resolved at unlock time by `config.py`.

### Config

TOML (default `~/.deskbridge/config.toml`, example in `deskbridge.example.toml`) validated by pydantic models in `config.py`. Projects reference identities by label; cross-references are validated at load. Internal data contracts (`TaskEnvelope`, `TaskUpdate`, `McpError`, enums) live in `models.py`. Group ids are configured per project (`groups = [...]`) and are config-authoritative — bootstrap overwrites `projects.groups_json` on every start.

### Conventions

- Logging is structlog with snake_case event names first: `log.info("dm_watcher_started", identity=...)`.
- Tests mock `McpClient`/`SessionBroker` per module; the real schema + temp SQLite comes from conftest fixtures.
