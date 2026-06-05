# DeskBridge Phase 9: Expanded Adapter Surface — Design

## Overview

Add Codex and Gemini CLI as first-class adapters alongside the existing Claude Code adapter. Replace the ad-hoc `agents: list[str]` config field with a single `adapter: str` field, extract adapter command-building into a dedicated module, and update `AgentRunner` to use it.

---

## Config

`ProjectConfig` in `deskbridge/config.py`:

```toml
[projects.my-project]
adapter = "claude-code"   # or "codex" or "gemini"
```

**Change:** `agents: list[str]` is removed and replaced with `adapter: str`.

```python
adapter: str = "claude-code"

@field_validator("adapter")
@classmethod
def adapter_must_be_known(cls, v: str) -> str:
    from deskbridge.agent.adapters import KNOWN_ADAPTERS
    if v not in KNOWN_ADAPTERS:
        raise ValueError(f"Unknown adapter {v!r}. Known: {sorted(KNOWN_ADAPTERS)}")
    return v
```

- Default: `"claude-code"`
- Valid values: `"claude-code"`, `"codex"`, `"gemini"` — defined in `adapters.py` as `KNOWN_ADAPTERS`
- Unknown value raises `ConfigError` at config parse time (Pydantic validator)
- `KNOWN_ADAPTERS` is the single source of truth — adding a new adapter only requires touching `adapters.py`

---

## New Component: `deskbridge/agent/adapters.py`

```python
KNOWN_ADAPTERS = {"claude-code", "codex", "gemini"}


def build_command(adapter: str, repo_path: str, prompt: str) -> list[str]:
    if adapter == "claude-code":
        return ["claude", "--project", repo_path, "--message", prompt]
    elif adapter == "codex":
        return ["codex", "--cwd", repo_path, "--no-interactive", prompt]
    elif adapter == "gemini":
        return ["gemini", "--yolo", "-p", prompt]
    raise ValueError(f"Unknown adapter: {adapter!r}")
```

**CLI flag notes:**

| Adapter | Binary | Working directory | One-shot flag | Prompt |
|---|---|---|---|---|
| `claude-code` | `claude` | `--project <path>` | implicit | `--message <text>` |
| `codex` | `codex` | `--cwd <path>` | `--no-interactive` | positional |
| `gemini` | `gemini` | `cwd=` on subprocess (implicit) | `--yolo` | `-p <text>` |

Gemini CLI uses the process working directory rather than an explicit flag; `cwd=project.repo_path` is already passed to `asyncio.create_subprocess_exec` in `runner.py`, so no additional flag is needed.

The `_APPROVAL_INSTRUCTION` prefix is the same for all adapters — it is plain natural language and all three CLIs follow it correctly.

---

## Database

### Schema migration

`apply_schema` in `deskbridge/db/schema.py` runs a migration list after the main DDL to add the `adapter` column to existing databases:

```python
_MIGRATIONS = [
    "ALTER TABLE projects ADD COLUMN adapter TEXT NOT NULL DEFAULT 'claude-code'",
]

async def apply_schema(conn: aiosqlite.Connection) -> None:
    await conn.executescript(_DDL)
    for migration in _MIGRATIONS:
        try:
            await conn.execute(migration)
            await conn.commit()
        except aiosqlite.OperationalError:
            pass  # column already exists — safe to ignore
    await conn.execute("PRAGMA foreign_keys = ON")
    ...
```

The `agents_json` column stays in the schema (SQLite ALTER TABLE DROP COLUMN requires SQLite ≥ 3.35 and is not worth the risk) — it is simply no longer written or read.

### `store.upsert_project`

Signature change: `agents_json: str` → `adapter: str`. The SQL is updated to write/read the new column:

```python
async def upsert_project(
    self,
    id: str,
    name: str,
    repo_path: str,
    identity_id: str,
    adapter: str,           # replaces agents_json
    boards_json: str,
    allowed_actions_json: str,
    escalation_dm_target: str | None,
) -> None:
    async with self._conn.execute(
        """
        INSERT INTO projects
            (id, name, repo_path, identity_id, adapter,
             boards_json, allowed_actions_json, escalation_dm_target)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            name                 = excluded.name,
            repo_path            = excluded.repo_path,
            identity_id          = excluded.identity_id,
            adapter              = excluded.adapter,
            boards_json          = excluded.boards_json,
            allowed_actions_json = excluded.allowed_actions_json,
            escalation_dm_target = excluded.escalation_dm_target
        """,
        (id, name, repo_path, identity_id, adapter,
         boards_json, allowed_actions_json, escalation_dm_target),
    ):
        pass
    await self._conn.commit()
```

### `bootstrap_accounts_from_config`

Updated to pass `adapter=project.adapter` instead of `agents_json=json.dumps(project.agents)`.

---

## Modified Component: `AgentRunner`

**File:** `deskbridge/agent/runner.py`

Two changes only:

**Step 1** — record the run:
```python
# was: adapter_type=project.agents[0]
await self._store.upsert_agent_run(
    id=run_id,
    work_item_id=work_item["id"],
    adapter_type=project.adapter,
)
```

**Step 2** — build CLI command:
```python
from deskbridge.agent.adapters import build_command

# replaces the if/else block
cmd = build_command(project.adapter, project.repo_path, prompt)
```

All subprocess lifecycle, heartbeat, store writes, and DM logic is untouched.

---

## Files Touched

| File | Change |
|---|---|
| `deskbridge/config.py` | Replace `agents: list[str]` with `adapter: str`; add validator |
| `deskbridge/db/schema.py` | Add `_MIGRATIONS` list; run `ALTER TABLE projects ADD COLUMN adapter` |
| `deskbridge/db/store.py` | `upsert_project`: `agents_json` → `adapter`; update SQL |
| `deskbridge/agent/adapters.py` | New — `KNOWN_ADAPTERS`, `build_command()` |
| `deskbridge/agent/runner.py` | Use `project.adapter` and `build_command()` |
| `tests/test_adapters.py` | New — command output tests for all three adapters |
| `tests/test_config.py` | Update for `adapter` field; add validation tests |
| `tests/test_agent_runner.py` | Update fixtures: `agents=["claude-code"]` → `adapter="claude-code"` |
| `tests/test_store.py` | Update `upsert_project` call sites; add migration test |

---

## Testing

**`tests/test_adapters.py`:**
- `test_claude_code_command` — asserts exact `cmd` list
- `test_codex_command` — asserts exact `cmd` list
- `test_gemini_command` — asserts exact `cmd` list
- `test_unknown_adapter_raises` — `ValueError` with "Unknown adapter" message

**`tests/test_config.py`:**
- `test_adapter_defaults_to_claude_code`
- `test_adapter_known_values_accepted` (parametrized: `"claude-code"`, `"codex"`, `"gemini"`)
- `test_adapter_unknown_value_raises_config_error`

**`tests/test_agent_runner.py`:**
- Existing fixtures updated to use `adapter="claude-code"` — no new runner tests needed (adapter dispatch is covered by `test_adapters.py`)

**`tests/test_store.py`:**
- `test_upsert_project_writes_adapter` — round-trip: write `adapter="codex"`, read back via `get_project_for_identity`, assert column value
- `test_apply_schema_migration_idempotent` — call `apply_schema` twice on the same DB, assert no error (column-already-exists path)
