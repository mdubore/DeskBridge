# DeskBridge Phase 9: Expanded Adapter Surface — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add Codex and Gemini CLI as first-class adapters by extracting adapter dispatch into `adapters.py`, replacing the broken `agents: list[str]` config field with a single `adapter: str`, and wiring the migration through the schema and store.

**Architecture:** A new `deskbridge/agent/adapters.py` module owns `KNOWN_ADAPTERS` and `build_command()`. `config.py` imports `KNOWN_ADAPTERS` for a Pydantic validator. `schema.py` adds an `ALTER TABLE` migration for the new `adapter` column. `store.py` and `runner.py` each drop one reference to the old `agents` field.

**Tech Stack:** Python 3.12, aiosqlite, Pydantic v2, pytest + pytest-asyncio (asyncio_mode=auto)

---

## File Map

| File | Role |
|---|---|
| `deskbridge/agent/adapters.py` | New — `KNOWN_ADAPTERS` set, `build_command()` function |
| `deskbridge/config.py` | Replace `agents: list[str]` with `adapter: str` + validator |
| `deskbridge/db/schema.py` | Add `_MIGRATIONS` list; run `ALTER TABLE projects ADD COLUMN adapter` |
| `deskbridge/db/store.py` | `upsert_project`: replace `agents_json` param with `adapter`; update SQL |
| `deskbridge/agent/runner.py` | Use `project.adapter` and `build_command()` instead of if/else |
| `tests/test_adapters.py` | New — unit tests for all three adapters + unknown raises |
| `tests/test_config.py` | Add adapter field tests |
| `tests/test_store.py` | Update `_seed_project` helper; add adapter round-trip and migration tests |
| `tests/test_agent_runner.py` | Update `PROJ`/`PROJ_NO_DM` fixtures from `agents=` to `adapter=` |

---

### Task 1: New module `deskbridge/agent/adapters.py`

**Files:**
- Create: `deskbridge/agent/adapters.py`
- Create: `tests/test_adapters.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/test_adapters.py`:

```python
import pytest
from deskbridge.agent.adapters import build_command, KNOWN_ADAPTERS


def test_claude_code_command():
    cmd = build_command("claude-code", "/repo", "do the thing")
    assert cmd == ["claude", "--project", "/repo", "--message", "do the thing"]


def test_codex_command():
    cmd = build_command("codex", "/repo", "do the thing")
    assert cmd == ["codex", "--cwd", "/repo", "--no-interactive", "do the thing"]


def test_gemini_command():
    cmd = build_command("gemini", "/repo", "do the thing")
    assert cmd == ["gemini", "--yolo", "-p", "do the thing"]


def test_unknown_adapter_raises():
    with pytest.raises(ValueError, match="Unknown adapter"):
        build_command("hermes", "/repo", "do the thing")


def test_known_adapters_contains_all_three():
    assert KNOWN_ADAPTERS == {"claude-code", "codex", "gemini"}
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
uv run pytest tests/test_adapters.py -v
```

Expected: `ModuleNotFoundError` — `deskbridge.agent.adapters` does not exist yet.

- [ ] **Step 3: Create `deskbridge/agent/adapters.py`**

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

- [ ] **Step 4: Run tests to verify they pass**

```bash
uv run pytest tests/test_adapters.py -v
```

Expected: 5 passed.

- [ ] **Step 5: Commit**

```bash
git add deskbridge/agent/adapters.py tests/test_adapters.py
git commit -m "feat: add adapters module with build_command for claude-code, codex, gemini"
```

---

### Task 2: Config — replace `agents: list[str]` with `adapter: str`

**Files:**
- Modify: `deskbridge/config.py`
- Modify: `tests/test_config.py`
- Modify: `tests/test_agent_runner.py`

- [ ] **Step 1: Write the failing config tests**

Add to `tests/test_config.py` (after the existing check_in tests at the end of the file):

```python
def test_adapter_defaults_to_claude_code():
    from deskbridge.config import ProjectConfig
    proj = ProjectConfig(
        id="p1", name="MyProj", repo_path="/repo",
        identity="alice", escalation_dm_target="npub1op",
    )
    assert proj.adapter == "claude-code"


@pytest.mark.parametrize("adapter", ["claude-code", "codex", "gemini"])
def test_adapter_known_values_accepted(adapter):
    from deskbridge.config import ProjectConfig
    proj = ProjectConfig(
        id="p1", name="MyProj", repo_path="/repo",
        identity="alice", escalation_dm_target="npub1op",
        adapter=adapter,
    )
    assert proj.adapter == adapter


def test_adapter_unknown_value_raises_config_error(tmp_path):
    from deskbridge.config import ConfigError, load_config
    config_text = """
[supervisor]
db_path = "/tmp/test.db"

[mcp]
command = "nostrdesk-mcp"

[[identities]]
label = "alice"
npub = "npub1alice"
passphrase_ref = "env:ALICE"

[[projects]]
id = "proj-1"
name = "MyProject"
repo_path = "/repo"
identity = "alice"
escalation_dm_target = "npub1op"
adapter = "hermes"
"""
    (tmp_path / "config.toml").write_text(config_text)
    with pytest.raises(ConfigError):
        load_config(tmp_path / "config.toml")
```

- [ ] **Step 2: Run the new tests to verify they fail**

```bash
uv run pytest tests/test_config.py::test_adapter_defaults_to_claude_code tests/test_config.py::test_adapter_known_values_accepted tests/test_config.py::test_adapter_unknown_value_raises_config_error -v
```

Expected: FAIL — `ProjectConfig` has no `adapter` field yet.

- [ ] **Step 3: Update `deskbridge/config.py`**

In `ProjectConfig`, replace:

```python
agents: list[str] = Field(default_factory=lambda: ["codex", "claude-code"])
```

with:

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

The full updated `ProjectConfig` class (show in context so line numbers are clear):

```python
class ProjectConfig(BaseModel):
    id: str
    name: str
    repo_path: str
    identity: str
    escalation_dm_target: str
    adapter: str = "claude-code"
    allowed_autonomous_actions: list[str] = Field(
        default_factory=lambda: ["read", "send_dm", "update_task_status"]
    )
    boards: list[str] = Field(default_factory=list)
    kanban_column_in_progress: str = "in_progress"
    kanban_column_done: str = "done"
    check_in_interval_hours: Annotated[float, Field(gt=0)] | None = None
    check_in_prompt: str = (
        "Perform a project status check-in and report any blockers or progress."
    )

    @field_validator("adapter")
    @classmethod
    def adapter_must_be_known(cls, v: str) -> str:
        from deskbridge.agent.adapters import KNOWN_ADAPTERS
        if v not in KNOWN_ADAPTERS:
            raise ValueError(f"Unknown adapter {v!r}. Known: {sorted(KNOWN_ADAPTERS)}")
        return v
```

- [ ] **Step 4: Update `tests/test_agent_runner.py` fixtures**

At the top of `tests/test_agent_runner.py`, `PROJ` and `PROJ_NO_DM` still use `agents=["claude-code"]` which no longer exists. Replace both:

```python
PROJ = ProjectConfig(
    id="proj-1", name="MyProj", repo_path="/repo",
    identity="alice", escalation_dm_target="npub1op", adapter="claude-code",
)
PROJ_NO_DM = ProjectConfig(
    id="proj-1", name="MyProj", repo_path="/repo",
    identity="alice", escalation_dm_target="", adapter="claude-code",
)
```

- [ ] **Step 5: Run tests to verify they pass**

```bash
uv run pytest tests/test_config.py tests/test_agent_runner.py -v
```

Expected: all pass. The runner tests will still pass because `project.agents[0]` in `runner.py` will fail at runtime (not at import time) — the runner tests mock the subprocess, so the broken `agents[0]` reference is not triggered. That will be fixed in Task 5.

- [ ] **Step 6: Commit**

```bash
git add deskbridge/config.py tests/test_config.py tests/test_agent_runner.py
git commit -m "feat: replace agents list with single adapter field in ProjectConfig"
```

---

### Task 3: Schema migration — add `adapter` column

**Files:**
- Modify: `deskbridge/db/schema.py`
- Modify: `tests/test_store.py`

- [ ] **Step 1: Write the failing migration tests**

Add to `tests/test_store.py` (after the existing tests, before or after the `_seed_project` helper):

```python
async def test_apply_schema_adds_adapter_column(tmp_path):
    db_path = tmp_path / "migration_test.db"
    async with aiosqlite.connect(db_path) as conn:
        conn.row_factory = aiosqlite.Row
        await apply_schema(conn)
        async with conn.execute("PRAGMA table_info(projects)") as cur:
            cols = {row["name"] for row in await cur.fetchall()}
    assert "adapter" in cols


async def test_apply_schema_migration_idempotent(tmp_path):
    db_path = tmp_path / "migration_test2.db"
    async with aiosqlite.connect(db_path) as conn:
        conn.row_factory = aiosqlite.Row
        await apply_schema(conn)
        await apply_schema(conn)  # second call must not raise
        async with conn.execute("PRAGMA table_info(projects)") as cur:
            cols = {row["name"] for row in await cur.fetchall()}
    assert "adapter" in cols
```

- [ ] **Step 2: Run to verify they fail**

```bash
uv run pytest tests/test_store.py::test_apply_schema_adds_adapter_column tests/test_store.py::test_apply_schema_migration_idempotent -v
```

Expected: FAIL — `adapter` column does not exist yet.

- [ ] **Step 3: Update `deskbridge/db/schema.py`**

Add `_MIGRATIONS` and update `apply_schema`. The full updated file:

```python
import aiosqlite

SCHEMA_VERSION = 1

TABLE_NAMES = [
    "schema_version",
    "accounts",
    "projects",
    "contacts",
    "work_items",
    "agent_runs",
    "cursors",
    "approvals",
    "outbox",
    "audit_log",
]

_DDL = """
CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS accounts (
    id          TEXT PRIMARY KEY,
    npub        TEXT NOT NULL UNIQUE,
    label       TEXT NOT NULL,
    passphrase_ref TEXT NOT NULL,
    session_id  TEXT,
    health      TEXT NOT NULL DEFAULT 'unknown',
    last_unlocked_at TEXT,
    created_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);

CREATE TABLE IF NOT EXISTS projects (
    id              TEXT PRIMARY KEY,
    name            TEXT NOT NULL,
    repo_path       TEXT NOT NULL,
    identity_id     TEXT NOT NULL REFERENCES accounts(id),
    agents_json     TEXT NOT NULL DEFAULT '[]',
    groups_json     TEXT NOT NULL DEFAULT '[]',
    boards_json     TEXT NOT NULL DEFAULT '[]',
    escalation_dm_target TEXT,
    allowed_actions_json TEXT NOT NULL DEFAULT '[]',
    created_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);

CREATE TABLE IF NOT EXISTS contacts (
    pubkey      TEXT PRIMARY KEY,
    aliases_json TEXT NOT NULL DEFAULT '[]',
    role        TEXT,
    projects_json TEXT NOT NULL DEFAULT '[]',
    preferred_channel TEXT,
    created_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);

CREATE TABLE IF NOT EXISTS work_items (
    id          TEXT PRIMARY KEY,
    source_type TEXT NOT NULL,
    source_id   TEXT NOT NULL,
    project_id  TEXT REFERENCES projects(id),
    identity_id TEXT REFERENCES accounts(id),
    priority    INTEGER NOT NULL DEFAULT 5,
    status      TEXT NOT NULL DEFAULT 'pending',
    assigned_agent TEXT,
    summary     TEXT,
    payload_json TEXT NOT NULL DEFAULT '{}',
    idempotency_key TEXT UNIQUE,
    created_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    updated_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);

CREATE TABLE IF NOT EXISTS agent_runs (
    id              TEXT PRIMARY KEY,
    work_item_id    TEXT NOT NULL REFERENCES work_items(id),
    adapter_type    TEXT NOT NULL,
    process_info_json TEXT,
    heartbeat_at    TEXT,
    status          TEXT NOT NULL DEFAULT 'running',
    checkpoint_summary TEXT,
    result_summary  TEXT,
    created_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    updated_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);

CREATE TABLE IF NOT EXISTS cursors (
    id              TEXT PRIMARY KEY,
    cursor_type     TEXT NOT NULL,
    identity_id     TEXT NOT NULL REFERENCES accounts(id),
    last_entity_id  TEXT,
    last_created_at TEXT,
    last_imported_at TEXT,
    raw_json        TEXT NOT NULL DEFAULT '{}',
    updated_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    UNIQUE (cursor_type, identity_id)
);

CREATE TABLE IF NOT EXISTS approvals (
    id                  TEXT PRIMARY KEY,
    mcp_approval_id     TEXT,
    work_item_id        TEXT REFERENCES work_items(id),
    identity_id         TEXT REFERENCES accounts(id),
    action_description  TEXT NOT NULL,
    scope               TEXT,
    request_text        TEXT,
    status              TEXT NOT NULL DEFAULT 'pending',
    expires_at          TEXT,
    created_at          TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    resolved_at         TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS approvals_mcp_approval_id_unique
    ON approvals (mcp_approval_id) WHERE mcp_approval_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS outbox (
    id              TEXT PRIMARY KEY,
    identity_id     TEXT NOT NULL REFERENCES accounts(id),
    dest_pubkey     TEXT,
    dest_group_id   TEXT,
    message_text    TEXT NOT NULL,
    idempotency_key TEXT UNIQUE,
    idempotency_ttl TEXT,
    delivery_attempts INTEGER NOT NULL DEFAULT 0,
    delivery_status TEXT NOT NULL DEFAULT 'pending',
    delivery_result_json TEXT,
    created_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    delivered_at    TEXT
);

CREATE TABLE IF NOT EXISTS audit_log (
    id          TEXT PRIMARY KEY,
    created_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    event_type  TEXT NOT NULL,
    identity_id TEXT,
    project_id  TEXT,
    work_item_id TEXT,
    payload_json TEXT NOT NULL DEFAULT '{}'
);
"""

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
    async with conn.execute("SELECT COUNT(*) FROM schema_version") as cur:
        row = await cur.fetchone()
    if row[0] == 0:
        await conn.execute(
            "INSERT INTO schema_version (version) VALUES (?)", (SCHEMA_VERSION,)
        )
        await conn.commit()
```

- [ ] **Step 4: Run to verify tests pass**

```bash
uv run pytest tests/test_store.py::test_apply_schema_adds_adapter_column tests/test_store.py::test_apply_schema_migration_idempotent -v
```

Expected: 2 passed.

- [ ] **Step 5: Commit**

```bash
git add deskbridge/db/schema.py tests/test_store.py
git commit -m "feat: add adapter column to projects table via schema migration"
```

---

### Task 4: Store — update `upsert_project` and `bootstrap_accounts_from_config`

**Files:**
- Modify: `deskbridge/db/store.py`
- Modify: `tests/test_store.py`

- [ ] **Step 1: Write the failing store test**

Add to `tests/test_store.py` (after the migration tests added in Task 3):

```python
async def test_upsert_project_writes_adapter(store: Store):
    await store.upsert_account(
        id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:A"
    )
    await store.upsert_project(
        id="proj-1",
        name="MyProject",
        repo_path="/repo",
        identity_id="acc-alice",
        adapter="codex",
        boards_json="[]",
        allowed_actions_json="[]",
        escalation_dm_target=None,
    )
    row = await store.get_project_for_identity("acc-alice")
    assert row is not None
    assert row["adapter"] == "codex"
```

- [ ] **Step 2: Run to verify it fails**

```bash
uv run pytest tests/test_store.py::test_upsert_project_writes_adapter -v
```

Expected: FAIL — `upsert_project` does not accept `adapter=` yet.

- [ ] **Step 3: Update `store.upsert_project` in `deskbridge/db/store.py`**

Replace the existing `upsert_project` method (currently lines 34–68) with:

```python
async def upsert_project(
    self,
    id: str,
    name: str,
    repo_path: str,
    identity_id: str,
    adapter: str,
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
            -- groups_json excluded: populated by relay sync, must survive restarts
        """,
        (id, name, repo_path, identity_id, adapter,
         boards_json, allowed_actions_json, escalation_dm_target),
    ):
        pass
    await self._conn.commit()
```

- [ ] **Step 4: Update `bootstrap_accounts_from_config` in `deskbridge/db/store.py`**

The `bootstrap_accounts_from_config` function (currently near line 447) passes `agents_json`. Replace the `upsert_project` call:

```python
async def bootstrap_accounts_from_config(store: Store, config: DeskBridgeConfig) -> None:
    for identity in config.identities:
        await store.upsert_account(
            id=f"acc-{identity.label}",
            npub=identity.npub,
            label=identity.label,
            passphrase_ref=identity.passphrase_ref,
        )
    for project in config.projects:
        await store.upsert_project(
            id=project.id,
            name=project.name,
            repo_path=project.repo_path,
            identity_id=f"acc-{project.identity}",
            adapter=project.adapter,
            boards_json=json.dumps(project.boards),
            allowed_actions_json=json.dumps(project.allowed_autonomous_actions),
            escalation_dm_target=project.escalation_dm_target,
        )
```

- [ ] **Step 5: Update `_seed_project` helper in `tests/test_store.py`**

The `_seed_project` helper (around line 280) uses a raw SQL insert with `agents_json`. After the migration adds the `adapter` column, `agents_json` still exists (it's an old column that's just orphaned), but the new column needs a value too. Update the helper to insert `adapter` instead of `agents_json`:

```python
async def _seed_project(conn, *, id="proj-1", identity_id="acc-alice") -> None:
    await conn.execute(
        "INSERT INTO projects (id, name, repo_path, identity_id, adapter) VALUES (?, ?, ?, ?, ?)",
        (id, "MyProject", "/repo/myproject", identity_id, "claude-code"),
    )
    await conn.commit()
```

- [ ] **Step 6: Run tests to verify they pass**

```bash
uv run pytest tests/test_store.py -v
```

Expected: all pass, including the new `test_upsert_project_writes_adapter`.

- [ ] **Step 7: Commit**

```bash
git add deskbridge/db/store.py tests/test_store.py
git commit -m "feat: update upsert_project to write adapter column, drop agents_json"
```

---

### Task 5: Runner — use `project.adapter` and `build_command()`

**Files:**
- Modify: `deskbridge/agent/runner.py`

No new tests are needed — the adapter command logic is fully covered by `tests/test_adapters.py`, and the runner integration is already tested by `tests/test_agent_runner.py`. This task replaces two statements in `_do_run`.

- [ ] **Step 1: Update `deskbridge/agent/runner.py`**

Add the import at the top of the file (after the existing imports):

```python
from deskbridge.agent.adapters import build_command
```

In `_do_run`, replace the entire Step 1 and Step 2 block (currently lines ~77–91):

```python
# 1. Record the run
await self._store.upsert_agent_run(
    id=run_id,
    work_item_id=work_item["id"],
    adapter_type=project.adapter,
)

# 2. Build CLI command
task_text = f"{work_item['summary']}\n\n{work_item['payload_json']}"
prompt = _APPROVAL_INSTRUCTION + task_text[:4000 - len(_APPROVAL_INSTRUCTION)]
cmd = build_command(project.adapter, project.repo_path, prompt)
```

The old block to remove:

```python
# 1. Record the run
await self._store.upsert_agent_run(
    id=run_id,
    work_item_id=work_item["id"],
    adapter_type=project.agents[0],
)

# 2. Build CLI command
adapter = project.agents[0]
task_text = f"{work_item['summary']}\n\n{work_item['payload_json']}"
prompt = _APPROVAL_INSTRUCTION + task_text[:4000 - len(_APPROVAL_INSTRUCTION)]
if adapter == "claude-code":
    cmd = ["claude", "--project", project.repo_path, "--message", prompt]
else:
    cmd = ["codex", "--dir", project.repo_path, prompt]
```

- [ ] **Step 2: Run the full test suite**

```bash
uv run pytest --tb=short -q
```

Expected: all tests pass (275+ passing, 0 failures).

- [ ] **Step 3: Commit**

```bash
git add deskbridge/agent/runner.py
git commit -m "feat: use build_command() in AgentRunner, drop inline adapter if/else"
```
