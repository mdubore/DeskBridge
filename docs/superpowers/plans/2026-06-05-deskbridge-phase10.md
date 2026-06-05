# DeskBridge Phase 10: Hermes and OpenClaw Adapters — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add Hermes and OpenClaw as first-class adapters in `adapters.py`, wire `openclaw_agent_id` through config, DB, and runner, and log adapter context at run start.

**Architecture:** Two new `build_command` branches (hermes, openclaw) with a keyword-only `agent_id` param; `ProjectConfig` gains `openclaw_agent_id` + a model validator; a DB migration adds the column; `runner.py` passes the id through and emits a start log.

**Tech Stack:** Python 3.12, Pydantic v2, aiosqlite, structlog, pytest + pytest-asyncio (asyncio_mode=auto), uv

---

## File Map

| File | Change |
|---|---|
| `tests/test_adapters.py` | Fix stale `"hermes"` fixture; add hermes/openclaw tests |
| `tests/test_config.py` | Fix stale `"hermes"` fixture; add openclaw validator tests |
| `deskbridge/agent/adapters.py` | Add hermes/openclaw to `KNOWN_ADAPTERS` and `build_command` |
| `deskbridge/config.py` | Add `openclaw_agent_id` field + `openclaw_agent_id_required` validator |
| `deskbridge/db/schema.py` | Add `openclaw_agent_id TEXT` migration |
| `deskbridge/db/store.py` | Add `openclaw_agent_id` to `upsert_project` SQL + `bootstrap_accounts_from_config` |
| `tests/test_store.py` | Add round-trip test; update `_seed_project` helper |
| `deskbridge/agent/runner.py` | Add start log; pass `agent_id=` to `build_command` |

---

### Task 1: Fix stale "hermes" test fixtures

Two existing tests use `"hermes"` as the stand-in for an unknown adapter. They must be updated before hermes is added to `KNOWN_ADAPTERS`, otherwise adding hermes will cause them to pass for the wrong reason (no longer raises).

**Files:**
- Modify: `tests/test_adapters.py`
- Modify: `tests/test_config.py`

- [ ] **Step 1: Confirm the tests currently pass with "hermes" as unknown**

```bash
uv run pytest tests/test_adapters.py::test_unknown_adapter_raises tests/test_config.py::test_adapter_unknown_value_raises_config_error -v
```

Expected: both PASS (hermes is not yet a known adapter, so it correctly raises).

- [ ] **Step 2: Update `test_unknown_adapter_raises` in `tests/test_adapters.py`**

Change the adapter name from `"hermes"` to `"voltron"`:

```python
def test_unknown_adapter_raises():
    with pytest.raises(ValueError, match="voltron"):
        build_command("voltron", "/repo", "do the thing")
```

- [ ] **Step 3: Update `test_adapter_unknown_value_raises_config_error` in `tests/test_config.py`**

Change the adapter name from `"hermes"` to `"voltron"`:

```python
def test_adapter_unknown_value_raises_config_error(tmp_path):
    cfg_file = tmp_path / "config.toml"
    cfg_file.write_text(MINIMAL_CONFIG.rstrip() + '\nadapter = "voltron"\n')
    with pytest.raises(ConfigError, match="voltron"):
        load_config(cfg_file)
```

- [ ] **Step 4: Run both tests to confirm they still pass**

```bash
uv run pytest tests/test_adapters.py::test_unknown_adapter_raises tests/test_config.py::test_adapter_unknown_value_raises_config_error -v
```

Expected: both PASS (voltron is not a known adapter).

- [ ] **Step 5: Commit**

```bash
git add tests/test_adapters.py tests/test_config.py
git commit -m "fix: replace 'hermes' with 'voltron' in unknown-adapter test fixtures"
```

---

### Task 2: Add hermes and openclaw to adapters.py

**Files:**
- Modify: `deskbridge/agent/adapters.py`
- Modify: `tests/test_adapters.py`

- [ ] **Step 1: Write failing tests for hermes and openclaw commands**

Add to `tests/test_adapters.py` (after the existing `test_gemini_command`):

```python
def test_hermes_command():
    cmd = build_command("hermes", "/repo", "do the thing")
    assert cmd == ["hermes", "chat", "-Q", "-q", "do the thing"]


def test_openclaw_command():
    cmd = build_command("openclaw", "/repo", "do the thing", agent_id="my-agent")
    assert cmd == ["openclaw", "agent", "--agent", "my-agent", "--local", "--message", "do the thing"]
```

- [ ] **Step 2: Run to confirm they fail**

```bash
uv run pytest tests/test_adapters.py::test_hermes_command tests/test_adapters.py::test_openclaw_command -v
```

Expected: both FAIL with `ValueError: Unknown adapter: 'hermes'` / `'openclaw'`.

- [ ] **Step 3: Update `deskbridge/agent/adapters.py`**

Replace the entire file:

```python
KNOWN_ADAPTERS = {"claude-code", "codex", "gemini", "hermes", "openclaw"}


def build_command(adapter: str, repo_path: str, prompt: str, *, agent_id: str | None = None) -> list[str]:
    if adapter == "claude-code":
        return ["claude", "--project", repo_path, "--message", prompt]
    elif adapter == "codex":
        return ["codex", "--cwd", repo_path, "--no-interactive", prompt]
    elif adapter == "gemini":
        # repo_path passed as cwd= on subprocess
        return ["gemini", "--yolo", "-p", prompt]
    elif adapter == "hermes":
        # repo_path passed as cwd= on subprocess; -Q suppresses banner/spinner
        return ["hermes", "chat", "-Q", "-q", prompt]
    elif adapter == "openclaw":
        # workspace is pre-configured per agent at setup time (no per-invocation flag)
        return ["openclaw", "agent", "--agent", agent_id, "--local", "--message", prompt]
    raise ValueError(f"Unknown adapter: {adapter!r}")
```

- [ ] **Step 4: Run the new tests plus the full adapter suite**

```bash
uv run pytest tests/test_adapters.py -v
```

Expected: all PASS. The `test_known_adapters_matches_build_command` test calls `build_command` for every entry in `KNOWN_ADAPTERS` — hermes and openclaw are now included (openclaw receives `agent_id=None`, which produces a list with `None` rather than raising; that is acceptable since the config validator prevents `None` reaching here at runtime).

- [ ] **Step 5: Commit**

```bash
git add deskbridge/agent/adapters.py tests/test_adapters.py
git commit -m "feat: add hermes and openclaw adapters to adapters.py"
```

---

### Task 3: Add openclaw_agent_id to ProjectConfig

**Files:**
- Modify: `deskbridge/config.py`
- Modify: `tests/test_config.py`

- [ ] **Step 1: Write failing tests**

Add to `tests/test_config.py` (after `test_adapter_unknown_value_raises_config_error`):

```python
def test_openclaw_requires_agent_id(tmp_path):
    cfg_file = tmp_path / "config.toml"
    cfg_file.write_text(MINIMAL_CONFIG.rstrip() + '\nadapter = "openclaw"\n')
    with pytest.raises(ConfigError, match="openclaw_agent_id"):
        load_config(cfg_file)


def test_openclaw_with_agent_id_accepted(tmp_path):
    cfg_file = tmp_path / "config.toml"
    cfg_file.write_text(
        MINIMAL_CONFIG.rstrip() + '\nadapter = "openclaw"\nopenclaw_agent_id = "my-agent"\n'
    )
    cfg = load_config(cfg_file)
    assert cfg.projects[0].openclaw_agent_id == "my-agent"
```

- [ ] **Step 2: Run to confirm they fail**

```bash
uv run pytest tests/test_config.py::test_openclaw_requires_agent_id tests/test_config.py::test_openclaw_with_agent_id_accepted -v
```

Expected: `test_openclaw_requires_agent_id` FAILS (no error raised yet); `test_openclaw_with_agent_id_accepted` FAILS (field doesn't exist yet).

- [ ] **Step 3: Update `deskbridge/config.py`**

**3a. Verify the `typing` import includes `Annotated`.** Find the `from typing import` line (search for it — don't rely on a line number). It should read:

```python
from typing import Annotated, Literal
```

If `Annotated` is missing, add it. `Annotated` is used by `check_in_interval_hours` in `ProjectConfig` and must be present.

**3b. Add `model_validator` to the pydantic import.** Find the `from pydantic import` line and add `model_validator`:

```python
from pydantic import BaseModel, Field, ValidationError, field_validator, model_validator
```

**3c. Add the new field and validator to `ProjectConfig`.** Find the `class ProjectConfig(BaseModel):` definition (search for it — line numbers may have shifted across phases). Add `openclaw_agent_id` and `openclaw_agent_id_required` after the existing `adapter_must_be_known` validator. The complete updated class body:

```python
class ProjectConfig(BaseModel):
    id: str
    name: str
    repo_path: str
    identity: str
    escalation_dm_target: str
    adapter: str = "claude-code"
    openclaw_agent_id: str | None = None
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
        # Deferred import prevents circular imports if adapters.py ever needs config types.
        from deskbridge.agent.adapters import KNOWN_ADAPTERS
        if v not in KNOWN_ADAPTERS:
            raise ValueError(f"Unknown adapter {v!r}. Known: {sorted(KNOWN_ADAPTERS)}")
        return v

    @model_validator(mode="after")
    def openclaw_agent_id_required(self) -> "ProjectConfig":
        if self.adapter == "openclaw" and not self.openclaw_agent_id:
            raise ValueError("openclaw_agent_id is required when adapter='openclaw'")
        return self
```

- [ ] **Step 4: Update `test_adapter_known_values_accepted` to handle openclaw**

After Task 2, `KNOWN_ADAPTERS` already includes `"openclaw"`, so the parametrize covers it. Now that the validator exists, `ProjectConfig(adapter="openclaw")` without `openclaw_agent_id` raises — meaning `test_adapter_known_values_accepted[openclaw]` will fail. Fix it now by passing `openclaw_agent_id` when needed:

```python
@pytest.mark.parametrize("adapter", sorted(KNOWN_ADAPTERS))
def test_adapter_known_values_accepted(adapter):
    from deskbridge.config import ProjectConfig
    extra = {"openclaw_agent_id": "test-agent"} if adapter == "openclaw" else {}
    proj = ProjectConfig(
        id="p1", name="MyProj", repo_path="/repo",
        identity="alice", escalation_dm_target="npub1op",
        adapter=adapter,
        **extra,
    )
    assert proj.adapter == adapter
```

- [ ] **Step 5: Run the full config suite**

```bash
uv run pytest tests/test_config.py -v
```

Expected: all PASS.

- [ ] **Step 6: Commit**

```bash
git add deskbridge/config.py tests/test_config.py
git commit -m "feat: add openclaw_agent_id field and validator to ProjectConfig"
```

---

### Task 4: Schema migration and store update

**Files:**
- Modify: `deskbridge/db/schema.py`
- Modify: `deskbridge/db/store.py`
- Modify: `tests/test_store.py`

- [ ] **Step 1: Write the failing store test**

Add to `tests/test_store.py` (after `test_upsert_project_writes_adapter`):

```python
async def test_upsert_project_writes_openclaw_agent_id(store: Store):
    await store.upsert_account(
        id="acc-alice", npub="npub1alice", label="alice", passphrase_ref="env:A"
    )
    await store.upsert_project(
        id="proj-1",
        name="MyProject",
        repo_path="/repo",
        identity_id="acc-alice",
        adapter="openclaw",
        openclaw_agent_id="my-agent",
        boards_json="[]",
        allowed_actions_json="[]",
        escalation_dm_target=None,
    )
    row = await store.get_project_for_identity("acc-alice")
    assert row is not None
    assert row["openclaw_agent_id"] == "my-agent"
```

- [ ] **Step 2: Run to confirm it fails**

```bash
uv run pytest tests/test_store.py::test_upsert_project_writes_openclaw_agent_id -v
```

Expected: FAIL — `upsert_project` doesn't accept `openclaw_agent_id` yet.

- [ ] **Step 3: Add migration to `deskbridge/db/schema.py`**

Update `_MIGRATIONS` (line 140):

```python
_MIGRATIONS = [
    "ALTER TABLE projects ADD COLUMN adapter TEXT NOT NULL DEFAULT 'claude-code'",
    "ALTER TABLE projects ADD COLUMN openclaw_agent_id TEXT",
]
```

No other change to `schema.py`.

- [ ] **Step 4: Update `upsert_project` in `deskbridge/db/store.py`**

Replace the `upsert_project` method (lines 34–65):

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
        allowed_actions_json: str,
        escalation_dm_target: str | None,
    ) -> None:
        async with self._conn.execute(
            """
            INSERT INTO projects
                (id, name, repo_path, identity_id, adapter, openclaw_agent_id,
                 boards_json, allowed_actions_json, escalation_dm_target)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                name                 = excluded.name,
                repo_path            = excluded.repo_path,
                identity_id          = excluded.identity_id,
                adapter              = excluded.adapter,
                openclaw_agent_id    = excluded.openclaw_agent_id,
                boards_json          = excluded.boards_json,
                allowed_actions_json = excluded.allowed_actions_json,
                escalation_dm_target = excluded.escalation_dm_target
                -- groups_json excluded: populated by relay sync, must survive restarts
            """,
            (id, name, repo_path, identity_id, adapter, openclaw_agent_id,
             boards_json, allowed_actions_json, escalation_dm_target),
        ):
            pass
        await self._conn.commit()
```

- [ ] **Step 5: Update `bootstrap_accounts_from_config` in `deskbridge/db/store.py`**

Update the `upsert_project` call inside `bootstrap_accounts_from_config` (line 444):

```python
    for project in config.projects:
        await store.upsert_project(
            id=project.id,
            name=project.name,
            repo_path=project.repo_path,
            identity_id=f"acc-{project.identity}",
            adapter=project.adapter,
            openclaw_agent_id=project.openclaw_agent_id,
            boards_json=json.dumps(project.boards),
            allowed_actions_json=json.dumps(project.allowed_autonomous_actions),
            escalation_dm_target=project.escalation_dm_target,
        )
```

- [ ] **Step 6: Update `_seed_project` helper in `tests/test_store.py`**

Update `_seed_project` (line 280) to include the new column:

```python
async def _seed_project(conn, *, id="proj-1", identity_id="acc-alice") -> None:
    await conn.execute(
        "INSERT INTO projects (id, name, repo_path, identity_id, adapter, openclaw_agent_id) VALUES (?, ?, ?, ?, ?, ?)",
        (id, "MyProject", "/repo/myproject", identity_id, "claude-code", None),
    )
    await conn.commit()
```

- [ ] **Step 7: Run the new test plus the full store suite**

```bash
uv run pytest tests/test_store.py -v
```

Expected: all PASS. `test_apply_schema_migration_idempotent` runs `apply_schema` twice and covers both migrations.

- [ ] **Step 8: Commit**

```bash
git add deskbridge/db/schema.py deskbridge/db/store.py tests/test_store.py
git commit -m "feat: add openclaw_agent_id column via migration, update upsert_project"
```

---

### Task 5: Update AgentRunner

**Files:**
- Modify: `deskbridge/agent/runner.py`

- [ ] **Step 1: Read the current run() method preamble**

Open `deskbridge/agent/runner.py` and locate the start of the `run()` method body (around line 74). The first comment is `# 1. Record the run`.

- [ ] **Step 2: Add the start log and update the build_command call**

Make two changes to `runner.py`:

**Change A** — add a start log immediately before step 1 (`# 1. Record the run`):

```python
        log_ctx: dict = {"run_id": run_id, "adapter": project.adapter}
        if project.adapter == "openclaw":
            log_ctx["openclaw_agent_id"] = project.openclaw_agent_id
        log.info("agent_runner_start", **log_ctx)

        # 1. Record the run
```

**Change B** — update the `build_command` call at step 2 (line 87):

```python
        cmd = build_command(project.adapter, project.repo_path, prompt, agent_id=project.openclaw_agent_id)
```

- [ ] **Step 3: Run the full test suite**

```bash
uv run pytest --tb=short -q
```

Expected: all existing tests PASS. The runner tests use `adapter="claude-code"` fixtures which have `openclaw_agent_id=None` — `build_command("claude-code", ..., agent_id=None)` works correctly since claude-code ignores `agent_id`.

- [ ] **Step 4: Commit**

```bash
git add deskbridge/agent/runner.py
git commit -m "feat: log adapter context at run start, pass agent_id to build_command"
```
