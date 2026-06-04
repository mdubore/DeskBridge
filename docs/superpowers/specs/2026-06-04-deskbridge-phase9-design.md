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
| `deskbridge/agent/adapters.py` | New — `KNOWN_ADAPTERS`, `build_command()` |
| `deskbridge/agent/runner.py` | Use `project.adapter` and `build_command()` |
| `tests/test_adapters.py` | New — command output tests for all three adapters |
| `tests/test_config.py` | Update for `adapter` field; add validation tests |
| `tests/test_agent_runner.py` | Update fixtures: `agents=["claude-code"]` → `adapter="claude-code"` |

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
