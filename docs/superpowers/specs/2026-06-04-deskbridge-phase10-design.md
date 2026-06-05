# DeskBridge Phase 10: Hermes and OpenClaw Adapters — Design

## Overview

Add Hermes and OpenClaw as first-class adapters alongside the existing claude-code, codex, and gemini adapters. This is a direct continuation of Phase 9's adapter surface expansion.

---

## CLI Invocation

| Adapter | Binary | Working directory | One-shot flag | Prompt |
|---|---|---|---|---|
| `hermes` | `hermes` | `cwd=` on subprocess | `chat -Q` | `-q <text>` |
| `openclaw` | `openclaw` | pre-configured per agent | `agent --local` | `--message <text>` |

### Hermes

Hermes (NousResearch) is invoked with:

```bash
hermes chat -Q -q "prompt"
```

- `-Q` suppresses the banner and spinner for clean subprocess output.
- `-q` runs a single query without entering interactive mode.
- No explicit working directory flag exists. `repo_path` is passed as `cwd=` on `asyncio.create_subprocess_exec`, the same pattern used by the Gemini adapter.

### OpenClaw

OpenClaw's workspace is configured at agent-setup time via `openclaw agents add <name> --workspace <dir>`. There is no per-invocation flag to override the workspace path (feature request openclaw/openclaw#32637 is open but unmerged). The correct usage for DeskBridge is:

1. The user pre-configures one OpenClaw agent per project repo: `openclaw agents add <name> --workspace <repo_path>`
2. DeskBridge stores the agent ID in `ProjectConfig.openclaw_agent_id`
3. DeskBridge invokes: `openclaw agent --agent <id> --local --message "prompt"`

`--local` bypasses the Gateway and runs the embedded agent inline.

---

## Config

### New field: `openclaw_agent_id`

`ProjectConfig` in `deskbridge/config.py`:

```python
openclaw_agent_id: str | None = None

@model_validator(mode="after")
def openclaw_agent_id_required(self) -> "ProjectConfig":
    if self.adapter == "openclaw" and not self.openclaw_agent_id:
        raise ValueError("openclaw_agent_id is required when adapter='openclaw'")
    return self
```

- Required when `adapter = "openclaw"`, optional (None) for all other adapters.
- Validated at config parse time; raises `ConfigError` with a clear message if missing.
- No database column needed — this field lives only in the TOML config and `ProjectConfig`.

Example TOML:

```toml
[projects.my-project]
adapter = "openclaw"
openclaw_agent_id = "my-project-agent"
```

---

## Modified Component: `deskbridge/agent/adapters.py`

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

The `agent_id` parameter is keyword-only and defaults to `None`. It is only used by the `openclaw` branch; all other adapters ignore it. The validator on `ProjectConfig` ensures `agent_id` is never `None` when `adapter="openclaw"`.

---

## Modified Component: `AgentRunner`

**File:** `deskbridge/agent/runner.py`

One-line change:

```python
# was:
cmd = build_command(project.adapter, project.repo_path, prompt)

# becomes:
cmd = build_command(project.adapter, project.repo_path, prompt, agent_id=project.openclaw_agent_id)
```

All other runner logic is untouched.

---

## Files Touched

| File | Change |
|---|---|
| `deskbridge/agent/adapters.py` | Add `"hermes"` and `"openclaw"` to `KNOWN_ADAPTERS`; add hermes/openclaw branches; add `agent_id` keyword arg |
| `deskbridge/config.py` | Add `openclaw_agent_id: str | None = None`; add `openclaw_agent_id_required` model validator |
| `deskbridge/agent/runner.py` | Pass `agent_id=project.openclaw_agent_id` to `build_command` |
| `tests/test_adapters.py` | Add `test_hermes_command`, `test_openclaw_command` |
| `tests/test_config.py` | Add `test_openclaw_requires_agent_id`, `test_openclaw_with_agent_id_accepted` |

No schema migration. No store changes. `test_agent_runner.py` fixtures require no update. The existing `test_adapter_known_values_accepted` parametrize drives from `sorted(KNOWN_ADAPTERS)` and will call `build_command` for hermes and openclaw automatically once they are added to the set. For openclaw this passes `agent_id=None`, producing a list with `None` rather than raising — that is acceptable because the dedicated `test_openclaw_command` test covers correct invocation with a real agent ID, and the config validator ensures `None` never reaches `build_command` at runtime.

---

## Testing

**`tests/test_adapters.py`:**

- `test_hermes_command` — `build_command("hermes", "/repo", "do the thing")` returns `["hermes", "chat", "-Q", "-q", "do the thing"]`
- `test_openclaw_command` — `build_command("openclaw", "/repo", "do the thing", agent_id="my-agent")` returns `["openclaw", "agent", "--agent", "my-agent", "--local", "--message", "do the thing"]`

**`tests/test_config.py`:**

- `test_openclaw_requires_agent_id` — `ProjectConfig(..., adapter="openclaw")` with no `openclaw_agent_id` raises `ConfigError` with `match="openclaw_agent_id"`
- `test_openclaw_with_agent_id_accepted` — `ProjectConfig(..., adapter="openclaw", openclaw_agent_id="my-agent")` parses without error
