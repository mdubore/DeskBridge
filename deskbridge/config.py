import os
import tomllib
from pathlib import Path
from typing import Annotated, Literal

from pydantic import BaseModel, Field, ValidationError, field_validator, model_validator

try:
    import keyring as kr
except ImportError:
    kr = None  # type: ignore[assignment]


class ConfigError(Exception):
    pass


class SupervisorConfig(BaseModel):
    db_path: str
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"
    heartbeat_interval_secs: float = 60.0


class McpConfig(BaseModel):
    command: str
    args: list[str] = Field(default_factory=list)
    startup_timeout_secs: int = 30


class IdentityConfig(BaseModel):
    label: str
    npub: str
    passphrase_ref: str
    operator_npub: str | None = None

    def resolve_passphrase(self) -> str:
        if self.passphrase_ref.startswith("env:"):
            var_name = self.passphrase_ref[4:]
            value = os.environ.get(var_name)
            if value is None:
                raise ConfigError(
                    f"Passphrase env var '{var_name}' is not set"
                )
            return value
        if self.passphrase_ref.startswith("keyring:"):
            parts = self.passphrase_ref[8:].split(":", 1)
            if len(parts) != 2:
                raise ConfigError(
                    f"Invalid keyring ref '{self.passphrase_ref}': "
                    "expected keyring:service:key"
                )
            service, key = parts
            if kr is None:
                raise ConfigError(
                    "keyring package is required for keyring: passphrase refs"
                )
            value = kr.get_password(service, key)
            if value is None:
                raise ConfigError(
                    f"No keyring entry for service='{service}' key='{key}'"
                )
            return value
        raise ConfigError(
            f"Unknown passphrase_ref format '{self.passphrase_ref}': "
            "expected env:VAR or keyring:service:key"
        )


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


class DeskBridgeConfig(BaseModel):
    supervisor: SupervisorConfig
    mcp: McpConfig
    identities: list[IdentityConfig]
    projects: list[ProjectConfig] = Field(default_factory=list)

    @field_validator("identities")
    @classmethod
    def identities_not_empty(cls, v: list[IdentityConfig]) -> list[IdentityConfig]:
        if not v:
            raise ValueError("must include at least one [[identities]] entry")
        return v


def load_config(path: Path) -> DeskBridgeConfig:
    try:
        with open(path, "rb") as f:
            raw = tomllib.load(f)
    except FileNotFoundError:
        raise ConfigError(f"Config file not found: {path}")
    except tomllib.TOMLDecodeError as e:
        raise ConfigError(f"Invalid TOML in {path}: {e}")

    try:
        config = DeskBridgeConfig.model_validate(raw)
    except ValidationError as e:
        raise ConfigError(f"Config validation failed: {e}")

    identity_labels = {i.label for i in config.identities}
    for project in config.projects:
        if project.identity not in identity_labels:
            raise ConfigError(
                f"Project '{project.id}' references unknown identity "
                f"'{project.identity}' (known: {sorted(identity_labels)})"
            )

    return config
