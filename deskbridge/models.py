import json
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, Field


class SourceType(StrEnum):
    DM = "dm"
    KANBAN = "kanban"
    MENTION = "mention"
    TIMER = "timer"


class TaskStatus(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    BLOCKED = "blocked"
    AWAITING_APPROVAL = "awaiting_approval"
    COMPLETE = "complete"
    FAILED = "failed"
    CANCELLED = "cancelled"


class ActionRiskLevel(StrEnum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"


class McpErrorCategory(StrEnum):
    INVALID_SESSION = "invalid_session"
    INVALID_CURSOR = "invalid_cursor"
    TRANSIENT_TRANSPORT = "transient_transport"
    APPROVAL_REQUIRED = "approval_required"
    VALIDATION_FAILED = "validation_failed"
    UNSUPPORTED_STATE = "unsupported_state"
    INTERNAL_ERROR = "internal_error"


_KNOWN_CATEGORIES = {c.value for c in McpErrorCategory}


class McpCursor(BaseModel):
    last_entity_id: str | None = None
    last_created_at: str | None = None
    last_imported_at: str | None = None

    @classmethod
    def from_mcp_response(cls, payload: dict[str, Any]) -> "McpCursor":
        return cls(
            last_entity_id=payload.get("last_entity_id"),
            last_created_at=payload.get("last_created_at"),
            last_imported_at=payload.get("last_imported_at"),
        )


class McpError(BaseModel):
    category: McpErrorCategory
    message: str
    approval_request_id: str | None = None

    @classmethod
    def from_tool_result_text(cls, text: str) -> "McpError":
        try:
            data = json.loads(text)
            if isinstance(data, dict) and "error" in data:
                err = data["error"]
                raw_category = err.get("category", "internal_error")
                category = (
                    McpErrorCategory(raw_category)
                    if raw_category in _KNOWN_CATEGORIES
                    else McpErrorCategory.INTERNAL_ERROR
                )
                return cls(
                    category=category,
                    message=err.get("message", text),
                    approval_request_id=err.get("approval_request_id"),
                )
        except (json.JSONDecodeError, KeyError, TypeError):
            pass
        return cls(
            category=McpErrorCategory.INTERNAL_ERROR,
            message=text,
        )


class TaskEnvelope(BaseModel):
    task_id: str
    project_id: str
    identity_id: str
    source_type: SourceType
    source_refs: list[str] = Field(default_factory=list)
    objective: str
    context_refs: list[str] = Field(default_factory=list)
    priority: int = 5
    deadline: str | None = None
    policy_mode: str = "autonomous"


class TaskUpdate(BaseModel):
    run_id: str
    status: TaskStatus
    checkpoint_summary: str | None = None
    blocker: str | None = None
    approval_request: str | None = None
    result_summary: str | None = None
    artifact_refs: list[str] = Field(default_factory=list)


class ActionRequest(BaseModel):
    action_type: str
    repo_action: str | None = None
    mcp_action: str | None = None
    shell_action: str | None = None
    outbound_message: str | None = None
    requested_risk_level: ActionRiskLevel = ActionRiskLevel.LOW
