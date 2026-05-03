import json
import pytest
from deskbridge.models import (
    McpCursor,
    McpError,
    McpErrorCategory,
    TaskEnvelope,
    TaskUpdate,
    ActionRequest,
    ActionRiskLevel,
    TaskStatus,
    SourceType,
)


def test_mcp_cursor_roundtrip():
    cursor = McpCursor(
        last_entity_id="msg-100",
        last_created_at="2026-01-01T00:00:00Z",
        last_imported_at=None,
    )
    raw = cursor.model_dump_json()
    restored = McpCursor.model_validate_json(raw)
    assert restored.last_entity_id == "msg-100"


def test_mcp_cursor_from_mcp_response():
    payload = {"last_entity_id": "dm-99", "last_created_at": "2026-01-02T00:00:00Z"}
    cursor = McpCursor.from_mcp_response(payload)
    assert cursor.last_entity_id == "dm-99"
    assert cursor.last_imported_at is None


def test_mcp_error_category_enum_values():
    assert McpErrorCategory.INVALID_SESSION == "invalid_session"
    assert McpErrorCategory.APPROVAL_REQUIRED == "approval_required"
    assert McpErrorCategory.TRANSIENT_TRANSPORT == "transient_transport"


def test_mcp_error_parses_category():
    raw = json.dumps({
        "error": {
            "category": "approval_required",
            "message": "action needs approval",
            "approval_request_id": "appr-xyz",
        }
    })
    err = McpError.from_tool_result_text(raw)
    assert err.category == McpErrorCategory.APPROVAL_REQUIRED
    assert err.approval_request_id == "appr-xyz"


def test_mcp_error_unknown_category_becomes_internal():
    raw = json.dumps({"error": {"category": "totally_unknown", "message": "oops"}})
    err = McpError.from_tool_result_text(raw)
    assert err.category == McpErrorCategory.INTERNAL_ERROR


def test_mcp_error_from_plain_string_becomes_internal():
    err = McpError.from_tool_result_text("something went wrong")
    assert err.category == McpErrorCategory.INTERNAL_ERROR
    assert "something went wrong" in err.message


def test_task_envelope_fields():
    env = TaskEnvelope(
        task_id="task-1",
        project_id="proj-1",
        identity_id="acc-1",
        source_type=SourceType.DM,
        source_refs=["dm-123"],
        objective="Fix the bug in auth.py",
        priority=3,
    )
    assert env.policy_mode == "autonomous"
    assert env.deadline is None


def test_action_request_risk_level():
    req = ActionRequest(
        action_type="send_dm",
        outbound_message="Task complete.",
        requested_risk_level=ActionRiskLevel.LOW,
    )
    assert req.requested_risk_level == ActionRiskLevel.LOW
