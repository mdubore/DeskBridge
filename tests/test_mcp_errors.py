import json

import pytest
from unittest.mock import AsyncMock, MagicMock

from deskbridge.mcp.client import McpClient, McpToolError
from deskbridge.mcp.errors import RoutingDecision, route_mcp_error
from deskbridge.models import McpError, McpErrorCategory


def make_error(category: McpErrorCategory, approval_id: str | None = None) -> McpError:
    return McpError(
        category=category,
        message="test error",
        approval_request_id=approval_id,
    )


def test_invalid_session_routes_to_reauth():
    err = make_error(McpErrorCategory.INVALID_SESSION)
    decision = route_mcp_error(err)
    assert decision == RoutingDecision.REAUTH


def test_transient_transport_routes_to_retry():
    err = make_error(McpErrorCategory.TRANSIENT_TRANSPORT)
    decision = route_mcp_error(err)
    assert decision == RoutingDecision.RETRY


def test_invalid_cursor_routes_to_reset_cursor():
    err = make_error(McpErrorCategory.INVALID_CURSOR)
    decision = route_mcp_error(err)
    assert decision == RoutingDecision.RESET_CURSOR


def test_approval_required_routes_to_escalate():
    err = make_error(McpErrorCategory.APPROVAL_REQUIRED, approval_id="appr-1")
    decision = route_mcp_error(err)
    assert decision == RoutingDecision.ESCALATE


def test_validation_failed_routes_to_reject():
    err = make_error(McpErrorCategory.VALIDATION_FAILED)
    decision = route_mcp_error(err)
    assert decision == RoutingDecision.REJECT


def test_unsupported_state_routes_to_reject():
    err = make_error(McpErrorCategory.UNSUPPORTED_STATE)
    decision = route_mcp_error(err)
    assert decision == RoutingDecision.REJECT


def test_internal_error_routes_to_retry():
    err = make_error(McpErrorCategory.INTERNAL_ERROR)
    decision = route_mcp_error(err)
    assert decision == RoutingDecision.RETRY


# --- McpError parser tests ---

def test_from_tool_result_text_known_category_preserves_raw():
    text = json.dumps({"error": {"category": "invalid_session", "message": "expired"}})
    err = McpError.from_tool_result_text(text)
    assert err.category == McpErrorCategory.INVALID_SESSION
    assert err.raw_category == "invalid_session"


def test_from_tool_result_text_unknown_category_falls_back_preserves_raw():
    text = json.dumps({"error": {"category": "approval_expired", "message": "too late"}})
    err = McpError.from_tool_result_text(text)
    assert err.category == McpErrorCategory.INTERNAL_ERROR
    assert err.raw_category == "approval_expired"


def test_from_tool_result_text_no_category_defaults_to_internal_error():
    text = json.dumps({"error": {"message": "oops"}})
    err = McpError.from_tool_result_text(text)
    assert err.category == McpErrorCategory.INTERNAL_ERROR
    assert err.raw_category == "internal_error"


def test_from_tool_result_text_invalid_json_falls_back():
    err = McpError.from_tool_result_text("not json at all")
    assert err.category == McpErrorCategory.INTERNAL_ERROR
    assert err.raw_category is None
    assert err.message == "not json at all"


def test_from_tool_result_text_data_dict_extracted():
    text = json.dumps({
        "error": {
            "category": "approval_required",
            "message": "needs approval",
            "data": {"category": "approval_expired", "approval_request_id": "req-abc"},
        }
    })
    err = McpError.from_tool_result_text(text)
    assert isinstance(err.data, dict)
    assert err.data["category"] == "approval_expired"
    assert err.data["approval_request_id"] == "req-abc"


def test_from_tool_result_text_data_non_dict_is_none():
    text = json.dumps({
        "error": {"category": "approval_required", "message": "x", "data": "not-a-dict"}
    })
    err = McpError.from_tool_result_text(text)
    assert err.data is None


def test_from_tool_result_text_no_data_field_is_none():
    text = json.dumps({"error": {"category": "approval_required", "message": "x"}})
    err = McpError.from_tool_result_text(text)
    assert err.data is None


def test_from_tool_result_text_data_empty_dict_preserved():
    text = json.dumps({"error": {"category": "approval_required", "message": "x", "data": {}}})
    err = McpError.from_tool_result_text(text)
    assert err.data == {}


def test_from_tool_result_text_approval_request_id_falls_back_to_nested_data():
    """When error.approval_request_id is absent, fall back to error.data.approval_request_id."""
    text = json.dumps({
        "error": {
            "category": "approval_required",
            "message": "wait",
            "data": {"category": "approval_expired", "approval_request_id": "req-nested"},
        }
    })
    err = McpError.from_tool_result_text(text)
    assert err.approval_request_id == "req-nested"


def test_from_tool_result_text_top_level_approval_request_id_takes_precedence():
    """Top-level error.approval_request_id wins over error.data.approval_request_id."""
    text = json.dumps({
        "error": {
            "category": "approval_required",
            "message": "wait",
            "approval_request_id": "req-top",
            "data": {"approval_request_id": "req-nested"},
        }
    })
    err = McpError.from_tool_result_text(text)
    assert err.approval_request_id == "req-top"


def test_from_tool_result_text_non_string_category_normalised():
    """Non-string category (e.g. integer) must not propagate — raw_category becomes 'internal_error'."""
    text = json.dumps({"error": {"category": 123, "message": "bad payload"}})
    err = McpError.from_tool_result_text(text)
    assert err.category == McpErrorCategory.INTERNAL_ERROR
    assert err.raw_category == "internal_error"


# --- McpToolError attribute tests (Path 1) ---

def test_mcp_tool_error_category_comes_from_data_dict():
    """McpToolError.category is data["category"], not the outer error.category."""
    text = json.dumps({
        "error": {
            "category": "approval_required",
            "message": "needs approval",
            "data": {
                "category": "approval_expired",
                "approval_request_id": "req-abc",
                "status": "pending",
            },
        }
    })
    err = McpError.from_tool_result_text(text)
    data = err.data
    tool_err = McpToolError(
        mcp_error=err,
        routing=RoutingDecision.RETRY,
        category=data.get("category") if data else None,
        data=data,
    )
    assert tool_err.category == "approval_expired"
    assert tool_err.data["approval_request_id"] == "req-abc"
    assert tool_err.data["status"] == "pending"


def test_mcp_tool_error_already_resolved_status_denied():
    text = json.dumps({
        "error": {
            "category": "internal_error",
            "message": "already resolved",
            "data": {"category": "approval_already_resolved", "status": "denied"},
        }
    })
    err = McpError.from_tool_result_text(text)
    data = err.data
    tool_err = McpToolError(
        mcp_error=err,
        routing=RoutingDecision.RETRY,
        category=data.get("category") if data else None,
        data=data,
    )
    assert tool_err.category == "approval_already_resolved"
    assert tool_err.data["status"] == "denied"


def test_mcp_tool_error_no_data_category_is_none():
    text = json.dumps({
        "error": {"category": "internal_error", "message": "oops", "data": "not-a-dict"}
    })
    err = McpError.from_tool_result_text(text)
    tool_err = McpToolError(mcp_error=err, routing=RoutingDecision.RETRY)
    assert tool_err.category is None
    assert tool_err.data is None


def test_mcp_parser_nested_approval_id_distinct_from_top_level():
    """approval_request_id in error.data is distinct from the top-level error.approval_request_id."""
    text = json.dumps({
        "error": {
            "category": "approval_required",
            "message": "wait for operator",
            "approval_request_id": "top-level-id",
            "data": {
                "category": "approval_expired",
                "approval_request_id": "nested-id",
            },
        }
    })
    err = McpError.from_tool_result_text(text)
    data = err.data
    tool_err = McpToolError(
        mcp_error=err,
        routing=RoutingDecision.RETRY,
        category=data.get("category") if data else None,
        data=data,
    )
    assert err.approval_request_id == "top-level-id"
    assert tool_err.data["approval_request_id"] == "nested-id"
    assert tool_err.category == "approval_expired"


# --- McpClient SDK exception tests (Path 2) ---

async def test_mcp_client_sdk_exception_with_data_dict_raises_tool_error():
    sdk_exc = Exception("sdk error")
    sdk_exc.data = {
        "category": "approval_expired",
        "approval_request_id": "req-abc",
        "status": "pending",
    }
    mock_session = MagicMock()
    mock_session.call_tool = AsyncMock(side_effect=sdk_exc)

    client = McpClient.__new__(McpClient)
    client._session = mock_session

    with pytest.raises(McpToolError) as exc_info:
        await client.call_tool("respond_to_approval", {})

    e = exc_info.value
    assert e.category == "approval_expired"
    assert e.data["approval_request_id"] == "req-abc"


async def test_mcp_client_sdk_exception_already_resolved_denied():
    sdk_exc = Exception("already done")
    sdk_exc.data = {"category": "approval_already_resolved", "status": "denied"}
    mock_session = MagicMock()
    mock_session.call_tool = AsyncMock(side_effect=sdk_exc)

    client = McpClient.__new__(McpClient)
    client._session = mock_session

    with pytest.raises(McpToolError) as exc_info:
        await client.call_tool("respond_to_approval", {})

    e = exc_info.value
    assert e.category == "approval_already_resolved"
    assert e.data["status"] == "denied"


async def test_mcp_client_sdk_exception_populates_mcp_error_data_and_approval_id():
    """Path 2: mcp_error.data and mcp_error.approval_request_id are populated from sdk_data."""
    sdk_exc = Exception("sdk error")
    sdk_exc.data = {
        "category": "approval_expired",
        "approval_request_id": "req-abc",
        "status": "pending",
    }
    mock_session = MagicMock()
    mock_session.call_tool = AsyncMock(side_effect=sdk_exc)

    client = McpClient.__new__(McpClient)
    client._session = mock_session

    with pytest.raises(McpToolError) as exc_info:
        await client.call_tool("respond_to_approval", {})

    e = exc_info.value
    assert e.mcp_error.data == sdk_exc.data
    assert e.mcp_error.approval_request_id == "req-abc"


async def test_mcp_client_sdk_exception_without_data_reraises():
    sdk_exc = RuntimeError("plain error")
    mock_session = MagicMock()
    mock_session.call_tool = AsyncMock(side_effect=sdk_exc)

    client = McpClient.__new__(McpClient)
    client._session = mock_session

    with pytest.raises(RuntimeError, match="plain error"):
        await client.call_tool("some_tool", {})


async def test_mcp_client_sdk_exception_with_non_dict_data_reraises():
    sdk_exc = RuntimeError("str data")
    sdk_exc.data = "not-a-dict"
    mock_session = MagicMock()
    mock_session.call_tool = AsyncMock(side_effect=sdk_exc)

    client = McpClient.__new__(McpClient)
    client._session = mock_session

    with pytest.raises(RuntimeError, match="str data"):
        await client.call_tool("some_tool", {})
