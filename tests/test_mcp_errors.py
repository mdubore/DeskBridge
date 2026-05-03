import pytest
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
