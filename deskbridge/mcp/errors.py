from enum import StrEnum

from deskbridge.models import McpError, McpErrorCategory


class RoutingDecision(StrEnum):
    RETRY = "retry"
    REAUTH = "reauth"
    RESET_CURSOR = "reset_cursor"
    ESCALATE = "escalate"
    REJECT = "reject"


_ROUTING_TABLE: dict[McpErrorCategory, RoutingDecision] = {
    McpErrorCategory.INVALID_SESSION:     RoutingDecision.REAUTH,
    McpErrorCategory.INVALID_CURSOR:      RoutingDecision.RESET_CURSOR,
    McpErrorCategory.TRANSIENT_TRANSPORT: RoutingDecision.RETRY,
    McpErrorCategory.APPROVAL_REQUIRED:   RoutingDecision.ESCALATE,
    McpErrorCategory.VALIDATION_FAILED:   RoutingDecision.REJECT,
    McpErrorCategory.UNSUPPORTED_STATE:   RoutingDecision.REJECT,
    McpErrorCategory.INTERNAL_ERROR:      RoutingDecision.RETRY,
}


def route_mcp_error(error: McpError) -> RoutingDecision:
    return _ROUTING_TABLE.get(error.category, RoutingDecision.RETRY)
