import json
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
import pytest
from deskbridge.mcp.client import McpClient, McpToolError
from deskbridge.mcp.errors import RoutingDecision
from deskbridge.models import McpErrorCategory


def make_tool_result(content_text: str, is_error: bool = False):
    result = MagicMock()
    result.isError = is_error
    content = MagicMock()
    content.text = content_text
    result.content = [content]
    return result


@pytest.fixture
def mock_session():
    session = AsyncMock()
    return session


@pytest.fixture
def client(mock_session):
    c = McpClient(command="nostrdesk-mcp", args=[], startup_timeout_secs=5)
    c._session = mock_session
    return c


async def test_call_tool_success(client, mock_session):
    mock_session.call_tool.return_value = make_tool_result('{"session_id": "sess-1"}')
    result = await client.call_tool("unlock_identity", {"npub": "npub1alice"})
    assert result == {"session_id": "sess-1"}
    mock_session.call_tool.assert_called_once_with(
        "unlock_identity", {"npub": "npub1alice"}
    )


async def test_call_tool_error_result_raises_mcp_tool_error(client, mock_session):
    error_json = json.dumps({
        "error": {
            "category": "invalid_session",
            "message": "session not found",
        }
    })
    mock_session.call_tool.return_value = make_tool_result(error_json, is_error=True)
    with pytest.raises(McpToolError) as exc_info:
        await client.call_tool("some_tool", {})
    assert exc_info.value.mcp_error.category == McpErrorCategory.INVALID_SESSION
    assert exc_info.value.routing == RoutingDecision.REAUTH


async def test_call_tool_plain_text_error_raises_internal(client, mock_session):
    mock_session.call_tool.return_value = make_tool_result(
        "something crashed", is_error=True
    )
    with pytest.raises(McpToolError) as exc_info:
        await client.call_tool("some_tool", {})
    assert exc_info.value.mcp_error.category == McpErrorCategory.INTERNAL_ERROR
    assert exc_info.value.routing == RoutingDecision.RETRY


async def test_session_required_to_call_tool():
    client = McpClient(command="nostrdesk-mcp", args=[], startup_timeout_secs=5)
    assert client._session is None
    with pytest.raises(RuntimeError, match="not connected"):
        await client.call_tool("some_tool", {})
