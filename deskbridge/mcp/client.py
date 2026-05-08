import asyncio
import json
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any

from mcp import ClientSession
from mcp.client.stdio import stdio_client, StdioServerParameters

from deskbridge.mcp.errors import RoutingDecision, route_mcp_error
from deskbridge.models import McpError, McpErrorCategory


class McpToolError(Exception):
    def __init__(
        self,
        mcp_error: McpError,
        routing: RoutingDecision,
        category: str | None = None,
        data: dict | None = None,
    ) -> None:
        super().__init__(f"[{mcp_error.category}] {mcp_error.message}")
        self.mcp_error = mcp_error
        self.routing = routing
        self.category = category
        self.data = data


class McpClient:
    def __init__(
        self,
        command: str,
        args: list[str],
        startup_timeout_secs: int,
    ) -> None:
        self._command = command
        self._args = args
        self._startup_timeout_secs = startup_timeout_secs
        self._session: ClientSession | None = None

    @asynccontextmanager
    async def connect(self) -> AsyncGenerator["McpClient", None]:
        server_params = StdioServerParameters(
            command=self._command,
            args=self._args,
        )
        async with stdio_client(server_params) as (read, write):
            async with ClientSession(read, write) as session:
                await asyncio.wait_for(
                    session.initialize(),
                    timeout=float(self._startup_timeout_secs),
                )
                self._session = session
                try:
                    yield self
                finally:
                    self._session = None

    async def call_tool(self, tool_name: str, arguments: dict[str, Any]) -> Any:
        if self._session is None:
            raise RuntimeError("McpClient is not connected — call within connect() context")

        try:
            result = await self._session.call_tool(tool_name, arguments)
        except Exception as e:
            sdk_data = getattr(e, "data", None)
            if isinstance(sdk_data, dict):
                raw_cat = sdk_data.get("category")
                cat_str = raw_cat if isinstance(raw_cat, str) else "internal_error"
                try:
                    cat_enum = McpErrorCategory(cat_str)
                except (ValueError, TypeError):
                    cat_enum = McpErrorCategory.INTERNAL_ERROR
                mcp_error = McpError(
                    category=cat_enum,
                    raw_category=cat_str,
                    message=str(e),
                    data=sdk_data,
                    approval_request_id=sdk_data.get("approval_request_id"),
                )
                routing = route_mcp_error(mcp_error)
                raise McpToolError(
                    mcp_error=mcp_error,
                    routing=routing,
                    category=cat_str,
                    data=sdk_data,
                ) from e
            raise

        text = result.content[0].text if result.content else ""

        if result.isError:
            mcp_error = McpError.from_tool_result_text(
                text or "MCP tool returned error with no content"
            )
            routing = route_mcp_error(mcp_error)
            raise McpToolError(
                mcp_error=mcp_error,
                routing=routing,
                category=mcp_error.data.get("category") if mcp_error.data else None,
                data=mcp_error.data,
            )

        try:
            return json.loads(text)
        except json.JSONDecodeError:
            return text
