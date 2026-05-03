import json
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any

from mcp import ClientSession
from mcp.client.stdio import stdio_client, StdioServerParameters

from deskbridge.mcp.errors import RoutingDecision, route_mcp_error
from deskbridge.models import McpError


class McpToolError(Exception):
    def __init__(self, mcp_error: McpError, routing: RoutingDecision) -> None:
        super().__init__(f"[{mcp_error.category}] {mcp_error.message}")
        self.mcp_error = mcp_error
        self.routing = routing


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
                await session.initialize()
                self._session = session
                try:
                    yield self
                finally:
                    self._session = None

    async def call_tool(self, tool_name: str, arguments: dict[str, Any]) -> Any:
        if self._session is None:
            raise RuntimeError("McpClient is not connected — call within connect() context")

        result = await self._session.call_tool(tool_name, arguments)

        text = result.content[0].text if result.content else ""

        if result.isError:
            mcp_error = McpError.from_tool_result_text(
                text or "MCP tool returned error with no content"
            )
            routing = route_mcp_error(mcp_error)
            raise McpToolError(mcp_error=mcp_error, routing=routing)

        try:
            return json.loads(text)
        except json.JSONDecodeError:
            return text
