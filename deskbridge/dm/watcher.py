import asyncio
import json
import uuid
import structlog

from deskbridge.db.store import Store
from deskbridge.mcp.client import McpClient, McpToolError
from deskbridge.mcp.errors import RoutingDecision
from deskbridge.mcp.session import SessionBroker

log = structlog.get_logger()


class DmWatcher:
    def __init__(
        self,
        identity_label: str,
        store: Store,
        client: McpClient,
        broker: SessionBroker,
        shutdown_event: asyncio.Event,
        poll_timeout_secs: int = 30,
    ) -> None:
        self._identity_label = identity_label
        self._account_id = f"acc-{identity_label}"
        self._store = store
        self._client = client
        self._broker = broker
        self._shutdown_event = shutdown_event
        self._poll_timeout_secs = poll_timeout_secs

    async def run(self) -> None:
        cursor_row = await self._store.get_cursor(
            cursor_type="dm_watcher", identity_id=self._account_id
        )
        after_message_id: str | None = cursor_row["last_entity_id"] if cursor_row else None
        after_created_at: str | None = cursor_row["last_created_at"] if cursor_row else None

        log.info("dm_watcher_started", identity=self._identity_label)

        while not self._shutdown_event.is_set():
            session_id = await self._broker.get_session_id(self._identity_label)
            if session_id is None:
                log.debug("dm_watcher_no_session", identity=self._identity_label)
                try:
                    await asyncio.wait_for(self._shutdown_event.wait(), timeout=5.0)
                except asyncio.TimeoutError:
                    pass
                continue

            try:
                result = await self._client.call_tool(
                    "wait_for_new_dms",
                    {
                        "session_id": session_id,
                        "after_message_id": after_message_id,
                        "after_created_at": after_created_at,
                        "timeout_seconds": self._poll_timeout_secs,
                    },
                )
                messages = result.get("messages", [])
                for msg in messages:
                    await self._store.upsert_work_item(
                        id=str(uuid.uuid4()),
                        source_type="dm",
                        source_id=msg["id"],
                        identity_id=self._account_id,
                        summary=msg["content"][:200],
                        payload_json=json.dumps(msg),
                        idempotency_key=msg["id"],
                    )
                if messages and result.get("last_message_id"):
                    after_message_id = result["last_message_id"]
                    after_created_at = result.get("last_created_at")
                    await self._store.upsert_cursor(
                        cursor_type="dm_watcher",
                        identity_id=self._account_id,
                        last_entity_id=after_message_id,
                        last_created_at=after_created_at,
                        last_imported_at=None,
                        raw_json=json.dumps(result),
                    )

            except McpToolError as e:
                if e.routing == RoutingDecision.REJECT:
                    log.error(
                        "dm_watcher_rejected",
                        identity=self._identity_label,
                        message=e.mcp_error.message,
                    )
                    return
                if e.routing == RoutingDecision.REAUTH:
                    log.warning("dm_watcher_reauth", identity=self._identity_label)
                else:
                    log.error(
                        "dm_watcher_error",
                        identity=self._identity_label,
                        routing=e.routing,
                        message=e.mcp_error.message,
                    )
                try:
                    await asyncio.wait_for(self._shutdown_event.wait(), timeout=5.0)
                except asyncio.TimeoutError:
                    pass

            except Exception:
                log.exception("dm_watcher_unexpected_error", identity=self._identity_label)
                try:
                    await asyncio.wait_for(self._shutdown_event.wait(), timeout=5.0)
                except asyncio.TimeoutError:
                    pass

        log.info("dm_watcher_stopped", identity=self._identity_label)
