import asyncio
import json
import uuid
import structlog

from deskbridge.db.store import Store
from deskbridge.mcp.client import McpClient, McpToolError
from deskbridge.mcp.errors import RoutingDecision
from deskbridge.mcp.session import SessionBroker

log = structlog.get_logger()


class ApprovalRequestWatcher:
    def __init__(
        self,
        identity_label: str,
        operator_npub: str | None,
        store: Store,
        client: McpClient,
        broker: SessionBroker,
        shutdown_event: asyncio.Event,
        poll_timeout_secs: int = 30,
    ) -> None:
        self._identity_label = identity_label
        self._account_id = f"acc-{identity_label}"
        self._operator_npub = operator_npub
        self._store = store
        self._client = client
        self._broker = broker
        self._shutdown_event = shutdown_event
        self._poll_timeout_secs = poll_timeout_secs

    async def run(self) -> None:
        cursor_row = await self._store.get_cursor(
            cursor_type="approval_watcher", identity_id=self._account_id
        )
        after_request_id: str | None = cursor_row["last_entity_id"] if cursor_row else None

        log.info("approval_watcher_started", identity=self._identity_label)

        while not self._shutdown_event.is_set():
            session_id = await self._broker.get_session_id(self._identity_label)
            if session_id is None:
                log.debug("approval_watcher_no_session", identity=self._identity_label)
                try:
                    await asyncio.wait_for(self._shutdown_event.wait(), timeout=5.0)
                except asyncio.TimeoutError:
                    pass
                continue

            try:
                result = await self._client.call_tool(
                    "wait_for_pending_approval_requests",
                    {
                        "session_id": session_id,
                        "after_request_id": after_request_id,
                        "timeout_seconds": self._poll_timeout_secs,
                    },
                )
                requests = result.get("requests", [])
                for req in requests:
                    dispatched = await self._store.get_latest_dispatched_work_item(
                        self._account_id
                    )
                    work_item_id = dispatched["id"] if dispatched else None
                    await self._store.insert_approval(
                        id=str(uuid.uuid4()),
                        mcp_approval_id=req["id"],
                        work_item_id=work_item_id,
                        action_description=req["description"],
                        scope=None,
                        request_text=None,
                        expires_at=None,
                    )
                    if self._operator_npub:
                        message = (
                            f"Approval required: {req['description']}\n\n"
                            f"Request ID: {req['id']}\n\n"
                            f"Reply 'approve' or 'reject'."
                        )
                        await self._store.insert_outbox_item(
                            str(uuid.uuid4()),
                            self._account_id,
                            self._operator_npub,
                            message,
                            f"approval-notify-{req['id']}",
                        )

                if requests:
                    new_cursor_id = result.get("last_request_id")
                    if new_cursor_id:
                        after_request_id = new_cursor_id
                        await self._store.upsert_cursor(
                            cursor_type="approval_watcher",
                            identity_id=self._account_id,
                            last_entity_id=after_request_id,
                            last_created_at=None,
                            last_imported_at=None,
                            raw_json=json.dumps(result),
                        )

            except McpToolError as e:
                if e.routing == RoutingDecision.REJECT:
                    log.error(
                        "approval_watcher_rejected",
                        identity=self._identity_label,
                        message=e.mcp_error.message,
                    )
                    return
                elif e.routing == RoutingDecision.REAUTH:
                    log.warning("approval_watcher_reauth", identity=self._identity_label)
                elif e.routing == RoutingDecision.RESET_CURSOR:
                    log.warning(
                        "approval_watcher_reset_cursor",
                        identity=self._identity_label,
                        message=e.mcp_error.message,
                    )
                    after_request_id = None
                else:
                    log.error(
                        "approval_watcher_error",
                        identity=self._identity_label,
                        routing=e.routing,
                        message=e.mcp_error.message,
                    )
                try:
                    await asyncio.wait_for(self._shutdown_event.wait(), timeout=5.0)
                except asyncio.TimeoutError:
                    pass

            except Exception:
                log.exception("approval_watcher_unexpected_error", identity=self._identity_label)
                try:
                    await asyncio.wait_for(self._shutdown_event.wait(), timeout=5.0)
                except asyncio.TimeoutError:
                    pass

        log.info("approval_watcher_stopped", identity=self._identity_label)
