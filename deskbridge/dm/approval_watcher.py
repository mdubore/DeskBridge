import asyncio
import json
import uuid
import structlog
from datetime import datetime, timezone

from deskbridge.db.store import Store
from deskbridge.mcp.client import McpClient, McpToolError
from deskbridge.mcp.errors import RoutingDecision
from deskbridge.mcp.session import SessionBroker

log = structlog.get_logger()


class ApprovalRequestWatcher:
    def __init__(
        self,
        identity_label: str,
        store: Store,
        client: McpClient,
        broker: SessionBroker,
        shutdown_event: asyncio.Event,
        operator_npub: str | None = None,
        poll_interval_secs: float = 2.0,
    ) -> None:
        self._identity_label = identity_label
        self._account_id = f"acc-{identity_label}"
        self._operator_npub = operator_npub
        self._store = store
        self._client = client
        self._broker = broker
        self._shutdown_event = shutdown_event
        self._poll_interval_secs = poll_interval_secs

    async def run(self) -> None:
        log.info("approval_watcher_started", identity=self._identity_label)

        while not self._shutdown_event.is_set():
            session_id = await self._broker.get_session_id(self._identity_label)
            if session_id is None:
                log.debug("approval_watcher_no_session", identity=self._identity_label)
                await self._sleep()
                continue

            try:
                result = await self._client.call_tool(
                    "list_pending_approvals",
                    {"session_id": session_id, "limit": 100},
                )
                if isinstance(result, list):
                    requests = result
                else:
                    log.warning(
                        "approval_watcher_unexpected_response_shape",
                        identity=self._identity_label,
                        result_type=type(result).__name__,
                    )
                    requests = []

                # One agent runs at a time per identity, so all approvals in this batch
                # belong to the latest dispatched work item.
                work_item_id = None
                if requests:
                    dispatched = await self._store.get_latest_dispatched_work_item(
                        self._account_id
                    )
                    work_item_id = dispatched["id"] if dispatched else None

                for req in requests:
                    if not isinstance(req, dict):
                        log.warning(
                            "approval_watcher_non_dict_row",
                            identity=self._identity_label,
                            row_type=type(req).__name__,
                        )
                        continue
                    await self._process_approval(req, work_item_id)

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
                else:
                    log.error(
                        "approval_watcher_error",
                        identity=self._identity_label,
                        routing=e.routing,
                        message=e.mcp_error.message,
                    )

            except Exception:
                log.exception("approval_watcher_unexpected_error", identity=self._identity_label)

            await self._sleep()

        log.info("approval_watcher_stopped", identity=self._identity_label)

    async def _process_approval(self, req: dict, work_item_id: str | None) -> None:
        req_id = req.get("id")
        if not req_id:
            log.warning(
                "approval_watcher_malformed_row",
                identity=self._identity_label,
                keys=list(req.keys()),
            )
            return

        raw_tool_name = req.get("tool_name")
        tool_name = raw_tool_name if isinstance(raw_tool_name, str) and raw_tool_name else "unknown tool"

        expires_at_raw = req.get("expires_at")
        expires_at_iso: str | None = None
        if isinstance(expires_at_raw, (int, float)):
            try:
                expires_at_iso = datetime.fromtimestamp(
                    expires_at_raw, tz=timezone.utc
                ).strftime("%Y-%m-%dT%H:%M:%SZ")
            except (OSError, OverflowError, ValueError):
                log.warning(
                    "approval_watcher_invalid_expires_at",
                    identity=self._identity_label,
                    req_id=req_id,
                    expires_at_raw=expires_at_raw,
                )

        display_raw = req.get("display_payload_json")
        raw_request_payload = req.get("request_payload_json")
        if raw_request_payload is None:
            request_payload = None
        elif isinstance(raw_request_payload, str):
            request_payload = raw_request_payload
        else:
            request_payload = json.dumps(raw_request_payload, default=str)
            log.warning(
                "approval_watcher_non_string_request_payload",
                identity=self._identity_label,
                req_id=req_id,
                payload_type=type(raw_request_payload).__name__,
            )

        if display_raw is None:
            dm_display = "(details unavailable)"
        elif not isinstance(display_raw, str):
            dm_display = json.dumps({"raw_display_payload": str(display_raw)})
            log.warning(
                "approval_watcher_invalid_display_payload",
                identity=self._identity_label,
                req_id=req_id,
            )
        else:
            try:
                json.loads(display_raw)
                dm_display = display_raw
            except (json.JSONDecodeError, ValueError):
                dm_display = json.dumps({"raw_display_payload": display_raw})
                log.warning(
                    "approval_watcher_invalid_display_payload",
                    identity=self._identity_label,
                    req_id=req_id,
                )

        action_description = f"{tool_name}: {dm_display}"

        await self._store.insert_approval(
            id=str(uuid.uuid4()),
            mcp_approval_id=req_id,
            work_item_id=work_item_id,
            action_description=action_description,
            scope=None,
            request_text=request_payload,
            expires_at=expires_at_iso,
            identity_id=self._account_id,
        )

        if self._operator_npub:
            message = (
                f"Approval required: {tool_name}\n\n"
                f"{dm_display}\n\n"
                f"Reference ID: {req_id}\n\n"
                f"Reply 'approve' or 'reject' for the latest pending approval."
            )
            await self._store.insert_outbox_item(
                str(uuid.uuid4()),
                self._account_id,
                self._operator_npub,
                message,
                f"approval-notify-{req_id}",
            )

    async def _sleep(self) -> None:
        try:
            await asyncio.wait_for(
                self._shutdown_event.wait(), timeout=self._poll_interval_secs
            )
        except asyncio.TimeoutError:
            pass
