import asyncio
import json
import structlog
from uuid import uuid4

from deskbridge.config import IdentityConfig
from deskbridge.db.store import Store
from deskbridge.mcp.client import McpClient, McpToolError
from deskbridge.mcp.errors import RoutingDecision
from deskbridge.mcp.session import SessionBroker

log = structlog.get_logger()


class OutboxDrainer:
    def __init__(
        self,
        store: Store,
        client: McpClient,
        broker: SessionBroker,
        identities: list[IdentityConfig],
        shutdown_event: asyncio.Event,
        drain_interval_secs: float = 5.0,
        max_attempts: int = 3,
    ) -> None:
        self._store = store
        self._client = client
        self._broker = broker
        self._account_to_label = {f"acc-{i.label}": i.label for i in identities}
        self._shutdown_event = shutdown_event
        self._drain_interval_secs = drain_interval_secs
        self._max_attempts = max_attempts

    async def run(self) -> None:
        log.info("outbox_drainer_started")

        while not self._shutdown_event.is_set():
            rows = await self._store.get_pending_outbox_items(max_attempts=self._max_attempts)
            for row in rows:
                await self._drain_row(row)
            try:
                await asyncio.wait_for(
                    self._shutdown_event.wait(), timeout=self._drain_interval_secs
                )
            except asyncio.TimeoutError:
                pass

        log.info("outbox_drainer_stopped")

    async def _drain_row(self, row) -> None:
        dest_pubkey = row["dest_pubkey"]
        dest_group_id = row["dest_group_id"]
        if not dest_pubkey and not dest_group_id:
            log.debug("outbox_drainer_skip_no_destination", id=row["id"])
            return

        label = self._account_to_label.get(row["identity_id"])
        if label is None:
            log.error(
                "outbox_drainer_unknown_identity",
                id=row["id"],
                identity_id=row["identity_id"],
            )
            return

        session_id = await self._broker.get_session_id(label)
        if session_id is None:
            log.debug("outbox_drainer_no_session", id=row["id"], identity=label)
            return

        if dest_pubkey:
            tool_name = "send_dm"
            arguments = {
                "session_id": session_id,
                "recipient_pubkey": dest_pubkey,
                "content": row["message_text"],
                "idempotency_key": row["idempotency_key"],
            }
        else:
            tool_name = "send_encrypted_message"
            arguments = {
                "session_id": session_id,
                "group_id": dest_group_id,
                "content": row["message_text"],
                "idempotency_key": row["idempotency_key"],
            }

        try:
            result = await self._client.call_tool(tool_name, arguments)
            await self._store.update_outbox_delivery(
                row["id"], "delivered", json.dumps(result)
            )
            log.info("outbox_drainer_delivered", id=row["id"])
            try:
                await self._store.log_audit(
                    id=str(uuid4()),
                    event_type="outbox_delivered",
                    identity_id=row["identity_id"],
                    payload_json=json.dumps({
                        "outbox_id": row["id"],
                        "dest_pubkey": dest_pubkey,
                        "dest_group_id": dest_group_id,
                    }),
                )
            except Exception:
                log.warning("audit_log_failed", event_type="outbox_delivered")

        except McpToolError as e:
            is_permanent = (
                e.routing == RoutingDecision.REJECT
                or row["delivery_attempts"] + 1 >= self._max_attempts
            )
            final_status = "failed" if is_permanent else "pending"
            error_json = json.dumps(
                {"error": e.mcp_error.message, "routing": str(e.routing)}
            )
            await self._store.update_outbox_delivery(row["id"], final_status, error_json)
            if is_permanent:
                try:
                    await self._store.log_audit(
                        id=str(uuid4()),
                        event_type="outbox_delivery_failed",
                        identity_id=row["identity_id"],
                        payload_json=json.dumps({
                            "outbox_id": row["id"],
                            "attempts": row["delivery_attempts"] + 1,
                        }),
                    )
                except Exception:
                    log.warning("audit_log_failed", event_type="outbox_delivery_failed")
            log.error(
                "outbox_drainer_send_failed",
                id=row["id"],
                status=final_status,
                routing=e.routing,
            )

        except Exception:
            log.exception("outbox_drainer_unexpected_error", id=row["id"])
            await self._store.update_outbox_delivery(
                row["id"], "pending", json.dumps({"error": "unexpected_error"})
            )
