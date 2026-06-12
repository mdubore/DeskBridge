import asyncio
import json
import uuid
import structlog

from deskbridge.db.store import Store
from deskbridge.dm.approval_resolution import resolve_approval_via_mcp
from deskbridge.mcp.client import McpClient
from deskbridge.mcp.session import SessionBroker

log = structlog.get_logger()


class ApprovalDecisionPoller:
    """Forwards operator decisions queued by the CLI (approve_requested /
    reject_requested approval rows) to MCP. The CLI itself never talks to MCP."""

    def __init__(
        self,
        identity_label: str,
        store: Store,
        client: McpClient,
        broker: SessionBroker,
        shutdown_event: asyncio.Event,
        poll_interval_secs: float = 2.0,
    ) -> None:
        self._identity_label = identity_label
        self._account_id = f"acc-{identity_label}"
        self._store = store
        self._client = client
        self._broker = broker
        self._shutdown_event = shutdown_event
        self._poll_interval_secs = poll_interval_secs

    async def run(self) -> None:
        log.info("approval_decision_poller_started", identity=self._identity_label)
        while not self._shutdown_event.is_set():
            try:
                await self._poll_once()
            except Exception:
                log.exception(
                    "approval_decision_poller_unexpected_error",
                    identity=self._identity_label,
                )
            await self._sleep()
        log.info("approval_decision_poller_stopped", identity=self._identity_label)

    async def _poll_once(self) -> None:
        rows = await self._store.get_requested_approval_decisions(self._account_id)
        for row in rows:
            approved = row["status"] == "approve_requested"
            local_status = "approved" if approved else "rejected"
            mcp_approval_id = row["mcp_approval_id"]

            if mcp_approval_id:
                # Session is looked up per row (not per cycle): local-only rows
                # need no session, and MCP-correlated rows need one at the
                # moment of forwarding.
                session_id = await self._broker.get_session_id(self._identity_label)
                if session_id is None:
                    log.debug(
                        "approval_decision_poller_no_session",
                        identity=self._identity_label,
                        approval_id=row["id"],
                    )
                    continue  # retry next cycle once a session exists
                resolved, _reply = await resolve_approval_via_mcp(
                    store=self._store,
                    client=self._client,
                    identity_label=self._identity_label,
                    account_id=self._account_id,
                    row=row,
                    mcp_approval_id=mcp_approval_id,
                    session_id=session_id,
                    approved=approved,
                )
                if not resolved:
                    # Row stays in *_requested status; retried next cycle.
                    log.warning(
                        "approval_decision_poller_forward_failed",
                        identity=self._identity_label,
                        approval_id=row["id"],
                    )
            else:
                await self._store.resolve_approval(row["id"], local_status)
                try:
                    await self._store.log_audit(
                        id=str(uuid.uuid4()),
                        event_type="approval_resolved",
                        identity_id=self._account_id,
                        work_item_id=row["work_item_id"],
                        payload_json=json.dumps({
                            "approval_id": row["id"],
                            "resolution": local_status,
                            "via": "cli",
                        }),
                    )
                except Exception:
                    log.warning("audit_log_failed", event_type="approval_resolved")
                log.info(
                    "approval_decision_poller_resolved_locally",
                    identity=self._identity_label,
                    approval_id=row["id"],
                    resolution=local_status,
                )

    async def _sleep(self) -> None:
        try:
            await asyncio.wait_for(
                self._shutdown_event.wait(), timeout=self._poll_interval_secs
            )
        except asyncio.TimeoutError:
            pass
