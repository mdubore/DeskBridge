import asyncio
import json
import re
import uuid
import structlog

from deskbridge.db.store import Store
from deskbridge.dm.intent import Intent, parse
from deskbridge.mcp.client import McpClient, McpToolError
from deskbridge.mcp.errors import RoutingDecision
from deskbridge.mcp.session import SessionBroker

log = structlog.get_logger()

_MENTION_RE_TEMPLATE = r"(?:nostr:|@){npub}"


class GroupWatcher:
    def __init__(
        self,
        identity_label: str,
        identity_npub: str,
        operator_npub: str | None,
        group_ids: list[str],
        store: Store,
        client: McpClient,
        broker: SessionBroker,
        shutdown_event: asyncio.Event,
        poll_timeout_secs: int = 30,
    ) -> None:
        self._identity_label = identity_label
        self._account_id = f"acc-{identity_label}"
        self._identity_npub = identity_npub
        self._operator_npub = operator_npub
        self._group_ids = group_ids
        self._store = store
        self._client = client
        self._broker = broker
        self._shutdown_event = shutdown_event
        self._poll_timeout_secs = poll_timeout_secs
        self._mention_re = re.compile(
            _MENTION_RE_TEMPLATE.format(npub=re.escape(identity_npub)), re.I
        )

    async def run(self) -> None:
        cursor_row = await self._store.get_cursor(
            cursor_type="group_watcher", identity_id=self._account_id
        )
        after_message_id: str | None = cursor_row["last_entity_id"] if cursor_row else None
        after_created_at: str | None = cursor_row["last_created_at"] if cursor_row else None

        log.info("group_watcher_started", identity=self._identity_label, groups=self._group_ids)

        while not self._shutdown_event.is_set():
            session_id = await self._broker.get_session_id(self._identity_label)
            if session_id is None:
                log.debug("group_watcher_no_session", identity=self._identity_label)
                try:
                    await asyncio.wait_for(self._shutdown_event.wait(), timeout=5.0)
                except asyncio.TimeoutError:
                    pass
                continue

            try:
                result = await self._client.call_tool(
                    "wait_for_new_group_messages",
                    {
                        "session_id": session_id,
                        "group_ids": self._group_ids,
                        "after_message_id": after_message_id,
                        "after_created_at": after_created_at,
                        "timeout_seconds": self._poll_timeout_secs,
                    },
                )
                messages = result.get("messages", [])
                for msg in messages:
                    if (
                        self._operator_npub is None
                        or msg.get("sender_pubkey") != self._operator_npub
                    ):
                        log.debug(
                            "group_watcher_unauthorized",
                            identity=self._identity_label,
                            sender=msg.get("sender_pubkey"),
                        )
                        continue
                    content = msg.get("content", "")
                    if not self._mention_re.search(content):
                        log.debug(
                            "group_watcher_no_mention",
                            identity=self._identity_label,
                            group_id=msg.get("group_id"),
                        )
                        continue
                    stripped = self._mention_re.sub("", content).strip()
                    intent = parse(stripped)
                    await self._dispatch(intent, msg)

                if messages:
                    new_cursor_id = result.get("last_message_id")
                    if new_cursor_id is None:
                        log.warning(
                            "group_watcher_missing_cursor_in_response",
                            identity=self._identity_label,
                        )
                    else:
                        after_message_id = new_cursor_id
                        after_created_at = result.get("last_created_at")
                        await self._store.upsert_cursor(
                            cursor_type="group_watcher",
                            identity_id=self._account_id,
                            last_entity_id=after_message_id,
                            last_created_at=after_created_at,
                            last_imported_at=None,
                            raw_json=json.dumps(result),
                        )

            except McpToolError as e:
                if e.routing == RoutingDecision.REJECT:
                    log.error(
                        "group_watcher_rejected",
                        identity=self._identity_label,
                        message=e.mcp_error.message,
                    )
                    return
                elif e.routing == RoutingDecision.REAUTH:
                    log.warning("group_watcher_reauth", identity=self._identity_label)
                else:
                    log.error(
                        "group_watcher_error",
                        identity=self._identity_label,
                        routing=e.routing,
                        message=e.mcp_error.message,
                    )
                try:
                    await asyncio.wait_for(self._shutdown_event.wait(), timeout=5.0)
                except asyncio.TimeoutError:
                    pass

            except Exception:
                log.exception("group_watcher_unexpected_error", identity=self._identity_label)
                try:
                    await asyncio.wait_for(self._shutdown_event.wait(), timeout=5.0)
                except asyncio.TimeoutError:
                    pass

        log.info("group_watcher_stopped", identity=self._identity_label)

    async def _dispatch(self, intent: Intent, msg: dict) -> None:
        handlers = {
            Intent.TASK: self._handle_task,
            Intent.STATUS: self._handle_status,
            Intent.CANCEL: self._handle_cancel,
            Intent.APPROVE: self._handle_approve,
            Intent.REJECT: self._handle_reject,
        }
        await handlers[intent](msg)

    async def _handle_task(self, msg: dict) -> None:
        try:
            stripped = self._mention_re.sub("", msg["content"]).strip()
            await self._store.upsert_work_item(
                id=str(uuid.uuid4()),
                source_type="group",
                source_id=msg["id"],
                identity_id=self._account_id,
                summary=stripped[:200],
                payload_json=json.dumps(msg),
                idempotency_key=msg["id"],
            )
        except Exception:
            log.exception("group_watcher_handle_task_error", identity=self._identity_label)

    async def _handle_status(self, msg: dict) -> None:
        try:
            row = await self._store.get_latest_work_item(self._account_id)
            if row is None:
                reply = "No tasks found."
            else:
                reply = f"Task [{row['summary']}] is {row['status']}."
            await self._store.insert_outbox_item(
                str(uuid.uuid4()),
                self._account_id,
                None,
                reply,
                f"status-reply-{msg['id']}",
                dest_group_id=msg["group_id"],
            )
        except Exception:
            log.exception("group_watcher_handle_status_error", identity=self._identity_label)

    async def _handle_cancel(self, msg: dict) -> None:
        try:
            row = await self._store.get_latest_dispatched_work_item(self._account_id)
            if row is None:
                reply = "No active task to cancel."
            else:
                await self._store.mark_work_item_cancel_requested(row["id"])
                reply = f"Cancel requested for task [{row['summary']}]."
            await self._store.insert_outbox_item(
                str(uuid.uuid4()),
                self._account_id,
                None,
                reply,
                f"cancel-reply-{msg['id']}",
                dest_group_id=msg["group_id"],
            )
        except Exception:
            log.exception("group_watcher_handle_cancel_error", identity=self._identity_label)

    async def _handle_approve(self, msg: dict) -> None:
        try:
            row = await self._store.get_pending_approval(self._account_id)
            if row is None:
                reply = "No pending approval to approve."
            else:
                await self._store.resolve_approval(row["id"], "approved")
                reply = "Approved."
            await self._store.insert_outbox_item(
                str(uuid.uuid4()),
                self._account_id,
                None,
                reply,
                f"approve-reply-{msg['id']}",
                dest_group_id=msg["group_id"],
            )
        except Exception:
            log.exception("group_watcher_handle_approve_error", identity=self._identity_label)

    async def _handle_reject(self, msg: dict) -> None:
        try:
            row = await self._store.get_pending_approval(self._account_id)
            if row is None:
                reply = "No pending approval to reject."
            else:
                await self._store.resolve_approval(row["id"], "rejected")
                reply = "Rejected."
            await self._store.insert_outbox_item(
                str(uuid.uuid4()),
                self._account_id,
                None,
                reply,
                f"reject-reply-{msg['id']}",
                dest_group_id=msg["group_id"],
            )
        except Exception:
            log.exception("group_watcher_handle_reject_error", identity=self._identity_label)
