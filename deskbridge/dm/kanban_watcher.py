import asyncio
import uuid
import structlog

from deskbridge.db.store import Store
from deskbridge.mcp.client import McpClient, McpToolError
from deskbridge.mcp.errors import RoutingDecision
from deskbridge.mcp.session import SessionBroker

log = structlog.get_logger()


class KanbanWatcher:
    def __init__(
        self,
        account_id: str,
        identity_label: str,
        store: Store,
        client: McpClient,
        broker: SessionBroker,
        shutdown_event: asyncio.Event,
        boards: list[str],
        operator_npub: str | None = None,
        poll_interval_secs: float = 30.0,
    ) -> None:
        self._account_id = account_id
        self._identity_label = identity_label
        self._store = store
        self._client = client
        self._broker = broker
        self._shutdown_event = shutdown_event
        self._boards = boards
        self._operator_npub = operator_npub
        self._poll_interval_secs = poll_interval_secs

    async def run(self) -> None:
        log.info("kanban_watcher_started", identity=self._identity_label)

        while not self._shutdown_event.is_set():
            session_id = await self._broker.get_session_id(self._identity_label)
            if session_id is None:
                log.debug("kanban_watcher_no_session", identity=self._identity_label)
                await self._sleep()
                continue

            for board_channel_id in self._boards:
                try:
                    cards = await self._client.call_tool(
                        "list_assigned_board_cards",
                        {
                            "session_id": session_id,
                            "channel_id": board_channel_id,
                            "limit": 50,
                        },
                    )
                    if not isinstance(cards, list):
                        log.warning(
                            "kanban_watcher_unexpected_response",
                            identity=self._identity_label,
                            board=board_channel_id,
                            result_type=type(cards).__name__,
                        )
                        continue
                    for card in cards:
                        await self._process_card(card)
                except McpToolError as e:
                    if e.routing == RoutingDecision.REJECT:
                        log.error(
                            "kanban_watcher_rejected",
                            identity=self._identity_label,
                            board=board_channel_id,
                        )
                        return
                    log.warning(
                        "kanban_watcher_poll_error",
                        identity=self._identity_label,
                        board=board_channel_id,
                    )
                except Exception:
                    log.exception(
                        "kanban_watcher_unexpected_error",
                        identity=self._identity_label,
                        board=board_channel_id,
                    )

            await self._sleep()

        log.info("kanban_watcher_stopped", identity=self._identity_label)

    async def _process_card(self, card: object) -> None:
        if not isinstance(card, dict):
            log.warning("kanban_watcher_non_dict_card", identity=self._identity_label)
            return

        card_id = card.get("id")
        if not card_id:
            log.warning("kanban_watcher_card_missing_id", identity=self._identity_label)
            return

        title = card.get("title") or "(untitled)"
        description = card.get("description") or ""

        idempotency_key = f"kanban-{card_id}"
        try:
            inserted = await self._store.upsert_work_item(
                id=str(uuid.uuid4()),
                source_type="kanban",
                source_id=card_id,
                identity_id=self._account_id,
                idempotency_key=idempotency_key,
                summary=title,
                payload_json=description,
            )
        except Exception:
            log.exception(
                "kanban_watcher_store_error",
                identity=self._identity_label,
                card_id=card_id,
            )
            return

        if inserted:
            log.info(
                "kanban_watcher_new_card",
                identity=self._identity_label,
                card_id=card_id,
            )
            if self._operator_npub:
                message = f"New task assigned: {title}\n\nCard ID: {card_id}"
                await self._store.insert_outbox_item(
                    str(uuid.uuid4()),
                    self._account_id,
                    self._operator_npub,
                    message,
                    f"kanban-notify-{card_id}",
                )

    async def _sleep(self) -> None:
        try:
            await asyncio.wait_for(
                self._shutdown_event.wait(), timeout=self._poll_interval_secs
            )
        except asyncio.TimeoutError:
            pass
