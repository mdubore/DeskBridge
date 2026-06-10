import asyncio
import json
import structlog
from datetime import datetime, timezone, timedelta
from uuid import uuid4

from deskbridge.agent.runner import AgentRunner
from deskbridge.config import DeskBridgeConfig, ProjectConfig
from deskbridge.db.store import Store
from deskbridge.mcp.client import McpClient
from deskbridge.mcp.session import SessionBroker

log = structlog.get_logger()


def _iso_offset(seconds: int) -> str:
    return (datetime.now(timezone.utc) + timedelta(seconds=seconds)).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )


class WorkItemPoller:
    def __init__(
        self,
        identity_label: str,
        store: Store,
        client: McpClient,
        broker: SessionBroker,
        config: DeskBridgeConfig,
        shutdown_event: asyncio.Event,
        poll_interval_secs: float = 10.0,
        kanban_column_in_progress: str = "in_progress",
        kanban_column_done: str = "done",
    ) -> None:
        self._identity_label = identity_label
        self._account_id = f"acc-{identity_label}"
        self._store = store
        self._client = client
        self._broker = broker
        self._config = config
        self._shutdown_event = shutdown_event
        self._poll_interval_secs = poll_interval_secs
        self._kanban_column_in_progress = kanban_column_in_progress
        self._kanban_column_done = kanban_column_done
        self._active_run_task: asyncio.Task | None = None
        self._active_work_item_id: str | None = None
        self._active_project_cfg: ProjectConfig | None = None

    async def _sync_card_column(
        self, card_id: str, column: str, idempotency_key: str
    ) -> None:
        session_id = await self._broker.get_session_id(self._identity_label)
        if session_id is None:
            log.warning("kanban_sync_no_session", card_id=card_id, column=column)
            return
        try:
            await self._client.call_tool(
                "update_board_card",
                {
                    "session_id": session_id,
                    "card_id": card_id,
                    "column": column,
                    "idempotency_key": idempotency_key,
                },
            )
            log.info("kanban_card_column_updated", card_id=card_id, column=column)
        except Exception:
            log.warning("kanban_sync_failed", card_id=card_id, column=column, exc_info=True)

    async def run(self) -> None:
        log.info("work_item_poller_started", identity=self._identity_label)
        try:
            while not self._shutdown_event.is_set():
                await self._poll_once()
                try:
                    await asyncio.wait_for(
                        self._shutdown_event.wait(), timeout=self._poll_interval_secs
                    )
                except asyncio.TimeoutError:
                    pass
        finally:
            if self._active_run_task is not None and not self._active_run_task.done():
                self._active_run_task.cancel()
                await asyncio.gather(self._active_run_task, return_exceptions=True)
            log.info("work_item_poller_stopped", identity=self._identity_label)

    async def _poll_once(self) -> None:
        # Detect completed runner
        if self._active_run_task is not None and self._active_run_task.done():
            if self._active_work_item_id is not None:
                completed_item = await self._store.get_work_item(self._active_work_item_id)
                if completed_item is not None:
                    final_status = completed_item["status"]
                    retried = False

                    if final_status == "failed":
                        if (
                            self._active_project_cfg is not None
                            and completed_item["attempt_count"] + 1
                            < self._active_project_cfg.max_agent_attempts
                        ):
                            next_retry_at = _iso_offset(60)
                            await self._store.retry_work_item(
                                completed_item["id"], next_retry_at
                            )
                            retried = True
                            try:
                                await self._store.log_audit(
                                    id=str(uuid4()),
                                    event_type="work_item_retry_queued",
                                    work_item_id=completed_item["id"],
                                    payload_json=json.dumps({
                                        "attempt_count": completed_item["attempt_count"],
                                        "next_retry_at": next_retry_at,
                                    }),
                                )
                            except Exception:
                                log.warning(
                                    "audit_log_failed",
                                    event_type="work_item_retry_queued",
                                )
                        else:
                            try:
                                await self._store.log_audit(
                                    id=str(uuid4()),
                                    event_type="work_item_terminal",
                                    work_item_id=completed_item["id"],
                                    payload_json=json.dumps({
                                        "status": "failed",
                                        "attempt_count": completed_item["attempt_count"],
                                    }),
                                )
                            except Exception:
                                log.warning(
                                    "audit_log_failed", event_type="work_item_terminal"
                                )
                    elif final_status in ("done", "interrupted", "cancelled"):
                        try:
                            await self._store.log_audit(
                                id=str(uuid4()),
                                event_type="work_item_terminal",
                                work_item_id=completed_item["id"],
                                payload_json=json.dumps({
                                    "status": final_status,
                                    "attempt_count": completed_item["attempt_count"],
                                }),
                            )
                        except Exception:
                            log.warning(
                                "audit_log_failed",
                                event_type="work_item_terminal",
                                status=final_status,
                            )

                    if (
                        not retried
                        and final_status in ("done", "failed")
                        and completed_item["source_type"] == "kanban"
                    ):
                        await self._sync_card_column(
                            completed_item["source_id"],
                            column=self._kanban_column_done,
                            idempotency_key=f"deskbridge-{self._active_work_item_id}-done",
                        )
            self._active_run_task = None
            self._active_work_item_id = None
            self._active_project_cfg = None

        # Cancel runner if operator requested cancellation
        if (
            self._active_run_task is not None
            and not self._active_run_task.done()
            and self._active_work_item_id is not None
        ):
            row = await self._store.get_work_item(self._active_work_item_id)
            if row is not None and row["status"] == "cancel_requested":
                self._active_run_task.cancel()
                await asyncio.gather(self._active_run_task, return_exceptions=True)
                await self._store.complete_work_item(self._active_work_item_id, "cancelled")
                self._active_run_task = None
                self._active_work_item_id = None
                self._active_project_cfg = None
                return

        project = await self._store.get_project_for_identity(self._account_id)
        if project is None:
            log.warning("work_item_poller_no_project", identity=self._identity_label)
            return

        rows = await self._store.get_pending_work_items(self._account_id, limit=10)
        for row in rows:
            if self._active_run_task is not None and not self._active_run_task.done():
                break  # one-at-a-time: wait for current runner to finish

            claimed = await self._store.claim_work_item(row["id"])
            if not claimed:
                continue

            if row["source_type"] == "kanban":
                await self._sync_card_column(
                    row["source_id"],
                    column=self._kanban_column_in_progress,
                    idempotency_key=f"deskbridge-{row['id']}-in-progress",
                )

            project_cfg = next(
                (p for p in self._config.projects if p.id == project["id"]), None
            )
            if project_cfg is None:
                log.error(
                    "work_item_poller_no_project_config",
                    identity=self._identity_label,
                    project_id=project["id"],
                )
                try:
                    await self._store.complete_work_item(row["id"], "failed")
                except Exception:
                    log.exception(
                        "work_item_poller_complete_failed",
                        identity=self._identity_label,
                        work_item_id=row["id"],
                    )
                continue

            try:
                await self._store.log_audit(
                    id=str(uuid4()),
                    event_type="work_item_dispatched",
                    work_item_id=row["id"],
                    payload_json=json.dumps({
                        "adapter": project_cfg.adapter,
                        "attempt_count": row["attempt_count"],
                    }),
                )
            except Exception:
                log.warning(
                    "audit_log_failed",
                    event_type="work_item_dispatched",
                    work_item_id=row["id"],
                )

            runner = AgentRunner(
                work_item=row,
                project=project_cfg,
                run_id=str(uuid4()),
                store=self._store,
                client=self._client,
                broker=self._broker,
            )
            self._active_run_task = asyncio.create_task(
                runner.run(), name=f"agent_run_{row['id']}"
            )
            self._active_work_item_id = row["id"]
            self._active_project_cfg = project_cfg
            break
