import asyncio
import signal
import threading
import structlog
from pathlib import Path

import aiosqlite

from deskbridge.config import DeskBridgeConfig
from deskbridge.db.schema import apply_schema
from deskbridge.db.store import Store, bootstrap_accounts_from_config
from deskbridge.dm.watcher import DmWatcher
from deskbridge.dm.group_watcher import GroupWatcher
from deskbridge.dm.approval_watcher import ApprovalRequestWatcher
from deskbridge.dm.outbox import OutboxDrainer
from deskbridge.agent.poller import WorkItemPoller
from deskbridge.mcp import McpClient, SessionBroker

log = structlog.get_logger()


class Supervisor:
    def __init__(self, config: DeskBridgeConfig) -> None:
        self._config = config
        self._shutdown_event = asyncio.Event()

    def request_shutdown(self) -> None:
        log.info("shutdown_requested")
        self._shutdown_event.set()

    async def run(self) -> None:
        db_path = Path(self._config.supervisor.db_path).expanduser()
        db_path.parent.mkdir(parents=True, exist_ok=True)

        async with aiosqlite.connect(db_path) as conn:
            conn.row_factory = aiosqlite.Row
            await apply_schema(conn)
            store = Store(conn)
            await bootstrap_accounts_from_config(store=store, config=self._config)

            mcp_cfg = self._config.mcp
            client = McpClient(
                command=mcp_cfg.command,
                args=mcp_cfg.args,
                startup_timeout_secs=mcp_cfg.startup_timeout_secs,
            )

            async with client.connect():
                broker = SessionBroker(
                    store=store,
                    client=client,
                    identities=self._config.identities,
                )

                is_main = threading.current_thread() is threading.main_thread()
                if is_main:
                    loop = asyncio.get_running_loop()
                    for sig in (signal.SIGTERM, signal.SIGINT):
                        loop.add_signal_handler(sig, self.request_shutdown)

                watcher_tasks: list[asyncio.Task] = []
                group_watcher_tasks: list[asyncio.Task] = []
                approval_watcher_tasks: list[asyncio.Task] = []
                drainer_task: asyncio.Task | None = None
                poller_tasks: list[asyncio.Task] = []

                try:
                    await broker.unlock_all()
                    log.info("supervisor_started")

                    watcher_tasks = [
                        asyncio.create_task(
                            DmWatcher(
                                identity_label=identity.label,
                                store=store,
                                client=client,
                                broker=broker,
                                shutdown_event=self._shutdown_event,
                                operator_npub=identity.operator_npub,
                            ).run(),
                            name=f"dm_watcher_{identity.label}",
                        )
                        for identity in self._config.identities
                    ]
                    approval_watcher_tasks = [
                        asyncio.create_task(
                            ApprovalRequestWatcher(
                                identity_label=identity.label,
                                operator_npub=identity.operator_npub,
                                store=store,
                                client=client,
                                broker=broker,
                                shutdown_event=self._shutdown_event,
                            ).run(),
                            name=f"approval_watcher_{identity.label}",
                        )
                        for identity in self._config.identities
                    ]
                    drainer_task = asyncio.create_task(
                        OutboxDrainer(
                            store=store,
                            client=client,
                            broker=broker,
                            identities=self._config.identities,
                            shutdown_event=self._shutdown_event,
                        ).run(),
                        name="outbox_drainer",
                    )
                    poller_tasks = [
                        asyncio.create_task(
                            WorkItemPoller(
                                identity_label=identity.label,
                                store=store,
                                client=client,
                                broker=broker,
                                config=self._config,
                                shutdown_event=self._shutdown_event,
                            ).run(),
                            name=f"work_item_poller_{identity.label}",
                        )
                        for identity in self._config.identities
                    ]
                    for identity in self._config.identities:
                        groups = await store.get_project_groups(f"acc-{identity.label}")
                        if groups:
                            group_watcher_tasks.append(
                                asyncio.create_task(
                                    GroupWatcher(
                                        identity_label=identity.label,
                                        identity_npub=identity.npub,
                                        operator_npub=identity.operator_npub,
                                        group_ids=groups,
                                        store=store,
                                        client=client,
                                        broker=broker,
                                        shutdown_event=self._shutdown_event,
                                    ).run(),
                                    name=f"group_watcher_{identity.label}",
                                )
                            )

                    interval = self._config.supervisor.heartbeat_interval_secs
                    while not self._shutdown_event.is_set():
                        await broker.refresh_if_needed()
                        try:
                            await asyncio.wait_for(
                                self._shutdown_event.wait(),
                                timeout=float(interval),
                            )
                        except asyncio.TimeoutError:
                            pass

                    log.info("supervisor_stopped")
                finally:
                    tasks_to_cancel = (
                        watcher_tasks
                        + group_watcher_tasks
                        + approval_watcher_tasks
                        + poller_tasks
                        + ([drainer_task] if drainer_task is not None else [])
                    )
                    for task in tasks_to_cancel:
                        task.cancel()
                    if tasks_to_cancel:
                        await asyncio.gather(*tasks_to_cancel, return_exceptions=True)
                    if is_main:
                        for sig in (signal.SIGTERM, signal.SIGINT):
                            loop.remove_signal_handler(sig)
