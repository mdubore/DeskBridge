import asyncio
import json
import time
from datetime import datetime, timezone
from uuid import uuid4

import structlog

from deskbridge.db.store import Store
from deskbridge.mcp.client import McpClient
from deskbridge.mcp.session import SessionBroker

log = structlog.get_logger()


class ScheduledCheckInWatcher:
    def __init__(
        self,
        identity_label: str,
        identity_id: str,
        operator_npub: str,
        interval_hours: float,
        prompt: str,
        store: Store,
        client: McpClient,
        broker: SessionBroker,
        shutdown_event: asyncio.Event,
    ) -> None:
        self._identity_label = identity_label
        self._identity_id = identity_id
        self._operator_npub = operator_npub
        self._interval_secs = interval_hours * 3600
        self._prompt = prompt
        self._store = store
        self._client = client
        self._broker = broker
        self._shutdown_event = shutdown_event

    async def run(self) -> None:
        log.info("checkin_watcher_started", identity=self._identity_label)
        try:
            while not self._shutdown_event.is_set():
                current_time = time.time()
                current_bucket = int(current_time // self._interval_secs)
                # Fire immediately for the current bucket — no initial sleep.
                # The idempotency key prevents duplicate work items if the daemon
                # restarts mid-interval. A restart near a bucket boundary may
                # trigger two check-ins in quick succession (one per bucket); this
                # is intentional and visible to the operator via two DMs.
                try:
                    await self._check_and_queue(current_bucket)
                except Exception:
                    log.warning(
                        "checkin_watcher_error",
                        identity=self._identity_label,
                        exc_info=True,
                    )
                next_bucket_time = (current_bucket + 1) * self._interval_secs
                sleep_duration = max(0.0, next_bucket_time - time.time())
                log.debug(
                    "checkin_sleeping",
                    next_checkin_utc=datetime.fromtimestamp(
                        next_bucket_time, tz=timezone.utc
                    ).strftime("%Y-%m-%dT%H:%M:%SZ"),
                    sleep_secs=round(sleep_duration, 1),
                )
                try:
                    await asyncio.wait_for(
                        self._shutdown_event.wait(), timeout=sleep_duration
                    )
                except asyncio.TimeoutError:
                    pass
        finally:
            log.info("checkin_watcher_stopped", identity=self._identity_label)

    async def _check_and_queue(self, current_bucket: int) -> None:
        active = await self._store.get_latest_dispatched_work_item(self._identity_id)
        if active is not None:
            log.info("checkin_skipped_agent_busy", identity=self._identity_label)
            return

        inserted = await self._store.upsert_work_item(
            id=str(uuid4()),
            source_type="scheduled",
            source_id="scheduled",
            identity_id=self._identity_id,
            summary=self._prompt[:200],
            payload_json=json.dumps(
                {"prompt": self._prompt, "operator_npub": self._operator_npub}
            ),
            idempotency_key=f"checkin-{self._identity_id}-{current_bucket}",
        )
        if inserted:
            log.info("checkin_work_item_created", identity=self._identity_label)
        else:
            log.info("checkin_already_queued", identity=self._identity_label)
