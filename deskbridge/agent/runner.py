import asyncio
import collections
import structlog
from datetime import datetime, timezone
from uuid import uuid4

from deskbridge.config import ProjectConfig
from deskbridge.db.store import Store
from deskbridge.mcp.client import McpClient
from deskbridge.mcp.session import SessionBroker

log = structlog.get_logger()

_ADAPTER_CLI: dict[str, str] = {
    "claude-code": "claude",
    "codex": "codex",
}


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class AgentRunner:
    def __init__(
        self,
        work_item,
        project: ProjectConfig,
        run_id: str,
        store: Store,
        client: McpClient,
        broker: SessionBroker,
        timeout_secs: float = 600.0,
        heartbeat_interval_secs: float = 30.0,
    ) -> None:
        self._work_item = work_item
        self._project = project
        self._run_id = run_id
        self._store = store
        self._client = client
        self._broker = broker
        self._timeout_secs = timeout_secs
        self._heartbeat_interval_secs = heartbeat_interval_secs

    async def run(self) -> None:
        try:
            await self._do_run()
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("agent_runner_unexpected_error", run_id=self._run_id)
            try:
                await self._store.complete_work_item(self._work_item["id"], "failed")
            except Exception:
                log.exception("agent_runner_complete_failed_on_error", run_id=self._run_id)
            if self._project.escalation_dm_target:
                try:
                    await self._store.insert_outbox_item(
                        id=str(uuid4()),
                        identity_id=f"acc-{self._project.identity}",
                        dest_pubkey=self._project.escalation_dm_target,
                        message_text="Agent run failed with unexpected error",
                        idempotency_key=f"deskbridge:{self._run_id}:result_notify",
                    )
                except Exception:
                    log.exception("agent_runner_outbox_failed_on_error", run_id=self._run_id)

    async def _do_run(self) -> None:
        work_item = self._work_item
        project = self._project
        run_id = self._run_id

        # 1. Record the run
        await self._store.upsert_agent_run(
            id=run_id,
            work_item_id=work_item["id"],
            adapter_type=project.agents[0],
        )

        # 2. Build CLI command
        adapter = project.agents[0]
        prompt = f"{work_item['summary']}\n\n{work_item['payload_json']}"[:4000]
        if adapter == "claude-code":
            cmd = ["claude", "--project", project.repo_path, "--message", prompt]
        else:
            cmd = ["codex", "--dir", project.repo_path, prompt]

        # 3. Spawn subprocess
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
            cwd=project.repo_path,
        )

        # 4. Start concurrent tasks
        output_buf: collections.deque = collections.deque(maxlen=200)

        async def _drain() -> None:
            async for line in proc.stdout:
                output_buf.append(line.decode(errors="replace").rstrip())

        async def _heartbeat() -> None:
            while True:
                await asyncio.sleep(self._heartbeat_interval_secs)
                await self._store.update_agent_run(run_id, heartbeat_at=_now_iso())

        drain_task = asyncio.create_task(_drain())
        heartbeat_task = asyncio.create_task(_heartbeat())

        # 5. Wait for process with timeout / cancellation handling
        final_status = "failed"
        try:
            await asyncio.wait_for(proc.wait(), timeout=self._timeout_secs)
            final_status = "done" if proc.returncode == 0 else "failed"
        except asyncio.TimeoutError:
            proc.terminate()
            try:
                await asyncio.wait_for(proc.wait(), timeout=10)
            except asyncio.TimeoutError:
                proc.kill()
            final_status = "interrupted"
        except asyncio.CancelledError:
            proc.terminate()
            try:
                await asyncio.wait_for(proc.wait(), timeout=10)
            except asyncio.TimeoutError:
                proc.kill()
            final_status = "interrupted"
            raise  # propagate — steps 6-9 are skipped
        finally:
            heartbeat_task.cancel()
            await asyncio.gather(heartbeat_task, drain_task, return_exceptions=True)

        # 6. Record final run result
        result_text = "\n".join(output_buf)[-2000:]
        await self._store.update_agent_run(run_id, status=final_status, result_summary=result_text)

        # 7. Update work item
        await self._store.complete_work_item(work_item["id"], status=final_status)

        # 8. Notify operator via DM (best-effort, only if target is set)
        if project.escalation_dm_target:
            await self._store.insert_outbox_item(
                id=str(uuid4()),
                identity_id=f"acc-{project.identity}",
                dest_pubkey=project.escalation_dm_target,
                message_text=result_text,
                idempotency_key=f"deskbridge:{run_id}:result_notify",
            )

        # 9. Update kanban board card (best-effort, only for kanban-sourced items)
        if work_item["source_type"] == "kanban" and work_item["source_id"] is not None:
            session_id = await self._broker.get_session_id(project.identity)
            if session_id is None:
                log.warning("agent_runner_no_session_for_board_update", run_id=run_id)
            else:
                try:
                    await self._client.call_tool(
                        "update_board_card",
                        {
                            "session_id": session_id,
                            "card_id": work_item["source_id"],
                            "description": "\n".join(output_buf)[-500:],
                            "idempotency_key": f"deskbridge:{run_id}:board_update",
                        },
                    )
                except Exception:
                    log.exception("agent_runner_board_update_failed", run_id=run_id)
