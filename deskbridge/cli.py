import asyncio
import concurrent.futures
import json
from pathlib import Path
from uuid import uuid4

import click
import aiosqlite
import structlog

from deskbridge.config import load_config, ConfigError, DeskBridgeConfig
from deskbridge.db.schema import apply_schema
from deskbridge.db.store import Store
from deskbridge.supervisor import Supervisor

log = structlog.get_logger()

DEFAULT_CONFIG = Path.home() / ".deskbridge" / "config.toml"

_CONFIG_OPTION = click.option(
    "--config",
    "config_path",
    type=click.Path(),
    default=str(DEFAULT_CONFIG),
    show_default=True,
    help="Path to config TOML file",
)


def _load_config_or_exit(config_path: str) -> DeskBridgeConfig:
    try:
        return load_config(Path(config_path))
    except ConfigError as e:
        click.echo(f"Config error: {e}", err=True)
        raise SystemExit(1)


def _require_db(config_path: str) -> Path:
    config = _load_config_or_exit(config_path)
    db_path = Path(config.supervisor.db_path).expanduser()
    if not db_path.exists():
        click.echo("No database found — DeskBridge has not been started yet.", err=True)
        raise SystemExit(1)
    return db_path


def _run_async(coro):
    # Run in a dedicated thread so asyncio.run() always gets a fresh event loop.
    # This avoids RuntimeError when the command is invoked from within an already-
    # running loop (e.g. during tests).
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        return pool.submit(asyncio.run, coro).result()


async def _log_audit_safe(
    store: Store,
    event_type: str,
    *,
    identity_id: str | None = None,
    work_item_id: str | None = None,
    payload: dict,
) -> None:
    try:
        await store.log_audit(
            id=str(uuid4()),
            event_type=event_type,
            identity_id=identity_id,
            work_item_id=work_item_id,
            payload_json=json.dumps(payload),
        )
    except Exception:
        log.warning("audit_log_failed", event_type=event_type)


@click.group()
def cli():
    pass


@cli.command()
@_CONFIG_OPTION
def start(config_path: str):
    """Start the DeskBridge supervisor daemon (foreground)."""
    config = _load_config_or_exit(config_path)

    click.echo(f"Starting DeskBridge (config: {config_path})")

    supervisor = Supervisor(config=config)
    try:
        asyncio.run(supervisor.run())
    except KeyboardInterrupt:
        pass
    click.echo("DeskBridge stopped.")


async def _show_status(db_path: Path) -> None:
    async with aiosqlite.connect(db_path) as conn:
        conn.row_factory = aiosqlite.Row

        # Accounts
        async with conn.execute(
            "SELECT label, health, session_id, last_unlocked_at FROM accounts"
        ) as cursor:
            accounts = await cursor.fetchall()
        click.echo("Accounts")
        if not accounts:
            click.echo("  (none)")
        else:
            for row in accounts:
                click.echo(
                    f"  [{row['health']}] {row['label']}  "
                    f"session={row['session_id'] or 'none'}  "
                    f"last_unlocked={row['last_unlocked_at'] or 'never'}"
                )

        # Work Queue
        async with conn.execute(
            "SELECT status, COUNT(*) AS n FROM work_items GROUP BY status"
        ) as cursor:
            counts = {r["status"]: r["n"] for r in await cursor.fetchall()}
        click.echo(
            f"\nWork Queue\n"
            f"  pending={counts.get('pending', 0)}"
            f"  dispatched={counts.get('dispatched', 0)}"
            f"  done={counts.get('done', 0)}"
            f"  failed={counts.get('failed', 0)}"
            f"  cancelled={counts.get('cancelled', 0)}"
        )

        # Approvals
        async with conn.execute(
            "SELECT COUNT(*) AS n FROM approvals WHERE status = 'pending'"
        ) as cursor:
            row = await cursor.fetchone()
        click.echo(f"\nApprovals\n  pending={row['n'] if row else 0}")

        # Recent Runs
        async with conn.execute(
            """
            SELECT ar.status, ar.adapter_type, ar.updated_at, wi.summary
            FROM agent_runs ar
            LEFT JOIN work_items wi ON wi.id = ar.work_item_id
            ORDER BY ar.updated_at DESC
            LIMIT 5
            """
        ) as cursor:
            runs = await cursor.fetchall()
        click.echo("\nRecent Runs (last 5)")
        if not runs:
            click.echo("  (none)")
        else:
            for run in runs:
                summary = (run["summary"] or "")[:40]
                click.echo(
                    f"  {run['status']:<12} {run['adapter_type']:<14}"
                    f" {run['updated_at']}  \"{summary}\""
                )

        # Watchers
        async with conn.execute(
            "SELECT cursor_type, identity_id, updated_at FROM cursors ORDER BY cursor_type"
        ) as cursor:
            cursors = await cursor.fetchall()
        click.echo("\nWatchers (last cursor update)")
        if not cursors:
            click.echo("  (none)")
        else:
            for c in cursors:
                click.echo(
                    f"  {c['cursor_type']:<12} {c['identity_id']:<20} {c['updated_at']}"
                )


@cli.command()
@_CONFIG_OPTION
def status(config_path: str):
    """Show current session health from the local SQLite database."""
    config = _load_config_or_exit(config_path)

    db_path = Path(config.supervisor.db_path).expanduser()
    if not db_path.exists():
        click.echo("No database found — DeskBridge has not been started yet.")
        return

    _run_async(_show_status(db_path))


async def _cancel_work_item(db_path: Path, work_item_id: str) -> tuple[bool, str]:
    async with aiosqlite.connect(db_path) as conn:
        conn.row_factory = aiosqlite.Row
        await apply_schema(conn)
        store = Store(conn)
        row = await store.get_work_item(work_item_id)
        if row is None:
            return False, f"Work item {work_item_id} not found."
        status = row["status"]
        if status == "pending":
            if not await store.cancel_pending_work_item(work_item_id):
                return False, (
                    f"Work item {work_item_id} is no longer pending — "
                    "run 'deskbridge status' and try again."
                )
            await _log_audit_safe(
                store, "work_item_terminal",
                identity_id=row["identity_id"], work_item_id=work_item_id,
                payload={"status": "cancelled", "via": "cli"},
            )
            return True, f"Work item {work_item_id} cancelled."
        if status == "dispatched":
            if not await store.mark_work_item_cancel_requested(work_item_id):
                return False, (
                    f"Work item {work_item_id} is no longer running — "
                    "run 'deskbridge status' and try again."
                )
            await _log_audit_safe(
                store, "work_item_cancel_requested",
                identity_id=row["identity_id"], work_item_id=work_item_id,
                payload={"via": "cli"},
            )
            return True, (
                f"Cancel requested for running work item {work_item_id} — "
                "the supervisor will stop the agent shortly."
            )
        if status == "cancel_requested":
            return False, f"Work item {work_item_id} already has a pending cancel request."
        return False, f"Work item {work_item_id} is {status} — nothing to cancel."


async def _retry_work_item(db_path: Path, work_item_id: str) -> tuple[bool, str]:
    async with aiosqlite.connect(db_path) as conn:
        conn.row_factory = aiosqlite.Row
        await apply_schema(conn)
        store = Store(conn)
        row = await store.get_work_item(work_item_id)
        if row is None:
            return False, f"Work item {work_item_id} not found."
        if row["status"] not in ("failed", "cancelled", "interrupted"):
            return False, (
                f"Work item {work_item_id} is {row['status']} — only failed, "
                "cancelled, or interrupted items can be retried."
            )
        if not await store.reset_work_item_for_retry(work_item_id):
            return False, (
                f"Work item {work_item_id} changed state — "
                "run 'deskbridge status' and try again."
            )
        await _log_audit_safe(
            store, "work_item_retry_queued",
            identity_id=row["identity_id"], work_item_id=work_item_id,
            payload={"via": "cli", "attempt_count": 0},
        )
        return True, f"Work item {work_item_id} re-queued (attempt counter reset)."


@cli.command()
@click.argument("work_item_id")
@_CONFIG_OPTION
def cancel(work_item_id: str, config_path: str):
    """Cancel a pending or running work item."""
    db_path = _require_db(config_path)
    ok, message = _run_async(_cancel_work_item(db_path, work_item_id))
    click.echo(message, err=not ok)
    if not ok:
        raise SystemExit(1)


@cli.command()
@click.argument("work_item_id")
@_CONFIG_OPTION
def retry(work_item_id: str, config_path: str):
    """Re-queue a failed, cancelled, or interrupted work item (resets attempt count)."""
    db_path = _require_db(config_path)
    ok, message = _run_async(_retry_work_item(db_path, work_item_id))
    click.echo(message, err=not ok)
    if not ok:
        raise SystemExit(1)


async def _request_approval_decision(
    db_path: Path, approval_id: str, approved: bool
) -> tuple[bool, str]:
    async with aiosqlite.connect(db_path) as conn:
        conn.row_factory = aiosqlite.Row
        await apply_schema(conn)
        store = Store(conn)
        row = await store.get_approval(approval_id)
        if row is None:
            return False, f"Approval {approval_id} not found."
        if row["status"] != "pending":
            return False, (
                f"Approval {approval_id} is {row['status']} — "
                "only pending approvals can be decided."
            )
        decision = "approve_requested" if approved else "reject_requested"
        if not await store.request_approval_decision(approval_id, decision):
            return False, (
                f"Approval {approval_id} changed state — "
                "run 'deskbridge status' and try again."
            )
        await _log_audit_safe(
            store, "approval_decision_requested",
            identity_id=row["identity_id"], work_item_id=row["work_item_id"],
            payload={
                "approval_id": approval_id,
                "decision": "approved" if approved else "rejected",
                "via": "cli",
            },
        )
        verb = "Approval" if approved else "Rejection"
        return True, (
            f"{verb} queued for {approval_id} — "
            "the supervisor will forward the decision to MCP."
        )


@cli.command()
@click.argument("approval_id")
@_CONFIG_OPTION
def approve(approval_id: str, config_path: str):
    """Approve a pending approval request (the supervisor forwards it to MCP)."""
    db_path = _require_db(config_path)
    ok, message = _run_async(_request_approval_decision(db_path, approval_id, approved=True))
    click.echo(message, err=not ok)
    if not ok:
        raise SystemExit(1)


@cli.command()
@click.argument("approval_id")
@_CONFIG_OPTION
def reject(approval_id: str, config_path: str):
    """Reject a pending approval request (the supervisor forwards it to MCP)."""
    db_path = _require_db(config_path)
    ok, message = _run_async(_request_approval_decision(db_path, approval_id, approved=False))
    click.echo(message, err=not ok)
    if not ok:
        raise SystemExit(1)
