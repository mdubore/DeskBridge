import asyncio
import concurrent.futures
from pathlib import Path

import click
import aiosqlite

from deskbridge.config import load_config, ConfigError
from deskbridge.supervisor import Supervisor


DEFAULT_CONFIG = Path.home() / ".deskbridge" / "config.toml"


@click.group()
def cli():
    pass


@cli.command()
@click.option(
    "--config",
    "config_path",
    type=click.Path(),
    default=str(DEFAULT_CONFIG),
    show_default=True,
    help="Path to config TOML file",
)
def start(config_path: str):
    """Start the DeskBridge supervisor daemon (foreground)."""
    try:
        config = load_config(Path(config_path))
    except ConfigError as e:
        click.echo(f"Config error: {e}", err=True)
        raise SystemExit(1)

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
@click.option(
    "--config",
    "config_path",
    type=click.Path(),
    default=str(DEFAULT_CONFIG),
    show_default=True,
    help="Path to config TOML file",
)
def status(config_path: str):
    """Show current session health from the local SQLite database."""
    try:
        config = load_config(Path(config_path))
    except ConfigError as e:
        click.echo(f"Config error: {e}", err=True)
        raise SystemExit(1)

    db_path = Path(config.supervisor.db_path).expanduser()
    if not db_path.exists():
        click.echo("No database found — DeskBridge has not been started yet.")
        return

    # Run in a dedicated thread so asyncio.run() always gets a fresh event loop.
    # This avoids RuntimeError when the command is invoked from within an already-
    # running loop (e.g. during tests).
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(asyncio.run, _show_status(db_path))
        future.result()
