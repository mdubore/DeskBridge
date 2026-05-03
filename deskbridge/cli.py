import asyncio
import sys
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
        sys.exit(1)

    click.echo(f"Starting DeskBridge (config: {config_path})")

    supervisor = Supervisor(config=config)
    try:
        asyncio.run(supervisor.run())
    except KeyboardInterrupt:
        pass
    click.echo("DeskBridge stopped.")


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
        sys.exit(1)

    db_path = Path(config.supervisor.db_path).expanduser()
    if not db_path.exists():
        click.echo("No database found — DeskBridge has not been started yet.")
        return

    async def _show_status():
        async with aiosqlite.connect(db_path) as conn:
            conn.row_factory = aiosqlite.Row
            async with conn.execute(
                "SELECT label, health, session_id, last_unlocked_at FROM accounts"
            ) as cursor:
                rows = await cursor.fetchall()
            if not rows:
                click.echo("No accounts found in database.")
                return
            for row in rows:
                click.echo(
                    f"  [{row['health']}] {row['label']}  "
                    f"session={row['session_id'] or 'none'}  "
                    f"last_unlocked={row['last_unlocked_at'] or 'never'}"
                )

    asyncio.run(_show_status())
