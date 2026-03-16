"""CLI entry point using Click."""

from __future__ import annotations

import sys

import click
from rich.console import Console
from rich.traceback import install as install_rich_traceback

from mysql_transfer.config import TransferConfig, apply_cli_overrides, load_config

# Pretty tracebacks
install_rich_traceback(show_locals=False)
console = Console()


# ---------------------------------------------------------------------------
# Shared CLI options
# ---------------------------------------------------------------------------

def common_options(fn):
    """Decorator that adds shared options to a command."""
    opts = [
        click.option("-c", "--config", "config_file", type=click.Path(), default=None, help="Path to YAML config file."),
        # Source
        click.option("--source-host", default=None, help="Source MySQL host."),
        click.option("--source-port", default=None, type=int, help="Source MySQL port."),
        click.option("--source-user", default=None, help="Source MySQL user."),
        click.option("--source-password", default=None, help="Source MySQL password."),
        click.option("--source-db", default=None, help="Source database name."),
        # Source SSH
        click.option("--source-ssh-host", default=None, help="Source SSH tunnel host."),
        click.option("--source-ssh-port", default=None, type=int, help="Source SSH tunnel port."),
        click.option("--source-ssh-user", default=None, help="Source SSH tunnel user."),
        click.option("--source-ssh-password", default=None, help="Source SSH tunnel password."),
        click.option("--source-ssh-key", default=None, type=click.Path(), help="Source SSH private key file."),
        # Dest
        click.option("--dest-host", default=None, help="Destination MySQL host."),
        click.option("--dest-port", default=None, type=int, help="Destination MySQL port."),
        click.option("--dest-user", default=None, help="Destination MySQL user."),
        click.option("--dest-password", default=None, help="Destination MySQL password."),
        click.option("--dest-db", default=None, help="Destination database name."),
        # Dest SSH
        click.option("--dest-ssh-host", default=None, help="Dest SSH tunnel host."),
        click.option("--dest-ssh-port", default=None, type=int, help="Dest SSH tunnel port."),
        click.option("--dest-ssh-user", default=None, help="Dest SSH tunnel user."),
        click.option("--dest-ssh-password", default=None, help="Dest SSH tunnel password."),
        click.option("--dest-ssh-key", default=None, type=click.Path(), help="Dest SSH private key file."),
    ]
    for opt in reversed(opts):
        fn = opt(fn)
    return fn


def transfer_options(fn):
    """Decorator for transfer-specific options."""
    opts = [
        click.option("-s", "--select", is_flag=True, default=False, help="Interactively select tables to transfer."),
        click.option("--tables", default=None, help="Comma-separated table names (default: all)."),
        click.option("--exclude-tables", default=None, help="Comma-separated tables to exclude."),
        click.option("--chunk-size", default=None, type=int, help="Rows per batch (default: 10000)."),
        click.option("--workers", default=None, type=int, help="Parallel workers (default: 4)."),
        click.option("--drop-existing", is_flag=True, default=None, help="Drop & recreate destination tables."),
        click.option("--truncate", is_flag=True, default=None, help="Truncate tables before insert."),
        click.option("--incremental", is_flag=True, default=None, help="Enable incremental/delta sync."),
        click.option("--incremental-column", default=None, help="Column for incremental sync."),
        click.option("--include-views", is_flag=True, default=None, help="Transfer views."),
        click.option("--include-routines", is_flag=True, default=None, help="Transfer stored procedures & functions."),
        click.option("--include-triggers", is_flag=True, default=None, help="Transfer triggers."),
        click.option("--dry-run", is_flag=True, default=None, help="Preview without executing."),
    ]
    for opt in reversed(opts):
        fn = opt(fn)
    return fn


def _build_config(config_file, **kwargs) -> TransferConfig:
    """Load config from file and apply CLI overrides."""
    # Pop --select before passing to config (it's not a config field)
    kwargs.pop("select", None)

    try:
        cfg = load_config(config_file)
    except FileNotFoundError as e:
        console.print(f"[red]Error:[/red] {e}")
        sys.exit(1)

    cfg = apply_cli_overrides(cfg, **kwargs)

    errors = cfg.validate()
    if errors:
        console.print("[red]Configuration errors:[/red]")
        for err in errors:
            console.print(f"  • {err}")
        sys.exit(1)

    return cfg


def _interactive_table_select(cfg: TransferConfig) -> list[str]:
    """Connect to source and let user interactively pick tables."""
    from InquirerPy import inquirer
    from mysql_transfer.connection import managed_connection
    from mysql_transfer.schema import get_tables

    console.print(f"[bold]Connecting to source [cyan]{cfg.source.host}[/cyan] / [cyan]{cfg.source.database}[/cyan]...[/bold]")
    console.print(f"[bold]Destination: [cyan]{cfg.dest.host}[/cyan] / [cyan]{cfg.dest.database}[/cyan][/bold]\n")
    with managed_connection(cfg.source) as conn:
        tables = get_tables(conn)

    if not tables:
        console.print("[red]No tables found in source database.[/red]")
        sys.exit(1)

    choices = []
    for t in tables:
        rows = t.get("row_count") or 0
        size = t.get("size_mb") or 0.0
        label = f"{t['table_name']}  ({rows:,} rows, {size:.2f} MB)"
        choices.append({"name": label, "value": t["table_name"]})

    selected = inquirer.fuzzy(
        message=f"Select tables to transfer ({len(tables)} available):",
        choices=choices,
        multiselect=True,
        cycle=True,
        instruction="(Type to search, ↑↓ move, Tab select, Enter confirm, Ctrl+A toggle all)",
    ).execute()

    if not selected:
        console.print("[yellow]No tables selected. Aborting.[/yellow]")
        sys.exit(0)

    console.print(f"  Selected [green]{len(selected)}[/green] table(s).\n")
    return selected


# ---------------------------------------------------------------------------
# CLI Group
# ---------------------------------------------------------------------------

@click.group()
@click.version_option(package_name="mysql-transfer")
def cli():
    """MySQL Transfer — Transfer MySQL databases between servers.

    Use a YAML config file (-c config.yaml) or pass connection options directly.
    Supports SSH tunnels, chunked streaming, parallel transfers, and incremental sync.
    """
    pass


# ---------------------------------------------------------------------------
# transfer
# ---------------------------------------------------------------------------

@cli.command()
@common_options
@transfer_options
def transfer(config_file, **kwargs):
    """Transfer schema and data from source to destination."""
    select = kwargs.get("select", False)
    cfg = _build_config(config_file, **kwargs)

    if select:
        cfg.tables = _interactive_table_select(cfg)

    from mysql_transfer.transfer import run_transfer
    run_transfer(cfg, schema=True, data=True)


# ---------------------------------------------------------------------------
# schema
# ---------------------------------------------------------------------------

@cli.command()
@common_options
@transfer_options
def schema(config_file, **kwargs):
    """Transfer schema only (tables, views, routines, triggers)."""
    select = kwargs.get("select", False)
    cfg = _build_config(config_file, **kwargs)

    if select:
        cfg.tables = _interactive_table_select(cfg)

    from mysql_transfer.transfer import run_transfer
    run_transfer(cfg, schema=True, data=False)


# ---------------------------------------------------------------------------
# data
# ---------------------------------------------------------------------------

@cli.command()
@common_options
@transfer_options
def data(config_file, **kwargs):
    """Transfer data only (assumes schema exists at destination)."""
    select = kwargs.get("select", False)
    cfg = _build_config(config_file, **kwargs)

    if select:
        cfg.tables = _interactive_table_select(cfg)

    from mysql_transfer.transfer import run_transfer
    run_transfer(cfg, schema=False, data=True)


# ---------------------------------------------------------------------------
# inspect
# ---------------------------------------------------------------------------

@cli.command()
@common_options
def inspect(config_file, **kwargs):
    """Inspect the source database (tables, views, routines, triggers)."""
    # For inspect, we only need source config
    cfg = _build_config(config_file, **kwargs)

    from mysql_transfer.transfer import run_inspect
    run_inspect(cfg)


# ---------------------------------------------------------------------------
# diff
# ---------------------------------------------------------------------------

@cli.command()
@common_options
def diff(config_file, **kwargs):
    """Compare source and destination schemas."""
    cfg = _build_config(config_file, **kwargs)

    from mysql_transfer.transfer import run_diff
    run_diff(cfg)


if __name__ == "__main__":
    cli()
