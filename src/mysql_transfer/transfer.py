"""Main transfer orchestration with parallel execution."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

from mysql_transfer.config import TransferConfig
from mysql_transfer.connection import (
    create_connection,
    managed_connection,
    managed_tunnel,
)
from mysql_transfer.data import get_row_count, transfer_table_data
from mysql_transfer.progress import ProgressManager, console
from mysql_transfer.schema import resolve_tables, transfer_schema


def run_transfer(cfg: TransferConfig, *, schema: bool = True, data: bool = True):
    """Run the full transfer pipeline."""
    progress = ProgressManager(dry_run=cfg.dry_run)

    if cfg.dry_run:
        console.print("[bold yellow]━━━ DRY RUN MODE ━━━[/bold yellow]\n")

    # Open SSH tunnels once and share across all connections
    with managed_tunnel(cfg.source) as src_tunnel, managed_tunnel(cfg.dest) as dst_tunnel:

        # --- Connect to source and resolve tables ---
        console.print("[bold]Connecting to source...[/bold]")
        source_conn = create_connection(cfg.source, tunnel=src_tunnel)
        try:
            tables = resolve_tables(source_conn, cfg)
        finally:
            source_conn.close()

        if not tables:
            console.print("[red]No tables found matching the filter.[/red]")
            return

        console.print(
            f"  Found [green]{len(tables)}[/green] table(s) to transfer.\n"
        )

        # --- Schema transfer ---
        if schema:
            console.print("[bold]Transferring schema...[/bold]")
            source_conn = create_connection(cfg.source, tunnel=src_tunnel)
            dest_conn = create_connection(cfg.dest, tunnel=dst_tunnel)
            try:
                transfer_schema(source_conn, dest_conn, cfg, progress, tables)
            finally:
                source_conn.close()
                dest_conn.close()
            console.print()

        # --- Data transfer ---
        if data:
            console.print("[bold]Transferring data...[/bold]")

            # Get row counts first (for progress bars)
            table_names = [t["table_name"] for t in tables]
            row_counts: dict[str, int] = {}
            console.print("  Counting rows...")
            source_conn = create_connection(cfg.source, tunnel=src_tunnel)
            try:
                for i, name in enumerate(table_names, 1):
                    try:
                        count = get_row_count(source_conn, name)
                        row_counts[name] = count
                        console.print(f"    [{i}/{len(table_names)}] {name}: [green]{count:,}[/green] rows")
                    except Exception:
                        row_counts[name] = 0
                        console.print(f"    [{i}/{len(table_names)}] {name}: [yellow]unknown[/yellow]")
            finally:
                source_conn.close()
            console.print()

            # Start progress display
            progress.start()

            # Create progress tasks
            task_ids: dict[str, int] = {}
            for name in table_names:
                task_ids[name] = progress.add_table_task(name, row_counts.get(name, 0))

            # Transfer tables in parallel
            results: list[dict[str, Any]] = []

            def _transfer_one(table_name: str) -> dict[str, Any]:
                return transfer_table_data(
                    source_cfg=cfg.source,
                    dest_cfg=cfg.dest,
                    table=table_name,
                    cfg=cfg,
                    progress=progress,
                    task_id=task_ids[table_name],
                    source_tunnel=src_tunnel,
                    dest_tunnel=dst_tunnel,
                )

            with ThreadPoolExecutor(max_workers=cfg.workers) as pool:
                futures = {
                    pool.submit(_transfer_one, name): name
                    for name in table_names
                }
                for future in as_completed(futures):
                    name = futures[future]
                    try:
                        stats = future.result()
                    except Exception as e:
                        stats = {
                            "table": name,
                            "rows_transferred": 0,
                            "chunks": 0,
                            "error": str(e),
                        }
                    progress.complete_task(task_ids[name])
                    progress.record_stats(stats)
                    results.append(stats)

            progress.stop()
            progress.print_summary()


def run_inspect(cfg: TransferConfig):
    """Inspect the source database."""
    from mysql_transfer.progress import print_inspection_table
    from mysql_transfer.schema import get_routines, get_tables, get_triggers, get_views

    console.print("[bold]Connecting to source...[/bold]")
    with managed_connection(cfg.source) as conn:
        tables = get_tables(conn)
        print_inspection_table(tables)

        views = get_views(conn)
        if views:
            console.print(f"  [bold]Views:[/bold] {', '.join(views)}")

        routines = get_routines(conn)
        if routines:
            procs = [r["name"] for r in routines if r["type"] == "PROCEDURE"]
            funcs = [r["name"] for r in routines if r["type"] == "FUNCTION"]
            if procs:
                console.print(f"  [bold]Procedures:[/bold] {', '.join(procs)}")
            if funcs:
                console.print(f"  [bold]Functions:[/bold] {', '.join(funcs)}")

        triggers = get_triggers(conn)
        if triggers:
            console.print(f"  [bold]Triggers:[/bold] {', '.join(triggers)}")

        console.print()


def run_diff(cfg: TransferConfig):
    """Compare source and destination schemas."""
    from mysql_transfer.progress import print_diff_table
    from mysql_transfer.schema import get_tables

    console.print("[bold]Comparing source and destination...[/bold]")

    with managed_connection(cfg.source) as src_conn:
        src_tables = {t["table_name"] for t in get_tables(src_conn)}

    with managed_connection(cfg.dest) as dst_conn:
        dst_tables = {t["table_name"] for t in get_tables(dst_conn)}

    print_diff_table(src_tables, dst_tables)
