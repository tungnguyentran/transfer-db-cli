"""Rich progress display and console output."""

from __future__ import annotations

import time
from typing import Any

from rich.console import Console
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)
from rich.table import Table


console = Console()


class ProgressManager:
    """Manages Rich progress bars for the transfer."""

    def __init__(self, *, dry_run: bool = False):
        self.dry_run = dry_run
        self._progress: Progress | None = None
        self._tasks: dict[str, int] = {}
        self._start_time = time.time()
        self._stats: list[dict[str, Any]] = []

    def start(self) -> Progress:
        self._progress = Progress(
            SpinnerColumn(),
            TextColumn("[bold blue]{task.description}"),
            BarColumn(bar_width=40),
            MofNCompleteColumn(),
            TextColumn("•"),
            TimeElapsedColumn(),
            TextColumn("•"),
            TimeRemainingColumn(),
            console=console,
            transient=False,
        )
        self._progress.start()
        return self._progress

    def stop(self):
        if self._progress:
            self._progress.stop()

    def add_table_task(self, table_name: str, total_rows: int) -> int:
        """Add a progress task for a table. Returns the task ID."""
        if self._progress is None:
            raise RuntimeError("Progress not started")
        task_id = self._progress.add_task(
            f"  {table_name}",
            total=total_rows,
        )
        self._tasks[table_name] = task_id
        return task_id

    def advance(self, task_id: int, amount: int = 1):
        """Advance a task's progress."""
        if self._progress:
            self._progress.advance(task_id, amount)

    def complete_task(self, task_id: int):
        """Mark a task as complete."""
        if self._progress:
            task = self._progress.tasks[task_id]
            self._progress.update(task_id, completed=task.total)

    def log(self, message: str):
        """Log a message through rich console."""
        if self._progress:
            self._progress.console.print(message)
        else:
            console.print(message)

    def record_stats(self, stats: dict[str, Any]):
        """Record transfer stats for summary display."""
        self._stats.append(stats)

    def print_summary(self):
        """Print a summary table of all transfers."""
        elapsed = time.time() - self._start_time

        console.print()
        console.print("[bold green]━━━ Transfer Summary ━━━[/bold green]")
        console.print()

        if not self._stats:
            console.print("  No data transferred.")
            return

        table = Table(show_header=True, header_style="bold cyan")
        table.add_column("Table", style="white")
        table.add_column("Rows", justify="right", style="green")
        table.add_column("Chunks", justify="right", style="yellow")
        table.add_column("Status", style="white")

        total_rows = 0
        errors = 0
        for s in self._stats:
            status = "✓ OK" if s.get("error") is None else f"✗ {s['error']}"
            style = "" if s.get("error") is None else "red"
            table.add_row(
                s["table"],
                f"{s['rows_transferred']:,}",
                str(s.get("chunks", 0)),
                status,
                style=style,
            )
            total_rows += s["rows_transferred"]
            if s.get("error"):
                errors += 1

        console.print(table)
        console.print()
        console.print(
            f"  [bold]Total:[/bold] {total_rows:,} rows  •  "
            f"Time: {elapsed:.1f}s  •  "
            f"Errors: {errors}"
        )
        if total_rows > 0 and elapsed > 0:
            console.print(f"  [bold]Speed:[/bold] {total_rows / elapsed:,.0f} rows/sec")
        console.print()


def print_inspection_table(tables: list[dict[str, Any]]):
    """Print a formatted table of database info."""
    tbl = Table(
        title="Source Database Tables",
        show_header=True,
        header_style="bold cyan",
    )
    tbl.add_column("Table", style="white")
    tbl.add_column("Engine", style="yellow")
    tbl.add_column("Rows", justify="right", style="green")
    tbl.add_column("Size (MB)", justify="right", style="blue")
    tbl.add_column("Collation", style="dim")

    total_rows = 0
    total_size = 0.0
    for t in tables:
        rows = t.get("row_count") or 0
        size = t.get("size_mb") or 0.0
        tbl.add_row(
            t["table_name"],
            t.get("engine", ""),
            f"{rows:,}",
            f"{size:.2f}",
            t.get("collation", ""),
        )
        total_rows += rows
        total_size += float(size)

    console.print()
    console.print(tbl)
    console.print(
        f"\n  [bold]Total:[/bold] {len(tables)} tables  •  "
        f"{total_rows:,} rows  •  {total_size:.2f} MB\n"
    )


def print_diff_table(
    source_tables: set[str],
    dest_tables: set[str],
):
    """Print a diff comparison of source vs destination tables."""
    all_tables = sorted(source_tables | dest_tables)

    tbl = Table(
        title="Schema Diff: Source vs Destination",
        show_header=True,
        header_style="bold cyan",
    )
    tbl.add_column("Table", style="white")
    tbl.add_column("Source", justify="center")
    tbl.add_column("Dest", justify="center")
    tbl.add_column("Status", style="white")

    for t in all_tables:
        in_src = t in source_tables
        in_dst = t in dest_tables
        src_mark = "[green]✓[/green]" if in_src else "[red]✗[/red]"
        dst_mark = "[green]✓[/green]" if in_dst else "[red]✗[/red]"

        if in_src and in_dst:
            status = "[green]Synced[/green]"
        elif in_src and not in_dst:
            status = "[yellow]Missing at dest[/yellow]"
        else:
            status = "[red]Extra at dest[/red]"

        tbl.add_row(t, src_mark, dst_mark, status)

    console.print()
    console.print(tbl)
    console.print(
        f"\n  Only in source: {len(source_tables - dest_tables)}  •  "
        f"Only in dest: {len(dest_tables - source_tables)}  •  "
        f"In both: {len(source_tables & dest_tables)}\n"
    )
