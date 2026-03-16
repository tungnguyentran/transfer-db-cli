"""Chunked / streaming data transfer."""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any

import pymysql
from sshtunnel import SSHTunnelForwarder

if TYPE_CHECKING:
    from mysql_transfer.config import ConnectionConfig, TransferConfig
    from mysql_transfer.progress import ProgressManager


def _build_insert_sql(table: str, columns: list[str], *, upsert: bool = False) -> str:
    """Build a parameterized INSERT (or REPLACE) statement."""
    cols = ", ".join(f"`{c}`" for c in columns)
    placeholders = ", ".join(["%s"] * len(columns))
    keyword = "REPLACE" if upsert else "INSERT"
    return f"{keyword} INTO `{table}` ({cols}) VALUES ({placeholders})"


def get_row_count(conn: pymysql.Connection, table: str) -> int:
    """Get exact row count for a table."""
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) AS cnt FROM `{table}`")
        row = cur.fetchone()
        return row["cnt"]


def get_columns(conn: pymysql.Connection, table: str) -> list[str]:
    """Get column names for a table."""
    with conn.cursor() as cur:
        cur.execute(f"SHOW COLUMNS FROM `{table}`")
        return [row["Field"] for row in cur.fetchall()]


def _do_transfer(
    source_cfg: ConnectionConfig,
    dest_cfg: ConnectionConfig,
    table: str,
    cfg: TransferConfig,
    progress: ProgressManager | None,
    task_id: int | None,
    source_tunnel: SSHTunnelForwarder | None,
    dest_tunnel: SSHTunnelForwarder | None,
) -> dict[str, Any]:
    """Inner transfer logic for a single table (no retries)."""
    from mysql_transfer.connection import create_connection

    stats: dict[str, Any] = {
        "table": table,
        "rows_transferred": 0,
        "chunks": 0,
        "error": None,
    }

    # Source connection with server-side cursor for streaming
    source_conn = create_connection(source_cfg, streaming=True, tunnel=source_tunnel)
    dest_conn = create_connection(dest_cfg, streaming=False, tunnel=dest_tunnel)

    try:
        # Get columns from source (need a non-streaming connection)
        tmp_conn = create_connection(source_cfg, streaming=False, tunnel=source_tunnel)
        try:
            columns = get_columns(tmp_conn, table)
        finally:
            tmp_conn.close()

        if not columns:
            stats["error"] = "No columns found"
            return stats

        # Build query
        query = f"SELECT * FROM `{table}`"
        incremental_max_val = None

        if cfg.incremental and cfg.incremental_column:
            try:
                with dest_conn.cursor() as dest_cur:
                    dest_cur.execute(
                        f"SELECT MAX(`{cfg.incremental_column}`) AS max_val FROM `{table}`"
                    )
                    row = dest_cur.fetchone()
                    if row and row["max_val"] is not None:
                        incremental_max_val = row["max_val"]
                        query += f" WHERE `{cfg.incremental_column}` > %s"
            except Exception:
                pass  # Table might not exist at dest yet

        upsert = cfg.incremental
        insert_sql = _build_insert_sql(table, columns, upsert=upsert)

        # Truncate if requested
        if cfg.truncate and not cfg.incremental:
            if not cfg.dry_run:
                with dest_conn.cursor() as dest_cur:
                    dest_cur.execute(f"TRUNCATE TABLE `{table}`")
                dest_conn.commit()

        if cfg.dry_run:
            tmp_conn = create_connection(source_cfg, streaming=False, tunnel=source_tunnel)
            try:
                count = get_row_count(tmp_conn, table)
            finally:
                tmp_conn.close()
            stats["rows_transferred"] = count
            if progress and task_id is not None:
                progress.log(f"  [dry-run] Would transfer {count:,} rows from `{table}`")
            return stats

        # Optimise destination for bulk loading
        with dest_conn.cursor() as dest_cur:
            dest_cur.execute("SET FOREIGN_KEY_CHECKS = 0")
            dest_cur.execute("SET UNIQUE_CHECKS = 0")
            dest_cur.execute("SET AUTOCOMMIT = 0")

        # Stream from source
        if progress:
            progress.log(f"  [dim]`{table}`:[/dim] querying source...")

        with source_conn.cursor() as src_cur:
            if incremental_max_val is not None:
                src_cur.execute(query, (incremental_max_val,))
            else:
                src_cur.execute(query)

            if progress:
                progress.log(f"  [dim]`{table}`:[/dim] streaming rows...")

            batch: list[tuple] = []
            for row_data in src_cur:
                values = tuple(row_data[c] for c in columns)
                batch.append(values)

                if len(batch) >= cfg.chunk_size:
                    with dest_conn.cursor() as dest_cur:
                        dest_cur.executemany(insert_sql, batch)
                    dest_conn.commit()
                    stats["rows_transferred"] += len(batch)
                    stats["chunks"] += 1
                    if progress and task_id is not None:
                        progress.advance(task_id, len(batch))
                    batch = []

            # Flush remaining
            if batch:
                with dest_conn.cursor() as dest_cur:
                    dest_cur.executemany(insert_sql, batch)
                dest_conn.commit()
                stats["rows_transferred"] += len(batch)
                stats["chunks"] += 1
                if progress and task_id is not None:
                    progress.advance(task_id, len(batch))

        # Restore settings
        with dest_conn.cursor() as dest_cur:
            dest_cur.execute("SET FOREIGN_KEY_CHECKS = 1")
            dest_cur.execute("SET UNIQUE_CHECKS = 1")
            dest_cur.execute("SET AUTOCOMMIT = 1")

    finally:
        # Close streaming cursor cleanly to avoid SSCursor cleanup warnings
        try:
            source_conn.close()
        except Exception:
            pass
        try:
            dest_conn.close()
        except Exception:
            pass

    return stats


def transfer_table_data(
    source_cfg: ConnectionConfig,
    dest_cfg: ConnectionConfig,
    table: str,
    cfg: TransferConfig,
    progress: ProgressManager | None = None,
    task_id: int | None = None,
    source_tunnel: SSHTunnelForwarder | None = None,
    dest_tunnel: SSHTunnelForwarder | None = None,
) -> dict[str, Any]:
    """Transfer data for a single table with retry logic.

    Tunnels are passed in from the orchestrator so they can be reused across
    parallel workers (each worker creates its own MySQL connection through the
    shared tunnel).

    Returns a dict with transfer stats.
    """
    max_retries = 3

    for attempt in range(1, max_retries + 1):
        try:
            if progress:
                retry_info = f" (retry {attempt - 1}/{max_retries - 1})" if attempt > 1 else ""
                progress.log(f"  [dim]`{table}`:[/dim] connecting...{retry_info}")

            return _do_transfer(
                source_cfg, dest_cfg, table, cfg,
                progress, task_id, source_tunnel, dest_tunnel,
            )

        except Exception as e:
            if attempt < max_retries:
                wait = 5 * attempt
                if progress:
                    progress.log(
                        f"  [yellow]`{table}`: {e} — retrying in {wait}s...[/yellow]"
                    )
                time.sleep(wait)
            else:
                return {
                    "table": table,
                    "rows_transferred": 0,
                    "chunks": 0,
                    "error": str(e),
                }

    # Should never reach here, but just in case
    return {"table": table, "rows_transferred": 0, "chunks": 0, "error": "Unknown error"}
