"""Schema extraction and transfer (tables, views, routines, triggers)."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pymysql

if TYPE_CHECKING:
    from mysql_transfer.config import TransferConfig
    from mysql_transfer.progress import ProgressManager


# ---------------------------------------------------------------------------
# Introspection helpers
# ---------------------------------------------------------------------------


def get_tables(conn: pymysql.Connection) -> list[dict[str, Any]]:
    """Return list of tables with row counts and sizes."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                TABLE_NAME   AS table_name,
                TABLE_ROWS   AS row_count,
                ROUND((DATA_LENGTH + INDEX_LENGTH) / 1024 / 1024, 2) AS size_mb,
                ENGINE       AS engine,
                TABLE_COLLATION AS collation
            FROM information_schema.TABLES
            WHERE TABLE_SCHEMA = DATABASE()
              AND TABLE_TYPE = 'BASE TABLE'
            ORDER BY TABLE_NAME
            """
        )
        return list(cur.fetchall())


def get_views(conn: pymysql.Connection) -> list[str]:
    """Return list of view names."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT TABLE_NAME
            FROM information_schema.VIEWS
            WHERE TABLE_SCHEMA = DATABASE()
            ORDER BY TABLE_NAME
            """
        )
        return [row["TABLE_NAME"] for row in cur.fetchall()]


def get_routines(conn: pymysql.Connection) -> list[dict[str, str]]:
    """Return list of stored procedures and functions."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT ROUTINE_NAME AS name, ROUTINE_TYPE AS type
            FROM information_schema.ROUTINES
            WHERE ROUTINE_SCHEMA = DATABASE()
            ORDER BY ROUTINE_TYPE, ROUTINE_NAME
            """
        )
        return list(cur.fetchall())


def get_triggers(conn: pymysql.Connection) -> list[str]:
    """Return list of trigger names."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT TRIGGER_NAME
            FROM information_schema.TRIGGERS
            WHERE TRIGGER_SCHEMA = DATABASE()
            ORDER BY TRIGGER_NAME
            """
        )
        return [row["TRIGGER_NAME"] for row in cur.fetchall()]


# ---------------------------------------------------------------------------
# DDL extraction
# ---------------------------------------------------------------------------


def get_create_table_ddl(conn: pymysql.Connection, table: str) -> str:
    with conn.cursor() as cur:
        cur.execute(f"SHOW CREATE TABLE `{table}`")
        row = cur.fetchone()
        return row["Create Table"]


def get_create_view_ddl(conn: pymysql.Connection, view: str) -> str:
    with conn.cursor() as cur:
        cur.execute(f"SHOW CREATE VIEW `{view}`")
        row = cur.fetchone()
        return row["Create View"]


def get_create_routine_ddl(
    conn: pymysql.Connection, name: str, routine_type: str
) -> str:
    """Get DDL for a stored procedure or function."""
    with conn.cursor() as cur:
        keyword = "PROCEDURE" if routine_type.upper() == "PROCEDURE" else "FUNCTION"
        cur.execute(f"SHOW CREATE {keyword} `{name}`")
        row = cur.fetchone()
        key = f"Create {keyword.capitalize()}"
        # MySQL may use 'Create Procedure' or 'Create Function'
        for k in row:
            if k.lower().startswith("create"):
                return row[k]
        return row.get(key, "")


def get_create_trigger_ddl(conn: pymysql.Connection, trigger: str) -> str:
    with conn.cursor() as cur:
        cur.execute(f"SHOW CREATE TRIGGER `{trigger}`")
        row = cur.fetchone()
        return row["SQL Original Statement"]


# ---------------------------------------------------------------------------
# Schema transfer
# ---------------------------------------------------------------------------


def _matches_pattern(name: str, patterns: list[str]) -> bool:
    """Check if a table name matches any of the given patterns (supports * wildcards)."""
    from fnmatch import fnmatch
    name_lower = name.lower()
    for pattern in patterns:
        pattern = pattern.strip().lower()
        if not pattern:
            continue
        if "*" in pattern or "?" in pattern:
            if fnmatch(name_lower, pattern):
                return True
        else:
            if name_lower == pattern:
                return True
    return False


def resolve_tables(
    conn: pymysql.Connection,
    cfg: TransferConfig,
) -> list[dict[str, Any]]:
    """Resolve the list of tables to transfer based on config filters.

    Supports wildcard patterns (e.g. 'django_*', '*_log') in both
    tables and exclude_tables.
    """
    all_tables = get_tables(conn)

    if cfg.tables:
        all_tables = [t for t in all_tables if _matches_pattern(t["table_name"], cfg.tables)]

    if cfg.exclude_tables:
        all_tables = [t for t in all_tables if not _matches_pattern(t["table_name"], cfg.exclude_tables)]

    return all_tables


def transfer_schema(
    source_conn: pymysql.Connection,
    dest_conn: pymysql.Connection,
    cfg: TransferConfig,
    progress: ProgressManager | None = None,
    tables: list[dict[str, Any]] | None = None,
) -> None:
    """Transfer schema objects from source to destination."""
    if tables is None:
        tables = resolve_tables(source_conn, cfg)

    # --- Tables ---
    if progress:
        progress.log(f"Transferring schema for {len(tables)} table(s)...")

    with dest_conn.cursor() as dest_cur:
        dest_cur.execute("SET FOREIGN_KEY_CHECKS = 0")

        for tbl in tables:
            name = tbl["table_name"]
            ddl = get_create_table_ddl(source_conn, name)

            if cfg.dry_run:
                if progress:
                    progress.log(f"  [dry-run] Would create table `{name}`")
                continue

            if cfg.drop_existing:
                dest_cur.execute(f"DROP TABLE IF EXISTS `{name}`")

            try:
                dest_cur.execute(ddl)
                if progress:
                    progress.log(f"  ✓ Created table `{name}`")
            except pymysql.err.OperationalError as e:
                if "already exists" in str(e).lower():
                    if progress:
                        progress.log(f"  • Table `{name}` already exists, skipping")
                else:
                    if progress:
                        progress.log(f"  ⚠ Failed to create table `{name}`: {e}")
                    else:
                        import warnings
                        warnings.warn(f"Failed to create table `{name}`: {e}")

        dest_cur.execute("SET FOREIGN_KEY_CHECKS = 1")
    dest_conn.commit()

    # --- Views ---
    if cfg.include_views:
        views = get_views(source_conn)
        if views and progress:
            progress.log(f"Transferring {len(views)} view(s)...")
        with dest_conn.cursor() as dest_cur:
            for view in views:
                ddl = get_create_view_ddl(source_conn, view)
                if cfg.dry_run:
                    if progress:
                        progress.log(f"  [dry-run] Would create view `{view}`")
                    continue
                if cfg.drop_existing:
                    dest_cur.execute(f"DROP VIEW IF EXISTS `{view}`")
                try:
                    dest_cur.execute(ddl)
                    if progress:
                        progress.log(f"  ✓ Created view `{view}`")
                except pymysql.err.OperationalError:
                    if progress:
                        progress.log(f"  ⚠ Failed to create view `{view}`")
        dest_conn.commit()

    # --- Routines ---
    if cfg.include_routines:
        routines = get_routines(source_conn)
        if routines and progress:
            progress.log(f"Transferring {len(routines)} routine(s)...")
        with dest_conn.cursor() as dest_cur:
            for r in routines:
                name, rtype = r["name"], r["type"]
                ddl = get_create_routine_ddl(source_conn, name, rtype)
                if cfg.dry_run:
                    if progress:
                        progress.log(f"  [dry-run] Would create {rtype} `{name}`")
                    continue
                keyword = "PROCEDURE" if rtype.upper() == "PROCEDURE" else "FUNCTION"
                if cfg.drop_existing:
                    dest_cur.execute(f"DROP {keyword} IF EXISTS `{name}`")
                try:
                    dest_cur.execute(ddl)
                    if progress:
                        progress.log(f"  ✓ Created {rtype.lower()} `{name}`")
                except pymysql.err.OperationalError:
                    if progress:
                        progress.log(f"  ⚠ Failed to create {rtype.lower()} `{name}`")
        dest_conn.commit()

    # --- Triggers ---
    if cfg.include_triggers:
        triggers = get_triggers(source_conn)
        if triggers and progress:
            progress.log(f"Transferring {len(triggers)} trigger(s)...")
        with dest_conn.cursor() as dest_cur:
            for trigger in triggers:
                ddl = get_create_trigger_ddl(source_conn, trigger)
                if cfg.dry_run:
                    if progress:
                        progress.log(f"  [dry-run] Would create trigger `{trigger}`")
                    continue
                if cfg.drop_existing:
                    dest_cur.execute(f"DROP TRIGGER IF EXISTS `{trigger}`")
                try:
                    dest_cur.execute(ddl)
                    if progress:
                        progress.log(f"  ✓ Created trigger `{trigger}`")
                except pymysql.err.OperationalError:
                    if progress:
                        progress.log(f"  ⚠ Failed to create trigger `{trigger}`")
        dest_conn.commit()
