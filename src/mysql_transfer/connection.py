"""MySQL connection management with optional SSH tunnel support."""

from __future__ import annotations

import contextlib
import warnings
from typing import TYPE_CHECKING, Generator

import pymysql
from pymysql.cursors import DictCursor, SSDictCursor
from sshtunnel import SSHTunnelForwarder

# Suppress noisy SSCursor cleanup warnings from pymysql
warnings.filterwarnings("ignore", message=".*SSCursor.*")
warnings.filterwarnings("ignore", message=".*NoneType.*settimeout.*")

if TYPE_CHECKING:
    from mysql_transfer.config import ConnectionConfig


def _create_ssh_tunnel(cfg: ConnectionConfig) -> SSHTunnelForwarder | None:
    """Create an SSH tunnel if SSH is configured."""
    if not cfg.ssh.enabled:
        return None

    ssh_kwargs: dict = {
        "ssh_address_or_host": (cfg.ssh.host, cfg.ssh.port),
        "ssh_username": cfg.ssh.user,
        "remote_bind_address": (cfg.host, cfg.port),
    }

    if cfg.ssh.key_file:
        ssh_kwargs["ssh_pkey"] = cfg.ssh.key_file
        if cfg.ssh.key_password:
            ssh_kwargs["ssh_private_key_password"] = cfg.ssh.key_password
    elif cfg.ssh.password:
        ssh_kwargs["ssh_password"] = cfg.ssh.password

    tunnel = SSHTunnelForwarder(**ssh_kwargs)
    try:
        tunnel.start()
    except Exception as e:
        from rich.console import Console
        Console().print(f"[red]Error:[/red] Could not connect to SSH gateway [bold]{cfg.ssh.host}:{cfg.ssh.port}[/bold] — {e}")
        import sys
        sys.exit(1)
    return tunnel


def create_connection(
    cfg: ConnectionConfig,
    *,
    streaming: bool = False,
    tunnel: SSHTunnelForwarder | None = None,
) -> pymysql.Connection:
    """Create a pymysql connection, optionally through an SSH tunnel.

    Args:
        cfg: Connection configuration.
        streaming: If True, use server-side cursor for streaming.
        tunnel: An already-started SSH tunnel (overrides host/port).
    """
    cursor_class = SSDictCursor if streaming else DictCursor

    host = cfg.host
    port = cfg.port

    # If a tunnel is provided, connect through it
    if tunnel is not None:
        host = "127.0.0.1"
        port = tunnel.local_bind_port

    conn = pymysql.connect(
        host=host,
        port=port,
        user=cfg.user,
        password=cfg.password,
        database=cfg.database,
        charset=cfg.charset,
        cursorclass=cursor_class,
        autocommit=False,
        connect_timeout=30,
        read_timeout=600,
        write_timeout=300,
    )

    # Extend MySQL session timeouts for large transfers
    try:
        with conn.cursor() as cur:
            cur.execute("SET SESSION net_read_timeout = 600")
            cur.execute("SET SESSION net_write_timeout = 600")
            cur.execute("SET SESSION wait_timeout = 28800")
    except Exception:
        pass  # Not critical if these fail

    return conn


@contextlib.contextmanager
def managed_connection(
    cfg: ConnectionConfig,
    *,
    streaming: bool = False,
) -> Generator[pymysql.Connection, None, None]:
    """Context manager that creates a connection (with optional SSH tunnel) and cleans up."""
    tunnel = _create_ssh_tunnel(cfg)
    try:
        conn = create_connection(cfg, streaming=streaming, tunnel=tunnel)
        try:
            yield conn
        finally:
            conn.close()
    finally:
        if tunnel is not None:
            tunnel.stop()


@contextlib.contextmanager
def managed_tunnel(cfg: ConnectionConfig) -> Generator[SSHTunnelForwarder | None, None, None]:
    """Context manager for just the SSH tunnel (for long-lived tunnel reuse)."""
    tunnel = _create_ssh_tunnel(cfg)
    try:
        yield tunnel
    finally:
        if tunnel is not None:
            tunnel.stop()


def test_connection(cfg: ConnectionConfig) -> tuple[bool, str]:
    """Test if a connection can be established. Returns (success, message)."""
    try:
        with managed_connection(cfg) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
        tunnel_info = " (via SSH tunnel)" if cfg.ssh.enabled else ""
        return True, f"Connection successful{tunnel_info}"
    except Exception as e:
        return False, str(e)
