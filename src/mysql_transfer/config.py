"""Configuration loading and validation."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml


@dataclass
class SSHConfig:
    """SSH tunnel configuration."""

    enabled: bool = False
    host: str = ""
    port: int = 22
    user: str = ""
    password: str | None = None
    key_file: str | None = None
    key_password: str | None = None

    def validate(self, label: str) -> list[str]:
        errors = []
        if self.enabled:
            if not self.host:
                errors.append(f"{label} SSH: host is required")
            if not self.user:
                errors.append(f"{label} SSH: user is required")
            if not self.password and not self.key_file:
                errors.append(f"{label} SSH: password or key_file is required")
        return errors


@dataclass
class ConnectionConfig:
    """MySQL connection configuration."""

    host: str = "localhost"
    port: int = 3306
    user: str = "root"
    password: str = ""
    database: str = ""
    charset: str = "utf8mb4"
    ssh: SSHConfig = field(default_factory=SSHConfig)

    def validate(self, label: str) -> list[str]:
        errors = []
        if not self.host:
            errors.append(f"{label}: host is required")
        if not self.database:
            errors.append(f"{label}: database is required")
        if not self.user:
            errors.append(f"{label}: user is required")
        errors.extend(self.ssh.validate(label))
        return errors


@dataclass
class TransferConfig:
    """Full transfer configuration."""

    source: ConnectionConfig = field(default_factory=ConnectionConfig)
    dest: ConnectionConfig = field(default_factory=ConnectionConfig)

    # Table selection
    tables: list[str] = field(default_factory=list)  # empty = all
    exclude_tables: list[str] = field(default_factory=list)

    # Transfer behaviour
    chunk_size: int = 10_000
    workers: int = 4
    drop_existing: bool = False
    truncate: bool = False

    # Incremental sync
    incremental: bool = False
    incremental_column: str = ""

    # Schema objects
    include_views: bool = False
    include_routines: bool = False
    include_triggers: bool = False

    # Misc
    dry_run: bool = False

    def validate(self) -> list[str]:
        errors = []
        errors.extend(self.source.validate("source"))
        errors.extend(self.dest.validate("dest"))
        if self.incremental and not self.incremental_column:
            errors.append("--incremental-column is required when --incremental is set")
        if self.chunk_size < 1:
            errors.append("--chunk-size must be >= 1")
        if self.workers < 1:
            errors.append("--workers must be >= 1")
        return errors


def _ssh_from_dict(d: dict[str, Any]) -> SSHConfig:
    return SSHConfig(
        enabled=bool(d.get("enabled", True)),
        host=str(d.get("host", "")),
        port=int(d.get("port", 22)),
        user=str(d.get("user", "")),
        password=d.get("password"),
        key_file=d.get("key_file"),
        key_password=d.get("key_password"),
    )


def _conn_from_dict(d: dict[str, Any]) -> ConnectionConfig:
    ssh = SSHConfig()
    if "ssh" in d:
        ssh = _ssh_from_dict(d["ssh"])
    return ConnectionConfig(
        host=str(d.get("host", "localhost")),
        port=int(d.get("port", 3306)),
        user=str(d.get("user", "root")),
        password=str(d.get("password", "")),
        database=str(d.get("database", "")),
        charset=str(d.get("charset", "utf8mb4")),
        ssh=ssh,
    )


def load_config(config_path: str | Path | None = None) -> TransferConfig:
    """Load configuration from a YAML file."""
    if config_path is None:
        return TransferConfig()

    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")

    with open(path) as f:
        raw: dict[str, Any] = yaml.safe_load(f) or {}

    cfg = TransferConfig()

    if "source" in raw:
        cfg.source = _conn_from_dict(raw["source"])
    if "dest" in raw or "destination" in raw:
        cfg.dest = _conn_from_dict(raw.get("dest") or raw.get("destination", {}))

    opts = raw.get("options", {})
    if opts:
        cfg.tables = opts.get("tables", [])
        cfg.exclude_tables = opts.get("exclude_tables", [])
        cfg.chunk_size = int(opts.get("chunk_size", 10_000))
        cfg.workers = int(opts.get("workers", 4))
        cfg.drop_existing = bool(opts.get("drop_existing", False))
        cfg.truncate = bool(opts.get("truncate", False))
        cfg.incremental = bool(opts.get("incremental", False))
        cfg.incremental_column = str(opts.get("incremental_column", ""))
        cfg.include_views = bool(opts.get("include_views", False))
        cfg.include_routines = bool(opts.get("include_routines", False))
        cfg.include_triggers = bool(opts.get("include_triggers", False))
        cfg.dry_run = bool(opts.get("dry_run", False))

    return cfg


def apply_cli_overrides(cfg: TransferConfig, **kwargs: Any) -> TransferConfig:
    """Override config values with CLI arguments (non-None values only)."""
    conn_fields = {
        "source_host": ("source", "host"),
        "source_port": ("source", "port"),
        "source_user": ("source", "user"),
        "source_password": ("source", "password"),
        "source_db": ("source", "database"),
        "dest_host": ("dest", "host"),
        "dest_port": ("dest", "port"),
        "dest_user": ("dest", "user"),
        "dest_password": ("dest", "password"),
        "dest_db": ("dest", "database"),
    }

    for cli_key, (target, attr) in conn_fields.items():
        val = kwargs.get(cli_key)
        if val is not None:
            setattr(getattr(cfg, target), attr, val)

    # SSH overrides
    ssh_fields = {
        "source_ssh_host": ("source", "host"),
        "source_ssh_port": ("source", "port"),
        "source_ssh_user": ("source", "user"),
        "source_ssh_password": ("source", "password"),
        "source_ssh_key": ("source", "key_file"),
        "dest_ssh_host": ("dest", "host"),
        "dest_ssh_port": ("dest", "port"),
        "dest_ssh_user": ("dest", "user"),
        "dest_ssh_password": ("dest", "password"),
        "dest_ssh_key": ("dest", "key_file"),
    }
    for cli_key, (target, attr) in ssh_fields.items():
        val = kwargs.get(cli_key)
        if val is not None:
            ssh = getattr(getattr(cfg, target), "ssh")
            ssh.enabled = True
            setattr(ssh, attr, val)

    simple_fields = [
        "chunk_size", "workers", "drop_existing", "truncate",
        "incremental", "incremental_column",
        "include_views", "include_routines", "include_triggers",
        "dry_run",
    ]
    for f in simple_fields:
        val = kwargs.get(f)
        if val is not None:
            setattr(cfg, f, val)

    tables = kwargs.get("tables")
    if tables:
        cfg.tables = [t.strip() for t in tables.split(",") if t.strip()]

    exclude_tables = kwargs.get("exclude_tables")
    if exclude_tables:
        cfg.exclude_tables = [t.strip() for t in exclude_tables.split(",") if t.strip()]

    return cfg
