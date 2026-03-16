# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Quick Start

**Installation & Setup:**
```bash
# Install dependencies
uv sync

# Copy and customize the config
cp config.example.yaml config.yaml
```

**Running the CLI:**
```bash
# Run the main transfer
uv run mysql-transfer transfer -c config.yaml

# Other commands
uv run mysql-transfer inspect -c config.yaml
uv run mysql-transfer schema -c config.yaml
uv run mysql-transfer data -c config.yaml
uv run mysql-transfer diff -c config.yaml
```

## Project Overview

**mysql-transfer** is a CLI tool for transferring MySQL databases (schema + data) between source and destination servers. It handles chunked streaming, parallel transfers, incremental sync, and SSH tunneling.

### Architecture

The codebase is organized by responsibility:

- **cli.py**: Click-based CLI entry point. Defines 5 commands (transfer, schema, data, inspect, diff) with shared options for source/dest connection and transfer behavior. Uses decorators (`common_options`, `transfer_options`) for DRY option reuse.

- **transfer.py**: Main orchestration layer. Runs the transfer pipeline:
  - Opens SSH tunnels once, shares across all connections
  - Resolves tables based on include/exclude filters
  - Transfers schema (tables, views, procedures, triggers)
  - Transfers data in parallel using ThreadPoolExecutor (workers configurable)
  - Handles dry-run mode

- **config.py**: Configuration management with dataclasses:
  - `SSHConfig`: SSH tunnel parameters (host, port, user, password/key)
  - `ConnectionConfig`: MySQL connection (host, port, user, password, database, charset, SSH)
  - `TransferConfig`: Full transfer settings (source, dest, tables, options like chunk_size, workers, drop_existing, truncate, incremental)
  - `load_config()`: Loads YAML → dataclasses
  - `apply_cli_overrides()`: CLI flags override file config
  - `.validate()`: Validation on all configs

- **connection.py**: MySQL connection management:
  - `_create_ssh_tunnel()`: Sets up SSHTunnelForwarder if SSH is enabled
  - `create_connection()`: Creates pymysql.Connection, optionally through SSH, optionally with server-side cursor (SSDictCursor for streaming)
  - `managed_connection()`, `managed_tunnel()`: Context managers for cleanup

- **schema.py**: Schema transfer (tables, views, stored procedures, functions, triggers)

- **data.py**: Row-by-row data transfer using chunked inserts and server-side cursors

- **progress.py**: Rich-based progress output (progress bars, summary tables, colored logging)

### Key Design Patterns

1. **SSH tunnel sharing**: Tunnels are opened once in `run_transfer()` and shared across all connection creations (source and dest). This is important for avoiding multiple SSH connections.

2. **Server-side cursors for streaming**: Data transfer uses `SSDictCursor` (streaming cursor) to avoid loading all rows into memory at once.

3. **Parallel execution**: `ThreadPoolExecutor` with configurable worker count transfers multiple tables concurrently.

4. **Configuration merging**: YAML config is loaded first, then CLI flags override specific fields. This allows both file-based and inline configuration.

5. **Dry-run support**: `cfg.dry_run` flag is passed through to skip actual SQL execution while showing what would happen.

## Configuration

Config is YAML-based (see `config.example.yaml`):
- **source/dest**: Connection details (host, port, user, password, database, charset) + optional SSH tunnel
- **options**: Table filters, chunk size, worker count, drop/truncate flags, incremental settings, schema options

Both can be overridden via CLI flags: `--source-host`, `--source-port`, `--dest-db`, `--chunk-size`, etc.

## Testing

Currently no tests. When adding:
- Unit tests for config validation, table filtering
- Integration tests for schema/data transfer (may require test databases)
- Consider using pytest + fixtures for managed test MySQL instances

## Future Improvements

- Add pytest for integration testing
- Add linting (ruff) and formatting (black)
- Add type checking (mypy)
- Performance profiling for large transfers
- Resume/checkpoint support for long-running transfers
