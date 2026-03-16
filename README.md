# mysql-transfer

A CLI tool to transfer MySQL databases (schema + data) between source and destination servers.

## Features

- **Schema transfer** — tables, views, stored procedures, functions, triggers
- **Chunked streaming** — memory-efficient data transfer via server-side cursors
- **Parallel transfers** — multiple tables transferred concurrently
- **Incremental sync** — delta transfers based on a timestamp/ID column
- **SSH tunnels** — connect through SSH bastion hosts
- **Rich output** — progress bars, summary tables, coloured logging
- **Flexible config** — YAML file, CLI flags, or both

## Installation

```bash
uv sync
```

## Quick Start

```bash
# 1. Copy and edit the config file
cp config.example.yaml config.yaml

# 2. Inspect your source database
uv run mysql-transfer inspect -c config.yaml

# 3. Full transfer (schema + data)
uv run mysql-transfer transfer -c config.yaml

# 4. Schema only / Data only
uv run mysql-transfer schema -c config.yaml
uv run mysql-transfer data -c config.yaml

# 5. Compare source vs destination
uv run mysql-transfer diff -c config.yaml

# 6. Dry run
uv run mysql-transfer transfer -c config.yaml --dry-run
```

## CLI Options

```bash
uv run mysql-transfer transfer \
  --source-host 10.0.0.1 --source-port 3306 \
  --source-user admin --source-password secret \
  --source-db production \
  --dest-host localhost --dest-port 3306 \
  --dest-user root --dest-password root \
  --dest-db staging
```

| Option | Default | Description |
|--------|---------|-------------|
| `--tables` | all | Comma-separated table names |
| `--exclude-tables` | none | Tables to skip |
| `--chunk-size` | 10000 | Rows per batch INSERT |
| `--workers` | 4 | Parallel worker count |
| `--drop-existing` | false | Drop tables at dest before creating |
| `--truncate` | false | Truncate tables before data insert |
| `--incremental` | false | Enable delta sync mode |
| `--incremental-column` | — | Column for delta detection |
| `--include-views` | false | Transfer views |
| `--include-routines` | false | Transfer stored procedures & functions |
| `--include-triggers` | false | Transfer triggers |
| `--dry-run` | false | Preview without executing |

## License

MIT
