# Data Absorb - Database Export Tool

A general database export tool that exports database tables to columnar file formats (Parquet, Arrow IPC).

## Features

- **Multi-database support**: PostgreSQL, MySQL, SQLite, Oracle, MSSQL
- **Output formats**: Parquet (with ZSTD compression), Arrow IPC
- **Batch processing**: Efficient batch writing to Arrow RecordBatch
- **Parallel execution**: Configurable worker pool for concurrent table exports
- **Error handling**: Single table failure skips with logging
- **CLI**: Simple command-line interface using `alexflint/go-arg`
- **Configuration**: TOML-based configuration

## Project Structure

```
.
├── cmd/
│   ├── data-absorb/          # CLI entry point
│   ├── integration_test/    # Integration test runner
│   └── benchmark/            # Performance benchmark tool
├── internal/
│   ├── config/               # Configuration parsing
│   ├── converter/            # SQL to Arrow type mapping
│   ├── db/                   # Database driver registration
│   ├── scheduler/            # Worker pool scheduler
│   └── writer/               # Parquet/Arrow IPC writers
├── configs/                  # Sample configurations
├── scripts/                  # Database init scripts
├── pkg/types/                # CLI argument types
├── testdata/                 # Test data directory
├── Taskfile.yaml             # Task automation
├── SPEC.md                   # Design specification
└── README.md                 # This file
```

## Quick Start

```bash
# Install Task (optional, or use go directly)
# https://taskfile.dev/installation/

# Run tests
task test

# Run integration test
task integration

# Build binary
task build
```

## Usage

```bash
# Run with config
go run ./cmd/data-absorb --config configs/example.toml

# Or build and run
task build
./bin/data-absorb --config configs/example.toml
```

## Configuration (TOML)

```toml
[global]
workers = 4           # Number of worker goroutines
batch_size = 10000    # Rows per batch
default_format = "parquet"  # parquet or arrow
output_dir = "./output"
overwrite = true
log_level = "info"

[[databases]]
name = "mydb"
driver = "sqlite3"
dsn = "./data.db"

[[tasks]]
db = "mydb"
tables = ["users", "orders", "products"]
```

## Supported Databases

| Database | Driver Name | Notes |
|----------|-------------|-------|
| PostgreSQL | `pgx` | Use `pgx` driver (stdlib) |
| MySQL/MariaDB | `mysql` | |
| SQLite | `sqlite3` | |
| Oracle | `oracle` | Use DSN format `oracle://user:pass@host:port/service` |
| MSSQL | `mssql` | |

## Database Connection Examples

```toml
# PostgreSQL
dsn = "postgres://user:password@localhost:5432/dbname?sslmode=disable"

# MySQL/MariaDB  
dsn = "user:password@tcp(localhost:3306)/dbname?charset=utf8mb4"

# SQLite
dsn = "/path/to/database.db"

# Oracle
dsn = "oracle://system:password@localhost:1521/XE"
```

## Table Name Handling

- **Oracle**: Use schema prefix for tables outside current schema, e.g., `system.ALL_TYPES`
- **MySQL/MariaDB**: Table names are backtick-quoted
- **SQLite**: Table names used as-is
- **PostgreSQL**: Table names used as-is

## Output Formats

- **Parquet**: Apache Parquet format with ZSTD compression
- **Arrow**: Arrow IPC (aka Feather) format

## Type Mapping

SQL types are mapped to Arrow types:

| SQL Type | Arrow Type |
|----------|------------|
| INT, INTEGER, BIGINT | Int64 |
| SMALLINT | Int32 |
| TINYINT | Int8 |
| FLOAT, REAL | Float32 |
| DOUBLE | Float64 |
| VARCHAR, CHAR, TEXT | String |
| BOOL, BOOLEAN | Boolean |
| DATE | Date32 |
| DATETIME, TIMESTAMP | Timestamp |
| BLOB, BYTEA | Binary |
| DECIMAL, NUMERIC | Decimal(38,10) |

## Development

### Using Taskfile

```bash
task build      # Build the binary
task test       # Run unit tests
task integration  # Run integration test
task lint       # Run linter
task clean      # Clean build artifacts
task all        # Build, test, and integration

# Database testing (requires Docker)
task db-up         # Start database containers
task db-init-*     # Initialize test data
task db-export-*   # Export from database
task db-test-*     # Verify exported data
task db-down       # Stop database containers
```

### Using Go Commands

```bash
# Build
go build -o bin/data-absorb ./cmd/data-absorb

# Build benchmark tool
go build -o bin/benchmark ./cmd/benchmark

# Test
go test ./...

# Integration test
go run ./cmd/integration_test

# Run benchmark
go run ./cmd/benchmark
```

## Database Testing

This section describes how to set up and run tests against real databases (PostgreSQL, MariaDB, Oracle).

### Prerequisites

- Docker and Docker Compose
- At least 4GB RAM available for database containers

### Start Database Containers

```bash
# Start all databases
docker-compose up -d

# Or start individually
docker-compose up -d postgres
docker-compose up -d mariadb
docker-compose up -d oracle
```

### Initialize Test Data

Database containers automatically initialize on first start using init scripts in `scripts/`:

- `init_postgres.sql` - PostgreSQL test data
- `init_mariadb.sql` - MariaDB test data
- `init_oracle.sql` - Oracle test data

If containers already exist with stale data, recreate them:

```bash
# Remove old containers and volumes
docker-compose down -v

# Start fresh
docker-compose up -d
```

### Test Data Description

Each database is initialized with:

| Table | Rows | Purpose |
|-------|------|---------|
| `all_types` | 1,000 | Various SQL data types |
| `nullable_test` | 5 | NULL handling |
| `huge_table` | 100,000 | Large table performance |
| `export_table_1` to `export_table_20` | 10,000 each | Parallel export testing |

### Run Database Export Tests

Using Taskfile (recommended):

```bash
# Start all databases
task db-up

# Export from each database
task db-export-postgres
task db-export-mariadb
task db-export-oracle

# Or run full test for each database
task db-test-postgres
task db-test-mariadb
task db-test-oracle

# Stop databases when done
task db-down
```

Using Go commands:

```bash
# Build the tool
go build -o data-absorb ./cmd/data-absorb

# Test PostgreSQL export
./data-absorb --config configs/test_postgres.toml

# Test MariaDB export
./data-absorb --config configs/test_mariadb.toml

# Test Oracle export
./data-absorb --config configs/test_oracle.toml
```

### Verify Exported Data

Use duckdb to verify Parquet files:

```bash
# Install duckdb CLI (if not available)
# brew install duckdb  # macOS
# apt-get install duckdb  # Ubuntu

# Verify PostgreSQL export
duckdb -c "SELECT * FROM read_parquet('/tmp/parquet_test/all_types.parquet') LIMIT 5;"

# Check row count
duckdb -c "SELECT COUNT(*) FROM read_parquet('/tmp/parquet_test/all_types.parquet');"
```

### Stop Database Containers

```bash
# Stop all containers
docker-compose down

# Stop and remove volumes (clears all data)
docker-compose down -v
```

### Quick Reference

```bash
# Full workflow
docker-compose down -v          # Clean start
docker-compose up -d            # Start databases
sleep 30                        # Wait for initialization
go build -o data-absorb ./cmd/data-absorb
./data-absorb --config configs/test_postgres.toml
./data-absorb --config configs/test_mariadb.toml
./data-absorb --config configs/test_oracle.toml
go test ./...                   # Run unit tests
docker-compose down             # Cleanup
```

## Benchmark Tool

The benchmark tool (`cmd/benchmark`) tests parallel export performance with multiple tables:

```bash
# Run benchmark
go run ./cmd/benchmark

# Or build and run
go build -o benchmark ./cmd/benchmark && ./benchmark
```

The benchmark:
- Tests with different row counts (10K, 100K, 1M)
- Tests with different table counts (4, 8 tables)
- Tests with different worker counts (1, 2, 4, 8)
- Reports throughput (rows/sec) and scalability (speedup, efficiency)

## Integration Tests

The integration test (`cmd/integration_test/main.go`) performs full end-to-end verification:

1. **Generate test database**: Creates temporary SQLite DB with test data
2. **Run export**: Executes data-absorb to export tables
3. **Verify results**:
   - Output files exist
   - Row counts match source
   - Data types are correct (BIGINT, VARCHAR, DECIMAL, TIMESTAMP)
   - Data values are accurate

### Verified Data Types

- `id` → BIGINT
- `name` → VARCHAR
- `amount` → DECIMAL(38,10)
- `created_at` → TIMESTAMP
- nullable columns → properly handled

## Dependencies

- **Apache Arrow**: [github.com/apache/arrow-go/v18](https://github.com/apache/arrow-go) - Arrow/Parquet 处理
- **SQLite**: [github.com/mattn/go-sqlite3](https://github.com/mattn/go-sqlite3) - SQLite 驱动
- **Logger**: [github.com/go-logr/logr](https://github.com/go-logr/logr) - 结构化日志
- **CLI**: [github.com/alexflint/go-arg](https://github.com/alexflint/go-arg) - 命令行参数解析
- **Config**: [github.com/BurntSushi/toml](https://github.com/BurntSushi/toml) - TOML 配置解析
- **PostgreSQL**: [github.com/jackc/pgx/v5](https://github.com/jackc/pgx) - PostgreSQL 驱动
- **MySQL**: [github.com/go-sql-driver/mysql](https://github.com/go-sql-driver/mysql) - MySQL 驱动
- **MSSQL**: [github.com/denisenkom/go-mssqldb](https://github.com/denisenkom/go-mssqldb) - MSSQL 驱动
- **Oracle**: [github.com/sijms/go-ora/v2](https://github.com/sijms/go-ora) - Oracle 驱动
- **duckdb**: CLI tool for verifying Parquet output

## License

MIT