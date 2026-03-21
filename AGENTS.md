# AGENTS.md - Guide for AI Agents Working in data-absorb

## Project Overview

**data-absorb** is a Go CLI tool that exports database tables to columnar file formats (Parquet, Arrow IPC). It supports multiple databases (PostgreSQL, MySQL, SQLite, Oracle, MSSQL) with parallel execution and batch processing.

## Essential Commands

### Build & Run
```bash
# Build binary
go build -o bin/data-absorb ./cmd/data-absorb

# Run with config
go run ./cmd/data-absorb --config configs/example.toml

# Or using Taskfile
task build
./bin/data-absorb --config configs/example.toml
```

### Testing
```bash
# Run all unit tests
go test ./...

# Run integration test (SQLite-based)
go run ./cmd/integration_test

# Or using Taskfile
task test           # Unit tests
task integration    # Integration test
task all            # Build + test + integration
```

### Database Testing (requires Docker)
```bash
task db-up              # Start PostgreSQL, MariaDB, Oracle containers
task db-wait            # Wait 45s for databases to initialize
task db-init-postgres   # Initialize PostgreSQL test data
task db-init-mariadb    # Initialize MariaDB test data
task db-init-oracle     # Initialize Oracle test data
task db-export-postgres # Export from PostgreSQL
task db-export-mariadb  # Export from MariaDB
task db-export-oracle   # Export from Oracle
task db-verify          # Verify exported parquet files
task db-down            # Stop containers and remove volumes
```

### Linting
```bash
go vet ./...
# Or: task lint
```

### Cleanup
```bash
task clean  # Removes bin/, testdata/integration_output, testdata/db_export, *.db files
```

## Project Structure

```
data-absorb/
├── cmd/
│   ├── data-absorb/main.go      # CLI entry point
│   ├── benchmark/main.go        # Performance benchmark tool
│   └── integration_test/main.go # End-to-end integration test
├── internal/
│   ├── config/config.go         # TOML config parsing & validation
│   ├── db/
│   │   ├── driver.go            # DriverRegistry - database connection pool
│   │   └── executor.go          # SQL query execution
│   ├── converter/
│   │   ├── schema.go            # SQL → Arrow type mapping (TypeMapper)
│   │   └── row.go               # RowConverter - row data conversion
│   ├── scheduler/worker.go      # Worker pool for parallel table exports
│   └── writer/
│       ├── factory.go           # WriterFactory interface
│       ├── parquet.go           # Parquet writer (ZSTD compression)
│       └── arrow.go             # Arrow IPC writer
├── pkg/types/types.go           # CLI Args struct
├── configs/                     # Sample TOML configurations
├── scripts/                     # Database init SQL scripts
├── testdata/                    # Test data directory
├── docker-compose.yaml          # Database containers for testing
├── Taskfile.yaml                # Task automation definitions
├── SPEC.md                      # Detailed design specification
└── README.md                    # Project documentation
```

## Code Patterns & Conventions

### Configuration Pattern
- Uses TOML format with `github.com/BurntSushi/toml`
- Config struct in `internal/config/config.go`
- Defaults: workers=4, batch_size=10000, default_format="parquet"
- Validation ensures task has either `tables` or `query` (not both)

### Error Codes
The codebase uses error codes for traceability:
- `E001`: Config parsing failure
- `E002`: Database connection failure
- `E003`: SQL execution failure
- `E004`: Type mapping failure
- `E005`: File write failure
- `E006`: Task execution failure

### Type Mapping (SQL → Arrow)
Defined in `internal/converter/schema.go`:
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
| TIMESTAMP, DATETIME | Timestamp(microsecond) |
| DECIMAL, NUMERIC | Decimal128(38,10) |
| BLOB, BYTEA | Binary |

### Worker Pool Pattern
In `internal/scheduler/worker.go`:
- Tasks distributed via channel
- Configurable worker count
- Single table failure skips table, logs error, continues
- Connection pool configured per database

### Writer Pattern
- Factory pattern in `internal/writer/factory.go`
- Writers implement `Writer` interface (`Write(record)`, `Close()`)
- Parquet: ZSTD compression, V2_LATEST format
- Arrow: IPC format with LZ4 compression
- Atomic writes: write to `.tmp` file, rename on close

### Database Driver Registration
In `internal/db/driver.go`:
- All drivers imported via `_` imports
- `DriverRegistry` manages connection pools
- Drivers: `pgx` (PostgreSQL), `mysql`, `sqlite3`, `oracle`, `mssql`

### Table Name Quoting
In `scheduler/worker.go` `quoteTableName()`:
- Oracle: `"TABLE_NAME"` (uppercase, double quotes)
- MySQL/MariaDB: `` `table_name` `` (backticks)
- Others: no quoting

## Testing Patterns

### Unit Tests
- Standard Go testing with `testing` package
- Test files named `*_test.go`
- Use `t.TempDir()` for temporary files
- Table-driven tests for validation cases (see `config_test.go`)

### Integration Tests
- Entry point: `cmd/integration_test/main.go`
- Creates temporary SQLite DB with test data
- Runs full export workflow
- Verifies: file existence, row counts, data types, values

### Test Data Generation
- `testdata/generate.go` creates test databases
- SQLite test database: `testdata/test.db`

## Important Gotchas

### Oracle-Specific
- Table names must be uppercase with double quotes
- Use schema prefix for tables outside current schema: `system.ALL_TYPES`
- DSN format: `oracle://user:pass@host:port/service`

### Memory Management
- Arrow builders must be released after use (`b.Release()`)
- Records must be released after writing (`record.Release()`)
- Batch processing prevents OOM on large tables

### Empty Tables
- Empty tables still create output files with schema only (0 rows)

### Parallel Execution
- Worker count controls concurrent table exports
- Connection pool sized to workers * 2
- Single table failure doesn't stop other tables

### File Overwrites
- Default: skip existing files
- Set `overwrite = true` in config to overwrite

## Dependencies

| Package | Purpose |
|---------|---------|
| `github.com/apache/arrow-go/v18` | Arrow/Parquet processing |
| `github.com/jackc/pgx/v5` | PostgreSQL driver |
| `github.com/go-sql-driver/mysql` | MySQL/MariaDB driver |
| `github.com/mattn/go-sqlite3` | SQLite driver (CGO) |
| `github.com/sijms/go-ora/v2` | Oracle driver |
| `github.com/denisenkom/go-mssqldb` | MSSQL driver |
| `github.com/BurntSushi/toml` | TOML parsing |
| `github.com/alexflint/go-arg` | CLI argument parsing |
| `github.com/go-logr/logr` | Structured logging |

## Database Container Ports

When using `docker-compose up -d`:
- PostgreSQL: `localhost:5432`
- MariaDB: `localhost:3306`
- Oracle: `localhost:1521`

## Configuration

### Task Modes
| Mode | Required Fields | Output Filename |
|------|-----------------|-----------------|
| Tables mode | `db`, `tables` | `{table_name}.parquet` or `{table_name}.arrow` |
| Query mode | `db`, `query`, `output` | Value of `output` field |

### Task Configuration Fields
| Field | Required | Description |
|-------|----------|-------------|
| `db` | Yes | Database name (references `[[databases]].name`) |
| `tables` | Conditional | List of tables to export (tables mode) |
| `query` | Conditional | Custom SQL query (query mode) |
| `output` | Query mode only | Output filename for query results |
| `format` | Optional | Override `global.default_format` (`parquet` or `arrow`) |
| `name` | Optional | Task name (not currently used for output) |

**Note**: Task must have either `tables` OR `query`, not both. Query mode requires `output`.

### Configuration Example

```toml
[global]
workers = 4
batch_size = 10000
default_format = "parquet"
output_dir = "./output"
overwrite = false
log_level = "info"

[[databases]]
name = "mydb"
driver = "sqlite3"
dsn = "./data.db"

# Tables mode: export multiple tables (one file per table)
[[tasks]]
db = "mydb"
tables = ["users", "orders"]

# Query mode: custom SQL with specified output filename
[[tasks]]
db = "mydb"
query = "SELECT id, name FROM users WHERE created_at > '2024-01-01'"
output = "recent_users.parquet"
format = "parquet"  # Optional: overrides global.default_format
```

## Common Tasks for Agents

### Adding a new database type
1. Add driver import to `cmd/data-absorb/main.go` and `internal/db/driver.go`
2. Add driver name to README table
3. Add DSN example to README
4. Create init script in `scripts/`
5. Create test config in `configs/`
6. Update `quoteTableName()` if quoting differs

### Adding a new output format
1. Create new writer in `internal/writer/`
2. Implement `Writer` interface
3. Add to `WriterFactory.Create()` in `factory.go`
4. Update config validation for format string

### Modifying type mapping
1. Edit `internal/converter/schema.go` `TypeMapper.typeMap`
2. Add case in `NewBuilder()` if new Arrow type
3. Update `ToArrowType()` if special handling needed
4. Update README and SPEC.md type tables

### Running performance benchmarks
```bash
go run ./cmd/benchmark
```
Tests different row counts, table counts, and worker counts.