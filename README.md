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
│   └── cli/
│       └── main.go           # CLI entry point
├── configs/
│   └── example.toml          # Example configuration
├── internal/
│   ├── config/
│   │   ├── config.go         # Configuration parsing
│   │   └── config_test.go    # Configuration tests
│   ├── converter/
│   │   ├── schema.go         # SQL to Arrow type mapping
│   │   └── row.go            # Row to RecordBatch conversion
│   ├── db/
│   │   ├── driver.go         # Database driver registration
│   │   ├── db_test.go        # Database tests
│   │   └── executor.go       # Query execution
│   ├── scheduler/
│   │   ├── worker.go         # Worker pool scheduler
│   │   └── integration_test.go # Integration tests
│   └── writer/
│       ├── factory.go        # Writer factory
│       ├── parquet.go        # Parquet writer (ZSTD)
│       ├── arrow.go          # Arrow IPC writer
│       └── writer_test.go    # Writer tests
├── pkg/
│   └── types/
│       └── types.go           # CLI argument types
├── testdata/
│   ├── test.db                # SQLite test database
│   ├── test_config.toml      # Test configuration
│   └── output/               # Output directory
├── SPEC.md                   # Design specification
└── README.md                 # This file
```

## Usage

```bash
# Build
go build -o data-absorb ./cmd/cli/

# Run with config
./data-absorb --config configs/example.toml

# Show version
./data-absorb --version
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

| Database | Driver Name |
|----------|-------------|
| PostgreSQL | `pgx` or `postgres` |
| MySQL | `mysql` |
| SQLite | `sqlite3` |
| Oracle | `oracle` |
| MSSQL | `mssql` |

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

```bash
# Run tests
go test ./...

# Run integration tests
go test -v -run "TestIntegration" ./internal/scheduler/

# Build binary
go build -o data-absorb ./cmd/cli/
```

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

## Integration Tests

The project includes integration tests that verify end-to-end functionality:

```bash
# Run all integration tests
go test -v -run "TestIntegration" ./internal/scheduler/
```

### Test Integration Methods

1. **TestIntegration_SQLiteToParquet**: Tests exporting a SQLite table to Parquet format
   - Creates a test database connection
   - Exports the `test_types` table
   - Verifies output file is created and non-empty

2. **TestIntegration_EmptyTable**: Tests handling of empty tables
   - Exports an empty table
   - Verifies schema-only file is created

3. **TestIntegration_MultipleTables**: Tests parallel processing of multiple tables
   - Exports multiple tables in a single task
   - Verifies all output files are created

## License

MIT