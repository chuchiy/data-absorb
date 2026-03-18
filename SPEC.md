# Data Absorb - 数据库导出工具详细设计文档

## 1. 项目概述

### 1.1 项目定位
通用数据库导出工具（DB → Columnar File），支持从多种关系型数据库导出数据到 Parquet 或 Arrow IPC 格式。

### 1.2 核心特性
- 多数据库支持：PostgreSQL、MySQL、SQLite、Oracle、MSSQL
- 输出格式：Parquet（ZSTD压缩）、Arrow IPC
- 批量处理：按 batch_size 分批写入 Arrow RecordBatch
- 并发执行：Worker Pool 模式，单表失败跳过
- 声明式配置：TOML 格式配置文件

---

## 2. 配置设计

### 2.1 配置文件结构

```toml
[global]
workers = 4                    # 并发 worker 数量
batch_size = 10000            # 每批次写入的行数
default_format = "parquet"    # 默认输出格式: parquet | arrow
output_dir = "./output"       # 输出目录
overwrite = false             # 是否覆盖已存在的文件
log_level = "info"            # 日志级别: debug | info | warn | error

[[databases]]
name = "db1"                  # 数据库标识（task 中引用）
driver = "postgres"           # 驱动类型
dsn = "..."                   # DSN 连接字符串
max_open_conns = 10           # 最大打开连接数
max_idle_conns = 5            # 最大空闲连接数

[[tasks]]
db = "db1"                    # 引用的数据库名
tables = ["table1", "table2"] # 要导出的表列表（紧凑模式）
# 或
query = "SELECT ..."          # 自定义 SQL
output = "custom.parquet"     # 自定义输出文件名（仅 query 模式需要）
format = "parquet"            # 输出格式（可选，覆盖 global.default_format）
```

### 2.2 Task 定义规则

| 模式 | 字段 | 说明 |
|------|------|------|
| 紧凑模式 | `tables` | 批量导出多表，输出文件名 = 表名 + 格式后缀 |
| SQL 模式 | `query` | 自定义查询，必须指定 `output` |

### 2.3 输出文件名规则

| 输入 | 格式 | 输出文件 |
|------|------|----------|
| `tables = ["users", "orders"]` | parquet | `users.parquet`, `orders.parquet` |
| `tables = ["users"]` | arrow | `users.arrow` |
| `query = "..."` + `output = "x.parquet"` | parquet | `x.parquet` |

---

## 3. 数据库抽象层

### 3.1 支持的驱动

| 数据库 | 驱动 | Go Import |
|--------|------|-----------|
| PostgreSQL | jackc/pgx/v5 | github.com/jackc/pgx/v5/stdlib |
| MySQL | go-sql-driver/mysql | github.com/go-sql-driver/mysql |
| SQLite | mattn/go-sqlite3 | github.com/mattn/go-sqlite3 |
| Oracle | sijms/go-ora/v2 | github.com/sijms/go-ora/v2 |
| MSSQL | denisenkom/go-mssqldb | github.com/denisenkom/go-mssqldb |

### 3.2 驱动注册接口

```go
// DriverRegistry 驱动注册表
type DriverRegistry struct {
    drivers map[string]driver.Driver
}

// Register 注册数据库驱动
func (r *DriverRegistry) Register(name, driverType string) error

// Get 获取数据库连接
func (r *DriverRegistry) Get(name string) (*sql.DB, error)
```

### 3.3 SQL 执行器接口

```go
// QueryExecutor SQL 执行器
type QueryExecutor interface {
    // Query 执行查询，返回列信息和行迭代器
    Query(ctx context.Context, query string) (ColumnTypes, Rows, error)
    // Close 关闭连接
    Close() error
}
```

### 3.4 流式读取策略

```go
// 避免 OOM：使用数据库原生游标
for rows.Next() {
    // 逐行扫描
    err := rows.Scan(values...)
    if err != nil {
        return err
    }
    
    // 累积到 batch
    batch = append(batch, values)
    
    // 达到 batch_size 后写入
    if len(batch) >= batchSize {
        err = writeBatch(batch)
        if err != nil {
            return err
        }
        batch = batch[:0]
    }
}
```

---

## 4. 类型映射系统

### 4.1 SQL → Arrow 类型映射表

| SQL 类型 | Arrow Type | 备注 |
|----------|------------|------|
| INT, INTEGER, SERIAL | INT64 | |
| BIGINT, BIGSERIAL | INT64 | |
| SMALLINT | INT32 | |
| TINYINT, INT8 | INT8 | |
| FLOAT, FLOAT4 | FLOAT32 | |
| DOUBLE, FLOAT8 | FLOAT64 | |
| DECIMAL(p,s), NUMERIC(p,s) | DECIMAL(38,10) | 固定精度 |
| VARCHAR(n), CHAR(n), TEXT | STRING | |
| BOOL, BOOLEAN | BOOL | |
| DATE | DATE32 | |
| TIME | TIME32 | |
| TIMESTAMP, DATETIME | TIMESTAMP(microsecond) | 不带时区 |
| BLOB, BYTEA, BINARY | BINARY | |
| JSON, JSONB | STRING | 序列化为字符串 |
| UUID | STRING | 序列化为字符串 |
| ARRAY | LIST | 展开为列表 |

### 4.2 类型映射实现

```go
// TypeMapper 类型映射器
type TypeMapper struct {
    typeMap map[string]arrow.Type
    decimalPrecision int
    decimalScale int
}

// NewTypeMapper 创建类型映射器
func NewTypeMapper() *TypeMapper {
    return &TypeMapper{
        typeMap: map[string]arrow.Type{
            "INT":       arrow.INT64,
            "INTEGER":   arrow.INT64,
            "BIGINT":    arrow.INT64,
            "SMALLINT":  arrow.INT32,
            "TINYINT":   arrow.INT8,
            "FLOAT":     arrow.FLOAT32,
            "DOUBLE":    arrow.FLOAT64,
            "DECIMAL":   arrow.DECIMAL,
            "NUMERIC":   arrow.DECIMAL,
            "VARCHAR":   arrow.STRING,
            "TEXT":      arrow.STRING,
            "BOOL":      arrow.BOOL,
            "BOOLEAN":   arrow.BOOL,
            "DATE":      arrow.DATE32,
            "TIME":      arrow.TIME32,
            "TIMESTAMP": arrow.TIMESTAMP,
            "DATETIME":  arrow.TIMESTAMP,
            "BLOB":      arrow.BINARY,
            "BYTEA":     arrow.BINARY,
            "BINARY":    arrow.BINARY,
            "JSON":      arrow.STRING,
            "JSONB":     arrow.STRING,
        },
        decimalPrecision: 38,
        decimalScale:     10,
    }
}

// ToArrowType 转换为 Arrow 类型
func (m *TypeMapper) ToArrowType(sqlType string, precision, scale int) (arrow.Type, error)
```

---

## 5. 数据转换层

### 5.1 Schema 构建

```go
// SchemaBuilder Schema 构建器
type SchemaBuilder struct {
    mapper *TypeMapper
}

// Build 从数据库列信息构建 Arrow Schema
func (b *SchemaBuilder) Build(columns []*sql.ColumnType) (*arrow.Schema, error)
```

### 5.2 行数据转换

```go
// RowConverter 行转换器
type RowConverter struct {
    schema *arrow.Schema
    fields []FieldConverter
}

// FieldConverter 字段转换器
type FieldConverter struct {
    arrowType arrow.Type
    convert   func(src interface{}) interface{}
}

// ConvertRow 转换单行数据
func (c *RowConverter) ConvertRow(values []interface{}) ([]array.Builder, error)

// BuildRecord 从 builders 构建 RecordBatch
func (c *RowConverter) BuildRecord(builders []array.Builder, n int64) arrow.Record
```

### 5.3 批量写入流程

```
1. 从数据库读取一批行
2. 遍历每行：
   - 对每个字段调用 FieldConverter.convert
   - 将转换后的值 append 到对应的 Builder
3. 构建 Arrow RecordBatch
4. 写入文件
5. 释放 Record 和 Builders
6. 重复直到数据读完
```

---

## 6. 输出层

### 6.1 写入器接口

```go
// Writer 数据写入器接口
type Writer interface {
    // Write 写入 RecordBatch
    Write(record arrow.Record) error
    // Close 关闭写入器
    Close() error
}

// WriterFactory 写入器工厂
type WriterFactory interface {
    // Create 创建写入器
    Create(output string, schema *arrow.Schema) (Writer, error)
}
```

### 6.2 Parquet 写入器

```go
// ParquetWriter Parquet 写入器
type ParquetWriter struct {
    writer *parquet.FileWriter
    file   *os.File
}

// 配置
- 压缩: ZSTD
- 行组大小: batch_size
- 列块大小: 128MB
- 版本: v2.6
```

### 6.3 Arrow IPC 写入器

```go
// ArrowWriter Arrow IPC 写入器
type ArrowWriter struct {
    writer *ipc.FileWriter
    file   *os.File
}

// 配置
- 格式: IPC (Arrow Columnar Format)
- 压缩: LZ4
- 同步写入: true
```

### 6.4 空文件处理

```go
// 空表处理：创建只有 schema 的空文件
if rowCount == 0 {
    // 仍然创建 RecordBatch (n=0)
    record := array.NewRecord(schema, arrays, 0)
    writer.Write(record)
    record.Release()
}
```

---

## 7. 并发调度器

### 7.1 Worker Pool 设计

```go
// Scheduler 任务调度器
type Scheduler struct {
    workers    int
    dbRegistry *DriverRegistry
    writerFactory WriterFactory
}

// Task 导出任务
type Task struct {
    Name      string          // 任务名（用于输出文件名）
    DB        string          // 数据库标识
    Query     string          // SQL 查询
    Tables    []string        // 表列表（紧凑模式）
    Output    string          // 输出文件名（query 模式）
    Format    string          // parquet | arrow
}

// Run 执行所有任务
func (s *Scheduler) Run(ctx context.Context, tasks []Task) error
```

### 7.2 并发控制

```go
// 限制并发数
semaphore := make(chan struct{}, s.workers)

// 每个 worker 处理一个任务
for _, task := range tasks {
    semaphore <- struct{}{}
    
    go func(t Task) {
        defer func() { <-semaphore }()
        s.executeTask(ctx, t)
    }(task)
}
```

### 7.3 错误处理策略

```go
// 单表失败：跳过并记录日志，继续执行其他任务
func (s *Scheduler) executeTask(ctx context.Context, task Task) error {
    err := s.doExport(ctx, task)
    if err != nil {
        log.Printf("WARN: task %s failed: %v, skipping", task.Name, err)
        return nil  // 不返回错误，继续执行
    }
    log.Printf("INFO: task %s completed", task.Name)
    return nil
}
```

### 7.4 连接池控制

```go
// 限制数据库连接数，避免打爆数据库
db.SetMaxOpenConns(s.workers * 2)
db.SetMaxIdleConns(s.workers)
db.SetConnMaxLifetime(5 * time.Minute)
```

---

## 8. CLI 设计

### 8.1 命令行接口

```bash
# 基本用法
data-absorb --config <config-file>

# 完整选项
data-absorb --config config.toml --log-level debug

# 查看帮助
data-absorb --help
```

### 8.2 go-arg 定义

```go
type Args struct {
    Config   string `arg:"required" help:"配置文件路径 (TOML格式)"`
    LogLevel string `arg:"--log-level" default:"info" help:"日志级别: debug, info, warn, error"`
    Version  bool   `arg:"--version" help:"显示版本信息"`
}
```

---

## 9. 项目结构

```
data-absorb/
├── cmd/
│   └── cli/
│       └── main.go              # CLI 入口
├── internal/
│   ├── config/
│   │   └── config.go            # 配置解析
│   ├── db/
│   │   ├── driver.go            # 驱动注册
│   │   └── executor.go          # SQL 执行
│   ├── converter/
│   │   ├── schema.go            # Schema 构建
│   │   └── row.go               # 行数据转换
│   ├── writer/
│   │   ├── factory.go           # 写入器工厂
│   │   ├── parquet.go           # Parquet 写入
│   │   └── arrow.go             # Arrow IPC 写入
│   └── scheduler/
│       └── worker.go            # 任务调度
├── pkg/
│   └── types/
│       └── types.go             # 公共类型
├── configs/
│   └── example.toml             # 示例配置
├── go.mod
├── main.go                      # 程序入口
└── README.md                    # 项目说明
```

---

## 10. 依赖清单

| 包 | 版本 | 用途 |
|----|------|------|
| github.com/alexflint/go-arg | v1.5.0 | CLI 解析 |
| github.com/BurntSushi/toml | v1.4.0 | 配置解析 |
| github.com/apache/arrow/go/v12 | v12.0.0 | Arrow/Parquet |
| github.com/apache/arrow/go/v12/parquet | v12.0.0 | Parquet 写入 |
| github.com/jackc/pgx/v5 | v5.5.0 | PostgreSQL |
| github.com/go-sql-driver/mysql | v1.7.1 | MySQL |
| github.com/mattn/go-sqlite3 | v1.14.18 | SQLite |
| github.com/sijms/go-ora/v2 | v2.8.0 | Oracle |
| github.com/denisenkom/go-mssqldb | v0.12.3 | MSSQL |

---

## 11. 错误码设计

| 错误码 | 含义 |
|--------|------|
| E001 | 配置文件解析失败 |
| E002 | 数据库连接失败 |
| E003 | SQL 执行失败 |
| E004 | 类型映射失败 |
| E005 | 文件写入失败 |
| E006 | 任务执行失败 |

---

## 12. 日志格式

```json
{
  "time": "2024-01-01T12:00:00Z",
  "level": "INFO",
  "msg": "task completed",
  "task": "users",
  "rows": 100000,
  "duration": "1.234s"
}
```

---

## 13. 性能考量

1. **零拷贝**: 使用 Arrow 内存布局，减少数据复制
2. **批量写入**: 按 batch_size 分批写入，减少 I/O 次数
3. **连接复用**: 使用连接池，避免频繁创建连接
4. **并发控制**: 限制并发数，避免打爆数据库
5. **ZSTD 压缩**: Parquet 使用 ZSTD，压缩率高且速度快