package scheduler

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"sync"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/memory"
	_ "github.com/mattn/go-sqlite3"

	"github.com/data-absorb/data-absorb/internal/config"
	"github.com/data-absorb/data-absorb/internal/converter"
	"github.com/data-absorb/data-absorb/internal/db"
	"github.com/data-absorb/data-absorb/internal/writer"
)

var _ = len(sql.Drivers()) > 0

type Scheduler struct {
	cfg           *config.Config
	registry      *db.DriverRegistry
	schemaBuilder *converter.SchemaBuilder
	wg            sync.WaitGroup
}

func New(cfg *config.Config) *Scheduler {
	return &Scheduler{
		cfg:           cfg,
		registry:      db.NewDriverRegistry(cfg.Global.Workers, cfg.Global.Workers),
		schemaBuilder: converter.NewSchemaBuilder(),
	}
}

func (s *Scheduler) Run(ctx context.Context) error {
	defer s.registry.Close()

	for _, dbConfig := range s.cfg.Databases {
		log.Printf("Registering database: %s (driver: %s, dsn: %s)", dbConfig.Name, dbConfig.Driver, dbConfig.DSN)
		if err := s.registry.Register(ctx, dbConfig.Name, dbConfig.Driver, dbConfig.DSN); err != nil {
			log.Printf("ERROR: Failed to register database %s: %v", dbConfig.Name, err)
			return fmt.Errorf("E002: 数据库 %s 连接失败: %w", dbConfig.Name, err)
		}
		log.Printf("Database %s registered successfully", dbConfig.Name)
	}

	taskCh := make(chan config.TaskConfig, len(s.cfg.Tasks))
	for _, task := range s.cfg.Tasks {
		taskCh <- task
	}
	close(taskCh)

	s.wg.Add(s.cfg.Global.Workers)
	for i := 0; i < s.cfg.Global.Workers; i++ {
		go s.worker(ctx, i, taskCh)
	}

	s.wg.Wait()
	return nil
}

func (s *Scheduler) worker(ctx context.Context, id int, tasks <-chan config.TaskConfig) {
	defer s.wg.Done()

	for task := range tasks {
		if err := s.executeTask(ctx, task); err != nil {
			log.Printf("Worker %d: Task %s failed: %v", id, task.Tables, err)
		}
	}
}

func (s *Scheduler) executeTask(ctx context.Context, task config.TaskConfig) error {
	dbHandle, err := s.registry.Get(task.DB)
	if err != nil {
		return fmt.Errorf("E003: 获取数据库 %s 失败: %w", task.DB, err)
	}

	columns, err := s.getColumns(ctx, dbHandle, task.Tables[0])
	if err != nil {
		return fmt.Errorf("E004: 获取表 %s 列信息失败: %w", task.Tables[0], err)
	}

	schema, err := s.schemaBuilder.Build(columns)
	if err != nil {
		return fmt.Errorf("E005: 构建 schema 失败: %w", err)
	}

	outputDir := s.cfg.Global.OutputDir
	format := s.cfg.Global.DefaultFormat
	if task.Format != "" {
		format = task.Format
	}

	factory := writer.NewWriterFactory(outputDir, s.cfg.Global.Overwrite)

	for _, table := range task.Tables {
		log.Printf("Processing table: %s", table)
		outputFile := table + ".parquet"
		if format == "arrow" {
			outputFile = table + ".arrow"
		}

		w, err := factory.Create(outputFile, format, schema)
		if err != nil {
			log.Printf("WARNING: Failed to create writer for table %s: %v (skipping)", table, err)
			continue
		}

		if err := s.exportTable(ctx, dbHandle, table, schema, w); err != nil {
			log.Printf("WARNING: Failed to export table %s: %v (skipping)", table, err)
			w.Close()
			continue
		}

		if err := w.Close(); err != nil {
			log.Printf("WARNING: Failed to close writer for table %s: %v", table, err)
		}
		log.Printf("Table %s exported successfully", table)
	}

	return nil
}

func (s *Scheduler) getColumns(ctx context.Context, dbHandle *sql.DB, table string) ([]*sql.ColumnType, error) {
	query := fmt.Sprintf("SELECT * FROM %s WHERE 1=0", table)
	rows, err := dbHandle.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return rows.ColumnTypes()
}

func (s *Scheduler) exportTable(ctx context.Context, dbHandle *sql.DB, table string, schema *arrow.Schema, w writer.Writer) error {
	query := fmt.Sprintf("SELECT * FROM %s", table)
	rows, err := dbHandle.QueryContext(ctx, query)
	if err != nil {
		return err
	}
	defer rows.Close()

	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return err
	}

	arrowSchema, err := s.schemaBuilder.Build(colTypes)
	if err != nil {
		return err
	}

	rowConverter := converter.NewRowConverter(arrowSchema)

	mem := memory.NewGoAllocator()
	_ = mem

	batchSize := s.cfg.Global.BatchSize
	_ = batchSize

	for rows.Next() {
		dest := make([]interface{}, len(arrowSchema.Fields()))
		for i := range dest {
			dest[i] = new(interface{})
		}

		if err := rows.Scan(dest...); err != nil {
			return err
		}

		row := make([]interface{}, len(arrowSchema.Fields()))
		for i, d := range dest {
			val := *d.(*interface{})
			row[i] = val
		}

		builders, err := rowConverter.ConvertRow(row)
		if err != nil {
			return err
		}

		arrays := make([]arrow.Array, len(builders))
		for i, b := range builders {
			arrays[i] = b.NewArray()
			b.Release()
		}

		record := array.NewRecord(arrowSchema, arrays, 1)
		for _, arr := range arrays {
			arr.Release()
		}

		if err := w.Write(record); err != nil {
			record.Release()
			return err
		}
		record.Release()
	}

	if err := rows.Err(); err != nil {
		return err
	}

	return nil
}
