package scheduler

import (
	"context"
	"database/sql"
	"fmt"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/go-logr/logr"
	"github.com/go-logr/stdr"
	_ "github.com/mattn/go-sqlite3"

	"github.com/data-absorb/data-absorb/internal/config"
	"github.com/data-absorb/data-absorb/internal/converter"
	"github.com/data-absorb/data-absorb/internal/db"
	"github.com/data-absorb/data-absorb/internal/writer"
)

var _ = len(sql.Drivers()) > 0

func init() {
	stdr.SetVerbosity(0)
}

type Scheduler struct {
	cfg           *config.Config
	registry      *db.DriverRegistry
	schemaBuilder *converter.SchemaBuilder
	log           logr.Logger
	wg            sync.WaitGroup
}

func New(cfg *config.Config, log logr.Logger) *Scheduler {
	if !log.Enabled() {
		log = stdr.New(nil)
	}
	return &Scheduler{
		cfg:           cfg,
		registry:      db.NewDriverRegistry(cfg.Global.Workers, cfg.Global.Workers),
		schemaBuilder: converter.NewSchemaBuilder(),
		log:           log,
	}
}

func (s *Scheduler) Run(ctx context.Context) error {
	defer s.registry.Close()

	for _, dbConfig := range s.cfg.Databases {
		s.log.Info("Registering database", "name", dbConfig.Name, "driver", dbConfig.Driver, "dsn", dbConfig.DSN)
		if err := s.registry.Register(ctx, dbConfig.Name, dbConfig.Driver, dbConfig.DSN); err != nil {
			s.log.Error(err, "Failed to register database", "name", dbConfig.Name)
			return fmt.Errorf("E002: 数据库 %s 连接失败: %w", dbConfig.Name, err)
		}
		s.log.Info("Database registered successfully", "name", dbConfig.Name)
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
			s.log.Error(err, "Task failed", "worker", id, "tables", task.Tables)
		}
	}
}

func (s *Scheduler) executeTask(ctx context.Context, task config.TaskConfig) error {
	dbHandle, err := s.registry.Get(task.DB)
	if err != nil {
		return fmt.Errorf("E003: 获取数据库 %s 失败: %w", task.DB, err)
	}

	outputDir := s.cfg.Global.OutputDir
	format := s.cfg.Global.DefaultFormat
	if task.Format != "" {
		format = task.Format
	}

	factory := writer.NewWriterFactory(outputDir, s.cfg.Global.Overwrite)

	for _, table := range task.Tables {
		s.log.Info("Processing table", "table", table)

		columns, err := s.getColumns(ctx, dbHandle, table)
		if err != nil {
			s.log.Error(err, "Failed to get table columns, skipping", "table", table)
			continue
		}

		schema, err := s.schemaBuilder.Build(columns)
		if err != nil {
			s.log.Error(err, "Failed to build schema for table, skipping", "table", table)
			continue
		}

		outputFile := table + ".parquet"
		if format == "arrow" {
			outputFile = table + ".arrow"
		}

		w, err := factory.Create(outputFile, format, schema)
		if err != nil {
			s.log.Error(err, "Failed to create writer for table, skipping", "table", table)
			continue
		}

		if err := s.exportTable(ctx, dbHandle, table, schema, w); err != nil {
			s.log.Error(err, "Failed to export table, skipping", "table", table)
			w.Close()
			continue
		}

		if err := w.Close(); err != nil {
			s.log.Error(err, "Failed to close writer for table", "table", table)
		}
		s.log.Info("Table exported successfully", "table", table)
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
