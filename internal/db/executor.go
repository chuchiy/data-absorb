package db

import (
	"context"
	"database/sql"
	"fmt"
)

type Rows interface {
	Close() error
	ColumnTypes() ([]*sql.ColumnType, error)
	Next() bool
	Scan(dest ...interface{}) error
	Err() error
}

type Executor struct {
	db *sql.DB
}

func NewExecutor(db *sql.DB) *Executor {
	return &Executor{db: db}
}

func (e *Executor) Query(ctx context.Context, query string) ([]*sql.ColumnType, Rows, error) {
	rows, err := e.db.QueryContext(ctx, query)
	if err != nil {
		return nil, nil, fmt.Errorf("查询执行失败: %w", err)
	}

	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		rows.Close()
		return nil, nil, fmt.Errorf("获取列类型失败: %w", err)
	}

	return columnTypes, rows, nil
}

func (e *Executor) Close() error {
	return nil
}
