package db

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/denisenkom/go-mssqldb"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
	_ "github.com/mattn/go-sqlite3"
	_ "github.com/sijms/go-ora/v2"
)

func init() {
	// Force import of all database drivers by accessing Drivers()
	_ = len(sql.Drivers())
}

type DriverRegistry struct {
	drivers   map[string]*sql.DB
	driverMap map[string]string
	maxConns  int
	idleConns int
}

func NewDriverRegistry(maxConns, idleConns int) *DriverRegistry {
	if maxConns <= 0 {
		maxConns = 10
	}
	if idleConns <= 0 {
		idleConns = 5
	}
	return &DriverRegistry{
		drivers:   make(map[string]*sql.DB),
		driverMap: make(map[string]string),
		maxConns:  maxConns,
		idleConns: idleConns,
	}
}

func (r *DriverRegistry) Register(ctx context.Context, name, driver, dsn string) error {
	db, err := sql.Open(driver, dsn)
	if err != nil {
		return fmt.Errorf("连接数据库 %s 失败: %w", name, err)
	}

	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return fmt.Errorf("测试数据库 %s 连接失败: %w", name, err)
	}

	db.SetMaxOpenConns(r.maxConns)
	db.SetMaxIdleConns(r.idleConns)

	r.drivers[name] = db
	r.driverMap[name] = driver
	return nil
}

func (r *DriverRegistry) Get(name string) (*sql.DB, error) {
	db, ok := r.drivers[name]
	if !ok {
		return nil, fmt.Errorf("数据库 %s 未注册", name)
	}
	return db, nil
}

func (r *DriverRegistry) GetDriver(name string) string {
	return r.driverMap[name]
}

func (r *DriverRegistry) Close() error {
	for _, db := range r.drivers {
		db.Close()
	}
	r.drivers = make(map[string]*sql.DB)
	return nil
}
