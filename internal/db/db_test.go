package db

import (
	"context"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

func TestDriverRegistry_Register(t *testing.T) {
	dbPath := "/home/dht/agentic/data-absorb/testdata/test.db"
	registry := NewDriverRegistry(10, 5)

	ctx := context.Background()
	err := registry.Register(ctx, "test", "sqlite3", dbPath)
	if err != nil {
		t.Fatalf("Failed to register database: %v", err)
	}

	dbConn, err := registry.Get("test")
	if err != nil {
		t.Fatalf("Failed to get database: %v", err)
	}

	if err := dbConn.Ping(); err != nil {
		t.Fatalf("Database ping failed: %v", err)
	}

	registry.Close()
}

func TestDriverRegistry_MultipleDatabases(t *testing.T) {
	dbPath := "/home/dht/agentic/data-absorb/testdata/test.db"
	registry := NewDriverRegistry(10, 5)

	ctx := context.Background()

	if err := registry.Register(ctx, "db1", "sqlite3", dbPath); err != nil {
		t.Fatalf("Failed to register db1: %v", err)
	}

	if err := registry.Register(ctx, "db2", "sqlite3", dbPath); err != nil {
		t.Fatalf("Failed to register db2: %v", err)
	}

	_, err := registry.Get("nonexistent")
	if err == nil {
		t.Error("Should return error for nonexistent database")
	}

	registry.Close()
}

func TestExecutor_Query(t *testing.T) {
	dbPath := "/home/dht/agentic/data-absorb/testdata/test.db"
	registry := NewDriverRegistry(10, 5)

	ctx := context.Background()
	if err := registry.Register(ctx, "test", "sqlite3", dbPath); err != nil {
		t.Fatalf("Failed to register database: %v", err)
	}

	dbConn, _ := registry.Get("test")
	executor := NewExecutor(dbConn)

	columnTypes, rows, err := executor.Query(ctx, "SELECT * FROM test_types LIMIT 1")
	if err != nil {
		t.Fatalf("Query error: %v", err)
	}
	defer rows.Close()

	if len(columnTypes) == 0 {
		t.Error("No columns returned")
	}

	if !rows.Next() {
		t.Error("No rows returned")
	}

	values := make([]interface{}, len(columnTypes))
	valuePtrs := make([]interface{}, len(columnTypes))
	for i := range values {
		valuePtrs[i] = &values[i]
	}

	if err := rows.Scan(valuePtrs...); err != nil {
		t.Fatalf("Scan error: %v", err)
	}

	t.Logf("First row values: %v", values[:5])

	registry.Close()
}
