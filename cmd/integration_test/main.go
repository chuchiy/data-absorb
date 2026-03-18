package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"strconv"

	_ "github.com/mattn/go-sqlite3"
	"github.com/data-absorb/data-absorb/internal/config"
	"github.com/data-absorb/data-absorb/internal/scheduler"

	"github.com/go-logr/logr"
)

type testLogSink struct{}

func (t *testLogSink) Init(info logr.RuntimeInfo) {}
func (t *testLogSink) Enabled(level int) bool     { return level >= 0 }
func (t *testLogSink) Info(level int, msg string, keysAndValues ...interface{}) {
	log.Printf("[INFO] "+msg, keysAndValues...)
}
func (t *testLogSink) Error(err error, msg string, keysAndValues ...interface{}) {
	log.Printf("[ERROR] "+msg+" err=%v", append(keysAndValues, err)...)
}
func (t *testLogSink) WithValues(keysAndValues ...interface{}) logr.LogSink { return t }
func (t *testLogSink) WithName(name string) logr.LogSink                    { return t }

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	dbPath := "./testdata/integration_test.db"
	outputDir := "./testdata/integration_output"

	if err := os.RemoveAll(outputDir); err != nil {
		return fmt.Errorf("remove output dir: %w", err)
	}
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return fmt.Errorf("create output dir: %w", err)
	}

	if err := generateTestDB(dbPath); err != nil {
		return fmt.Errorf("generate test db: %w", err)
	}
	defer os.Remove(dbPath)

	cfg := &config.Config{
		Global: config.GlobalConfig{
			Workers:       2,
			BatchSize:     100,
			DefaultFormat: "parquet",
			OutputDir:     outputDir,
			Overwrite:     true,
		},
		Databases: []config.DatabaseConfig{
			{
				Name:   "testdb",
				Driver: "sqlite3",
				DSN:    dbPath,
			},
		},
		Tasks: []config.TaskConfig{
			{
				DB:     "testdb",
				Tables: []string{"test_types", "test_nulls", "test_empty"},
			},
		},
	}

	log.Println("Running data-absorb export...")
	s := scheduler.New(cfg, logr.New(&testLogSink{}))
	if err := s.Run(context.Background()); err != nil {
		return fmt.Errorf("scheduler run: %w", err)
	}

	log.Println("Verifying export results...")
	if err := verifyResults(outputDir); err != nil {
		return fmt.Errorf("verify results: %w", err)
	}

	log.Println("Cleaning up...")
	os.RemoveAll(outputDir)

	log.Println("All tests passed!")
	return nil
}

func generateTestDB(dbPath string) error {
	log.Printf("Generating test database: %s", dbPath)

	if err := os.Remove(dbPath); err != nil && !os.IsNotExist(err) {
		return err
	}

	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return err
	}
	defer db.Close()

	schema := `
	CREATE TABLE test_types (
		id INTEGER PRIMARY KEY,
		name TEXT,
		amount DECIMAL(10,2),
		created_at TIMESTAMP
	);
	CREATE TABLE test_nulls (
		id INTEGER PRIMARY KEY,
		value TEXT,
		nullable TEXT
	);
	CREATE TABLE test_empty (
		id INTEGER PRIMARY KEY,
		name TEXT
	);
	`
	if _, err := db.Exec(schema); err != nil {
		return fmt.Errorf("create schema: %w", err)
	}

	tx, err := db.Begin()
	if err != nil {
		return err
	}
	stmt, err := tx.Prepare("INSERT INTO test_types (id, name, amount, created_at) VALUES (?, ?, ?, ?)")
	if err != nil {
		return err
	}
	defer stmt.Close()

	data := []struct {
		id        int
		name      string
		amount    string
		createdAt string
	}{
		{1, "Alice", "100.50", "2024-01-01 10:00:00"},
		{2, "Bob", "200.75", "2024-01-02 11:00:00"},
		{3, "Charlie", "300.25", "2024-01-03 12:00:00"},
	}
	for _, d := range data {
		if _, err := stmt.Exec(d.id, d.name, d.amount, d.createdAt); err != nil {
			tx.Rollback()
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}

	tx, err = db.Begin()
	if err != nil {
		return err
	}
	stmt, err = tx.Prepare("INSERT INTO test_nulls (id, value, nullable) VALUES (?, ?, ?)")
	if err != nil {
		return err
	}
	defer stmt.Close()

	nullData := []struct {
		id        int
		value     interface{}
		nullable  interface{}
	}{
		{1, "has value", "not null"},
		{2, "has value", nil},
		{3, nil, nil},
	}
	for _, d := range nullData {
		if _, err := stmt.Exec(d.id, d.value, d.nullable); err != nil {
			tx.Rollback()
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}

	log.Println("Test database generated")
	return nil
}

func verifyResults(outputDir string) error {
	tables := []string{"test_types", "test_nulls", "test_empty"}

	log.Println("Checking output files exist...")
	for _, table := range tables {
		parquetFile := filepath.Join(outputDir, table+".parquet")

		info, err := os.Stat(parquetFile)
		if err != nil {
			return fmt.Errorf("output file not found: %s", parquetFile)
		}

		if table == "test_empty" {
			if info.Size() == 0 {
				return fmt.Errorf("empty table should have schema but got empty file")
			}
			log.Printf("  ✓ %s: file exists (size=%d, schema only)", table, info.Size())
			continue
		}

		log.Printf("  ✓ %s: file exists (size=%d)", table, info.Size())
	}

	log.Println("Verifying data content with duckdb...")
	if err := verifyDataWithDuckDB(outputDir); err != nil {
		return fmt.Errorf("duckdb verification: %w", err)
	}

	log.Println("All verifications passed!")
	return nil
}

func verifyDataWithDuckDB(outputDir string) error {
	log.Println("  Verifying row counts and data types...")

	if err := verifyRowCounts(outputDir); err != nil {
		return err
	}

	log.Println("  Verifying data types...")
	if err := verifyDataTypes(outputDir); err != nil {
		return err
	}

	log.Println("  Verifying data values...")
	if err := verifyDataValues(outputDir); err != nil {
		return err
	}

	return nil
}

func verifyRowCounts(outputDir string) error {
	tables := []string{"test_types", "test_nulls", "test_empty"}
	expectedCounts := map[string]int{
		"test_types": 3,
		"test_nulls": 3,
		"test_empty": 0,
	}

	for _, table := range tables {
		parquetFile := filepath.Join(outputDir, table+".parquet")

		query := fmt.Sprintf("SELECT COUNT(*) FROM read_parquet('%s')", parquetFile)
		cmd := exec.Command("duckdb", "-csv", "-noheader", ":memory:", "-c", query)
		out, err := cmd.Output()
		if err != nil {
			return fmt.Errorf("duckdb query failed for %s: %w", table, err)
		}

		count, err := strconv.Atoi(strings.TrimSpace(string(out)))
		if err != nil {
			return fmt.Errorf("parse count for %s: %w", table, err)
		}

		expected := expectedCounts[table]
		if count != expected {
			return fmt.Errorf("%s: expected %d rows, got %d", table, expected, count)
		}
		log.Printf("    ✓ %s: %d rows", table, count)
	}

	return nil
}

func verifyDataTypes(outputDir string) error {
	parquetFile := filepath.Join(outputDir, "test_types.parquet")

	query := fmt.Sprintf("DESCRIBE SELECT * FROM read_parquet('%s')", parquetFile)
	cmd := exec.Command("duckdb", "-csv", ":memory:", "-c", query)
	out, err := cmd.Output()
	if err != nil {
		return fmt.Errorf("duckdb describe failed: %w", err)
	}

	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	if len(lines) < 2 {
		return fmt.Errorf("unexpected describe output: %s", string(out))
	}

	typeMap := make(map[string]string)
	for i := 1; i < len(lines); i++ {
		fields := strings.Split(lines[i], ",")
		if len(fields) >= 2 {
			col := strings.TrimSpace(fields[0])
			typ := strings.Trim(fields[1], `"`)
			typeMap[col] = typ
		}
	}

	expectedTypes := map[string]string{
		"id":         "BIGINT",
		"name":       "VARCHAR",
		"amount":     "DECIMAL",
		"created_at": "TIMESTAMP",
	}

	for col, expectedType := range expectedTypes {
		actualType := typeMap[col]
		if actualType == "" {
			return fmt.Errorf("column %s not found in schema", col)
		}
		if !strings.HasPrefix(actualType, expectedType) {
			return fmt.Errorf("column %s: expected type %s, got %s", col, expectedType, actualType)
		}
		log.Printf("    ✓ %s: %s", col, actualType)
	}

	nullParquetFile := filepath.Join(outputDir, "test_nulls.parquet")
	cmd = exec.Command("duckdb", "-csv", ":memory:", "-c",
		fmt.Sprintf("DESCRIBE SELECT * FROM read_parquet('%s')", nullParquetFile))
	out, err = cmd.Output()
	if err != nil {
		return fmt.Errorf("duckdb describe failed for test_nulls: %w", err)
	}

	lines = strings.Split(strings.TrimSpace(string(out)), "\n")
	nullTypeMap := make(map[string]string)
	for i := 1; i < len(lines); i++ {
		fields := strings.Split(lines[i], ",")
		if len(fields) >= 2 {
			col := strings.TrimSpace(fields[0])
			typ := strings.Trim(fields[1], `"`)
			nullTypeMap[col] = typ
		}
	}

	for col, typ := range nullTypeMap {
		log.Printf("    ✓ %s: %s (nullable)", col, typ)
	}

	return nil
}

func verifyDataValues(outputDir string) error {
	parquetFile := filepath.Join(outputDir, "test_types.parquet")

	query := fmt.Sprintf("SELECT name, amount FROM read_parquet('%s') ORDER BY id", parquetFile)
	cmd := exec.Command("duckdb", "-csv", "-noheader", ":memory:", "-c", query)
	out, err := cmd.Output()
	if err != nil {
		return fmt.Errorf("duckdb query failed: %w", err)
	}

	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	expected := []struct{ name string; amount string }{
		{"Alice", "100.5"},
		{"Bob", "200.75"},
		{"Charlie", "300.25"},
	}

	for i, line := range lines {
		fields := strings.Split(line, "|")
		if len(fields) >= 2 {
			name := strings.TrimSpace(fields[0])
			amount := strings.TrimSpace(fields[1])
			if name != expected[i].name {
				return fmt.Errorf("row %d: expected name %s, got %s", i+1, expected[i].name, name)
			}
			if amount != expected[i].amount {
				return fmt.Errorf("row %d: expected amount %s, got %s", i+1, expected[i].amount, amount)
			}
			log.Printf("    ✓ row %d: %s, %s", i+1, name, amount)
		}
	}

	nullParquetFile := filepath.Join(outputDir, "test_nulls.parquet")
	cmd = exec.Command("duckdb", "-csv", "-noheader", ":memory:", "-c",
		fmt.Sprintf("SELECT value, nullable FROM read_parquet('%s') ORDER BY id", nullParquetFile))
	out, err = cmd.Output()
	if err != nil {
		return fmt.Errorf("duckdb query failed for nulls: %w", err)
	}

	lines = strings.Split(strings.TrimSpace(string(out)), "\n")
	expectedNulls := []struct{ value string; nullable string }{
		{"has value", "not null"},
		{"has value", ""},
		{"", ""},
	}

	for i, line := range lines {
		fields := strings.Split(line, "|")
		if len(fields) >= 2 {
			value := strings.TrimSpace(fields[0])
			nullable := strings.TrimSpace(fields[1])
			if value != expectedNulls[i].value {
				return fmt.Errorf("null row %d: expected value %q, got %q", i+1, expectedNulls[i].value, value)
			}
			log.Printf("    ✓ null row %d: value=%q, nullable=%q", i+1, value, nullable)
		}
	}

	return nil
}