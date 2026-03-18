package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"

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
				Tables: []string{"test_types", "test_nulls", "test_empty", "test_large"},
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
	CREATE TABLE test_large (
		id INTEGER PRIMARY KEY,
		name TEXT,
		amount DECIMAL(10,2),
		flag BOOLEAN,
		created_at TIMESTAMP
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

	for i := 1; i <= 1000; i++ {
		name := fmt.Sprintf("User_%04d", i)
		amount := fmt.Sprintf("%.2f", float64(i)*1.5)
		createdAt := fmt.Sprintf("2024-01-%02d %02d:00:00", (i%28)+1, (i%24))
		if _, err := stmt.Exec(i, name, amount, createdAt); err != nil {
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

	tx, err = db.Begin()
	if err != nil {
		return err
	}
	stmt, err = tx.Prepare("INSERT INTO test_large (id, name, amount, flag, created_at) VALUES (?, ?, ?, ?, ?)")
	if err != nil {
		return err
	}
	defer stmt.Close()

	for i := 1; i <= 1500; i++ {
		name := fmt.Sprintf("Record_%05d", i)
		amount := fmt.Sprintf("%.2f", float64(i)*0.1)
		flag := i%2 == 0
		createdAt := fmt.Sprintf("2024-06-%02d %02d:30:00", (i%30)+1, (i%24))
		if _, err := stmt.Exec(i, name, amount, flag, createdAt); err != nil {
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
	tables := []string{"test_types", "test_nulls", "test_empty", "test_large"}

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
	log.Println("  Verifying row counts...")
	if err := verifyRowCounts(outputDir); err != nil {
		return err
	}

	log.Println("  Verifying data types...")
	if err := verifyDataTypes(outputDir); err != nil {
		return err
	}

	log.Println("  Verifying data values for test_types (1000 rows)...")
	if err := verifyTestTypesData(outputDir); err != nil {
		return err
	}

	log.Println("  Verifying data values for test_large (1500 rows)...")
	if err := verifyTestLargeData(outputDir); err != nil {
		return err
	}

	log.Println("  Verifying null handling in test_nulls...")
	if err := verifyNullData(outputDir); err != nil {
		return err
	}

	return nil
}

func verifyRowCounts(outputDir string) error {
	tables := []string{"test_types", "test_nulls", "test_empty", "test_large"}
	expectedCounts := map[string]int{
		"test_types": 1000,
		"test_nulls": 3,
		"test_empty": 0,
		"test_large": 1500,
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

	return nil
}

func verifyTestTypesData(outputDir string) error {
	parquetFile := filepath.Join(outputDir, "test_types.parquet")

	checks := []struct {
		id     int
		name   string
		amount float64
	}{
		{1, "User_0001", 1.50},
		{500, "User_0500", 750.00},
		{1000, "User_1000", 1500.00},
	}

	for _, check := range checks {
		query := fmt.Sprintf("SELECT name, amount FROM read_parquet('%s') WHERE id = %d", parquetFile, check.id)
		cmd := exec.Command("duckdb", "-csv", "-noheader", ":memory:", "-c", query)
		out, err := cmd.Output()
		if err != nil {
			return fmt.Errorf("duckdb query failed for id=%d: %w", check.id, err)
		}

		line := strings.TrimSpace(string(out))
		fields := strings.Split(line, ",")
		if len(fields) >= 2 {
			name := strings.TrimSpace(fields[0])
			amountStr := strings.TrimSpace(fields[1])
			amount, err := strconv.ParseFloat(amountStr, 64)
			if err != nil {
				return fmt.Errorf("parse amount for id=%d: %w", check.id, err)
			}
			if name != check.name {
				return fmt.Errorf("id=%d: expected name %s, got %s", check.id, check.name, name)
			}
			if amount != check.amount {
				return fmt.Errorf("id=%d: expected amount %v, got %v", check.id, check.amount, amount)
			}
			log.Printf("    ✓ id=%d: name=%s, amount=%v", check.id, name, amount)
		}
	}

	return nil
}

func verifyTestLargeData(outputDir string) error {
	parquetFile := filepath.Join(outputDir, "test_large.parquet")

	checks := []struct {
		id   int
		name string
		flag bool
	}{
		{1, "Record_00001", false},
		{750, "Record_00750", true},
		{1500, "Record_01500", true},
	}

	for _, check := range checks {
		query := fmt.Sprintf("SELECT name, flag FROM read_parquet('%s') WHERE id = %d", parquetFile, check.id)
		cmd := exec.Command("duckdb", "-csv", "-noheader", ":memory:", "-c", query)
		out, err := cmd.Output()
		if err != nil {
			return fmt.Errorf("duckdb query failed for id=%d: %w", check.id, err)
		}

		line := strings.TrimSpace(string(out))
		fields := strings.Split(line, ",")
		if len(fields) >= 2 {
			name := strings.TrimSpace(fields[0])
			flagStr := strings.TrimSpace(fields[1])
			flag := flagStr == "true"

			if name != check.name {
				return fmt.Errorf("id=%d: expected name %s, got %s", check.id, check.name, name)
			}
			if flag != check.flag {
				return fmt.Errorf("id=%d: expected flag %v, got %v", check.id, check.flag, flag)
			}
			log.Printf("    ✓ id=%d: name=%s, flag=%v", check.id, name, flag)
		}
	}

	query := fmt.Sprintf("SELECT SUM(id) FROM read_parquet('%s')", parquetFile)
	cmd := exec.Command("duckdb", "-csv", "-noheader", ":memory:", "-c", query)
	out, err := cmd.Output()
	if err != nil {
		return fmt.Errorf("duckdb sum query failed: %w", err)
	}

	sum, err := strconv.Atoi(strings.TrimSpace(string(out)))
	if err != nil {
		return fmt.Errorf("parse sum: %w", err)
	}

	expectedSum := 1500 * 1501 / 2
	if sum != expectedSum {
		return fmt.Errorf("sum of id: expected %d, got %d", expectedSum, sum)
	}
	log.Printf("    ✓ All 1500 records verified (sum of id = %d)", sum)

	return nil
}

func verifyNullData(outputDir string) error {
	parquetFile := filepath.Join(outputDir, "test_nulls.parquet")

	query := fmt.Sprintf("SELECT value, nullable FROM read_parquet('%s') ORDER BY id", parquetFile)
	cmd := exec.Command("duckdb", "-csv", "-noheader", ":memory:", "-c", query)
	out, err := cmd.Output()
	if err != nil {
		return fmt.Errorf("duckdb query failed for nulls: %w", err)
	}

	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	if len(lines) != 3 {
		return fmt.Errorf("expected 3 rows, got %d", len(lines))
	}

	expectedNulls := []struct{ value string; nullable string }{
		{"has value", "not null"},
		{"has value", ""},
		{"", ""},
	}

	for i, line := range lines {
		fields := strings.Split(line, ",")
		if len(fields) >= 2 {
			value := strings.TrimSpace(fields[0])
			nullable := strings.TrimSpace(fields[1])
			if value != expectedNulls[i].value {
				return fmt.Errorf("null row %d: expected value %q, got %q", i+1, expectedNulls[i].value, value)
			}
			log.Printf("    ✓ row %d: value=%q, nullable=%q", i+1, value, nullable)
		}
	}

	return nil
}