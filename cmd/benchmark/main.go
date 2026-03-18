package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/data-absorb/data-absorb/internal/config"
	"github.com/data-absorb/data-absorb/internal/scheduler"
	"github.com/go-logr/stdr"
	_ "github.com/mattn/go-sqlite3"
)

type BenchmarkResult struct {
	Workers   int
	Tables    int
	Duration  time.Duration
	RowCount  int64
	Throughput float64
}

func main() {
	stdr.SetVerbosity(0)
	log := stdr.New(nil)

	testCases := []struct {
		name   string
		dbType string
		rows   int
		tables int
	}{
		{"SQLite-4Tables", "sqlite", 10000, 4},
		{"SQLite-8Tables", "sqlite", 10000, 8},
		{"PostgreSQL-4Tables", "postgres", 10000, 4},
		{"PostgreSQL-8Tables", "postgres", 10000, 8},
	}

	workerCounts := []int{1, 2, 4, 8}

	results := make(map[string][]BenchmarkResult)

	for _, tc := range testCases {
		var dbPath, dsn string

		if tc.dbType == "sqlite" {
			dbPath = filepath.Join("/tmp", fmt.Sprintf("benchmark_%d.db", tc.rows))
			setupSQLiteDB(dbPath, tc.rows, tc.tables)
			dsn = dbPath
		} else {
			dsn = "postgres://testuser:testpass@localhost:5432/testdb?sslmode=disable"
			setupPostgreSQLData(tc.rows, tc.tables)
		}

		for _, workers := range workerCounts {
			fmt.Printf("Running: %s, workers=%d\n", tc.name, workers)

			outputDir := filepath.Join("/tmp", fmt.Sprintf("bench_%s_w%d", tc.name, workers))
			os.RemoveAll(outputDir)
			os.MkdirAll(outputDir, 0755)

			var cfg *config.Config
			if tc.dbType == "sqlite" {
				cfg = &config.Config{
					Global: config.GlobalConfig{
						Workers:      workers,
						BatchSize:    1000,
						DefaultFormat: "parquet",
						OutputDir:    outputDir,
						Overwrite:    true,
					},
					Databases: []config.DatabaseConfig{
						{
							Name:   "bench",
							Driver: "sqlite3",
							DSN:    dbPath,
						},
					},
					Tasks: []config.TaskConfig{
						{
							DB:     "bench",
							Tables: generateTableNames(tc.tables),
						},
					},
				}
			} else {
				tableNames := make([]string, tc.tables)
				for i := 0; i < tc.tables; i++ {
					tableNames[i] = fmt.Sprintf("test_table_%d", i+1)
				}
				cfg = &config.Config{
					Global: config.GlobalConfig{
						Workers:      workers,
						BatchSize:    1000,
						DefaultFormat: "parquet",
						OutputDir:    outputDir,
						Overwrite:    true,
					},
					Databases: []config.DatabaseConfig{
						{
							Name:   "bench",
							Driver: "pgx",
							DSN:    dsn,
						},
					},
					Tasks: []config.TaskConfig{
						{
							DB:     "bench",
							Tables: tableNames,
						},
					},
				}
			}

			start := time.Now()
			sched := scheduler.New(cfg, log)
			ctx := context.Background()

			if err := sched.Run(ctx); err != nil {
				fmt.Printf("  Error: %v\n", err)
				continue
			}

			duration := time.Since(start)
			rowCount := int64(tc.rows * tc.tables)
			throughput := float64(rowCount) / duration.Seconds()

			result := BenchmarkResult{
				Workers:   workers,
				Tables:    tc.tables,
				Duration:  duration,
				RowCount:  rowCount,
				Throughput: throughput,
			}

			key := fmt.Sprintf("%s-%dT", tc.name, tc.tables)
			results[key] = append(results[key], result)
			fmt.Printf("  Duration: %v, Throughput: %.2f rows/sec\n", duration.Round(time.Millisecond), throughput)
		}

		if tc.dbType == "sqlite" {
			os.Remove(dbPath)
		}
	}

	printSummary(results)
}

func generateTableNames(count int) []string {
	names := make([]string, count)
	for i := 0; i < count; i++ {
		names[i] = fmt.Sprintf("test_table_%d", i+1)
	}
	return names
}

func setupSQLiteDB(dbPath string, rows, tables int) {
	os.Remove(dbPath)

	db, err := openSQLite(dbPath)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	for t := 1; t <= tables; t++ {
		tableName := fmt.Sprintf("test_table_%d", t)
		_, err = db.Exec(fmt.Sprintf(`
			CREATE TABLE %s (
				id INTEGER PRIMARY KEY,
				value1 INTEGER,
				value2 INTEGER,
				value3 INTEGER,
				value4 INTEGER,
				text1 VARCHAR(100),
				text2 VARCHAR(100),
				decimal1 DECIMAL(18,4),
				decimal2 DECIMAL(18,4)
			);
		`, tableName))
		if err != nil {
			panic(err)
		}

		tx, err := db.Begin()
		if err != nil {
			panic(err)
		}
		stmt, err := tx.Prepare(fmt.Sprintf(`
			INSERT INTO %s VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, tableName))
		if err != nil {
			panic(err)
		}

		for i := 1; i <= rows; i++ {
			_, err = stmt.Exec(
				i,
				i*1, i*2, i*3, i*4,
				fmt.Sprintf("text_%d", i),
				fmt.Sprintf("desc_%d", i),
				float64(i)*0.1234,
				float64(i)*5.6789,
			)
			if err != nil {
				panic(err)
			}
		}

		if err := tx.Commit(); err != nil {
			panic(err)
		}
		stmt.Close()
	}
}

func setupPostgreSQLData(rows, tables int) {
	// PostgreSQL data should already exist from init script
	// This function can create additional tables if needed
	_ = rows
	_ = tables
}

func openSQLite(path string) (*sql.DB, error) {
	return sql.Open("sqlite3", path)
}

func printSummary(results map[string][]BenchmarkResult) {
	fmt.Println("\n" + strings.Repeat("=", 80))
	fmt.Println("BENCHMARK SUMMARY")
	fmt.Println(strings.Repeat("=", 80))

	for testName, testResults := range results {
		fmt.Printf("\n%s\n", testName)
		fmt.Printf("%-10s %-15s %-15s\n", "Workers", "Duration", "Throughput")
		fmt.Println(strings.Repeat("-", 50))
		for _, r := range testResults {
			fmt.Printf("%-10d %-15v %-15.2f\n", r.Workers, r.Duration.Round(time.Millisecond), r.Throughput)
		}
	}

	fmt.Println("\n" + strings.Repeat("=", 80))
	fmt.Println("SCALABILITY ANALYSIS")
	fmt.Println(strings.Repeat("=", 80))

	for testName, testResults := range results {
		if len(testResults) > 1 {
			baseThroughput := testResults[0].Throughput
			fmt.Printf("\n%s:\n", testName)
			for _, r := range testResults {
				speedup := r.Throughput / baseThroughput
				efficiency := (speedup / float64(r.Workers)) * 100
				fmt.Printf("  %d workers: %.2fx speedup (efficiency: %.1f%%)\n",
					r.Workers, speedup, efficiency)
			}
		}
	}
}