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
	Duration  time.Duration
	RowCount  int64
	Throughput float64
}

func main() {
	stdr.SetVerbosity(0)
	log := stdr.New(nil)

	testCases := []struct {
		name string
		rows int
	}{
		{"10K", 10000},
		{"100K", 100000},
		{"1M", 1000000},
	}

	workerCounts := []int{1, 2, 4, 8}

	results := make(map[string][]BenchmarkResult)

	for _, tc := range testCases {
		dbPath := filepath.Join("/tmp", fmt.Sprintf("benchmark_%d.db", tc.rows))
		
		setupBenchmarkDB(dbPath, tc.rows)
		
		for _, workers := range workerCounts {
			fmt.Printf("Running benchmark: %s rows, %d workers\n", tc.name, workers)
			
			outputDir := filepath.Join("/tmp", fmt.Sprintf("bench_%d_w%d", tc.rows, workers))
			os.RemoveAll(outputDir)
			os.MkdirAll(outputDir, 0755)
			
			cfg := &config.Config{
				Global: config.GlobalConfig{
					Workers:      workers,
					BatchSize:    10000,
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
						Tables: []string{"test_data"},
					},
				},
			}

			start := time.Now()
			sched := scheduler.New(cfg, log)
			ctx := context.Background()
			
			if err := sched.Run(ctx); err != nil {
				fmt.Printf("Error: %v\n", err)
				continue
			}
			
			duration := time.Since(start)
			throughput := float64(tc.rows) / duration.Seconds()
			
			result := BenchmarkResult{
				Workers:   workers,
				Duration: duration,
				RowCount:  int64(tc.rows),
				Throughput: throughput,
			}
			
			results[tc.name] = append(results[tc.name], result)
			fmt.Printf("  Duration: %v, Throughput: %.2f rows/sec\n", duration, throughput)
		}
		
		os.Remove(dbPath)
	}

	printSummary(results)
}

func setupBenchmarkDB(dbPath string, rows int) {
	os.Remove(dbPath)
	
	db, err := openSQLite(dbPath)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	_, err = db.Exec(`
		CREATE TABLE test_data (
			id INTEGER PRIMARY KEY,
			value1 INTEGER,
			value2 INTEGER,
			value3 INTEGER,
			value4 INTEGER,
			value5 INTEGER,
			value6 INTEGER,
			value7 INTEGER,
			value8 INTEGER,
			text1 VARCHAR(100),
			text2 VARCHAR(100),
			decimal1 DECIMAL(18,4),
			decimal2 DECIMAL(18,4)
		);
	`)
	if err != nil {
		panic(err)
	}

	tx, err := db.Begin()
	if err != nil {
		panic(err)
	}
	stmt, err := tx.Prepare(`
		INSERT INTO test_data VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		panic(err)
	}
	defer stmt.Close()

	for i := 1; i <= rows; i++ {
		_, err = stmt.Exec(
			i,
			i*1, i*2, i*3, i*4, i*5, i*6, i*7, i*8,
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
}

func openSQLite(path string) (*sql.DB, error) {
	return sql.Open("sqlite3", path)
}

func printSummary(results map[string][]BenchmarkResult) {
	fmt.Println("\n" + strings.Repeat("=", 80))
	fmt.Println("BENCHMARK SUMMARY")
	fmt.Println(strings.Repeat("=", 80))
	
	for testName, testResults := range results {
		fmt.Printf("\nTest: %s rows\n", testName)
		fmt.Printf("%-10s %-15s %-15s\n", "Workers", "Duration", "Throughput")
		fmt.Println("-")
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
			fmt.Printf("\n%s rows:\n", testName)
			for _, r := range testResults {
				speedup := r.Throughput / baseThroughput
				efficiency := (speedup / float64(r.Workers)) * 100
				fmt.Printf("  %d workers: %.2fx speedup (efficiency: %.1f%%)\n", 
					r.Workers, speedup, efficiency)
			}
		}
	}
}