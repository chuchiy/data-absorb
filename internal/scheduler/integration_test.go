package scheduler

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/data-absorb/data-absorb/internal/config"
	_ "github.com/mattn/go-sqlite3"
)

func TestIntegration_SQLiteToParquet(t *testing.T) {
	outputDir := "./testdata/integration_output"
	os.RemoveAll(outputDir)
	os.MkdirAll(outputDir, 0755)

	cfg := &config.Config{
		Global: config.GlobalConfig{
			Workers:       1,
			BatchSize:     1000,
			DefaultFormat: "parquet",
			OutputDir:     outputDir,
			Overwrite:     true,
		},
		Databases: []config.DatabaseConfig{
			{
				Name:   "testdb",
				Driver: "sqlite3",
				DSN:    "/home/dht/agentic/data-absorb/testdata/test.db",
			},
		},
		Tasks: []config.TaskConfig{
			{
				DB:     "testdb",
				Tables: []string{"test_types"},
			},
		},
	}

	s := New(cfg)
	if err := s.Run(context.Background()); err != nil {
		t.Fatalf("Scheduler run failed: %v", err)
	}

	outputFile := filepath.Join(outputDir, "test_types.parquet")
	if _, err := os.Stat(outputFile); err != nil {
		t.Fatalf("Output file not created: %v", err)
	}

	t.Logf("Output file created: %s", outputFile)
	info, _ := os.Stat(outputFile)
	t.Logf("File size: %d bytes", info.Size())

	if info.Size() == 0 {
		t.Error("Output file is empty")
	}
}

func TestIntegration_EmptyTable(t *testing.T) {
	outputDir := "./testdata/integration_empty"
	os.RemoveAll(outputDir)
	os.MkdirAll(outputDir, 0755)

	cfg := &config.Config{
		Global: config.GlobalConfig{
			Workers:       1,
			BatchSize:     100,
			DefaultFormat: "parquet",
			OutputDir:     outputDir,
			Overwrite:     true,
		},
		Databases: []config.DatabaseConfig{
			{
				Name:   "testdb",
				Driver: "sqlite3",
				DSN:    "/home/dht/agentic/data-absorb/testdata/test.db",
			},
		},
		Tasks: []config.TaskConfig{
			{
				DB:     "testdb",
				Tables: []string{"test_empty"},
			},
		},
	}

	s := New(cfg)
	if err := s.Run(context.Background()); err != nil {
		t.Fatalf("Scheduler run failed: %v", err)
	}

	outputFile := filepath.Join(outputDir, "test_empty.parquet")
	if _, err := os.Stat(outputFile); err != nil {
		t.Fatalf("Output file not created: %v", err)
	}

	info, _ := os.Stat(outputFile)
	t.Logf("Empty table file size: %d bytes", info.Size())
}

func TestIntegration_MultipleTables(t *testing.T) {
	outputDir := "./testdata/integration_multi"
	os.RemoveAll(outputDir)
	os.MkdirAll(outputDir, 0755)

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
				DSN:    "/home/dht/agentic/data-absorb/testdata/test.db",
			},
		},
		Tasks: []config.TaskConfig{
			{
				DB:     "testdb",
				Tables: []string{"test_types", "test_empty"},
			},
		},
	}

	s := New(cfg)
	if err := s.Run(context.Background()); err != nil {
		t.Fatalf("Scheduler run failed: %v", err)
	}

	expectedFiles := []string{"test_types.parquet", "test_empty.parquet"}
	for _, f := range expectedFiles {
		outputFile := filepath.Join(outputDir, f)
		if _, err := os.Stat(outputFile); err != nil {
			t.Errorf("Expected output file not found: %s", f)
		}
	}
}
