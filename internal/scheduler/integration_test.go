package scheduler

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-logr/logr"
	"github.com/data-absorb/data-absorb/internal/config"
	_ "github.com/mattn/go-sqlite3"
)

type testLogSink struct {
	t *testing.T
}

func (t *testLogSink) Init(info logr.RuntimeInfo) {}

func (t *testLogSink) Enabled(level int) bool { return true }

func (t *testLogSink) Info(level int, msg string, keysAndValues ...interface{}) {
	t.t.Logf(msg, keysAndValues...)
}

func (t *testLogSink) Error(err error, msg string, keysAndValues ...interface{}) {
	t.t.Logf("ERROR: %s: %v", msg, err)
}

func (t *testLogSink) WithValues(keysAndValues ...interface{}) logr.LogSink {
	return t
}

func (t *testLogSink) WithName(name string) logr.LogSink {
	return t
}

func getLogger(t *testing.T) logr.Logger {
	return logr.New(&testLogSink{t: t})
}

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

	s := New(cfg, getLogger(t))
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

	s := New(cfg, getLogger(t))
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

	s := New(cfg, getLogger(t))
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