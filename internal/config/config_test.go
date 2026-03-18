package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoad_ValidConfig(t *testing.T) {
	content := `
[global]
workers = 4
batch_size = 10000
default_format = "parquet"
output_dir = "./output"
overwrite = false
log_level = "info"

[[databases]]
name = "test_db"
driver = "sqlite"
dsn = "./test.db"
max_open_conns = 10
max_idle_conns = 5

[[tasks]]
db = "test_db"
tables = ["users", "orders"]
`

	tmpDir := t.TempDir()
	configFile := filepath.Join(tmpDir, "config.toml")
	if err := os.WriteFile(configFile, []byte(content), 0644); err != nil {
		t.Fatalf("Failed to write config file: %v", err)
	}

	cfg, err := Load(configFile)
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}

	if cfg.Global.Workers != 4 {
		t.Errorf("Workers = %d, want 4", cfg.Global.Workers)
	}
	if cfg.Global.BatchSize != 10000 {
		t.Errorf("BatchSize = %d, want 10000", cfg.Global.BatchSize)
	}
	if cfg.Global.DefaultFormat != "parquet" {
		t.Errorf("DefaultFormat = %s, want parquet", cfg.Global.DefaultFormat)
	}
	if len(cfg.Databases) != 1 {
		t.Errorf("Databases count = %d, want 1", len(cfg.Databases))
	}
	if cfg.Databases[0].Name != "test_db" {
		t.Errorf("Database name = %s, want test_db", cfg.Databases[0].Name)
	}
	if len(cfg.Tasks) != 1 {
		t.Errorf("Tasks count = %d, want 1", len(cfg.Tasks))
	}
}

func TestLoad_Defaults(t *testing.T) {
	content := `
[[databases]]
name = "test_db"
driver = "sqlite"
dsn = "./test.db"

[[tasks]]
db = "test_db"
tables = ["users"]
`

	tmpDir := t.TempDir()
	configFile := filepath.Join(tmpDir, "config.toml")
	if err := os.WriteFile(configFile, []byte(content), 0644); err != nil {
		t.Fatalf("Failed to write config file: %v", err)
	}

	cfg, err := Load(configFile)
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}

	if cfg.Global.Workers != 4 {
		t.Errorf("Workers = %d, want 4 (default)", cfg.Global.Workers)
	}
	if cfg.Global.BatchSize != 10000 {
		t.Errorf("BatchSize = %d, want 10000 (default)", cfg.Global.BatchSize)
	}
	if cfg.Global.DefaultFormat != "parquet" {
		t.Errorf("DefaultFormat = %s, want parquet (default)", cfg.Global.DefaultFormat)
	}
}

func TestLoad_InvalidConfig(t *testing.T) {
	tests := []struct {
		name    string
		content string
		wantErr bool
	}{
		{
			name: "missing db field",
			content: `
[[tasks]]
db = "missing_db"
tables = ["users"]
`,
			wantErr: true,
		},
		{
			name: "missing tables and query",
			content: `
[[databases]]
name = "test_db"
driver = "sqlite"
dsn = "./test.db"

[[tasks]]
db = "test_db"
`,
			wantErr: true,
		},
		{
			name: "both tables and query",
			content: `
[[databases]]
name = "test_db"
driver = "sqlite"
dsn = "./test.db"

[[tasks]]
db = "test_db"
tables = ["users"]
query = "SELECT * FROM users"
`,
			wantErr: true,
		},
		{
			name: "query without output",
			content: `
[[databases]]
name = "test_db"
driver = "sqlite"
dsn = "./test.db"

[[tasks]]
db = "test_db"
query = "SELECT * FROM users"
`,
			wantErr: true,
		},
		{
			name: "missing database name",
			content: `
[[databases]]
driver = "sqlite"
dsn = "./test.db"

[[tasks]]
db = "test_db"
tables = ["users"]
`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			configFile := filepath.Join(tmpDir, "config.toml")
			if err := os.WriteFile(configFile, []byte(tt.content), 0644); err != nil {
				t.Fatalf("Failed to write config file: %v", err)
			}

			_, err := Load(configFile)
			if (err != nil) != tt.wantErr {
				t.Errorf("Load() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestGetDatabase(t *testing.T) {
	content := `
[[databases]]
name = "db1"
driver = "sqlite"
dsn = "./test1.db"

[[databases]]
name = "db2"
driver = "sqlite"
dsn = "./test2.db"

[[tasks]]
db = "db1"
tables = ["users"]
`

	tmpDir := t.TempDir()
	configFile := filepath.Join(tmpDir, "config.toml")
	if err := os.WriteFile(configFile, []byte(content), 0644); err != nil {
		t.Fatalf("Failed to write config file: %v", err)
	}

	cfg, err := Load(configFile)
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}

	db1 := cfg.GetDatabase("db1")
	if db1 == nil {
		t.Error("GetDatabase(db1) returned nil")
	}

	db2 := cfg.GetDatabase("db2")
	if db2 == nil {
		t.Error("GetDatabase(db2) returned nil")
	}

	db3 := cfg.GetDatabase("nonexistent")
	if db3 != nil {
		t.Error("GetDatabase(nonexistent) should return nil")
	}
}

func TestConfig_QueryMode(t *testing.T) {
	content := `
[[databases]]
name = "test_db"
driver = "sqlite"
dsn = "./test.db"

[[tasks]]
db = "test_db"
query = "SELECT id, name FROM users WHERE created_at > '2024-01-01'"
output = "recent_users.parquet"
`

	tmpDir := t.TempDir()
	configFile := filepath.Join(tmpDir, "config.toml")
	if err := os.WriteFile(configFile, []byte(content), 0644); err != nil {
		t.Fatalf("Failed to write config file: %v", err)
	}

	cfg, err := Load(configFile)
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}

	if len(cfg.Tasks) != 1 {
		t.Fatalf("Tasks count = %d, want 1", len(cfg.Tasks))
	}

	task := cfg.Tasks[0]
	if task.Query == "" {
		t.Error("Task query is empty")
	}
	if task.Output != "recent_users.parquet" {
		t.Errorf("Task output = %s, want recent_users.parquet", task.Output)
	}
}
