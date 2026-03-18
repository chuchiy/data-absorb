package config

import (
	"fmt"

	"github.com/BurntSushi/toml"
)

type Config struct {
	Global    GlobalConfig     `toml:"global"`
	Databases []DatabaseConfig `toml:"databases"`
	Tasks     []TaskConfig     `toml:"tasks"`
}

type GlobalConfig struct {
	Workers       int    `toml:"workers"`
	BatchSize     int    `toml:"batch_size"`
	DefaultFormat string `toml:"default_format"`
	OutputDir     string `toml:"output_dir"`
	Overwrite     bool   `toml:"overwrite"`
	LogLevel      string `toml:"log_level"`
}

type DatabaseConfig struct {
	Name         string `toml:"name"`
	Driver       string `toml:"driver"`
	DSN          string `toml:"dsn"`
	MaxOpenConns int    `toml:"max_open_conns"`
	MaxIdleConns int    `toml:"max_idle_conns"`
}

type TaskConfig struct {
	Name   string   `toml:"name"`
	DB     string   `toml:"db"`
	Tables []string `toml:"tables"`
	Query  string   `toml:"query"`
	Output string   `toml:"output"`
	Format string   `toml:"format"`
}

func Load(path string) (*Config, error) {
	var cfg Config
	_, err := toml.DecodeFile(path, &cfg)
	if err != nil {
		return nil, fmt.Errorf("配置文件解析失败: %w", err)
	}

	if cfg.Global.Workers <= 0 {
		cfg.Global.Workers = 4
	}
	if cfg.Global.BatchSize <= 0 {
		cfg.Global.BatchSize = 10000
	}
	if cfg.Global.DefaultFormat == "" {
		cfg.Global.DefaultFormat = "parquet"
	}
	if cfg.Global.OutputDir == "" {
		cfg.Global.OutputDir = "./output"
	}

	if err := cfg.validate(); err != nil {
		return nil, err
	}

	return &cfg, nil
}

func (c *Config) validate() error {
	dbNames := make(map[string]bool)
	for _, db := range c.Databases {
		if db.Name == "" {
			return fmt.Errorf("数据库配置缺少 name 字段")
		}
		if db.Driver == "" {
			return fmt.Errorf("数据库 %s 缺少 driver 字段", db.Name)
		}
		if db.DSN == "" {
			return fmt.Errorf("数据库 %s 缺少 dsn 字段", db.Name)
		}
		dbNames[db.Name] = true
	}

	for _, task := range c.Tasks {
		if task.DB == "" {
			return fmt.Errorf("任务配置缺少 db 字段")
		}
		if !dbNames[task.DB] {
			return fmt.Errorf("任务引用的数据库 %s 不存在", task.DB)
		}

		hasTables := len(task.Tables) > 0
		hasQuery := task.Query != ""
		hasOutput := task.Output != ""

		if hasTables && hasQuery {
			return fmt.Errorf("任务 %s 不能同时指定 tables 和 query", task.Name)
		}
		if !hasTables && !hasQuery {
			return fmt.Errorf("任务 %s 必须指定 tables 或 query", task.Name)
		}
		if hasQuery && !hasOutput {
			return fmt.Errorf("任务 %s 使用 query 模式时必须指定 output", task.Name)
		}
	}

	return nil
}

func (c *Config) GetTasks() []TaskConfig {
	return c.Tasks
}

func (c *Config) GetDatabase(name string) *DatabaseConfig {
	for i := range c.Databases {
		if c.Databases[i].Name == name {
			return &c.Databases[i]
		}
	}
	return nil
}
