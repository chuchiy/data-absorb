package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"os/signal"

	_ "github.com/denisenkom/go-mssqldb"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
	_ "github.com/mattn/go-sqlite3"
	_ "github.com/sijms/go-ora/v2"

	"github.com/alexflint/go-arg"
	stdr2 "github.com/go-logr/stdr"

	"github.com/data-absorb/data-absorb/internal/config"
	"github.com/data-absorb/data-absorb/internal/scheduler"
	"github.com/data-absorb/data-absorb/pkg/types"
)

func init() {
	_ = sql.Drivers()
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	if err := Run(ctx, os.Args[1:]); err != nil {
		log.Fatal(err)
	}
}

func Run(ctx context.Context, args []string) error {
	var a types.Args
	arg.MustParse(&a)

	if a.Version {
		fmt.Printf("data-absorb version %s\n", types.Version)
		return nil
	}

	logLevel := a.LogLevel
	var verbosity int
	switch logLevel {
	case "debug":
		verbosity = 1
	case "info":
		verbosity = 0
	case "warn":
		verbosity = -1
	case "error":
		verbosity = -2
	default:
		verbosity = 0
	}
	stdr2.SetVerbosity(verbosity)
	logger := stdr2.New(log.New(os.Stderr, "", 0))

	logger.Info("Loading config", "config", a.Config)
	cfg, err := config.Load(a.Config)
	if err != nil {
		return fmt.Errorf("E001: 配置解析失败: %w", err)
	}

	logger.Info("Config loaded", "databases", len(cfg.Databases), "tasks", len(cfg.Tasks), "outputDir", cfg.Global.OutputDir)

	s := scheduler.New(cfg, logger)
	if err := s.Run(ctx); err != nil {
		return fmt.Errorf("E006: 任务执行失败: %w", err)
	}

	logger.Info("All tasks completed successfully")
	return nil
}
