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

	setLogLevel(a.LogLevel)

	log.Printf("Loading config from: %s", a.Config)
	cfg, err := config.Load(a.Config)
	if err != nil {
		return fmt.Errorf("E001: 配置解析失败: %w", err)
	}

	log.Printf("Config loaded: %d databases, %d tasks", len(cfg.Databases), len(cfg.Tasks))
	log.Printf("Output dir: %s", cfg.Global.OutputDir)

	s := scheduler.New(cfg)
	if err := s.Run(ctx); err != nil {
		return fmt.Errorf("E006: 任务执行失败: %w", err)
	}

	log.Printf("All tasks completed successfully")
	return nil
}

func setLogLevel(level string) {
	switch level {
	case "debug":
		log.SetFlags(log.LstdFlags | log.Lshortfile)
		log.SetOutput(os.Stderr)
	case "info":
		log.SetFlags(log.LstdFlags)
		log.SetOutput(os.Stderr)
	case "warn":
		log.SetFlags(log.LstdFlags)
		log.SetOutput(os.Stderr)
	case "error":
		log.SetFlags(log.LstdFlags)
		log.SetOutput(os.Stderr)
	default:
		log.SetFlags(log.LstdFlags)
		log.SetOutput(os.Stderr)
	}
}
