package main

import (
	"errors"
	"fmt"
	"log"
	"os"

	"github.com/YusovID/order-service/internal/config"
	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/ilyakaznacheev/cleanenv"
	"github.com/joho/godotenv"
)

type MigrationCfg struct {
	ConnStr         string
	MigrationsPath  string
	MigrationsTable string
}

func init() {
	err := godotenv.Load()
	if err != nil {
		log.Fatalf("can't load config: %v", err)
	}
}

func main() {
	migration := MustLoad()

	m, err := migrate.New(
		"file://"+migration.MigrationsPath,
		fmt.Sprintf("%s?sslmode=disable&x-migrations-table=%s", migration.ConnStr, migration.MigrationsTable),
	)
	if err != nil {
		log.Fatalf("can't create new migration: %v", err)
	}

	if err := m.Up(); err != nil {
		if errors.Is(err, migrate.ErrNoChange) {
			fmt.Println("no migrations to apply")

			return
		}

		log.Fatalf("can't do migrations: %v", err)
	}

	fmt.Println("migrations applied successfully")
}

func MustLoad() *MigrationCfg {
	configPath := os.Getenv("CONFIG_PATH")
	if configPath == "" {
		log.Fatalf("CONFIG_PATH is not set")
	}

	if _, err := os.Stat(configPath); err != nil {
		log.Fatalf("file '%s' doesn't exist: %v", configPath, err)
	}

	migrationsPath := os.Getenv("MIGRATIONS_PATH")
	if migrationsPath == "" {
		log.Fatalf("MIGRATIONS_PATH is not set")
	}

	migrationsTable := os.Getenv("MIGRATIONS_TABLE")
	if migrationsTable == "" {
		log.Fatalf("MIGRATIONS_TABLE is not set")
	}

	var cfg config.Config
	if err := cleanenv.ReadConfig(configPath, &cfg); err != nil {
		log.Fatalf("can't read config: %v", err)
	}

	connStr := fmt.Sprintf("postgres://%s:%s@%s:%s/%s",
		cfg.Postgres.Username,
		cfg.Postgres.Password,
		cfg.Postgres.Host,
		cfg.Postgres.Port,
		cfg.Postgres.Database,
	)

	return &MigrationCfg{
		ConnStr:         connStr,
		MigrationsPath:  migrationsPath,
		MigrationsTable: migrationsTable,
	}
}
