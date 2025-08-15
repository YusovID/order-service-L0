package postgres

import (
	"database/sql"
	"fmt"
	"log/slog"

	"github.com/YusovID/order-service/internal/config"
	_ "github.com/lib/pq"
)

type Storage struct {
	db *sql.DB
}

func New(cfg config.Postgres, log *slog.Logger) (*Storage, error) {
	const fn = "storage.postgres.New"
	log.With("fn", fn)

	log.Info("starting storage initialization...")

	connStr := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable",
		cfg.Username,
		cfg.Password,
		cfg.Host,
		cfg.Port,
		cfg.Database,
	)

	// open database
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("%s: can't open database: %v", fn, err)
	}

	// check if we can connect to database
	if err = db.Ping(); err != nil {
		return nil, fmt.Errorf("%s: can't connect to database: %v", fn, err)
	}

	return &Storage{db: db}, nil
}
