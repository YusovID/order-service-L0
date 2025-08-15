package main

import (
	"log"
	"log/slog"
	"os"

	"github.com/YusovID/order-service/internal/config"
	"github.com/YusovID/order-service/internal/storage/postgres"
	"github.com/YusovID/order-service/lib/logger/sl"
	"github.com/YusovID/order-service/lib/logger/slogpretty"
	"github.com/joho/godotenv"
)

func init() {
	err := godotenv.Load()
	if err != nil {
		log.Fatalf("can't load env")
	}
}

func main() {
	cfg := config.MustLoad()

	log := slogpretty.SetupLogger(cfg.Env)

	log.Info("starting order service", slog.String("env", cfg.Env))

	storage, err := postgres.New(cfg.Postgres, log)
	if err != nil {
		log.Error("failed to init storage", sl.Err(err))
		os.Exit(1)
	}

	log.Info("storage initialization successful")

	_ = storage

	// TODO добавить кэш

	// TODO создать подключение к Kafka

	// TODO реализовать сервер
}
