package main

import (
	"context"
	"log"
	"log/slog"
	"os"

	"github.com/YusovID/order-service/internal/config"
	"github.com/YusovID/order-service/internal/storage/postgres"
	"github.com/YusovID/order-service/internal/storage/redis"
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
	ctx := context.Background()

	cfg := config.MustLoad()

	log := slogpretty.SetupLogger(cfg.Env)

	log.Info("starting order service", slog.String("env", cfg.Env))

	_, err := postgres.New(cfg.Postgres, log)
	if err != nil {
		log.Error("failed to init storage", sl.Err(err))
		os.Exit(1)
	}

	log.Info("storage initialization successful")

	_, err = redis.New(ctx, cfg.Redis, log)
	if err != nil {
		log.Error("failed to init cache", sl.Err(err))
		os.Exit(1)
	}

	log.Info("cache initialization successful")

	// TODO создать подключение к Kafka

	// TODO реализовать сервер
}
