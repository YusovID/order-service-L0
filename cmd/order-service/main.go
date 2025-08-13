package main

import (
	"log"
	"log/slog"

	"github.com/YusovID/order-service/internal/config"
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

	// TODO реализовать сервер

	// TODO создать подключение к Kafka

	// TODO подключить PostgreSQL

	// TODO добавить кэш
}
