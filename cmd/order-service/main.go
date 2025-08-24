package main

import (
	"context"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"sync"

	"github.com/YusovID/order-service/internal/config"
	"github.com/YusovID/order-service/internal/storage/kafka"
	"github.com/YusovID/order-service/internal/storage/postgres"
	"github.com/YusovID/order-service/internal/storage/redis"
	"github.com/YusovID/order-service/lib/logger/sl"
	"github.com/YusovID/order-service/lib/logger/slogpretty"
	"github.com/joho/godotenv"
)

func init() {
	err := godotenv.Load()
	if err != nil {
		log.Fatalf("can't load env: %v", err)
	}
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())

	wg := &sync.WaitGroup{}

	cfg := config.MustLoad()

	log := slogpretty.SetupLogger(cfg.Env)

	log.Info("starting order service", slog.String("env", cfg.Env))

	_, err := postgres.New(cfg.Postgres)
	if err != nil {
		log.Error("failed to init storage", sl.Err(err))
		os.Exit(1)
	}

	log.Info("storage init successful")

	_, err = redis.New(ctx, cfg.Redis)
	if err != nil {
		log.Error("failed to init cache", sl.Err(err))
		os.Exit(1)
	}

	log.Info("cache init successful")

	c, err := kafka.NewConsumer(cfg.Kafka, log)
	if err != nil {
		log.Error("failed to init consumer", sl.Err(err))
		os.Exit(1)
	}

	log.Info("consumer init successful")

	log.Info("listening messages")

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, os.Interrupt)

	wg.Add(1)
	go c.ProcessMessages(ctx, cfg.Kafka.Topic, wg)

	<-sigchan
	cancel()

	wg.Wait()

	log.Info("shutting down consumer")
	c.Consumer.Close()

	// TODO реализовать сервер
}
