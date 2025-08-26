package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/YusovID/order-service/internal/config"
	"github.com/YusovID/order-service/internal/storage/kafka"
	"github.com/YusovID/order-service/lib/logger/sl"
	"github.com/YusovID/order-service/lib/logger/slogpretty"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())

	cfg := config.MustLoad()

	log := slogpretty.SetupLogger(cfg.Env)

	log.Info("starting order generator", slog.String("env", cfg.Env))

	p, err := kafka.NewProducer(cfg.Kafka, log)
	if err != nil {
		log.Error("failed to init producer", sl.Err(err))
		os.Exit(1)
	}

	log.Info("producer init successful")

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	wg := &sync.WaitGroup{}

	wg.Add(1)
	go p.ProduceMessage(ctx, cfg.Kafka.Topic, wg)

	wg.Add(1)
	go p.HandleResult(ctx, wg)

	<-sigchan
	cancel()

	wg.Wait()

	log.Info("stopping producer")
	p.Producer.Close()
}
