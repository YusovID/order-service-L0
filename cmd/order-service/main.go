package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"

	"github.com/YusovID/order-service/internal/config"
	"github.com/YusovID/order-service/internal/http-server/handlers/url/get"
	mwLogger "github.com/YusovID/order-service/internal/http-server/middleware/logger"
	"github.com/YusovID/order-service/internal/storage/kafka"
	"github.com/YusovID/order-service/internal/storage/postgres"
	"github.com/YusovID/order-service/internal/storage/redis"
	"github.com/YusovID/order-service/lib/logger/sl"
	"github.com/YusovID/order-service/lib/logger/slogpretty"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())

	wg := &sync.WaitGroup{}

	cfg := config.MustLoad()

	log := slogpretty.SetupLogger(cfg.Env)

	log.Info("starting order service", slog.String("env", cfg.Env))

	storage, err := postgres.New(cfg.Postgres, log)
	if err != nil {
		log.Error("failed to init storage", sl.Err(err))

		os.Exit(1)
	}

	log.Info("storage init successful")

	orderChan := make(chan []byte)

	wg.Add(1)
	go storage.ProcessOrder(ctx, orderChan, wg)

	cache, err := redis.New(ctx, cfg.Redis)
	if err != nil {
		log.Error("failed to init cache", sl.Err(err))

		os.Exit(1)
	}

	log.Info("cache init successful")

	wg.Add(1)
	go func() {
		err := cache.Fill(ctx, storage, wg)
		if err != nil {
			log.Error("failed to fill cache", sl.Err(err))
		}
	}()

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
	go c.ProcessMessages(ctx, cfg.Kafka.Topic, orderChan, wg)

	router := chi.NewRouter()

	router.Use(middleware.RequestID)
	router.Use(middleware.Logger)
	router.Use(mwLogger.New(log))
	router.Use(middleware.Recoverer)
	router.Use(middleware.URLFormat)

	router.Get("/order/{order_uid}", get.New(ctx, log, cache, storage))

	router.Handle("/", http.FileServer(http.Dir("./web")))

	log.Info("starting server", slog.String("address", cfg.HTTPServer.Address))

	srv := &http.Server{
		Addr:         cfg.HTTPServer.Address,
		Handler:      router,
		ReadTimeout:  cfg.HTTPServer.Timeout,
		WriteTimeout: cfg.HTTPServer.Timeout,
		IdleTimeout:  cfg.HTTPServer.IdleTimeout,
	}

	go func() {
		if err := srv.ListenAndServe(); err != nil {
			slog.Error("failed to start server", sl.Err(err))

			os.Exit(1)
		}
	}()

	<-sigchan
	cancel()

	wg.Wait()

	log.Info("shutting down consumer")
	if err = c.Consumer.Close(); err != nil {
		slog.Error("failed to close consumer", sl.Err(err))

		os.Exit(1)
	}

	log.Info("stopping server")

	err = srv.Shutdown(ctx)
	if err != nil {
		log.Error("failed to shutdown server", sl.Err(err))

		os.Exit(1)
	}
}
