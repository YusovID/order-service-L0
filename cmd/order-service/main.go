// Package main является точкой входа для основного микросервиса обработки заказов.
//
// Сервис выполняет следующие ключевые функции:
//  1. Подписывается на топик в Apache Kafka для получения сообщений о новых заказах.
//  2. Обрабатывает полученные сообщения: валидирует и сохраняет данные в базу данных PostgreSQL.
//  3. Кэширует данные заказов в Redis для обеспечения быстрого доступа.
//  4. При старте восстанавливает кэш из данных, хранящихся в PostgreSQL.
//  5. Запускает HTTP-сервер с API-эндпоинтом для получения информации о заказе по его ID.
//  6. Предоставляет простой веб-интерфейс для взаимодействия с API.
//
// Приложение спроектировано с поддержкой graceful shutdown, что позволяет корректно
// завершать все активные процессы (обработку сообщений, HTTP-сервер) при получении
// сигнала об остановке.
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"

	"github.com/IBM/sarama"
	"github.com/YusovID/order-service/internal/config"
	"github.com/YusovID/order-service/internal/http-server/handlers/url/get"
	mwLogger "github.com/YusovID/order-service/internal/http-server/middleware/logger"
	processor "github.com/YusovID/order-service/internal/processor/order"
	"github.com/YusovID/order-service/internal/storage/kafka"
	"github.com/YusovID/order-service/internal/storage/postgres"
	"github.com/YusovID/order-service/internal/storage/redis"
	"github.com/YusovID/order-service/lib/logger/sl"
	"github.com/YusovID/order-service/lib/logger/slogpretty"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
)

// main инициализирует и запускает все компоненты сервиса.
//
// Процесс запуска включает:
//   - Настройку контекста для graceful shutdown.
//   - Загрузку конфигурации и инициализацию логгера.
//   - Подключение к PostgreSQL (основное хранилище).
//   - Создание каналов для обмена сообщениями между Kafka-консьюмером и обработчиком.
//   - Запуск обработчика заказов (processor) в отдельной горутине.
//   - Подключение к Redis (кэш).
//   - Запуск процесса наполнения кэша из PostgreSQL в отдельной горутине.
//   - Инициализацию Kafka-консьюмера и запуск прослушивания сообщений.
//   - Настройку и запуск HTTP-сервера с API и веб-интерфейсом.
//   - Ожидание сигнала завершения (SIGINT, SIGTERM) для корректной остановки всех компонентов.
func main() {
	// Создаем корневой контекст с функцией отмены для управления graceful shutdown.
	ctx, cancel := context.WithCancel(context.Background())
	// WaitGroup для ожидания завершения всех фоновых процессов.
	wg := &sync.WaitGroup{}

	// Загружаем конфигурацию.
	cfg := config.MustLoad()

	// Настраиваем логгер.
	log := slogpretty.SetupLogger(cfg.Env)

	log.Info("starting order service", slog.String("env", cfg.Env))

	// Инициализируем подключение к PostgreSQL.
	storage, err := postgres.New(cfg.Postgres, log)
	if err != nil {
		log.Error("failed to init storage", sl.Err(err))
		os.Exit(1)
	}
	log.Info("storage init successful")

	// Каналы для передачи сообщений от консьюмера к обработчику (orderChan)
	// и для подтверждения обработки обратно консьюмеру (commitChan).
	orderChan := make(chan *sarama.ConsumerMessage)
	commitChan := make(chan *sarama.ConsumerMessage)

	// Создаем экземпляр обработчика заказов.
	processor := processor.New(storage, orderChan, commitChan, log)

	// Запускаем горутину, которая будет постоянно читать из orderChan и обрабатывать заказы.
	wg.Add(1)
	go processor.ProcessOrders(ctx, wg)

	// Инициализируем подключение к Redis.
	cache, err := redis.New(ctx, cfg.Redis)
	if err != nil {
		log.Error("failed to init cache", sl.Err(err))
		os.Exit(1)
	}
	log.Info("cache init successful")

	// Запускаем горутину для первоначального заполнения кэша данными из PostgreSQL.
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := cache.Fill(ctx, storage)
		if err != nil {
			log.Error("failed to fill cache", sl.Err(err))
		}
	}()

	// Инициализируем Kafka-консьюмера.
	c, err := kafka.NewConsumer(cfg.Kafka, orderChan, commitChan, log)
	if err != nil {
		log.Error("failed to init consumer", sl.Err(err))
		os.Exit(1)
	}
	log.Info("consumer init successful")

	log.Info("listening messages")
	// Запускаем горутину для чтения сообщений из Kafka.
	wg.Add(1)
	go c.ProcessMessages(ctx, cfg.Kafka.Topic, wg)

	// Настраиваем HTTP-роутер.
	router := chi.NewRouter()
	router.Use(middleware.RequestID) // Добавляет ID каждому запросу.
	router.Use(middleware.Logger)    // Стандартный логгер chi.
	router.Use(mwLogger.New(log))    // Наш кастомный логгер на базе slog.
	router.Use(middleware.Recoverer) // Восстанавливается после паник.
	router.Use(middleware.URLFormat) // Форматирует URL.

	// Регистрируем API-хендлер для получения заказа по ID.
	router.Get("/order/{order_uid}", get.New(log, cache, storage))
	// Отдаем статичные файлы для веб-интерфейса.
	router.Handle("/", http.FileServer(http.Dir("./web")))

	log.Info("starting server", slog.String("address", cfg.HTTPServer.Address))

	// Создаем и настраиваем HTTP-сервер.
	srv := &http.Server{
		Addr:         cfg.HTTPServer.Address,
		Handler:      router,
		ReadTimeout:  cfg.HTTPServer.Timeout,
		WriteTimeout: cfg.HTTPServer.Timeout,
		IdleTimeout:  cfg.HTTPServer.IdleTimeout,
	}

	// Запускаем HTTP-сервер в отдельной горутине.
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := srv.ListenAndServe(); err != nil {
			slog.Error("failed to start server", sl.Err(err))
			os.Exit(1)
		}
	}()

	// Ожидаем сигнал для начала graceful shutdown.
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, os.Interrupt)
	<-sigchan
	cancel() // Отменяем контекст, сигнализируя всем горутинам о завершении.

	// Ждем завершения всех фоновых процессов.
	wg.Wait()

	// Корректно останавливаем Kafka-консьюмер.
	log.Info("shutting down consumer")
	if err = c.Consumer.Close(); err != nil {
		slog.Error("failed to close consumer", sl.Err(err))
		os.Exit(1)
	}

	// Корректно останавливаем HTTP-сервер.
	log.Info("stopping server")
	if err := srv.Shutdown(context.Background()); err != nil {
		log.Error("failed to shutdown server", sl.Err(err))
		os.Exit(1)
	}
}
