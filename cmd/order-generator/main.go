// package main является точкой входа для сервиса-генератора заказов.
// Его основная задача — создавать случайные данные о заказах и отправлять их
// в виде сообщений в топик Apache Kafka.
// Сервис поддерживает graceful shutdown: при получении сигналов SIGINT или SIGTERM
// он корректно завершает работу, дожидаясь окончания всех активных горутин
// и закрывая соединение с Kafka.
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

// main инициализирует и запускает сервис генерации заказов.
//
// Функция выполняет следующие шаги:
// 1. Создает контекст для управления жизненным циклом приложения и graceful shutdown.
// 2. Загружает конфигурацию из файла и переменных окружения.
// 3. Инициализирует логгер в зависимости от окружения (prod, dev, local).
// 4. Создает и настраивает асинхронного продюсера для Kafka.
// 5. Настраивает обработку системных сигналов (SIGINT, SIGTERM) для корректного завершения.
// 6. Запускает в отдельных горутинах процессы генерации сообщений и обработки ответов от Kafka.
// 7. Ожидает сигнала о завершении, после чего инициирует остановку всех процессов.
// 8. Корректно закрывает соединение с продюсером Kafka.
func main() {
	// Создаем корневой контекст с функцией отмены для управления graceful shutdown.
	ctx, cancel := context.WithCancel(context.Background())

	// Загружаем конфигурацию. В случае ошибки приложение завершится.
	cfg := config.MustLoad()

	// Настраиваем логгер в соответствии с текущим окружением (ENV).
	log := slogpretty.SetupLogger(cfg.Env)

	log.Info("starting order generator", slog.String("env", cfg.Env))

	// Инициализируем продюсера Kafka.
	p, err := kafka.NewProducer(cfg.Kafka, log)
	if err != nil {
		log.Error("failed to init producer", sl.Err(err))
		os.Exit(1)
	}
	log.Info("producer init successful")

	// Создаем канал для прослушивания системных сигналов.
	sigchan := make(chan os.Signal, 1)
	// Регистрируем нотификацию о сигналах SIGINT (Ctrl+C) и SIGTERM.
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	// WaitGroup для ожидания завершения всех запущенных горутин.
	wg := &sync.WaitGroup{}

	// Запускаем горутину, которая будет генерировать и отправлять сообщения в Kafka.
	wg.Add(1)
	go p.ProduceMessage(ctx, cfg.Kafka.Topic, wg)

	// Запускаем горутину для обработки ответов от Kafka (успех/ошибка).
	wg.Add(1)
	go p.HandleResult(ctx, wg)

	// Блокируем выполнение до получения сигнала в канал sigchan.
	<-sigchan
	// После получения сигнала вызываем cancel(), что приведет к завершению
	// контекста ctx и сигнализирует всем горутинам о необходимости остановиться.
	cancel()

	// Ожидаем, пока все горутины, добавленные в wg, завершат свою работу.
	wg.Wait()

	log.Info("stopping producer")
	// Закрываем продюсера, освобождая ресурсы.
	p.Producer.Close()
}
