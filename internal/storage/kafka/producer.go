package kafka

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/YusovID/order-service/internal/config"
	orderGen "github.com/YusovID/order-service/lib/generator/order"
	"github.com/YusovID/order-service/lib/logger/sl"
)

const (
	// MaxTimeToSleep определяет максимальную случайную задержку (в миллисекундах)
	// между отправкой сообщений. Это помогает эмулировать неравномерный поток данных.
	MaxTimeToSleep = 1000
)

// Producer представляет собой обертку над асинхронным продюсером `sarama.AsyncProducer`.
// Он отвечает за генерацию и отправку сообщений о заказах в Kafka.
type Producer struct {
	Producer sarama.AsyncProducer
	Log      *slog.Logger
}

// NewProducer создает и настраивает нового асинхронного продюсера Kafka.
//
// Конфигурация включает важные параметры для обеспечения надежности доставки:
//   - Idempotence (идемпотентность): гарантирует, что сообщения не будут дублироваться
//     при повторных отправках.
//   - RequiredAcks: уровень подтверждения доставки от брокеров.
//   - TransactionalID: позволяет отправлять сообщения в рамках транзакций,
//     обеспечивая атомарность записи в несколько партиций.
func NewProducer(cfg config.Kafka, log *slog.Logger) (*Producer, error) {
	config := sarama.NewConfig()

	config.Producer.Return.Successes = true // Включаем получение подтверждений об успехе.
	config.Producer.Return.Errors = true    // Включаем получение сообщений об ошибках.
	config.Producer.RequiredAcks = sarama.RequiredAcks(cfg.Producer.Acks)
	config.Producer.Idempotent = cfg.Producer.EnableIdempotence
	config.Net.MaxOpenRequests = 1 // Важно для идемпотентности и транзакций.
	config.Producer.Retry.Max = cfg.Producer.Retries
	config.Producer.Transaction.ID = cfg.Producer.TransactionalId

	p, err := sarama.NewAsyncProducer(cfg.BootstrapServers, config)
	if err != nil {
		return nil, fmt.Errorf("can't create producer: %v", err)
	}

	return &Producer{
		Producer: p,
		Log:      log,
	}, nil
}

// ProduceMessage запускает бесконечный цикл генерации и отправки сообщений.
//
// Логика работы:
//  1. Начинает транзакцию в Kafka.
//  2. В цикле генерирует новые данные о заказе.
//  3. Отправляет их как сообщение в Kafka.
//  4. Делает случайную задержку для эмуляции реального потока.
//  5. Периодически (раз в секунду) коммитит текущую транзакцию и начинает новую.
//  6. При отмене контекста (graceful shutdown) коммитит последнюю транзакцию и завершает работу.
func (p *Producer) ProduceMessage(ctx context.Context, topic string, wg *sync.WaitGroup) {
	defer wg.Done()

	// Начинаем первую транзакцию.
	if err := p.Producer.BeginTxn(); err != nil {
		p.Log.Error("can't begin transaction", sl.Err(err))
		return
	}

	// Тикер для периодического коммита транзакций.
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		// Обработка сигнала завершения.
		case <-ctx.Done():
			// Пытаемся закоммитить последнюю пачку сообщений.
			if err := p.Producer.CommitTxn(); err != nil {
				// Если коммит не удался, откатываем транзакцию.
				if abortErr := p.Producer.AbortTxn(); abortErr != nil {
					p.Log.Error("can't abort transaction", sl.Err(abortErr))
				}
				p.Log.Error("can't commit transaction", sl.Err(err))
			}
			return

		// Периодический коммит по тикеру.
		case <-ticker.C:
			if err := p.Producer.CommitTxn(); err != nil {
				if abortErr := p.Producer.AbortTxn(); abortErr != nil {
					p.Log.Error("can't abort transaction", sl.Err(abortErr))
				}
				p.Log.Error("can't commit transaction", sl.Err(err))
			}

			// Начинаем новую транзакцию.
			if err := p.Producer.BeginTxn(); err != nil {
				p.Log.Error("can't begin transaction", sl.Err(err))
				time.Sleep(100 * time.Millisecond) // Короткая пауза перед повторной попыткой.
				continue
			}

		// Основной цикл генерации и отправки.
		default:
			// Генерируем случайные данные для заказа.
			orderUID, order := orderGen.GenerateOrder()

			msg := &sarama.ProducerMessage{}
			msg.Key = sarama.StringEncoder(orderUID) // Ключ сообщения для партиционирования.
			msg.Value = sarama.StringEncoder(order)  // Тело сообщения.

			err := p.PushMessageToQueue(topic, msg)
			if err != nil {
				p.Log.Error("can't push message to queue", sl.Err(err))
			}

			// Создаем случайную задержку.
			timeToSleep := rand.IntN(MaxTimeToSleep + 1)
			time.Sleep(time.Duration(timeToSleep) * time.Millisecond)
		}
	}
}

// PushMessageToQueue отправляет одно сообщение в очередь продюсера.
// Так как продюсер асинхронный, эта функция не блокируется.
func (p *Producer) PushMessageToQueue(topic string, message *sarama.ProducerMessage) error {
	message.Topic = topic
	// Отправляем сообщение во внутренний канал (input channel) продюсера.
	p.Producer.Input() <- message
	return nil
}

// HandleResult обрабатывает результаты отправки сообщений (успехи и ошибки).
// Эта функция должна работать в отдельной горутине, чтобы асинхронно
// читать из каналов `Successes()` и `Errors()` продюсера.
func (p *Producer) HandleResult(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		select {
		case <-ctx.Done():
			p.Log.Info("stopping to handle results")
			return
		// Канал для успешных сообщений.
		case success := <-p.Producer.Successes():
			p.Log.Info("message sent successfully",
				slog.Int("partition", int(success.Partition)),
				slog.Int64("offset", success.Offset),
			)
		// Канал для сообщений с ошибками.
		case err := <-p.Producer.Errors():
			p.Log.Error("failed to send message", sl.Err(err))
		}
	}
}
