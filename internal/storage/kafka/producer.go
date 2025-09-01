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
	MaxTimeToSleep = 1000
)

type Producer struct {
	Producer sarama.AsyncProducer
	Log      *slog.Logger
}

func NewProducer(cfg config.Kafka, log *slog.Logger) (*Producer, error) {
	config := sarama.NewConfig()

	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	config.Producer.RequiredAcks = sarama.RequiredAcks(cfg.Producer.Acks)
	config.Producer.Idempotent = cfg.Producer.EnableIdempotence
	config.Net.MaxOpenRequests = 1
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

func (p *Producer) ProduceMessage(ctx context.Context, topic string, wg *sync.WaitGroup) {
	defer wg.Done()

	if err := p.Producer.BeginTxn(); err != nil {
		p.Log.Error("can't begin transaction", sl.Err(err))
		return
	}

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			if err := p.Producer.CommitTxn(); err != nil {
				if abortErr := p.Producer.AbortTxn(); abortErr != nil {
					p.Log.Error("can't abort transaction", sl.Err(err))
				}
				p.Log.Error("can't commit transaction", sl.Err(err))
			}

			return

		case <-ticker.C:
			if err := p.Producer.CommitTxn(); err != nil {
				if abortErr := p.Producer.AbortTxn(); abortErr != nil {
					p.Log.Error("can't abort transaction", sl.Err(err))
				}

				p.Log.Error("can't commit transaction", sl.Err(err))
			}

			if err := p.Producer.BeginTxn(); err != nil {
				p.Log.Error("can't begin transaction", sl.Err(err))

				time.Sleep(100 * time.Millisecond)
				continue
			}

		default:
			orderUID, order := orderGen.GenerateOrder()

			msg := &sarama.ProducerMessage{}

			msg.Key = sarama.StringEncoder(orderUID)
			msg.Value = sarama.StringEncoder(order)

			err := p.PushMessageToQueue(topic, msg)
			if err != nil {
				p.Log.Error("can't push message to queue", sl.Err(err))
			}

			timeToSleep := rand.IntN(MaxTimeToSleep + 1)

			time.Sleep(time.Duration(timeToSleep) * time.Millisecond)
		}
	}
}

func (p *Producer) PushMessageToQueue(topic string, message *sarama.ProducerMessage) error {
	message.Topic = topic

	p.Producer.Input() <- message

	return nil
}

func (p *Producer) HandleResult(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		select {
		case <-ctx.Done():
			p.Log.Info("stopping to handle results")
			return

		case success := <-p.Producer.Successes():
			p.Log.Info("message sent successfully",
				slog.Int("partition", int(success.Partition)),
				slog.Int64("offset", success.Offset),
			)

		case err := <-p.Producer.Errors():
			p.Log.Error("failed to send message", sl.Err(err))
		}
	}
}
