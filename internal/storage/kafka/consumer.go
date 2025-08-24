package kafka

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/YusovID/order-service/internal/config"
	"github.com/YusovID/order-service/lib/logger/sl"
)

type Consumer struct {
	Consumer sarama.ConsumerGroup
	Log      *slog.Logger
}

func NewConsumer(cfg config.Kafka, log *slog.Logger) (*Consumer, error) {
	config := sarama.NewConfig()

	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = sarama.OffsetNewest
	config.Consumer.IsolationLevel = sarama.ReadCommitted
	config.Consumer.Offsets.AutoCommit.Enable = false

	cg, err := sarama.NewConsumerGroup(cfg.BootstrapServers, cfg.Consumer.GroupId, config)
	if err != nil {
		return nil, fmt.Errorf("can't create consumer: %v", err)
	}

	return &Consumer{
		Consumer: cg,
		Log:      log,
	}, nil
}

func (c *Consumer) ProcessMessages(ctx context.Context, topic string, wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		select {
		case <-ctx.Done():
			return

		default:
			if err := c.Consumer.Consume(ctx, []string{topic}, &consumerHandler{Log: c.Log}); err != nil {
				c.Log.Error("error from consumer", sl.Err(err))
			}
		}
	}
}

type consumerHandler struct {
	Log *slog.Logger
}

func (h *consumerHandler) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

func (h *consumerHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (h *consumerHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	go func() {
		for range ticker.C {
			session.Commit()
		}
	}()

	for msg := range claim.Messages() {
		h.Log.Info(
			"recieved message",
			slog.String("key", string(msg.Key)),
			slog.String("value", string(msg.Value)),
		)

		session.MarkMessage(msg, "")
	}

	session.Commit()

	return nil
}
