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

const batchsize = 100

type Consumer struct {
	Consumer sarama.ConsumerGroup
	Log      *slog.Logger
}

func NewConsumer(cfg config.Kafka, log *slog.Logger) (*Consumer, error) {
	config := sarama.NewConfig()

	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
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

func (c *Consumer) ProcessMessages(ctx context.Context, topic string, orderChan, commitChan chan *sarama.ConsumerMessage, wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		err := c.Consumer.Consume(ctx, []string{topic}, &consumerHandler{
			OrderChan:  orderChan,
			CommitChan: commitChan,
			Log:        c.Log,
		})
		if err != nil {
			if err == sarama.ErrClosedConsumerGroup {
				c.Log.Info("consumer group closed, exiting process messages loop")
				return
			}
			c.Log.Error("error from consumer", sl.Err(err))
		}

		if ctx.Err() != nil {
			return
		}
	}
}

type consumerHandler struct {
	OrderChan  chan *sarama.ConsumerMessage
	CommitChan chan *sarama.ConsumerMessage
	Log        *slog.Logger
}

func (h *consumerHandler) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

func (h *consumerHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (h *consumerHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	processed := 0

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case msg, ok := <-claim.Messages():
			if !ok {
				return nil
			}

			h.Log.Info(
				"recieved message",
				slog.Int("partition", int(msg.Partition)),
				slog.Int("offset", int(msg.Offset)),
			)

			h.OrderChan <- msg

		case msg := <-h.CommitChan:
			session.MarkMessage(msg, "")

			processed++

			if processed >= batchsize {
				h.Log.Info("commiting messages")
				session.Commit()
				processed = 0
			}

		case <-session.Context().Done():
			session.Commit()

			return nil
		}
	}
}
