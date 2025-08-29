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
	Consumer   sarama.ConsumerGroup
	orderChan  chan<- *sarama.ConsumerMessage
	commitChan <-chan *sarama.ConsumerMessage
	log        *slog.Logger
}

func NewConsumer(
	cfg config.Kafka,
	orderChan chan<- *sarama.ConsumerMessage,
	commitChan <-chan *sarama.ConsumerMessage,
	log *slog.Logger,
) (*Consumer, error) {
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
		Consumer:   cg,
		orderChan:  orderChan,
		commitChan: commitChan,
		log:        log,
	}, nil
}

func (c *Consumer) ProcessMessages(ctx context.Context, topic string, wg *sync.WaitGroup) {
	defer wg.Done()

	const fn = "storage.kafka.ProcessMessages"

	log := c.log.With("fn", fn)

	for {
		select {
		case <-ctx.Done():
			log.Info("stopping message processing")
			return

		default:
			err := c.Consumer.Consume(ctx, []string{topic}, &consumerHandler{
				orderChan:  c.orderChan,
				commitChan: c.commitChan,
				Log:        c.log,
			})
			if err != nil {
				if err == sarama.ErrClosedConsumerGroup {
					c.log.Info("consumer group closed, exiting process messages loop")
					return
				}
				c.log.Error("error from consumer", sl.Err(err))
			}
		}
	}
}

type consumerHandler struct {
	orderChan  chan<- *sarama.ConsumerMessage
	commitChan <-chan *sarama.ConsumerMessage
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

			h.orderChan <- msg

		case msg := <-h.commitChan:
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
