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
			// generateOrder()

			err := p.PushMessageToQueue(topic, "order")
			if err != nil {
				p.Log.Error("can't push message to queue", sl.Err(err))
			}

			timeToSleep := rand.IntN(MaxTimeToSleep + 1)

			time.Sleep(time.Duration(timeToSleep) * time.Millisecond)
		}
	}
}

func (p *Producer) PushMessageToQueue(topic string, message string) error {
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(message),
	}

	p.Producer.Input() <- msg

	return nil
}

func (p *Producer) HandleResult(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	go func() {
		for success := range p.Producer.Successes() {
			p.Log.Info("message sent successfully",
				slog.Int("partition", int(success.Partition)),
				slog.Int64("offset", success.Offset),
			)
		}
	}()

	go func() {
		for err := range p.Producer.Errors() {
			p.Log.Error("failed to send message", sl.Err(err))
		}
	}()

	<-ctx.Done()
}

func generateOrder() []byte {
	// 	order := `{
	//    "order_uid": "b563feb7b2b84b6test",
	//    "track_number": "WBILMTESTTRACK",
	//    "entry": "WBIL",
	//    "delivery": {
	//       "name": "Test Testov",
	//       "phone": "+9720000000",
	//       "zip": "2639809",
	//       "city": "Kiryat Mozkin",
	//       "address": "Ploshad Mira 15",
	//       "region": "Kraiot",
	//       "email": "test@gmail.com"
	//    },
	//    "payment": {
	//       "transaction": "b563feb7b2b84b6test",
	//       "request_id": "",
	//       "currency": "USD",
	//       "provider": "wbpay",
	//       "amount": 1817,
	//       "payment_dt": 1637907727,
	//       "bank": "alpha",
	//       "delivery_cost": 1500,
	//       "goods_total": 317,
	//       "custom_fee": 0
	//    },
	//    "items": [
	//       {
	//          "chrt_id": 9934930,
	//          "track_number": "WBILMTESTTRACK",
	//          "price": 453,
	//          "rid": "ab4219087a764ae0btest",
	//          "name": "Mascaras",
	//          "sale": 30,
	//          "size": "0",
	//          "total_price": 317,
	//          "nm_id": 2389212,
	//          "brand": "Vivienne Sabo",
	//          "status": 202
	//       }
	//    ],
	//    "locale": "en",
	//    "internal_signature": "",
	//    "customer_id": "test",
	//    "delivery_service": "meest",
	//    "shardkey": "9",
	//    "sm_id": 99,
	//    "date_created": "2021-11-26T06:22:19Z",
	//    "oof_shard": "1"
	// }`

	return []byte{}
}
