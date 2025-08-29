package processor

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"

	"github.com/IBM/sarama"
	"github.com/YusovID/order-service/internal/models"
	"github.com/YusovID/order-service/lib/logger/sl"
	wp "github.com/YusovID/order-service/lib/workerpool"
)

type Storage interface {
	SaveOrder(ctx context.Context, orderData *models.OrderData) error
}

type IPool interface {
	Create()
	Handle(context.Context, *sarama.ConsumerMessage) error
	Wait()
}

type Processor struct {
	Storage    Storage
	orderChan  <-chan *sarama.ConsumerMessage
	commitChan chan<- *sarama.ConsumerMessage
	log        *slog.Logger
}

func New(
	storage Storage,
	orderChan <-chan *sarama.ConsumerMessage,
	commitChan chan<- *sarama.ConsumerMessage,
	log *slog.Logger,
) *Processor {
	return &Processor{
		Storage:    storage,
		orderChan:  orderChan,
		commitChan: commitChan,
		log:        log,
	}
}

func (p *Processor) ProcessOrders(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	const fn = "storage.postgres.ProcessOrder"
	log := p.log.With("fn", fn)

	orders := make([]*sarama.ConsumerMessage, 0, wp.MaxWorkersCount)

	pool := wp.New(p.processOrder)

	for {
		select {
		case <-ctx.Done():
			if len(orders) != 0 {
				p.processBatch(ctx, orders, pool)
			}

			log.Info("stopping processing order by context")
			return

		case order := <-p.orderChan:
			orders = append(orders, order)

			if len(orders) == wp.MaxWorkersCount {
				p.processBatch(ctx, orders, pool)

				orders = make([]*sarama.ConsumerMessage, 0, wp.MaxWorkersCount)
			}
		}
	}
}

func (p *Processor) processBatch(ctx context.Context, orders []*sarama.ConsumerMessage, pool IPool) {
	pool.Create()

	wg := &sync.WaitGroup{}

	for _, order := range orders {
		wg.Add(1)

		go func(currentOrder *sarama.ConsumerMessage) {
			defer wg.Done()

			err := pool.Handle(ctx, currentOrder)
			if err != nil {

				p.log.Error("failed to handle order message", sl.Err(err))
			} else {
				p.commitChan <- currentOrder
			}
		}(order)
	}

	wg.Wait()
	pool.Wait()
}

func (p *Processor) processOrder(ctx context.Context, order *sarama.ConsumerMessage) error {
	p.log.Info("received new order")

	var orderData models.OrderData
	if err := json.Unmarshal(order.Value, &orderData); err != nil {
		p.log.Error("can't unmarshal json", sl.Err(err))

		return fmt.Errorf("can't unmarshal json: %v", err)
	}

	p.log.Info("saving order in database")

	if err := p.Storage.SaveOrder(ctx, &orderData); err != nil {
		p.log.Error("failed to save order in database", sl.Err(err))

		return fmt.Errorf("failed to save order in database: %v", err)
	}

	p.log.Info("saving was successful")

	return nil
}
