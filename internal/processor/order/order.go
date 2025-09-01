// Package processor содержит основную бизнес-логику для обработки заказов.
// Он действует как связующее звено между получением сообщений из Kafka
// и сохранением их в хранилище данных.
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

// Storage определяет интерфейс для хранилища, куда будут сохраняться заказы.
// Использование интерфейса позволяет легко подменять реализацию,
// например, для тестов (in-memory) или при смене БД.
type Storage interface {
	SaveOrder(ctx context.Context, orderData *models.OrderData) error
}

// IPool определяет интерфейс для пула воркеров.
// Это позволяет абстрагироваться от конкретной реализации worker pool.
type IPool interface {
	Create()
	Handle(context.Context, *sarama.ConsumerMessage) error
	Wait()
}

// Processor инкапсулирует логику обработки заказов.
// Он читает сообщения из канала `orderChan`, обрабатывает их и отправляет
// сообщения для коммита в `commitChan`.
type Processor struct {
	Storage    Storage
	orderChan  <-chan *sarama.ConsumerMessage // Канал для получения сообщений от Kafka-консьюмера.
	commitChan chan<- *sarama.ConsumerMessage // Канал для отправки подтверждений (коммитов) консьюмеру.
	log        *slog.Logger
}

// New создает новый экземпляр Processor.
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

// ProcessOrders запускает бесконечный цикл для чтения и обработки сообщений о заказах.
//
// Функция работает как демон: она постоянно слушает канал `orderChan`.
// Для повышения производительности сообщения обрабатываются пачками (батчами).
// При накоплении достаточного количества сообщений или по истечении времени
// они отправляются на параллельную обработку в пул воркеров.
//
// Принимает `ctx` для graceful shutdown: при отмене контекста цикл завершается.
func (p *Processor) ProcessOrders(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	const fn = "processor.order.ProcessOrders"
	log := p.log.With("fn", fn)

	// Слайс для накопления сообщений перед пакетной обработкой.
	orders := make([]*sarama.ConsumerMessage, 0, wp.MaxWorkersCount)
	pool := wp.New(p.processOrder) // Создаем пул воркеров с нашей функцией обработки.

	for {
		select {
		// Если контекст отменен, обрабатываем оставшиеся сообщения и выходим.
		case <-ctx.Done():
			if len(orders) != 0 {
				p.processBatch(ctx, orders, pool)
			}
			log.Info("stopping processing order by context")
			return

		// Читаем новое сообщение из канала.
		case order := <-p.orderChan:
			orders = append(orders, order)

			// Когда накоплена пачка, отправляем ее на обработку.
			if len(orders) == wp.MaxWorkersCount {
				p.processBatch(ctx, orders, pool)
				// Очищаем слайс для следующей пачки.
				orders = make([]*sarama.ConsumerMessage, 0, wp.MaxWorkersCount)
			}
		}
	}
}

// processBatch отправляет пачку сообщений на параллельную обработку в пул воркеров.
func (p *Processor) processBatch(ctx context.Context, orders []*sarama.ConsumerMessage, pool IPool) {
	pool.Create() // Инициализируем (заполняем) пул воркерами.
	wg := &sync.WaitGroup{}

	for _, order := range orders {
		wg.Add(1)
		go func(currentOrder *sarama.ConsumerMessage) {
			defer wg.Done()
			// Передаем сообщение в пул. Handle заблокируется, пока не освободится воркер.
			err := pool.Handle(ctx, currentOrder)
			if err != nil {
				// TODO реализовать retry + DLQ

				// Если обработка не удалась, логируем ошибку. Сообщение не будет подтверждено.
				p.log.Error("failed to handle order message", sl.Err(err))
			} else {
				// Если обработка успешна, отправляем сообщение в канал для коммита.
				p.commitChan <- currentOrder
			}
		}(order)
	}

	wg.Wait()
	pool.Wait() // Ожидаем, пока все воркеры в пуле завершат работу.
}

// processOrder является основной функцией-обработчиком одного сообщения.
// Она десериализует JSON, валидирует данные и сохраняет их в хранилище.
func (p *Processor) processOrder(ctx context.Context, order *sarama.ConsumerMessage) error {
	p.log.Info("received new order")

	var orderData models.OrderData
	// Десериализуем тело сообщения в структуру OrderData.
	if err := json.Unmarshal(order.Value, &orderData); err != nil {
		p.log.Error("can't unmarshal json, skipping message", sl.Err(err))
		// Возвращаем nil, чтобы "пропустить" невалидное сообщение и подтвердить его,
		// иначе оно будет постоянно повторяться. Если бы нужна была Dead Letter Queue,
		// логика была бы другой.
		return fmt.Errorf("can't unmarshal json: %v", err)
	}

	p.log.Info("saving order in database", slog.String("order_uid", orderData.OrderUID))

	// Сохраняем заказ в базу данных.
	if err := p.Storage.SaveOrder(ctx, &orderData); err != nil {
		p.log.Error("failed to save order in database", sl.Err(err))
		return fmt.Errorf("failed to save order in database: %w", err)
	}

	p.log.Info("saving was successful", slog.String("order_uid", orderData.OrderUID))

	return nil
}
