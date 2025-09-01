// Package kafka предоставляет реализации для взаимодействия с Apache Kafka,
// включая продюсера для отправки сообщений и консьюмера для их получения.
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

// batchsize - это количество сообщений, после обработки которых
// будет произведен коммит офсетов.
const batchsize = 100

// Consumer представляет собой обертку над `sarama.ConsumerGroup` для
// удобной интеграции в приложение. Он читает сообщения из Kafka и
// передает их в `orderChan` для дальнейшей обработки.
type Consumer struct {
	Consumer   sarama.ConsumerGroup
	orderChan  chan<- *sarama.ConsumerMessage // Канал для отправки полученных сообщений обработчику.
	commitChan <-chan *sarama.ConsumerMessage // Канал для получения сообщений, которые нужно "закоммитить".
	log        *slog.Logger
}

// NewConsumer создает и настраивает новую группу консьюмеров Kafka.
// Он инициализирует конфигурацию sarama, устанавливая ручное управление
// коммитами и другие важные параметры, после чего создает ConsumerGroup.
func NewConsumer(
	cfg config.Kafka,
	orderChan chan<- *sarama.ConsumerMessage,
	commitChan <-chan *sarama.ConsumerMessage,
	log *slog.Logger,
) (*Consumer, error) {
	config := sarama.NewConfig()

	config.Consumer.Return.Errors = true                  // Включаем возврат ошибок в канал Errors().
	config.Consumer.Offsets.Initial = sarama.OffsetOldest // Начинаем чтение с самого старого сообщения, если нет сохраненного офсета.
	config.Consumer.IsolationLevel = sarama.ReadCommitted // Читаем только "закоммиченные" сообщения от транзакционных продюсеров.
	config.Consumer.Offsets.AutoCommit.Enable = false     // Отключаем автокоммит, так как управляем им вручную.

	// Создаем новую группу консьюмеров.
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

// ProcessMessages запускает бесконечный цикл прослушивания сообщений из Kafka.
// При отмене контекста `ctx` (graceful shutdown) цикл завершается.
// Метод использует `consumerHandler` для фактической обработки сообщений.
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
			// `Consume` блокирует выполнение и запускает сессию консьюмера.
			// Он будет выполняться до тех пор, пока не произойдет ошибка
			// или не будет отменен контекст.
			err := c.Consumer.Consume(ctx, []string{topic}, &consumerHandler{
				orderChan:  c.orderChan,
				commitChan: c.commitChan,
				Log:        c.log,
			})
			if err != nil {
				// sarama.ErrClosedConsumerGroup - это ожидаемая ошибка при штатном завершении.
				if err == sarama.ErrClosedConsumerGroup {
					c.log.Info("consumer group closed, exiting process messages loop")
					return
				}
				c.log.Error("error from consumer", sl.Err(err))
			}
		}
	}
}

// consumerHandler реализует интерфейс `sarama.ConsumerGroupHandler`.
// Sarama вызывает методы этого типа во время сессии консьюмера.
type consumerHandler struct {
	orderChan  chan<- *sarama.ConsumerMessage
	commitChan <-chan *sarama.ConsumerMessage
	Log        *slog.Logger
}

// Setup вызывается один раз в начале сессии консьюмера, перед ConsumeClaim.
func (h *consumerHandler) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

// Cleanup вызывается один раз в конце сессии, после завершения всех циклов ConsumeClaim.
func (h *consumerHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim является основным циклом обработки сообщений.
// Он запускается для каждой партиции топика, назначенной этому консьюмеру.
func (h *consumerHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	processed := 0 // Счетчик обработанных сообщений для батч-коммита.

	// Тикер для периодического коммита, если сообщений мало.
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		// Читаем сообщение из канала партиции (`claim.Messages()`).
		case msg, ok := <-claim.Messages():
			if !ok {
				// Канал закрыт, значит сессия завершается.
				return nil
			}

			h.Log.Info(
				"received message",
				slog.Int("partition", int(msg.Partition)),
				slog.Int("offset", int(msg.Offset)),
			)
			// Отправляем сообщение на обработку в `Processor`.
			h.orderChan <- msg

		// Читаем из канала подтверждений.
		case msg := <-h.commitChan:
			// Помечаем сообщение как обработанное. Фактический коммит произойдет позже.
			session.MarkMessage(msg, "")
			processed++

			// Если накопили достаточное количество, делаем коммит.
			if processed >= batchsize {
				h.Log.Info("committing messages")
				session.Commit()
				processed = 0
			}

		// Если контекст сессии завершен (например, при ребалансировке или shutdown).
		case <-session.Context().Done():
			// Коммитим все, что было обработано, и выходим.
			session.Commit()
			return nil
		}
	}
}
