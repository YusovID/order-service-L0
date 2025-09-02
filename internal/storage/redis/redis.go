// Package redis предоставляет реализацию кэш-хранилища с использованием Redis.
// Этот пакет реализует интерфейс `storage.Storage`, что позволяет ему выступать
// в роли быстрого слоя доступа к данным перед обращением к основной базе данных (PostgreSQL).
package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"

	"github.com/YusovID/order-service/internal/config"
	"github.com/YusovID/order-service/internal/models"
	"github.com/YusovID/order-service/internal/storage"
	"github.com/redis/go-redis/v9"
)

// Client является оберткой над стандартным клиентом `redis.Client`,
// что позволяет в будущем расширить его функциональность, не изменяя
// публичный API пакета.
type Client struct {
	*redis.Client
}

// Storage определяет интерфейс для хранилища, из которого будут извлекаться
// данные для наполнения кэша. Это сделано для того, чтобы `redis.Client`
// не зависел напрямую от `postgres.Storage`, следуя принципу инверсии зависимостей.
type Storage interface {
	GetOrders(ctx context.Context) ([]*models.OrderData, error)
}

// New создает и настраивает новый клиент для подключения к Redis.
// Функция проверяет соединение с помощью команды PING и возвращает ошибку,
// если Redis недоступен.
func New(ctx context.Context, cfg config.Redis) (*Client, error) {
	address := net.JoinHostPort(cfg.Host, cfg.Port)

	client := redis.NewClient(&redis.Options{
		Addr:     address,
		Password: cfg.Password,
		DB:       cfg.DB,
	})

	// Проверяем, что соединение с Redis установлено и сервер отвечает.
	if _, err := client.Ping(ctx).Result(); err != nil {
		return nil, fmt.Errorf("can't ping redis: %v", err)
	}

	return &Client{client}, nil
}

// SaveOrder сохраняет данные одного заказа в Redis.
// Данные заказа сериализуются в JSON и сохраняются как строковое значение.
// Ключом является `OrderUID` заказа. Запись не имеет срока жизни (TTL=0).
func (c *Client) SaveOrder(ctx context.Context, orderData *models.OrderData) error {
	const fn = "storage.redis.SaveOrder"

	orderBytes, err := json.Marshal(orderData)
	if err != nil {
		return fmt.Errorf("%s: can't marshal order data: %v", fn, err)
	}

	if err := c.Set(ctx, orderData.OrderUID, orderBytes, 0).Err(); err != nil {
		return fmt.Errorf("%s: can't set order: %v", fn, err)
	}

	return nil
}

// GetOrder извлекает данные заказа из Redis по его `orderUID`.
// Если ключ не найден, функция возвращает ошибку `storage.ErrNoOrder`,
// что позволяет вызывающему коду понять, что нужно обратиться к основной БД.
// Если данные найдены, они десериализуются из JSON в структуру `models.OrderData`.
func (c *Client) GetOrder(ctx context.Context, orderUID string) (*models.OrderData, error) {
	const fn = "storage.redis.GetOrder"

	// Выполняем команду GET.
	orderJSON, err := c.Get(ctx, orderUID).Result()
	// `redis.Nil` - это специальная ошибка, означающая, что ключ не найден.
	// Мы преобразуем ее в нашу доменную ошибку `storage.ErrNoOrder`.
	if errors.Is(err, redis.Nil) {
		return nil, storage.ErrNoOrder
	}
	if err != nil {
		return nil, fmt.Errorf("%s: can't get order: %v", fn, err)
	}

	orderData := &models.OrderData{}
	err = json.Unmarshal([]byte(orderJSON), orderData)
	if err != nil {
		return nil, fmt.Errorf("%s: can't unmarshal order json: %v", fn, err)
	}

	return orderData, nil
}

// Fill загружает все заказы из основного хранилища (например, PostgreSQL)
// и сохраняет их в Redis. Этот метод вызывается при старте приложения
// для "прогрева" кэша, чтобы обеспечить быстрый доступ к уже существующим данным.
func (c *Client) Warm(ctx context.Context, storage Storage) error {
	const fn = "storage.redis.Fill"

	// Получаем все заказы из основного хранилища.
	orders, err := storage.GetOrders(ctx)
	if err != nil {
		return fmt.Errorf("can't get orders: %v", err)
	}

	// Итерируемся по всем заказам и сохраняем каждый в Redis.
	for _, order := range orders {
		orderJSON, err := json.Marshal(order)
		if err != nil {
			return fmt.Errorf("%s: can't marshal order: %v", fn, err)
		}

		if err := c.Set(ctx, order.OrderUID, orderJSON, 0).Err(); err != nil {
			return fmt.Errorf("%s: can't set order: %v", fn, err)
		}
	}

	return nil
}
