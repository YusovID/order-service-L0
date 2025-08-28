package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sync"

	"github.com/YusovID/order-service/internal/config"
	"github.com/YusovID/order-service/internal/models"
	"github.com/YusovID/order-service/internal/storage"
	"github.com/YusovID/order-service/internal/storage/postgres"
	"github.com/redis/go-redis/v9"
)

type Client struct {
	*redis.Client
}

func New(ctx context.Context, cfg config.Redis) (*Client, error) {
	address := net.JoinHostPort(cfg.Host, cfg.Port)

	client := redis.NewClient(&redis.Options{
		Addr:     address,
		Password: cfg.Password,
		DB:       cfg.DB,
	})

	if _, err := client.Ping(ctx).Result(); err != nil {
		return nil, fmt.Errorf("can't ping redis: %v", err)
	}

	return &Client{client}, nil
}

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

func (c *Client) GetOrder(ctx context.Context, orderUID string) (*models.OrderData, error) {
	const fn = "storage.redis.GetOrder"

	orderJSON, err := c.Get(ctx, orderUID).Result()
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

func (c *Client) Fill(ctx context.Context, storage *postgres.Storage, wg *sync.WaitGroup) error {
	const fn = "storage.redis.Fill"

	defer wg.Done()

	orders, err := storage.GetOrders(ctx)
	if err != nil {
		return fmt.Errorf("can't get orders: %v", err)
	}

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
