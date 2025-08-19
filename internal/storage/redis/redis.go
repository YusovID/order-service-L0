package redis

import (
	"context"
	"fmt"
	"log/slog"
	"net"

	"github.com/YusovID/order-service/internal/config"
	"github.com/redis/go-redis/v9"
)

type Client struct {
	Cli *redis.Client
}

func New(ctx context.Context, cfg config.Redis, log *slog.Logger) (*Client, error) {
	address := net.JoinHostPort(cfg.Host, cfg.Port)

	client := redis.NewClient(&redis.Options{
		Addr:     address,
		Password: cfg.Password,
		DB:       cfg.DB,
	})

	if _, err := client.Ping(ctx).Result(); err != nil {
		return nil, fmt.Errorf("can't ping redis: %v", err)
	}

	return &Client{Cli: client}, nil
}
