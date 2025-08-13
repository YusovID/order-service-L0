package config

import (
	"log"
	"os"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/ilyakaznacheev/cleanenv"
)

type Config struct {
	Env     string  `yaml:"env" env-required:"true"`
	Kafka   Kafka   `yaml:"kafka" env-required:"true"`
	Storage Storage `yaml:"storage" env-required:"true"`
}

type Kafka struct {
	Producer *kafka.ConfigMap `yaml:"producer" env-required:"true"`
	Consumer *kafka.ConfigMap `yaml:"consumer" env-required:"true"`
}

type Storage struct {
	Postgres Postgres `yaml:"postgres"`
}

type Postgres struct {
	Host     string `yaml:"host" env-required:"true"`
	Port     string `yaml:"port" env-required:"true"`
	Database string `yaml:"database" env-required:"true"`
	Username string `yaml:"username" env-required:"true"`
	Password string `yaml:"password" env-required:"true"`
}

func MustLoad() *Config {
	configPath := os.Getenv("CONFIG_PATH")
	if configPath == "" {
		log.Fatal("CONFIG_PATH is not set")
	}

	if _, err := os.Stat(configPath); err != nil {
		log.Fatalf("file doesn't exist: %s", configPath)
	}

	var cfg Config

	if err := cleanenv.ReadConfig(configPath, &cfg); err != nil {
		log.Fatalf("can't read config: %v", err)
	}

	return &cfg
}
