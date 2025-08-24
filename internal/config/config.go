package config

import (
	"log"
	"os"

	"github.com/ilyakaznacheev/cleanenv"
)

type Config struct {
	Env      string   `yaml:"env" env:"ENV" env-required:"true"`
	Postgres Postgres `yaml:"postgres" env-required:"true"`
	Redis    Redis    `yaml:"redis" env-required:"true"`
	Kafka    Kafka    `yaml:"kafka" env-required:"true"`
}

type Postgres struct {
	Username string `yaml:"username" env:"POSTGRES_USER" env-required:"true"`
	Password string `yaml:"password" env:"POSTGRES_PASSWORD" env-required:"true"`
	Host     string `yaml:"host" env:"POSTGRES_HOST" env-required:"true"`
	Port     string `yaml:"port" env:"POSTGRES_PORT" env-required:"true"`
	Database string `yaml:"database" env:"POSTGRES_DB" env-required:"true"`
}

type Redis struct {
	Host     string `yaml:"host" env:"REDIS_HOST" env-required:"true"`
	Port     string `yaml:"port" env:"REDIS_PORT" env-required:"true"`
	DB       int    `yaml:"database" env:"REDIS_DB"`
	Password string `yaml:"password" env:"REDIS_PASSWORD"`
}

type Kafka struct {
	BootstrapServers []string `yaml:"bootstrap.servers" env:"KAFKA_BOOTSTRAP_SERVERS" env-required:"true"`
	Topic            string   `yaml:"topic" env-required:"true"`
	Producer         Producer `yaml:"producer" env-required:"true"`
	Consumer         Consumer `yaml:"consumer" env-required:"true"`
}

type Producer struct {
	Acks              int    `yaml:"acks" env-required:"true"`
	EnableIdempotence bool   `yaml:"enable.idempotence"`
	Retries           int    `yaml:"retries"`
	TransactionalId   string `yaml:"transactional.id"`
}

type Consumer struct {
	GroupId          string `yaml:"group.id" env-required:"true"`
	AutoOffsetReset  string `yaml:"auto.offset.reset" env-required:"true"`
	EnableAutoCommit bool   `yaml:"enable.auto.commit"`
	SecurityProtocol string `yaml:"security.protocol"`
	IsolationLevel   int8   `yaml:"isolation.level"`
}

func MustLoad() *Config {
	configPath := os.Getenv("CONFIG_PATH")
	if configPath == "" {
		log.Fatal("CONFIG_PATH is not set")
	}

	if _, err := os.Stat(configPath); err != nil {
		log.Fatalf("file '%s' doesn't exist: %v", configPath, err)
	}

	var cfg Config

	if err := cleanenv.ReadConfig(configPath, &cfg); err != nil {
		log.Fatalf("can't read config: %v", err)
	}

	return &cfg
}
