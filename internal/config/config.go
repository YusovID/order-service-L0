// Package config определяет структуры для конфигурации всего приложения
// и предоставляет функцию для их загрузки из YAML-файла и переменных окружения.
// Использование библиотеки cleanenv позволяет гибко управлять конфигурацией,
// совмещая чтение из файла с переопределением через environment variables,
// что удобно для запуска как локально, так и в Docker-контейнерах.
package config

import (
	"log"
	"os"
	"time"

	"github.com/ilyakaznacheev/cleanenv"
)

// Config - это корневая структура, объединяющая все конфигурационные
// параметры приложения. Она загружается при старте сервиса.
type Config struct {
	Env        string     `yaml:"env" env:"ENV" env-required:"true"`
	Postgres   Postgres   `yaml:"postgres" env-required:"true"`
	Redis      Redis      `yaml:"redis" env-required:"true"`
	Kafka      Kafka      `yaml:"kafka" env-required:"true"`
	HTTPServer HTTPServer `yaml:"http_server" env-required:"true"`
}

// Postgres содержит параметры для подключения к базе данных PostgreSQL.
type Postgres struct {
	Username string `yaml:"username" env:"POSTGRES_USER" env-required:"true"`
	Password string `yaml:"password" env:"POSTGRES_PASSWORD" env-required:"true"`
	Host     string `yaml:"host" env:"POSTGRES_HOST" env-required:"true"`
	Port     string `yaml:"port" env:"POSTGRES_PORT" env-required:"true"`
	Database string `yaml:"database" env:"POSTGRES_DB" env-required:"true"`
}

// Redis содержит параметры для подключения к серверу Redis.
type Redis struct {
	Host     string `yaml:"host" env:"REDIS_HOST" env-required:"true"`
	Port     string `yaml:"port" env:"REDIS_PORT" env-required:"true"`
	DB       int    `yaml:"db" env:"REDIS_DB"`
	Password string `yaml:"password" env:"REDIS_PASSWORD"`
}

// Kafka содержит параметры для взаимодействия с Apache Kafka,
// включая настройки для продюсера и консьюмера.
type Kafka struct {
	BootstrapServers []string `yaml:"bootstrap.servers" env:"KAFKA_BOOTSTRAP_SERVERS" env-required:"true"`
	Topic            string   `yaml:"topic" env-required:"true"`
	Producer         Producer `yaml:"producer" env-required:"true"`
	Consumer         Consumer `yaml:"consumer" env-required:"true"`
}

// Producer определяет настройки для Kafka-продюсера.
type Producer struct {
	Acks              int    `yaml:"acks" env-required:"true"`
	EnableIdempotence bool   `yaml:"enable.idempotence"`
	Retries           int    `yaml:"retries"`
	TransactionalId   string `yaml:"transactional.id"`
}

// Consumer определяет настройки для Kafka-консьюмера.
type Consumer struct {
	GroupId          string `yaml:"group.id" env-required:"true"`
	AutoOffsetReset  string `yaml:"auto.offset.reset" env-required:"true"`
	EnableAutoCommit bool   `yaml:"enable.auto.commit"`
	SecurityProtocol string `yaml:"security.protocol"`
	IsolationLevel   int8   `yaml:"isolation.level"`
}

// HTTPServer содержит параметры для запуска встроенного HTTP-сервера.
type HTTPServer struct {
	Address     string        `yaml:"address" env-required:"true"`
	Timeout     time.Duration `yaml:"timeout" env-default:"4s"`
	IdleTimeout time.Duration `yaml:"idle_timeout" env-default:"60s"`
}

// MustLoad читает конфигурацию из файла, путь к которому указан в переменной
// окружения CONFIG_PATH, и переменных окружения.
//
// Функция имеет префикс "Must", так как она вызывает log.Fatalf (паникует)
// при любой ошибке во время загрузки или парсинга конфигурации. Такой подход
// используется при старте приложения, поскольку его дальнейшая работа без
// валидной конфигурации невозможна.
//
// Возвращает указатель на заполненную структуру Config.
func MustLoad() *Config {
	// Получаем путь к файлу конфигурации из переменной окружения.
	configPath := os.Getenv("CONFIG_PATH")
	if configPath == "" {
		log.Fatal("CONFIG_PATH is not set")
	}

	// Проверяем, существует ли файл по указанному пути.
	if _, err := os.Stat(configPath); err != nil {
		log.Fatalf("config file does not exist: %s", configPath)
	}

	var cfg Config

	// Читаем YAML-файл и переменные окружения в структуру Config.
	// cleanenv автоматически сопоставляет поля структуры с данными из источников.
	if err := cleanenv.ReadConfig(configPath, &cfg); err != nil {
		log.Fatalf("cannot read config: %s", err)
	}

	return &cfg
}
