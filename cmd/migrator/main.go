// package main запускает утилиту для применения миграций базы данных.
// Эта утилита читает SQL-файлы миграций из указанной директории
// и последовательно применяет их к базе данных PostgreSQL.
// Конфигурация для подключения к БД и пути к файлам миграций
// загружаются из переменных окружения и конфигурационного файла.
package main

import (
	"errors"
	"fmt"
	"log"
	"os"

	"github.com/YusovID/order-service/internal/config"
	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres" // Драйвер для поддержки PostgreSQL в migrate
	_ "github.com/golang-migrate/migrate/v4/source/file"       // Драйвер для чтения миграций из файлов
	"github.com/ilyakaznacheev/cleanenv"
)

// MigrationCfg хранит конфигурацию, необходимую для запуска миграций.
type MigrationCfg struct {
	ConnStr         string // Строка подключения к базе данных (DSN).
	MigrationsPath  string // Путь к директории с файлами миграций.
	MigrationsTable string // Название таблицы в БД для хранения истории миграций.
}

// main является точкой входа в программу.
// Она загружает конфигурацию, создает новый экземпляр мигратора,
// применяет миграции и обрабатывает результаты.
func main() {
	// Загружаем конфигурацию. В случае ошибки приложение завершится с паникой.
	migration := MustLoad()

	// Создаем новый экземпляр мигратора.
	// Источник миграций - "file://...", что указывает на чтение из локальной файловой системы.
	// База данных - строка подключения, дополненная параметрами для migrate.
	m, err := migrate.New(
		"file://"+migration.MigrationsPath,
		fmt.Sprintf("%s?sslmode=disable&x-migrations-table=%s", migration.ConnStr, migration.MigrationsTable),
	)
	if err != nil {
		log.Fatalf("can't create new migration: %v", err)
	}

	// Применяем все доступные "up" миграции.
	if err := m.Up(); err != nil {
		// migrate.ErrNoChange не является реальной ошибкой. Она означает, что
		// все миграции уже были применены и новых нет. Это нормальный сценарий.
		if errors.Is(err, migrate.ErrNoChange) {
			fmt.Println("no migrations to apply")
			return
		}
		// В случае любой другой ошибки - завершаем приложение.
		log.Fatalf("can't do migrations: %v", err)
	}

	fmt.Println("migrations applied successfully")
}

// MustLoad загружает конфигурацию из переменных окружения и YAML-файла.
// Функция имеет префикс "Must", так как она вызывает log.Fatalf (паникует)
// при любой ошибке, что останавливает выполнение программы. Это используется
// при старте, так как без конфигурации работа приложения невозможна.
func MustLoad() *MigrationCfg {
	// Получаем путь к основному файлу конфигурации.
	configPath := os.Getenv("CONFIG_PATH")
	if configPath == "" {
		log.Fatalf("CONFIG_PATH is not set")
	}

	// Проверяем, что файл конфигурации существует.
	if _, err := os.Stat(configPath); err != nil {
		log.Fatalf("file '%s' doesn't exist: %v", configPath, err)
	}

	// Получаем путь к директории с файлами миграций.
	migrationsPath := os.Getenv("MIGRATIONS_PATH")
	if migrationsPath == "" {
		log.Fatalf("MIGRATIONS_PATH is not set")
	}

	// Получаем имя таблицы для хранения истории миграций.
	migrationsTable := os.Getenv("MIGRATIONS_TABLE")
	if migrationsTable == "" {
		log.Fatalf("MIGRATIONS_TABLE is not set")
	}

	// Читаем основной конфигурационный файл в структуру config.Config.
	// Нам нужны только данные для подключения к БД.
	var cfg config.Config
	if err := cleanenv.ReadConfig(configPath, &cfg); err != nil {
		log.Fatalf("can't read config: %v", err)
	}

	// Собираем строку подключения к PostgreSQL из загруженных данных.
	connStr := fmt.Sprintf("postgres://%s:%s@%s:%s/%s",
		cfg.Postgres.Username,
		cfg.Postgres.Password,
		cfg.Postgres.Host,
		cfg.Postgres.Port,
		cfg.Postgres.Database,
	)

	// Возвращаем готовую структуру с конфигурацией для мигратора.
	return &MigrationCfg{
		ConnStr:         connStr,
		MigrationsPath:  migrationsPath,
		MigrationsTable: migrationsTable,
	}
}
