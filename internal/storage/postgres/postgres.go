// Package postgres предоставляет реализацию хранилища данных с использованием
// базы данных PostgreSQL. Он отвечает за все операции CRUD (Create, Read, Update, Delete)
// связанные с заказами. Пакет использует `sqlx` для удобной работы с SQL и
// `squirrel` для декларативного построения запросов.
package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/Masterminds/squirrel"
	"github.com/YusovID/order-service/internal/config"
	"github.com/YusovID/order-service/internal/models"
	"github.com/YusovID/order-service/internal/storage"
	"github.com/YusovID/order-service/lib/logger/sl"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq" // Драйвер PostgreSQL.
)

// Storage инкапсулирует подключение к базе данных и предоставляет методы
// для работы с данными заказов.
type Storage struct {
	db  *sqlx.DB
	log *slog.Logger
	sq  squirrel.StatementBuilderType // Построитель запросов squirrel.
}

// OrderDB представляет структуру таблицы `orders` в базе данных.
// Поля, хранящиеся в формате JSONB, представлены как json.RawMessage
// для отложенного анмаршалинга.
type OrderDB struct {
	OrderUID        string          `db:"order_uid"`
	TrackNumber     string          `db:"track_number"`
	CustomerID      string          `db:"customer_id"`
	DeliveryService string          `db:"delivery_service"`
	DateCreated     time.Time       `db:"date_created"`
	PaymentData     json.RawMessage `db:"payment_data"`
	DeliveryData    json.RawMessage `db:"delivery_data"`
	AdditionalData  json.RawMessage `db:"additional_data"`
}

// ItemDB представляет структуру таблицы `order_items` в базе данных.
type ItemDB struct {
	ID          int     `db:"id"`
	OrderUID    string  `db:"order_uid"`
	ChrtID      int     `db:"chrt_id"`
	TrackNumber string  `db:"track_number"`
	Price       float64 `db:"price"`
	Rid         string  `db:"rid"`
	Name        string  `db:"name"`
	Sale        float64 `db:"sale"`
	Size        string  `db:"size"`
	TotalPrice  float64 `db:"total_price"`
	NmID        int     `db:"nm_id"`
	Brand       string  `db:"brand"`
	Status      int     `db:"status"`
}

// JoinedRow используется для сканирования результатов JOIN-запроса между
// таблицами `orders` и `order_items`. Она встраивает обе структуры.
type JoinedRow struct {
	OrderDB
	ItemDB
}

// New создает и возвращает новый экземпляр Storage, устанавливая
// соединение с базой данных PostgreSQL.
func New(cfg config.Postgres, log *slog.Logger) (*Storage, error) {
	connStr := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable",
		cfg.Username, cfg.Password, cfg.Host, cfg.Port, cfg.Database,
	)

	db, err := sqlx.Connect("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("can't connect to database: %v", err)
	}

	return &Storage{
		db:  db,
		log: log,
		sq:  squirrel.StatementBuilder.PlaceholderFormat(squirrel.Dollar),
	}, nil
}

// SaveOrder сохраняет полную информацию о заказе (заказ и его товары)
// в базу данных в рамках одной транзакции.
// Если любая из операций вставки завершается ошибкой, вся транзакция откатывается.
func (s *Storage) SaveOrder(ctx context.Context, orderData *models.OrderData) (err error) {
	const fn = "storage.postgres.SaveOrder"

	tx, err := s.db.Beginx()
	if err != nil {
		return fmt.Errorf("%s: can't start transaction: %v", fn, err)
	}
	// `defer` с именованным возвращаемым значением `err` гарантирует,
	// что откат транзакции произойдет только в случае ошибки.
	defer func() {
		if err != nil {
			if txErr := tx.Rollback(); txErr != nil {
				s.log.Error("can't rollback transaction", slog.String("fn", fn), sl.Err(txErr))
			}
		}
	}()

	if err = s.saveOrder(ctx, tx, orderData); err != nil {
		return fmt.Errorf("%s: can't save order: %v", fn, err)
	}
	if err = s.saveItems(ctx, tx, orderData.Items, orderData.OrderUID); err != nil {
		return fmt.Errorf("%s: can't save items: %v", fn, err)
	}

	return tx.Commit()
}

// saveOrder (unexported) выполняет вставку одной записи в таблицу `orders`.
// Использует `ON CONFLICT DO NOTHING` для игнорирования дубликатов по `order_uid`.
func (s *Storage) saveOrder(ctx context.Context, tx *sqlx.Tx, orderData *models.OrderData) error {
	order, err := convertOrder(orderData)
	if err != nil {
		return err
	}

	query, args, err := s.sq.Insert("orders").
		Columns(
			"order_uid", "track_number", "customer_id", "delivery_service", "date_created",
			"payment_data", "delivery_data", "additional_data",
		).
		Values(
			order.OrderUID, order.TrackNumber, order.CustomerID, order.DeliveryService,
			order.DateCreated, order.PaymentData, order.DeliveryData, order.AdditionalData,
		).
		Suffix("ON CONFLICT (order_uid) DO NOTHING").
		ToSql()
	if err != nil {
		return fmt.Errorf("failed to build save order query: %v", err)
	}

	_, err = tx.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("failed to execute save order query: %v", err)
	}

	return nil
}

// saveItems (unexported) выполняет пакетную вставку товаров заказа в таблицу `order_items`.
// Использует `NamedExecContext` для удобного маппинга полей структуры на параметры запроса.
func (s *Storage) saveItems(ctx context.Context, tx *sqlx.Tx, itemsData []models.Item, orderUID string) error {
	if len(itemsData) == 0 {
		return nil
	}

	items, err := convertItems(orderUID, itemsData)
	if err != nil {
		return err
	}

	query := `
		INSERT INTO order_items (
			order_uid, chrt_id, track_number, price, rid, name,
			sale, size, total_price, nm_id, brand, status
		) VALUES (
			:order_uid, :chrt_id, :track_number, :price, :rid, :name,
			:sale, :size, :total_price, :nm_id, :brand, :status
		)`

	_, err = tx.NamedExecContext(ctx, query, items)
	if err != nil {
		return fmt.Errorf("failed to execute save items query: %v", err)
	}

	return nil
}

// GetOrder извлекает один заказ вместе со всеми его товарами по `order_uid`.
// Выполняет JOIN-запрос и затем агрегирует результаты в одну структуру `models.OrderData`.
func (s *Storage) GetOrder(ctx context.Context, orderUID string) (*models.OrderData, error) {
	const fn = "storage.postgres.GetOrder"

	query, args, err := s.sq.Select(
		"o.order_uid", "o.track_number", "o.customer_id", "o.delivery_service",
		"o.date_created", "o.payment_data", "o.delivery_data", "o.additional_data",
		"i.id", "i.chrt_id", "i.track_number", "i.price", "i.rid", "i.name",
		"i.sale", "i.size", "i.total_price", "i.nm_id", "i.brand", "i.status",
	).
		From("orders o").
		Join("order_items i ON o.order_uid = i.order_uid").
		Where(squirrel.Eq{"o.order_uid": orderUID}).
		ToSql()
	if err != nil {
		return nil, fmt.Errorf("%s: failed to build get order query: %v", fn, err)
	}

	var joinedRows []JoinedRow
	if err := s.db.SelectContext(ctx, &joinedRows, query, args...); err != nil {
		return nil, fmt.Errorf("%s: failed to execute get order query: %v", fn, err)
	}

	if len(joinedRows) == 0 {
		return nil, storage.ErrNoOrder
	}

	// Создаем основной объект заказа из первой строки.
	firstRow := joinedRows[0]
	orderData, err := fillOrderData(firstRow)
	if err != nil {
		return nil, fmt.Errorf("%s: can't fill order data: %v", fn, err)
	}

	// Добавляем все товары из всех полученных строк.
	for _, row := range joinedRows {
		appendItems(row, orderData)
	}

	return orderData, nil
}

// GetOrders извлекает все заказы из базы данных.
// Используется для первоначального заполнения кэша при старте сервиса.
func (s *Storage) GetOrders(ctx context.Context) ([]*models.OrderData, error) {
	const fn = "storage.postgres.GetOrders"

	query, args, err := s.sq.Select(
		"o.order_uid", "o.track_number", "o.customer_id", "o.delivery_service",
		"o.date_created", "o.payment_data", "o.delivery_data", "o.additional_data",
		"i.id", "i.chrt_id", "i.track_number", "i.price", "i.rid", "i.name",
		"i.sale", "i.size", "i.total_price", "i.nm_id", "i.brand", "i.status",
	).
		From("orders o").
		Join("order_items i ON o.order_uid = i.order_uid").
		ToSql()
	if err != nil {
		return nil, fmt.Errorf("%s: failed to build get orders query: %v", fn, err)
	}

	var joinedRows []JoinedRow
	if err := s.db.SelectContext(ctx, &joinedRows, query, args...); err != nil {
		return nil, fmt.Errorf("%s: failed to execute get orders query: %v", fn, err)
	}

	if len(joinedRows) == 0 {
		return nil, storage.ErrNoOrder
	}

	// Используем мапу для группировки товаров по заказам.
	ordersMap := make(map[string]*models.OrderData)
	for _, row := range joinedRows {
		orderData, exists := ordersMap[row.OrderDB.OrderUID]
		if !exists {
			orderData, err = fillOrderData(row)
			if err != nil {
				return nil, fmt.Errorf("%s: can't fill order data: %v", fn, err)
			}
			ordersMap[row.OrderDB.OrderUID] = orderData
		}
		appendItems(row, orderData)
	}

	// Преобразуем мапу в слайс.
	orders := make([]*models.OrderData, 0, len(ordersMap))
	for _, order := range ordersMap {
		orders = append(orders, order)
	}

	return orders, nil
}

// convertOrder преобразует модель `models.OrderData` в `OrderDB` для сохранения в БД.
func convertOrder(orderData *models.OrderData) (*OrderDB, error) {
	order := &OrderDB{
		OrderUID:        orderData.OrderUID,
		TrackNumber:     orderData.TrackNumber,
		CustomerID:      orderData.CustomerID,
		DeliveryService: orderData.DeliveryService,
		DateCreated:     orderData.DateCreated,
	}

	var err error
	if order.PaymentData, err = json.Marshal(orderData.Payment); err != nil {
		return nil, fmt.Errorf("can't marshal payment: %v", err)
	}
	if order.DeliveryData, err = json.Marshal(orderData.Delivery); err != nil {
		return nil, fmt.Errorf("can't marshal delivery: %v", err)
	}
	if order.AdditionalData, err = json.Marshal(orderData.AdditionalData); err != nil {
		return nil, fmt.Errorf("can't marshal additional data: %v", err)
	}

	return order, nil
}

// convertItems преобразует слайс `models.Item` в `[]ItemDB` для сохранения в БД.
func convertItems(orderUID string, itemsData []models.Item) ([]ItemDB, error) {
	items := make([]ItemDB, 0, len(itemsData))
	for _, itemData := range itemsData {
		items = append(items, ItemDB{
			OrderUID:    orderUID,
			ChrtID:      itemData.ChrtID,
			TrackNumber: itemData.TrackNumber,
			Price:       itemData.Price,
			Rid:         itemData.Rid,
			Name:        itemData.Name,
			Sale:        itemData.Sale,
			Size:        itemData.Size,
			TotalPrice:  itemData.TotalPrice,
			NmID:        itemData.NmID,
			Brand:       itemData.Brand,
			Status:      itemData.Status,
		})
	}
	return items, nil
}

// fillOrderData создает `models.OrderData` из строки `JoinedRow`, полученной из БД.
func fillOrderData(row JoinedRow) (*models.OrderData, error) {
	orderData := &models.OrderData{
		OrderUID:        row.OrderDB.OrderUID,
		TrackNumber:     row.OrderDB.TrackNumber,
		CustomerID:      row.OrderDB.CustomerID,
		DeliveryService: row.OrderDB.DeliveryService,
		DateCreated:     row.OrderDB.DateCreated,
		Items:           make([]models.Item, 0),
	}

	if err := json.Unmarshal(row.PaymentData, &orderData.Payment); err != nil {
		return nil, fmt.Errorf("can't unmarshal payment data: %v", err)
	}
	if err := json.Unmarshal(row.DeliveryData, &orderData.Delivery); err != nil {
		return nil, fmt.Errorf("can't unmarshal delivery data: %v", err)
	}
	if err := json.Unmarshal(row.AdditionalData, &orderData.AdditionalData); err != nil {
		return nil, fmt.Errorf("can't unmarshal additional data: %v", err)
	}

	return orderData, nil
}

// appendItems добавляет товар из `JoinedRow` в существующий `models.OrderData`.
func appendItems(row JoinedRow, orderData *models.OrderData) {
	orderData.Items = append(orderData.Items, models.Item{
		ChrtID:      row.ItemDB.ChrtID,
		TrackNumber: row.ItemDB.TrackNumber,
		Price:       row.ItemDB.Price,
		Rid:         row.ItemDB.Rid,
		Name:        row.ItemDB.Name,
		Sale:        row.ItemDB.Sale,
		Size:        row.ItemDB.Size,
		TotalPrice:  row.ItemDB.TotalPrice,
		NmID:        row.ItemDB.NmID,
		Brand:       row.ItemDB.Brand,
		Status:      row.ItemDB.Status,
	})
}
