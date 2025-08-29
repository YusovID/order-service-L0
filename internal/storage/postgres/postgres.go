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
	_ "github.com/lib/pq"
)

type Storage struct {
	db  *sqlx.DB
	log *slog.Logger
	sq  squirrel.StatementBuilderType
}

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

type JoinedRow struct {
	OrderDB
	ItemDB
}

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

func (s *Storage) SaveOrder(ctx context.Context, orderData *models.OrderData) (err error) {
	const fn = "storage.postgres.SaveOrder"

	tx, err := s.db.Beginx()
	if err != nil {
		return fmt.Errorf("%s: can't start transaction: %v", fn, err)
	}
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

	firstRow := joinedRows[0]
	orderData, err := fillOrderData(firstRow)
	if err != nil {
		return nil, fmt.Errorf("%s: can't fill order data: %v", fn, err)
	}

	for _, row := range joinedRows {
		appendItems(row, orderData)
	}

	return orderData, nil
}

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
		return nil, fmt.Errorf("%s: faield to execute get orders query: %v", fn, err)
	}

	if len(joinedRows) == 0 {
		return nil, storage.ErrNoOrder
	}

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

	orders := make([]*models.OrderData, 0, len(ordersMap))

	for _, order := range ordersMap {
		orders = append(orders, order)
	}

	return orders, nil
}

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
