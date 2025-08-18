package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/YusovID/order-service/internal/config"
	"github.com/YusovID/order-service/internal/models"
	"github.com/YusovID/order-service/lib/logger/sl"
	"github.com/google/uuid"
	_ "github.com/lib/pq"
)

var (
	orderColumns = []string{
		"order_uid", "track_number", "customer_id", "delivery_service", "date_created",
		"payment_data", "delivery_data", "additional_data",
	}
	orderItemsColumns = []string{
		"order_uid", "chrt_id", "track_number", "price", "rid", "name",
		"sale", "size", "total_price", "nm_id", "brand", "status",
	}
)

type Storage struct {
	db *sql.DB
}

type OrderDB struct {
	OrderUID        uuid.UUID
	TrackNumber     string
	CustomerID      string
	DeliveryService string
	DateCreated     time.Time
	PaymentData     json.RawMessage
	DeliveryData    json.RawMessage
	AdditionalData  json.RawMessage
}

type ItemDB struct {
	ID          int
	OrderUID    uuid.UUID
	ChrtID      int
	TrackNumber string
	Price       float64
	Rid         string
	Name        string
	Sale        float64
	Size        string
	TotalPrice  float64
	NmID        int
	Brand       string
	Status      int
}

func New(cfg config.Postgres, log *slog.Logger) (*Storage, error) {
	const fn = "storage.postgres.New"
	log.With("fn", fn)

	log.Info("starting storage initialization...")

	connStr := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable",
		cfg.Username,
		cfg.Password,
		cfg.Host,
		cfg.Port,
		cfg.Database,
	)

	// open database
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("%s: can't open database: %v", fn, err)
	}

	// check if we can connect to database
	if err = db.Ping(); err != nil {
		return nil, fmt.Errorf("%s: can't connect to database: %v", fn, err)
	}

	return &Storage{db: db}, nil
}

func (s *Storage) SaveOrder(ctx context.Context, orderData *models.OrderData) (err error) {
	const fn = "storage.postgres.SaveOrder"

	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelDefault})
	if err != nil {
		return fmt.Errorf("%s: can't start transaction: %v", fn, err)
	}
	defer func() {
		if err != nil {
			if txErr := tx.Rollback(); txErr != nil {
				slog.Error("can't rollback transaction", slog.String("fn", fn), sl.Err(txErr))
			}
		}
	}()

	orderUID, err := uuid.Parse(orderData.OrderUID)
	if err != nil {
		return fmt.Errorf("%s: can't parse order uid: %v", fn, err)
	}

	if err := s.saveOrder(ctx, tx, orderData, orderUID); err != nil {
		return fmt.Errorf("%s: can't save order: %v", fn, err)
	}

	if err := s.saveItems(ctx, tx, orderData.Items, orderUID); err != nil {
		return fmt.Errorf("%s: can't save items: %v", fn, err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("%s: can't commit transaction: %v", fn, err)
	}

	return nil
}

func (s *Storage) saveOrder(ctx context.Context, tx *sql.Tx, orderData *models.OrderData, orderUID uuid.UUID) error {
	fn := "storage.postgres.saveOrder"

	order, err := convertOrder(orderData, orderUID)
	if err != nil {
		return fmt.Errorf("%s: can't convert order: %v", fn, err)
	}

	orderQuery := buildInsertQuery("orders", 1, orderColumns)

	_, err = tx.ExecContext(
		ctx,
		orderQuery,
		order.OrderUID, order.TrackNumber, order.CustomerID, order.DeliveryService,
		order.DateCreated, order.PaymentData, order.DeliveryData, order.AdditionalData,
	)
	if err != nil {
		return fmt.Errorf("%s: can't insert order: %v", fn, err)
	}

	return nil
}

func (s *Storage) saveItems(ctx context.Context, tx *sql.Tx, itemsData []models.Item, orderUID uuid.UUID) error {
	fn := "storage.postgres.saveItems"

	items, err := convertItems(orderUID, itemsData)
	if err != nil {
		return fmt.Errorf("%s: can't convert items: %v", fn, err)
	}

	itemsQuery := buildInsertQuery("order_items", len(items), orderItemsColumns)

	itemsArgs := make([]any, 0, len(itemsData)*len(orderItemsColumns))

	for _, item := range items {
		itemsArgs = append(itemsArgs,
			orderUID, item.ChrtID, item.TrackNumber, item.Price, item.Rid, item.Name,
			item.Sale, item.Size, item.TotalPrice, item.NmID, item.Brand, item.Status,
		)
	}

	if _, err = tx.ExecContext(ctx, itemsQuery, itemsArgs...); err != nil {
		return fmt.Errorf("%s: can't insert items: %v", fn, err)
	}

	return nil
}

func convertOrder(orderData *models.OrderData, orderUID uuid.UUID) (*OrderDB, error) {
	fn := "storage.postgres.convertOrder"

	order := &OrderDB{}

	order.OrderUID = orderUID
	order.TrackNumber = orderData.TrackNumber
	order.CustomerID = orderData.CustomerID
	order.DeliveryService = orderData.DeliveryService
	order.DateCreated = orderData.DateCreated

	paymentDataByte, err := json.Marshal(orderData.Payment)
	if err != nil {
		return nil, fmt.Errorf("%s: can't marshal payment: %v", fn, err)
	}

	order.PaymentData = paymentDataByte

	deliveryDataByte, err := json.Marshal(orderData.Delivery)
	if err != nil {
		return nil, fmt.Errorf("%s: can't marshal delivery: %v", fn, err)
	}

	order.DeliveryData = deliveryDataByte

	var Additional struct {
		Entry             string `json:"entry"`
		Locale            string `json:"locale"`
		InternalSignature string `json:"internal_signature"`
		Shardkey          string `json:"shardkey"`
		SmID              int    `json:"sm_id"`
		OofShard          string `json:"oof_shard"`
	}

	Additional.Entry = orderData.Entry
	Additional.Locale = orderData.Locale
	Additional.InternalSignature = orderData.InternalSignature
	Additional.Shardkey = orderData.Shardkey
	Additional.SmID = orderData.SmID
	Additional.OofShard = orderData.OofShard

	additionalDataByte, err := json.Marshal(Additional)
	if err != nil {
		return nil, fmt.Errorf("%s: can't marshal additional data: %v", fn, err)
	}

	order.AdditionalData = additionalDataByte

	return order, nil
}

func convertItems(orderUID uuid.UUID, itemsData []models.Item) ([]*ItemDB, error) {
	items := make([]*ItemDB, 0, len(itemsData))

	for _, itemData := range itemsData {
		item := ItemDB{}

		item.OrderUID = orderUID
		item.ChrtID = itemData.ChrtID
		item.TrackNumber = itemData.TrackNumber
		item.Price = itemData.Price
		item.Rid = itemData.Rid
		item.Name = itemData.Name
		item.Sale = itemData.Sale
		item.Size = itemData.Size
		item.TotalPrice = itemData.TotalPrice
		item.NmID = itemData.NmID
		item.Brand = itemData.Brand
		item.Status = itemData.Status

		items = append(items, &item)
	}

	return items, nil
}

func buildInsertQuery(tableName string, rowCount int, columns []string) string {
	var sb strings.Builder

	columnNames := strings.Join(columns, ",")

	sb.WriteString(fmt.Sprintf("INSERT INTO %s (%s) VALUES ", tableName, columnNames))

	for i := 0; i < rowCount; i++ {
		start := i * len(columns)

		var placeholders []string

		for j := 0; j < len(columns); j++ {
			placeholders = append(placeholders, fmt.Sprintf("$%d", start+j+1))
		}

		sb.WriteString(fmt.Sprintf("(%s)", strings.Join(placeholders, ",")))

		if i < rowCount-1 {
			sb.WriteString(",\n")
		}
	}

	sb.WriteString(";")

	return sb.String()
}
