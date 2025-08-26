package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/YusovID/order-service/internal/config"
	"github.com/YusovID/order-service/internal/models"
	"github.com/YusovID/order-service/internal/storage"
	"github.com/YusovID/order-service/lib/logger/sl"
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
	db  *sql.DB
	log *slog.Logger
}

type OrderDB struct {
	OrderUID        string
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
	OrderUID    string
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
		return nil, fmt.Errorf("can't open database: %v", err)
	}

	// check if we can connect to database
	if err = db.Ping(); err != nil {
		return nil, fmt.Errorf("can't connect to database: %v", err)
	}

	return &Storage{
		db:  db,
		log: log,
	}, nil
}

func (s *Storage) ProcessOrder(ctx context.Context, orderChan chan []byte, wg *sync.WaitGroup) {
	defer wg.Done()

	const fn = "storage.postgres.ProcessOrder"
	s.log = s.log.With("fn", fn)

	for {
		select {
		case <-ctx.Done():
			s.log.Info("stopped processing order")
			return

		case order := <-orderChan:
			go func() {
				s.log.Info("saving order in database")

				var orderData *models.OrderData

				err := json.Unmarshal(order, &orderData)
				if err != nil {
					s.log.Error("can't unmarshal json", sl.Err(err))
					return
				}

				err = s.SaveOrder(ctx, orderData)
				if err != nil {
					s.log.Info("failed to save order: %v", sl.Err(err))
					return
				}

				s.log.Info("saving was succesful")
			}()
		}
	}
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

	if err := s.saveOrder(ctx, tx, orderData); err != nil {
		return fmt.Errorf("%s: can't save order: %v", fn, err)
	}

	if err := s.saveItems(ctx, tx, orderData.Items, orderData.OrderUID); err != nil {
		return fmt.Errorf("%s: can't save items: %v", fn, err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("%s: can't commit transaction: %v", fn, err)
	}

	return nil
}

func (s *Storage) GetOrder(ctx context.Context, orderUID string) (*models.OrderData, error) {
	const fn = "storage.postgres.GetOrder"

	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelDefault})
	if err != nil {
		return nil, fmt.Errorf("%s: can't start transaction: %v", fn, err)
	}
	defer func() {
		if err != nil {
			if txErr := tx.Rollback(); txErr != nil {
				slog.Error("can't rollback transaction", slog.String("fn", fn), sl.Err(txErr))
			}
		}
	}()

	orderData, err := s.getOrder(ctx, tx, orderUID)
	if err != nil {
		return nil, fmt.Errorf("%s: can't get order: %v", fn, err)
	}

	err = s.getItems(ctx, tx, orderData)
	if err != nil {
		return nil, fmt.Errorf("%s: can't get items: %v", fn, err)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("%s: can't commit transaction: %v", fn, err)
	}

	return orderData, nil
}

func (s *Storage) saveOrder(ctx context.Context, tx *sql.Tx, orderData *models.OrderData) error {
	fn := "storage.postgres.saveOrder"

	order, err := convertOrder(orderData)
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

func (s *Storage) saveItems(ctx context.Context, tx *sql.Tx, itemsData []models.Item, orderUID string) error {
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

func (s *Storage) getOrder(ctx context.Context, tx *sql.Tx, orderUID string) (*models.OrderData, error) {
	orderDB := &OrderDB{}

	orderRow := tx.QueryRowContext(ctx, "SELECT * FROM orders WHERE order_uid = ($1)", orderUID)

	err := orderRow.Scan(
		&orderDB.OrderUID, &orderDB.TrackNumber, &orderDB.CustomerID, &orderDB.DeliveryService,
		&orderDB.DateCreated, &orderDB.PaymentData, &orderDB.DeliveryData, &orderDB.AdditionalData,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, storage.ErrNoOrder
		}
		return nil, fmt.Errorf("can't scan order: %v", err)
	}

	orderData := &models.OrderData{}

	orderData.OrderUID = orderDB.OrderUID
	orderData.TrackNumber = orderDB.TrackNumber
	orderData.CustomerID = orderDB.CustomerID
	orderData.DeliveryService = orderDB.DeliveryService
	orderData.DateCreated = orderDB.DateCreated

	deliveryData := models.Delivery{}
	if err := json.Unmarshal(orderDB.DeliveryData, &deliveryData); err != nil {
		return nil, fmt.Errorf("can't unmarshall delivery data")
	}
	orderData.Delivery = deliveryData

	paymentData := models.Payment{}
	if err := json.Unmarshal(orderDB.PaymentData, &paymentData); err != nil {
		return nil, fmt.Errorf("can't unmarshall payment data")
	}
	orderData.Payment = paymentData

	var Additional struct {
		Entry             string `json:"entry"`
		Locale            string `json:"locale"`
		InternalSignature string `json:"internal_signature"`
		Shardkey          string `json:"shardkey"`
		SmID              int    `json:"sm_id"`
		OofShard          string `json:"oof_shard"`
	}

	additinalData := Additional
	if err := json.Unmarshal(orderDB.AdditionalData, &additinalData); err != nil {
		return nil, fmt.Errorf("can't unmarshal additional data: %v", err)
	}

	orderData.Entry = additinalData.Entry
	orderData.Locale = additinalData.Locale
	orderData.InternalSignature = additinalData.InternalSignature
	orderData.Shardkey = additinalData.Shardkey
	orderData.SmID = additinalData.SmID
	orderData.OofShard = additinalData.OofShard

	return orderData, nil
}

func (s *Storage) getItems(ctx context.Context, tx *sql.Tx, orderData *models.OrderData) error {
	items, err := tx.QueryContext(ctx, "SELECT * FROM order_items WHERE order_uid = ($1)", orderData.OrderUID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return storage.ErrEmptyOrder
		}
		return fmt.Errorf("query error: %v", err)
	}
	defer items.Close()

	for items.Next() {
		var itemDB ItemDB

		if err := items.Scan(
			&itemDB.ID, &itemDB.OrderUID, &itemDB.ChrtID, &itemDB.TrackNumber,
			&itemDB.Price, &itemDB.Rid, &itemDB.Name, &itemDB.Sale, &itemDB.Size,
			&itemDB.TotalPrice, &itemDB.NmID, &itemDB.Brand, &itemDB.Status,
		); err != nil {
			return fmt.Errorf("can't scan item: %v", err)
		}

		var itemData models.Item

		itemData.ChrtID = itemDB.ChrtID
		itemData.TrackNumber = itemDB.TrackNumber
		itemData.Price = itemDB.Price
		itemData.Rid = itemDB.Rid
		itemData.Name = itemDB.Name
		itemData.Sale = itemDB.Sale
		itemData.Size = itemDB.Size
		itemData.TotalPrice = itemDB.TotalPrice
		itemData.NmID = itemDB.NmID
		itemData.Brand = itemDB.Brand
		itemData.Status = itemDB.Status

		orderData.Items = append(orderData.Items, itemData)
	}

	if err := items.Err(); err != nil {
		return fmt.Errorf("error while items iteration: %v", err)
	}

	return nil
}

func convertOrder(orderData *models.OrderData) (*OrderDB, error) {
	fn := "storage.postgres.convertOrder"

	order := &OrderDB{}

	order.OrderUID = orderData.OrderUID
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

func convertItems(orderUID string, itemsData []models.Item) ([]*ItemDB, error) {
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
