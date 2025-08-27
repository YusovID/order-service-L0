package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/YusovID/order-service/internal/config"
	"github.com/YusovID/order-service/internal/models"
	"github.com/YusovID/order-service/internal/storage"
	"github.com/YusovID/order-service/lib/builder/sql/insert"
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
			// TODO реализовать worker pool
			go func() {
				s.log.Info("recieved new order")

				var orderData *models.OrderData

				err := json.Unmarshal(order, &orderData)
				if err != nil {
					s.log.Error("can't unmarshal json", sl.Err(err))
					return
				}

				s.log.Info("saving order in database")

				err = s.SaveOrder(ctx, orderData)
				if err != nil {
					s.log.Info("failed to save order in database", sl.Err(err))
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
				s.log.Error("can't rollback transaction", slog.String("fn", fn), sl.Err(txErr))
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

func (s *Storage) saveOrder(ctx context.Context, tx *sql.Tx, orderData *models.OrderData) error {
	fn := "storage.postgres.saveOrder"

	order, err := convertOrder(orderData)
	if err != nil {
		return fmt.Errorf("%s: can't convert order: %v", fn, err)
	}

	orderQuery := insert.BuildQuery("orders", 1, orderColumns)

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

	itemsQuery := insert.BuildQuery("order_items", len(items), orderItemsColumns)

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

func (s *Storage) GetOrder(ctx context.Context, orderUID string) (*models.OrderData, error) {
	const fn = "storage.postgres.GetOrder"

	orderData, err := s.getOrder(ctx, orderUID)
	if err != nil {
		return nil, fmt.Errorf("%s: can't get order: %v", fn, err)
	}

	return orderData, nil
}

func (s *Storage) getOrder(ctx context.Context, orderUID string) (*models.OrderData, error) {
	query := `SELECT o.*, i.* FROM orders o
	JOIN order_items i
	ON o.order_uid = i.order_uid
	WHERE order_uid = ($1)`

	orderRows, err := s.db.QueryContext(ctx, query, orderUID)
	if err != nil {
		return nil, fmt.Errorf("can't do query: %v", err)
	}
	defer orderRows.Close()

	var orderData *models.OrderData

	for orderRows.Next() {
		var (
			orderUID, trackNumber, customerID, deliveryService string
			dateCreated                                        time.Time
			paymentData, deliveryData, additionalData          []byte

			id, chrtID, nmID, status                              int
			itemOrderUID, itemTrackNumber, rid, name, size, brand string
			price, sale, totalPrice                               float64
		)

		if err := orderRows.Scan(
			&orderUID, &trackNumber, &customerID,
			&deliveryService, &dateCreated,
			&paymentData, &deliveryData, &additionalData,

			&id, &itemOrderUID, &chrtID, &itemTrackNumber, &price, &rid, &name,
			&sale, &size, &totalPrice, &nmID, &brand, &status,
		); err != nil {
			return nil, fmt.Errorf("can't scan row: %v", err)
		}

		if orderData == nil {
			orderData = &models.OrderData{}

			orderData.OrderUID = orderUID
			orderData.TrackNumber = trackNumber
			orderData.CustomerID = customerID
			orderData.DeliveryService = deliveryService
			orderData.DateCreated = dateCreated

			err = json.Unmarshal(deliveryData, &orderData.Delivery)
			if err != nil {
				return nil, fmt.Errorf("can't unmarshal delivery data: %v", err)
			}

			err := json.Unmarshal(paymentData, &orderData.Payment)
			if err != nil {
				return nil, fmt.Errorf("can't unmarshal payment data: %v", err)
			}

			err = json.Unmarshal(additionalData, &orderData.AdditionalData)
			if err != nil {
				return nil, fmt.Errorf("can't unmarshal additional data: %v", err)
			}
		}

		item := models.Item{
			ChrtID: chrtID, TrackNumber: itemTrackNumber, Price: price,
			Rid: rid, Name: name, Sale: sale, Size: size, TotalPrice: totalPrice,
			NmID: nmID, Brand: brand, Status: status,
		}

		orderData.Items = append(orderData.Items, item)
	}
	if err := orderRows.Err(); err != nil {
		return nil, fmt.Errorf("failed to scan rows: %v", err)
	}

	if orderData == nil {
		return nil, storage.ErrNoOrder
	}

	return orderData, nil
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

	additionalDataByte, err := json.Marshal(orderData.AdditionalData)
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
