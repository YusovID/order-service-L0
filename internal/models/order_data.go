// Package models определяет структуры данных, используемые в приложении.
// Эти структуры представляют собой Go-аналоги JSON-модели заказа и используются
// для десериализации данных из Kafka, сохранения в базу данных и
// отправки через HTTP API.
package models

import "time"

// OrderData представляет полную информацию о заказе.
// Это корневая структура, которая объединяет все связанные данные,
// включая информацию о доставке, оплате и товарах.
type OrderData struct {
	OrderUID        string    `json:"order_uid"`        // Уникальный идентификатор заказа.
	TrackNumber     string    `json:"track_number"`     // Номер для отслеживания заказа.
	CustomerID      string    `json:"customer_id"`      // Идентификатор клиента.
	DeliveryService string    `json:"delivery_service"` // Служба доставки.
	DateCreated     time.Time `json:"date_created"`     // Дата и время создания заказа.

	Items []Item `json:"items"` // Список товаров в заказе.

	Delivery Delivery `json:"delivery"` // Информация о доставке.
	Payment  Payment  `json:"payment"`  // Информация об оплате.
	AdditionalData
}

// Delivery содержит информацию, необходимую для доставки заказа.
type Delivery struct {
	Name    string `json:"name"`    // Имя и фамилия получателя.
	Phone   string `json:"phone"`   // Контактный телефон.
	Zip     string `json:"zip"`     // Почтовый индекс.
	City    string `json:"city"`    // Город доставки.
	Address string `json:"address"` // Адрес доставки (улица, дом).
	Region  string `json:"region"`  // Регион/область.
	Email   string `json:"email"`   // Электронная почта получателя.
}

// Payment содержит информацию об оплате заказа.
type Payment struct {
	Transaction  string `json:"transaction"`   // ID транзакции, обычно совпадает с OrderUID.
	RequestID    string `json:"request_id"`    // Внутренний ID запроса на оплату.
	Currency     string `json:"currency"`      // Валюта платежа.
	Provider     string `json:"provider"`      // Платежный провайдер.
	Amount       int    `json:"amount"`        // Общая сумма к оплате.
	PaymentDT    int    `json:"payment_dt"`    // Unix-время транзакции.
	Bank         string `json:"bank"`          // Банк, через который прошел платеж.
	DeliveryCost int    `json:"delivery_cost"` // Стоимость доставки.
	GoodsTotal   int    `json:"goods_total"`   // Суммарная стоимость товаров.
	CustomFee    int    `json:"custom_fee"`    // Таможенный сбор.
}

// AdditionalData содержит дополнительные метаданные о заказе.
type AdditionalData struct {
	Entry             string `json:"entry"`              // Поле, указывающее на источник (например, "WBIL").
	Locale            string `json:"locale"`             // Язык пользователя (например, "en").
	InternalSignature string `json:"internal_signature"` // Внутренняя подпись (может быть пустой).
	Shardkey          string `json:"shardkey"`           // Ключ для шардирования базы данных.
	SmID              int    `json:"sm_id"`              // ID в какой-то из внутренних систем.
	OofShard          string `json:"oof_shard"`          // Ключ для шардирования OOF (Out Of Stock).
}

// Item представляет один товар в заказе.
type Item struct {
	ChrtID      int     `json:"chrt_id"`      // Уникальный идентификатор товара.
	TrackNumber string  `json:"track_number"` // Номер отслеживания, обычно совпадает с общим.
	Price       float64 `json:"price"`        // Цена товара до применения скидок.
	Rid         string  `json:"rid"`          // Уникальный идентификатор строки заказа.
	Name        string  `json:"name"`         // Название товара.
	Sale        float64 `json:"sale"`         // Скидка в процентах.
	Size        string  `json:"size"`         // Размер товара.
	TotalPrice  float64 `json:"total_price"`  // Итоговая цена товара с учетом скидки.
	NmID        int     `json:"nm_id"`        // Артикул товара от WB.
	Brand       string  `json:"brand"`        // Бренд товара.
	Status      int     `json:"status"`       // Статус товара в системе поставщика.
}
