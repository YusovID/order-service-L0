// Package orderGen предоставляет функции для генерации случайных, но
// структурно-валидных данных о заказах. Это основной инструмент для
// сервиса `order-generator`, который эмулирует поток реальных данных.
// Для создания фейковых данных используется библиотека `gofakeit`.
package orderGen

import (
	"encoding/json"
	"fmt"

	"github.com/YusovID/order-service/internal/models"
	"github.com/brianvoe/gofakeit/v7"
)

// Предопределенные срезы строк для выбора случайных значений.
// Это делает сгенерированные данные более похожими на настоящие.
var (
	deliveryServices = []string{"meest", "dhl", "fedex", "cdek"}
	providers        = []string{"wbpay", "payu", "stripe", "visa", "mastercard"}
	banks            = []string{"alpha", "sber", "vtb", "tinkoff"}
)

// GenerateOrder создает полную структуру заказа (`models.OrderData`) со случайными данными.
//
// Функция последовательно генерирует все части заказа:
//  1. Основные атрибуты: `order_uid`, `track_number`.
//  2. Случайное количество товаров (`items`) с помощью `generateItem`.
//  3. Данные о доставке (`delivery`) и оплате (`payment`).
//  4. Дополнительные метаданные.
//
// Затем вся структура сериализуется в JSON.
//
// Возвращает:
//   - `string`: сгенерированный `order_uid`, который используется как ключ сообщения в Kafka.
//   - `[]byte`: JSON-представление сгенерированного заказа.
func GenerateOrder() (string, []byte) {
	orderUID := gofakeit.UUID()
	trackNumber := gofakeit.LetterN(4) + gofakeit.DigitN(8)
	dateCreated := gofakeit.Date()

	// Генерируем от 1 до 3 товаров в заказе.
	itemsCount := gofakeit.Number(1, 3)
	items := make([]models.Item, itemsCount)
	var goodsTotal int
	for i := 0; i < itemsCount; i++ {
		item := generateItem(trackNumber)
		items[i] = item
		goodsTotal += int(item.TotalPrice)
	}

	delivery := models.Delivery{
		Name:    gofakeit.Name(),
		Phone:   gofakeit.Phone(),
		Zip:     gofakeit.Zip(),
		City:    gofakeit.City(),
		Address: gofakeit.Address().Address,
		Region:  gofakeit.State(),
		Email:   gofakeit.Email(),
	}

	deliveryCost := gofakeit.Number(500, 2500)
	payment := models.Payment{
		Transaction:  orderUID,
		RequestID:    "", // Оставляем пустым, как в модели.
		Currency:     "USD",
		Provider:     gofakeit.RandomString(providers),
		Amount:       goodsTotal + deliveryCost, // Общая сумма = товары + доставка.
		PaymentDT:    int(dateCreated.Unix()),
		Bank:         gofakeit.RandomString(banks),
		DeliveryCost: deliveryCost,
		GoodsTotal:   goodsTotal,
		CustomFee:    0,
	}

	order := models.OrderData{
		OrderUID:        orderUID,
		TrackNumber:     trackNumber,
		CustomerID:      gofakeit.UUID(),
		DeliveryService: gofakeit.RandomString(deliveryServices),
		DateCreated:     dateCreated,
		Items:           items,
		Delivery:        delivery,
		Payment:         payment,
		AdditionalData: models.AdditionalData{
			Entry:             "WBIL",
			Locale:            gofakeit.LanguageAbbreviation(),
			InternalSignature: "",
			Shardkey:          gofakeit.Digit(),
			SmID:              gofakeit.Number(1, 100),
			OofShard:          "1",
		},
	}

	jsonData, err := json.Marshal(order)
	if err != nil {
		// В данном контексте (генератор) просто выводим ошибку в консоль.
		fmt.Println("Error marshaling to JSON:", err)
		return "", nil
	}

	return orderUID, jsonData
}

// generateItem создает один случайный товар (`models.Item`).
// Принимает `trackNumber`, чтобы у всех товаров одного заказа
// был одинаковый номер отслеживания.
func generateItem(trackNumber string) models.Item {
	price := gofakeit.Price(100, 1000)
	sale := gofakeit.Number(0, 50)
	totalPrice := price - (price*float64(sale))/100

	return models.Item{
		ChrtID:      gofakeit.Number(1000000, 9999999),
		TrackNumber: trackNumber,
		Price:       price,
		Rid:         gofakeit.UUID(),
		Name:        gofakeit.ProductName(),
		Sale:        float64(sale),
		Size:        gofakeit.RandomString([]string{"S", "M", "L", "XL"}),
		TotalPrice:  totalPrice,
		NmID:        gofakeit.Number(1000000, 9999999),
		Brand:       gofakeit.Company(),
		Status:      202,
	}
}
