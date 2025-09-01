package orderGen

import (
	"encoding/json"
	"fmt"

	"github.com/YusovID/order-service/internal/models"
	"github.com/brianvoe/gofakeit/v7"
)

var (
	deliveryServices = []string{"meest", "dhl", "fedex", "cdek"}
	providers        = []string{"wbpay", "payu", "stripe", "visa", "mastercard"}
	banks            = []string{"alpha", "sber", "vtb", "tinkoff"}
)

func GenerateOrder() (string, []byte) {
	orderUID := gofakeit.UUID()
	trackNumber := gofakeit.LetterN(4) + gofakeit.DigitN(8)
	dateCreated := gofakeit.Date()

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
		RequestID:    "",
		Currency:     "USD",
		Provider:     gofakeit.RandomString(providers),
		Amount:       goodsTotal + deliveryCost,
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
		fmt.Println("Error marshaling to JSON:", err)
		return "", nil
	}

	return orderUID, jsonData
}

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
