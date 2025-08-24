package orderGen

import (
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/YusovID/order-service/internal/models"
	"github.com/google/uuid"
)

var (
	deliveryServices = []string{"meest", "dhl", "fedex", "cdek"}
	providers        = []string{"wbpay", "payu", "stripe", "visa", "mastercard"}
	banks            = []string{"alpha", "sber", "vtb", "tinkoff"}
	firstNames       = []string{"Иван", "Мария", "Алексей", "Елена", "Дмитрий"}
	lastNames        = []string{"Иванов", "Петрова", "Смирнов", "Соколова", "Кузнецов"}
	cities           = []string{"Москва", "Санкт-Петербург", "Новосибирск", "Екатеринбург", "Казань"}
	brands           = []string{"Vivienne Sabo", "Nike", "Adidas", "H&M", "ZARA"}
	itemNames        = []string{"Mascaras", "T-shirt", "Jeans", "Sneakers", "Dress"}
)

func GenerateOrder() []byte {
	orderUID := uuid.New().String()
	customerUID := uuid.New().String()
	transactionID := orderUID
	dateCreated := time.Now().UTC()

	itemsCount := randomInt(1, 3)
	items := make([]models.Item, itemsCount)
	goodsTotal := 0.0
	for i := 0; i < itemsCount; i++ {
		item := generateItem()
		items[i] = item
		goodsTotal += item.TotalPrice
	}

	randomFirstName := randomChoice(firstNames)
	randomLastName := randomChoice(lastNames)

	delivery := models.Delivery{
		Name:    fmt.Sprintf("%s %s", randomFirstName, randomLastName),
		Phone:   "+972" + fmt.Sprint(randomInt(100000000, 999999999)),
		Zip:     fmt.Sprint(randomInt(1000000, 9999999)),
		City:    randomChoice(cities),
		Address: "Random street " + fmt.Sprint(randomInt(1, 100)),
		Region:  "Kraiot",
		Email:   fmt.Sprintf("%s%s@test.com", randomFirstName, randomLastName),
	}

	deliveryCost := float64(randomInt(500, 2500))
	payment := models.Payment{
		Transaction:  transactionID,
		RequestID:    "",
		Currency:     "USD",
		Provider:     randomChoice(providers),
		Amount:       int(goodsTotal + deliveryCost),
		PaymentDT:    int(dateCreated.Unix()),
		Bank:         randomChoice(banks),
		DeliveryCost: int(deliveryCost),
		GoodsTotal:   int(goodsTotal),
		CustomFee:    0,
	}

	order := models.OrderData{
		OrderUID:          orderUID,
		TrackNumber:       "WBILMTESTTRACK",
		Entry:             "WBIL",
		Delivery:          delivery,
		Payment:           payment,
		Items:             items,
		Locale:            "en",
		InternalSignature: "",
		CustomerID:        customerUID,
		DeliveryService:   randomChoice(deliveryServices),
		Shardkey:          fmt.Sprint(randomInt(1, 9)),
		SmID:              randomInt(1, 100),
		DateCreated:       dateCreated,
		OofShard:          "1",
	}

	jsonData, err := json.Marshal(order)
	if err != nil {
		return nil
	}

	return jsonData
}

func randomChoice(slice []string) string {
	return slice[rand.IntN(len(slice))]
}

func randomInt(min, max int) int {
	return rand.IntN(max-min+1) + min
}

func generateItem() models.Item {
	price := randomInt(100, 1000)
	sale := randomInt(0, 50)
	totalPrice := price - (price*sale)/100

	return models.Item{
		ChrtID:      randomInt(1000000, 9999999),
		TrackNumber: "WBILMTESTTRACK",
		Price:       float64(price),
		Rid:         uuid.New().String(),
		Name:        randomChoice(itemNames),
		Sale:        float64(sale),
		Size:        "0",
		TotalPrice:  float64(totalPrice),
		NmID:        randomInt(1000000, 9999999),
		Brand:       randomChoice(brands),
		Status:      202,
	}
}
