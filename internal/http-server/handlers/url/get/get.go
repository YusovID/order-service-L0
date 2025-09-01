// Package get содержит HTTP-хендлер для обработки запросов на получение
// данных о заказе по его уникальному идентификатору (order_uid).
package get

import (
	"context"
	"errors"
	"log/slog"
	"net/http"

	"github.com/YusovID/order-service/internal/models"
	strg "github.com/YusovID/order-service/internal/storage"
	resp "github.com/YusovID/order-service/lib/api/response"
	"github.com/YusovID/order-service/lib/logger/sl"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/go-chi/render"
)

// Response определяет структуру ответа для успешного запроса.
// Она встраивает стандартную структуру ответа и добавляет поле с данными заказа.
type Response struct {
	resp.Response
	Order *models.OrderData `json:"order"`
}

// Storage определяет интерфейс для хранилищ (кэша и основной БД),
// с которыми взаимодействует хендлер. Это позволяет использовать
// разные реализации хранилищ (например, Redis и PostgreSQL) взаимозаменяемо.
type Storage interface {
	SaveOrder(ctx context.Context, orderData *models.OrderData) error
	GetOrder(ctx context.Context, orderUID string) (*models.OrderData, error)
}

// New создает и возвращает http.HandlerFunc для получения данных о заказе.
//
// Этот хендлер реализует следующую логику:
//  1. Извлекает `order_uid` из URL-параметра.
//  2. Сначала пытается найти заказ в `cache` (быстрое хранилище, например, Redis).
//  3. Если в кэше заказ не найден, он обращается к `storage` (основное хранилище, например, PostgreSQL).
//  4. Если заказ найден в основном хранилище, он асинхронно (в горутине) сохраняется в кэш для ускорения последующих запросов.
//  5. Если заказ не найден ни в одном из хранилищ, возвращается ошибка 404.
//  6. В случае успеха, данные заказа возвращаются в формате JSON.
//
// Параметры:
//   - log: логгер для записи информации о ходе выполнения запроса.
//   - cache: реализация интерфейса Storage для кэша.
//   - storage: реализация интерфейса Storage для основного хранилища.
func New(log *slog.Logger, cache Storage, storage Storage) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		const fn = "handlers.url.get.New"

		// Дополняем логгер контекстной информацией о текущем запросе.
		log = log.With(
			slog.String("fn", fn),
			slog.String("request_id", middleware.GetReqID(r.Context())),
		)

		// Получаем order_uid из URL.
		orderUID := chi.URLParam(r, "order_uid")
		if orderUID == "" {
			log.Error("order uid is empty")
			render.JSON(w, r, resp.Error("order uid is empty"))
			return
		}

		log.Info("request received", slog.String("order uid", orderUID))

		var orderData *models.OrderData
		var err error

		// 1. Пытаемся получить данные из кэша.
		orderData, err = cache.GetOrder(r.Context(), orderUID)
		if errors.Is(err, strg.ErrNoOrder) {
			log.Info("order not found in cache")

			// 2. Если в кэше нет, идем в основное хранилище.
			orderData, err = storage.GetOrder(r.Context(), orderUID)
			if errors.Is(err, strg.ErrNoOrder) {
				// Если и в хранилище нет, возвращаем ошибку.
				log.Info("order not found", slog.String("order_uid", orderUID))
				render.JSON(w, r, resp.Error("order not found"))
				return
			}
			// Если в хранилище есть, асинхронно сохраняем в кэш.
			if err == nil {
				go func() {
					log.Info("saving order in cache")
					// Используем фоновый контекст, так как основной запрос уже может завершиться.
					errCache := cache.SaveOrder(context.Background(), orderData)
					if errCache != nil {
						log.Error("failed to save order in cache", sl.Err(errCache))
					}
				}()
			}
		}

		// Обрабатываем прочие возможные ошибки при получении данных.
		if err != nil {
			if errors.Is(err, strg.ErrEmptyOrder) {
				log.Info("empty order", slog.String("order_uid", orderUID))
				render.JSON(w, r, resp.Error("empty order"))
				return
			}

			log.Error("failed to get order", sl.Err(err))
			render.JSON(w, r, resp.Error("failed to get order"))
			return
		}

		log.Info("got order successfully", slog.String("order_uid", orderUID))

		// Отправляем успешный ответ с данными заказа.
		render.JSON(w, r, Response{
			Response: resp.OK(),
			Order:    orderData,
		})
	}
}
