package get

import (
	"context"
	"errors"
	"log/slog"
	"net/http"

	"github.com/YusovID/order-service/internal/models"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/go-chi/render"

	strg "github.com/YusovID/order-service/internal/storage"
	resp "github.com/YusovID/order-service/lib/api/response"
	"github.com/YusovID/order-service/lib/logger/sl"
)

type Response struct {
	resp.Response
	Order *models.OrderData `json:"order"`
}

type Storage interface {
	SaveOrder(ctx context.Context, orderData *models.OrderData) error
	GetOrder(ctx context.Context, orderUID string) (*models.OrderData, error)
}

func New(ctx context.Context, log *slog.Logger, cache Storage, storage Storage) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		const fn = "handlers.url.get.New"

		log = log.With(
			slog.String("fn", fn),
			slog.String("request_id", middleware.GetReqID(r.Context())),
		)

		orderUID := chi.URLParam(r, "order_uid")
		if orderUID == "" {
			log.Error("order uid is empty")

			render.JSON(w, r, resp.Error("order uid is empty"))

			return
		}

		log.Info("request recieved", slog.String("order uid", orderUID))

		var orderData *models.OrderData

		orderData, err := cache.GetOrder(ctx, orderUID)
		if errors.Is(err, strg.ErrNoOrder) {
			log.Info("order not found in cache")

			orderData, err = storage.GetOrder(ctx, orderUID)
			if errors.Is(err, strg.ErrNoOrder) {
				log.Info("order not found", slog.String("order_uid", orderUID))

				render.JSON(w, r, resp.Error("order not found"))

				return
			}

			go func() {
				log.Info("saving order in cache")

				err = cache.SaveOrder(ctx, orderData)
				if err != nil {
					log.Info("failed to save order in cache", sl.Err(err))
				}
			}()
		}

		if errors.Is(err, strg.ErrEmptyOrder) {
			log.Info("empty order", slog.String("order_uid", orderUID))

			render.JSON(w, r, resp.Error("empty order"))

			return
		}

		if err != nil {
			log.Error("failed to get order", sl.Err(err))

			render.JSON(w, r, resp.Error("failed to get order"))

			return
		}

		log.Info("got order successfully", slog.String("order_uid", orderUID))

		render.JSON(w, r, Response{
			Response: resp.OK(),
			Order:    orderData,
		})
	}
}
