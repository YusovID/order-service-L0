package get

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	"github.com/YusovID/order-service/internal/models"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/go-chi/render"
	"github.com/go-playground/validator/v10"

	strg "github.com/YusovID/order-service/internal/storage"
	resp "github.com/YusovID/order-service/lib/api/response"
	"github.com/YusovID/order-service/lib/logger/sl"
)

type Request struct {
	ID string `json:"id" validate:"required,uuid"`
}

type Response struct {
	resp.Response
	Order []byte `json:"order"`
}

type OrderGetter interface {
	GetOrder(ctx context.Context, orderUID string) (*models.OrderData, error)
}

func New(ctx context.Context, log *slog.Logger, cache OrderGetter, storage OrderGetter) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		const fn = "handlers.url.get.New"

		log = log.With(
			slog.String("fn", fn),
			slog.String("request_id", middleware.GetReqID(r.Context())),
		)

		var req Request

		err := render.DecodeJSON(r.Body, &req)
		if err != nil {
			log.Error("failed to decode json body", sl.Err(err))

			render.JSON(w, r, resp.Error("failed to decode request"))

			return
		}

		log.Info("request body decoded", slog.Any("request", req))

		if err := validator.New().Struct(req); err != nil {
			validateErr := err.(validator.ValidationErrors)

			log.Error("invalid request", sl.Err(err))

			render.JSON(w, r, resp.ValidationError(validateErr))

			return
		}

		var orderData *models.OrderData

		orderData, err = cache.GetOrder(ctx, req.ID)
		if errors.Is(err, strg.ErrNoOrder) {
			orderData, err = storage.GetOrder(ctx, req.ID)
			if errors.Is(err, strg.ErrNoOrder) {
				log.Info("order not found", slog.String("order_uid", req.ID))

				render.JSON(w, r, resp.Error("order not found"))

				return
			}
		}

		if errors.Is(err, strg.ErrEmptyOrder) {
			log.Info("empty order", slog.String("order_uid", req.ID))

			render.JSON(w, r, resp.Error("empty order"))

			return
		}

		if err != nil {
			log.Error("failed to get order", sl.Err(err))

			render.JSON(w, r, resp.Error("failed to get order"))

			return
		}

		log.Info("got order successfully", slog.String("order_uid", req.ID))

		order, err := json.Marshal(orderData)
		if err != nil {
			log.Error("failed to marshal order", sl.Err(err))

			render.JSON(w, r, resp.Error("internal error"))

			return
		}

		render.JSON(w, r, Response{
			Response: resp.OK(),
			Order:    order,
		})
	}
}
