FROM golang:1.25-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./

RUN go mod download

COPY . .
COPY web/ /app/web/

WORKDIR /app/cmd/order-service

RUN CGO_ENABLED=0 GOOS=linux go build -o /order-service .

FROM alpine:3.22

WORKDIR /

COPY --from=builder /order-service /order-service
COPY ./config/local.yml /config/local.yml
COPY --from=builder /app/web/ /web/

EXPOSE 8080

CMD ["/order-service"]