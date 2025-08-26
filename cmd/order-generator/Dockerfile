FROM golang:1.25-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./

RUN go mod download

COPY . .

WORKDIR /app/cmd/order-generator

RUN CGO_ENABLED=0 GOOS=linux go build -o /order-generator .

FROM alpine:3.22

WORKDIR /

COPY --from=builder /order-generator /order-generator
COPY ./config/local.yml /config/local.yml

EXPOSE 8081

CMD ["/order-generator"]