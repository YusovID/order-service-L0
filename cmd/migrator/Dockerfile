FROM golang:1.25-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./

RUN go mod tidy

COPY . .

WORKDIR /app/cmd/migrator

RUN CGO_ENABLED=0 GOOS=linux go build -o /migrator .

FROM alpine:3.22

WORKDIR /

COPY --from=builder /migrator /migrator
COPY ./config/local.yml /config/local.yml
COPY ./repository /repository

EXPOSE 8082

CMD ["/migrator"]