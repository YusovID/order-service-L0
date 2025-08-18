CREATE TABLE IF NOT EXISTS orders (
    order_uid        TEXT PRIMARY KEY,
    track_number     TEXT NOT NULL,
    customer_id      TEXT NOT NULL,
    delivery_service TEXT NOT NULL,
    date_created     TIMESTAMP WITH TIME ZONE NOT NULL,
    payment_data     JSONB NOT NULL,
    delivery_data    JSONB NOT NULL,
    additional_data  JSONB NOT NULL
);

CREATE TABLE IF NOT EXISTS order_items (
    id           SERIAL PRIMARY KEY,
    order_uid    TEXT REFERENCES orders(order_uid) NOT NULL,
    chrt_id      INTEGER NOT NULL,
	track_number TEXT NOT NULL,
	price        NUMERIC NOT NULL,
	rid          TEXT NOT NULL,
	name         TEXT NOT NULL,
	sale         NUMERIC NOT NULL,
	size         TEXT NOT NULL,
	total_price  NUMERIC NOT NULL,
	nm_id        INTEGER NOT NULL,
	brand        TEXT NOT NULL,
	status       INTEGER NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS customer_id_idx ON orders (customer_id);
