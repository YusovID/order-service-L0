-- Этот файл миграции "up" создает начальную структуру базы данных для сервиса заказов.
-- Он выполняется один раз при первом запуске мигратора или при накате на пустую БД.
-- `IF NOT EXISTS` используется для того, чтобы миграция не вызывала ошибок
-- при повторном запуске, хотя система миграций и так должна это предотвращать.

-- Создаем основную таблицу `orders` для хранения общей информации о заказах.
CREATE TABLE IF NOT EXISTS orders (
    order_uid        TEXT PRIMARY KEY,                     -- Уникальный идентификатор заказа, используется как первичный ключ.
    track_number     TEXT NOT NULL,                        -- Номер для отслеживания.
    customer_id      TEXT NOT NULL,                        -- Идентификатор покупателя.
    delivery_service TEXT NOT NULL,                        -- Название службы доставки.
    date_created     TIMESTAMP WITH TIME ZONE NOT NULL,    -- Дата создания заказа с учетом часового пояса.
    payment_data     JSONB NOT NULL,                       -- Детали платежа, хранятся в эффективном бинарном формате JSONB.
    delivery_data    JSONB NOT NULL,                       -- Детали доставки (адрес, получатель), также в JSONB.
    additional_data  JSONB NOT NULL                        -- Дополнительные метаданные заказа в JSONB.
);

-- Создаем таблицу `order_items` для хранения товаров, входящих в каждый заказ.
-- Эта таблица связана с `orders` через внешний ключ `order_uid`.
CREATE TABLE IF NOT EXISTS order_items (
    id           SERIAL PRIMARY KEY,                         -- Автоинкрементный числовой ID для каждой записи.
    order_uid    TEXT REFERENCES orders(order_uid) NOT NULL, -- Внешний ключ, связывающий товар с конкретным заказом.
    chrt_id      INTEGER NOT NULL,                           -- Уникальный ID товара.
	track_number TEXT NOT NULL,                              -- Номер отслеживания (обычно совпадает с заказом).
	price        NUMERIC NOT NULL,                           -- Цена товара. NUMERIC используется для точных финансовых расчетов.
	rid          TEXT NOT NULL,                              -- Уникальный ID записи о товаре в заказе.
	name         TEXT NOT NULL,                              -- Название товара.
	sale         NUMERIC NOT NULL,                           -- Скидка.
	size         TEXT NOT NULL,                              -- Размер.
	total_price  NUMERIC NOT NULL,                           -- Итоговая цена.
	nm_id        INTEGER NOT NULL,                           -- Артикул товара.
	brand        TEXT NOT NULL,                              -- Бренд.
	status       INTEGER NOT NULL                            -- Статус товара.
);

-- Создаем индекс на поле `customer_id` в таблице `orders`.
-- Это значительно ускорит поиск заказов по конкретному покупателю,
-- что является частым сценарием использования.
CREATE INDEX IF NOT EXISTS customer_id_idx ON orders (customer_id);
