CREATE TABLE IF NOT EXISTS dds.fct_deliveries (
    id SERIAL PRIMARY KEY,
    delivery_id VARCHAR NOT NULL UNIQUE,
    order_ts INT REFERENCES dds.dm_timestamps(id) NOT NULL,
    delivery_ts INT REFERENCES dds.dm_timestamps(id) NOT NULL,
    order_id INT REFERENCES dds.dm_orders(id) NOT NULL,
    courier_id INT REFERENCES dds.dm_couriers(id) NOT NULL,
    rate SMALLINT CHECK(rate BETWEEN 1 AND 5) NOT NULL,
    sum NUMERIC(14, 2) CHECK(sum >= 0.00) NOT NULL,
    tip_sum NUMERIC(14, 2) CHECK(tip_sum >= 0.00) NOT NULL
);
