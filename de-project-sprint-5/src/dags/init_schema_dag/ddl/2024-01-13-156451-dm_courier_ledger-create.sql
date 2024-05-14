CREATE TABLE IF NOT EXISTS cdm.dm_courier_ledger (
    id SERIAL PRIMARY KEY,
    courier_id VARCHAR NOT NULL,
    courier_name VARCHAR NOT NULL,
    settlement_year SMALLINT CHECK (settlement_year BETWEEN 2022 AND 2500),
    settlement_month SMALLINT CHECK (settlement_month BETWEEN 1 AND 12),
    orders_count INT DEFAULT 0 CHECK (orders_count >= 0),
    orders_total_sum NUMERIC(14, 2) DEFAULT 0.00 CHECK (orders_total_sum >= 0.00),
    rate_avg NUMERIC(4, 2) DEFAULT 0.00 CHECK (rate_avg BETWEEN 0.00 AND 5.00),
    order_processing_fee NUMERIC(14, 2) DEFAULT 0.00 CHECK (order_processing_fee >= 0.00),
    courier_order_sum NUMERIC(14, 2) DEFAULT 0.00 CHECK (courier_order_sum >= 0.00),
    courier_tips_sum NUMERIC(14, 2) DEFAULT 0.00 CHECK (courier_tips_sum >= 0.00),
    courier_reward_sum NUMERIC(14, 2) DEFAULT 0.00 CHECK (courier_reward_sum >= 0.00),
    CONSTRAINT dm_courier_ledger_unique UNIQUE (courier_id, settlement_year, settlement_month)
);
