CREATE TABLE IF NOT EXISTS cdm.dm_settlement_report (
    id SERIAL CONSTRAINT id_pkey PRIMARY KEY,
    restaurant_id VARCHAR NOT NULL,
    restaurant_name VARCHAR NOT NULL,
    settlement_date DATE NOT NULL CONSTRAINT dm_settlement_report_settlement_date_check CHECK ((settlement_date >= '2022-01-01'::date) AND (settlement_date < '2500-01-01'::date)),
    orders_count INTEGER DEFAULT 0 NOT NULL CONSTRAINT orders_count_more_then_zero CHECK (orders_count >= 0),
    orders_total_sum NUMERIC(14, 2) DEFAULT 0 NOT NULL CONSTRAINT orders_total_sum_more_then_zero CHECK (orders_total_sum >= (0)::numeric),
    orders_bonus_payment_sum NUMERIC(14, 2) DEFAULT 0 NOT NULL CONSTRAINT orders_bonus_payment_sum_more_then_zero CHECK (orders_bonus_payment_sum >= (0)::numeric),
    orders_bonus_granted_sum NUMERIC(14, 2) DEFAULT 0 NOT NULL CONSTRAINT orders_bonus_granted_sum_more_then_zero CHECK (orders_bonus_granted_sum >= (0)::numeric),
    order_processing_fee NUMERIC(14, 2) DEFAULT 0 NOT NULL CONSTRAINT order_processing_fee_more_then_zero CHECK (order_processing_fee >= (0)::numeric),
    restaurant_reward_sum NUMERIC(14, 2) DEFAULT 0 NOT NULL CONSTRAINT restaurant_reward_sum_more_then_zero CHECK (restaurant_reward_sum >= (0)::numeric),
    CONSTRAINT restaurant_id_check UNIQUE (restaurant_id, settlement_date)
);
