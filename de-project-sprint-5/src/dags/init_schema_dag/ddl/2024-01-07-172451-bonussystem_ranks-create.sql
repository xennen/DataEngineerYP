CREATE TABLE IF NOT EXISTS stg.bonussystem_ranks (
    id INTEGER NOT NULL PRIMARY KEY,
    name VARCHAR(2048) NOT NULL,
    bonus_percent NUMERIC(19, 5) DEFAULT 0 NOT NULL 
        CONSTRAINT ranks_bonus_percent_check CHECK (bonus_percent >= 0),
    min_payment_threshold NUMERIC(19, 5) DEFAULT 0 NOT NULL
);


