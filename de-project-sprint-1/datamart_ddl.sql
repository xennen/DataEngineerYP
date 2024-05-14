CREATE TABLE analysis.dm_rfm_segments (
    user_id INT NOT NULL PRIMARY KEY,
    recency INT NOT NULL CHECK (recency BETWEEN 1 AND 5),
    frequency INT NOT NULL CHECK (frequency BETWEEN 1 AND 5),
    monetary_value INT NOT NULL CHECK (monetary_value BETWEEN 1 AND 5)
);
