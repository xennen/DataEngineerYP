CREATE TABLE shipping_status (
    shipping_id int,
    status text,
    state text,
    shipping_start_fact_datetime timestamp,
    shipping_end_fact_datetime timestamp,
    PRIMARY KEY (shipping_id)
);

INSERT INTO shipping_status (
    shipping_id, 
    status, 
    state, 
    shipping_start_fact_datetime, 
    shipping_end_fact_datetime
)
WITH cte AS (
    SELECT 
        *,
        LEAD(shipping_end_fact_datetime) OVER w AS next_end_fact_datetime
    FROM (
        SELECT 
            shippingid, 
            state_datetime,
            shipping_start_fact_datetime,
            shipping_end_fact_datetime,
            state,
            LAG(state) OVER w AS prev_state
        FROM 
            shipping
        WINDOW w AS (
            PARTITION BY shippingid
            ORDER BY state_datetime
        )
    ) AS subquery
    WHERE 
        (state = 'booked' AND prev_state IS DISTINCT FROM 'booked') OR
        (state = 'recieved' AND next_end_fact_datetime IS NULL)
)

SELECT 
    shippingid AS shipping_id, 
    state AS status, 
    state, 
    shipping_start_fact_datetime, 
    shipping_end_fact_datetime
FROM 
    cte
ORDER BY 
    shipping_id;
