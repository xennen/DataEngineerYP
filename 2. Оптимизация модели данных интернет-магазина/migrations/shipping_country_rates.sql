CREATE TABLE shipping_country_rates (
    id SERIAL,
    shipping_country TEXT,
    shipping_country_base_rate NUMERIC(14,3),
    PRIMARY KEY (id)
);

INSERT INTO shipping_country_rates (shipping_country, shipping_country_base_rate)
SELECT 
    shipping_country, 
    shipping_country_base_rate
FROM shipping s
GROUP BY 1, 2;
