INSERT INTO analysis.tmp_rfm_monetary_value 
WITH cte AS (
	SELECT user_id, SUM(CASE 
					    WHEN status = 4 
					    THEN cost end) AS sum_of_closed_orders
	FROM analysis.orders
	GROUP BY 1
	ORDER BY 1
)
SELECT user_id, ntile(5) OVER w AS monetary_value
FROM cte
WINDOW w AS (ORDER BY sum_of_closed_orders NULLS FIRST);