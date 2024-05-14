INSERT INTO analysis.tmp_rfm_frequency 
WITH cte AS (
	SELECT user_id, COUNT(CASE 
						WHEN status = 4 
						THEN 1 end) AS count_closed_orders
	FROM analysis.orders
	GROUP BY 1
	ORDER BY 1
)
SELECT user_id, ntile(5) OVER w AS frequency
FROM cte
WINDOW w AS (ORDER BY count_closed_orders NULLS LAST);