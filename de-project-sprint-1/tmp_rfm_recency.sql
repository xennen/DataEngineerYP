INSERT INTO analysis.tmp_rfm_recency
WITH cte AS (
	SELECT user_id, max(diff) AS last_purchase_date
	FROM (SELECT user_id, CASE 
					WHEN status = 4 
					THEN 
					DATE_PART('day', (SELECT max(order_ts) FROM analysis.orders WHERE status = 4) - max(order_ts))
					ELSE NULL END AS diff
			FROM analysis.orders
			GROUP BY 1, status
			ORDER BY 1
		) AS f
GROUP BY 1
ORDER BY 2)
SELECT user_id, ntile(5) OVER w AS recency
FROM cte
WINDOW w AS (ORDER BY last_purchase_date DESC);