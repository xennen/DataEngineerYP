INSERT INTO analysis.dm_rfm_segments (user_id, recency, frequency, monetary_value)
SELECT f.user_id, r.recency, f.frequency, mv.monetary_value
FROM analysis.tmp_rfm_frequency f
JOIN analysis.tmp_rfm_recency r ON f.user_id = r.user_id
JOIN analysis.tmp_rfm_monetary_value mv ON f.user_id = mv.user_id;
