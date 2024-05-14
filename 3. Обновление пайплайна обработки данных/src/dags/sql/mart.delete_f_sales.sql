DELETE FROM mart.f_sales AS fs
USING mart.d_calendar AS dc
WHERE fs.date_id = dc.date_id 
AND dc.date_actual::Date = '{{ds}}';
