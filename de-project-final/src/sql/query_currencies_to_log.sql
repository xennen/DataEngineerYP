SELECT COUNT(*)
  FROM public.currencies
 WHERE date_update::date = %(load_date)s;