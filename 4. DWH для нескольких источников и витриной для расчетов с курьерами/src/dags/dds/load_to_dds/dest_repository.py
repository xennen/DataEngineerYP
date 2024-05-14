from psycopg import Connection

class DestRepository:
    
    def query_orders(self, conn: Connection,order_obj) -> None: 
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_orders(user_id, restaurant_id, timestamp_id, order_key, order_status)
                    VALUES (%(user_id)s, %(restaurant_id)s, %(timestamp_id)s, %(order_key)s, %(order_status)s)
                    ON CONFLICT (order_key) DO UPDATE
                    SET 
                        restaurant_id = EXCLUDED.restaurant_id,
                        order_status = EXCLUDED.order_status,
                        timestamp_id = EXCLUDED.timestamp_id,
                        user_id = EXCLUDED.user_id;
                """,
                {
                    "order_status": order_obj.order_status,
                    "order_key": order_obj.order_key,
                    "restaurant_id": order_obj.restaurant_id,
                    "timestamp_id": order_obj.timestamp_id,
                    "user_id": order_obj.user_id,
                }
            )
            
    def query_users(self, conn: Connection, user_obj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_users(user_id, user_name, user_login)
                    VALUES (%(user_id)s, %(user_name)s, %(user_login)s)
                    ON CONFLICT (user_id) DO UPDATE
                    SET
                        user_name = EXCLUDED.user_name,
                        user_login = EXCLUDED.user_login;
                """,
                {
                    "user_id": user_obj.user_id,
                    "user_name": user_obj.name,
                    "user_login":user_obj.login
                }
            )
            
    def query_sales(self, conn: Connection, sales_obj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.fct_product_sales(product_id, order_id, count, price, total_sum, bonus_payment, bonus_grant)
                    VALUES (%(product_id)s, %(order_id)s, %(count)s, %(price)s, %(total_sum)s, %(bonus_payment)s, %(bonus_grant)s)
                """,
                {
                    "product_id": sales_obj.product_id,
                    "order_id": sales_obj.order_id,
                    "count": sales_obj.count,
                    "price": sales_obj.price,
                    "total_sum": sales_obj.total_sum,
                    "bonus_payment": sales_obj.bonus_payment,
                    "bonus_grant": sales_obj.bonus_grant
                }
            )
            
    def query_restaurants(self, conn: Connection, res_obj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_restaurants(restaurant_id, restaurant_name, active_from, active_to)
                    VALUES (%(restaurant_id)s, %(restaurant_name)s, %(active_from)s, %(active_to)s)
                    ON CONFLICT (restaurant_id) DO UPDATE
                    SET 
                        id = EXCLUDED.id,
                        restaurant_name = EXCLUDED.restaurant_name,
                        active_from = EXCLUDED.active_from,
                        active_to = EXCLUDED.active_to;
                """,
                {
                    "restaurant_id": res_obj.restaurant_id,
                    "restaurant_name": res_obj.restaurant_name,
                    "active_from": res_obj.active_from,
                    "active_to": res_obj.active_to
                }
            )
            
    def query_products(self, conn: Connection, pr_obj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_products(restaurant_id, product_id, product_name, product_price, active_from, active_to)
                    VALUES (%(restaurant_id)s, %(product_id)s, %(product_name)s, %(product_price)s, %(active_from)s, %(active_to)s)
                    ON CONFLICT (product_id) DO UPDATE
                    SET 
                        restaurant_id = EXCLUDED.restaurant_id,
                        product_name = EXCLUDED.product_name,
                        active_from = EXCLUDED.active_from,
                        active_to = EXCLUDED.active_to;
                """,
                {
                    "product_id": pr_obj.product_id,
                    "product_name": pr_obj.product_name,
                    "product_price": pr_obj.product_price,
                    "active_from": pr_obj.active_from,
                    "active_to": pr_obj.active_to,
                    "restaurant_id": pr_obj.restaurant_id
                }
            )
    
    def query_timestamps(self, conn: Connection, ts_obj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_timestamps(ts, year, month, day, time, date)
                    VALUES (%(ts)s, %(year)s, %(month)s, %(day)s,%(time)s, %(date)s)
                """,
                {
                    "ts": ts_obj.ts,
                    "year": ts_obj.year,
                    "month": ts_obj.month,
                    "day": ts_obj.day,
                    "time": ts_obj.time,
                    "date": ts_obj.date
                    
                }
            )
            
    def query_couriers(self, conn: Connection, courier_obj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_couriers(courier_id, courier_name)
                    VALUES (%(courier_id)s, %(courier_name)s)
                """,
                {
                    "courier_id": courier_obj.courier_id,
                    "courier_name": courier_obj.courier_name        
                }
            )
            
    def query_deliveries(self, conn: Connection, delivery_obj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.fct_deliveries(delivery_id, order_ts, delivery_ts, order_id, courier_id, rate, sum, tip_sum)
                    VALUES (%(delivery_id)s, %(order_ts)s, %(delivery_ts)s, %(order_id)s,%(courier_id)s, %(rate)s,%(sum)s, %(tip_sum)s)
                """,
                {
                    "delivery_id": delivery_obj.delivery_id,
                    "order_ts": delivery_obj.order_ts,    
                    "delivery_ts": delivery_obj.delivery_ts,
                    "order_id": delivery_obj.order_id,  
                    "courier_id": delivery_obj.courier_id,
                    "rate": delivery_obj.rate,  
                    "sum": delivery_obj.sum,
                    "tip_sum": delivery_obj.tip_sum  
                }
            )
            
            
            
            
            