from psycopg import Connection

class DestRepository:

    def insert_settlement_report(self, conn: Connection, report) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO cdm.dm_settlement_report(restaurant_id, restaurant_name, settlement_date, orders_count, orders_total_sum, orders_bonus_payment_sum, orders_bonus_granted_sum, order_processing_fee, restaurant_reward_sum)
                    VALUES (%(restaurant_id)s, %(restaurant_name)s, %(settlement_date)s, %(orders_count)s, %(orders_total_sum)s, %(orders_bonus_payment_sum)s, %(orders_bonus_granted_sum)s, %(order_processing_fee)s, %(restaurant_reward_sum)s)
                    ON CONFLICT (restaurant_id, settlement_date) DO UPDATE 
                    SET
                            orders_count = EXCLUDED.orders_count,
                            orders_total_sum = EXCLUDED.orders_total_sum,
                            orders_bonus_payment_sum = EXCLUDED.orders_bonus_payment_sum,
                            orders_bonus_granted_sum = EXCLUDED.orders_bonus_granted_sum,
                            order_processing_fee = EXCLUDED.order_processing_fee,
                            restaurant_reward_sum = EXCLUDED.restaurant_reward_sum;
                """,
                {
                    "restaurant_id": report.restaurant_id,
                    "restaurant_name": report.restaurant_name,
                    "settlement_date": report.settlement_date,
                    "orders_count":report.orders_count,
                    "orders_total_sum": report.orders_total_sum,
                    "orders_bonus_payment_sum": report.orders_bonus_payment_sum,
                    "orders_bonus_granted_sum": report.orders_bonus_granted_sum,
                    "order_processing_fee": report.order_processing_fee,
                    "restaurant_reward_sum": report.restaurant_reward_sum
                }
            )
            
    
    def insert_courier_ledger_report(self, conn: Connection, report) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO cdm.dm_courier_ledger(courier_id,
                                   courier_name,
                                   settlement_year,
                                   settlement_month,
                                   orders_count,
                                   orders_total_sum,
                                   rate_avg,
                                   order_processing_fee,
                                   courier_order_sum,
                                   courier_tips_sum,
                                   courier_reward_sum)
                    VALUES (%(courier_id)s, %(courier_name)s, %(settlement_year)s, %(settlement_month)s, 
                    %(orders_count)s, %(orders_total_sum)s, %(rate_avg)s, %(order_processing_fee)s,
                    %(courier_order_sum)s, %(courier_tips_sum)s, %(courier_reward_sum)s)
                    ON CONFLICT (courier_id, settlement_year, settlement_month) DO UPDATE
                                set courier_name         = EXCLUDED.courier_name,
                                orders_count         = EXCLUDED.orders_count,
                                orders_total_sum     = EXCLUDED.orders_total_sum,
                                rate_avg             = EXCLUDED.rate_avg,
                                order_processing_fee = EXCLUDED.order_processing_fee,
                                courier_order_sum    = EXCLUDED.courier_order_sum,
                                courier_tips_sum     = EXCLUDED.courier_tips_sum,
                                courier_reward_sum   = EXCLUDED.courier_reward_sum;
                """,
                {
                    "courier_id": report.courier_id,
                    "courier_name": report.courier_name,
                    "settlement_year": report.settlement_year,
                    "settlement_month":report.settlement_month,
                    "orders_count": report.orders_count,
                    "orders_total_sum": report.orders_total_sum,
                    "rate_avg": report.rate_avg,
                    "order_processing_fee": report.order_processing_fee,
                    "courier_order_sum": report.courier_order_sum,
                    "courier_tips_sum": report.courier_tips_sum,
                    "courier_reward_sum": report.courier_reward_sum
                }
            )