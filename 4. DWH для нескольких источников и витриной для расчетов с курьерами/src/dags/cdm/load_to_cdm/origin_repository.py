from typing import List

from lib import PgConnect
from psycopg.rows import class_row
from pydantic import BaseModel
from datetime import date

class SettlementReportObj(BaseModel):
    restaurant_id: int
    restaurant_name: str
    settlement_date: date
    orders_count: int
    orders_total_sum: float
    orders_bonus_payment_sum: float
    orders_bonus_granted_sum: float
    order_processing_fee: float
    restaurant_reward_sum: float
    
    
class CourierLedgerReportObj(BaseModel):
    courier_id: int
    courier_name: str
    settlement_year: int
    settlement_month: int
    orders_count: int
    orders_total_sum: float
    rate_avg: float
    order_processing_fee: float
    courier_order_sum: float
    courier_tips_sum: float
    courier_reward_sum: float


class OriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_settlement_report(self) -> List[SettlementReportObj]:
        with self._db.client().cursor(row_factory=class_row(SettlementReportObj)) as cur:
            cur.execute(
                """
                        WITH aggregated_data AS (
                            SELECT
                                dr.id as restaurant_id,
                                dr.restaurant_name,
                                date AS settlement_date,
                                COUNT(DISTINCT doo.id) AS orders_count,
                                SUM(total_sum) AS orders_total_sum,
                                SUM(bonus_payment) AS orders_bonus_payment_sum,
                                SUM(bonus_grant) AS orders_bonus_granted_sum,
                                SUM(total_sum / 100 * 25) AS order_processing_fee,
                                SUM(total_sum - (total_sum / 100 * 25) - bonus_payment) AS restaurant_reward_sum
                            FROM
                                dds.dm_restaurants dr
                            JOIN dds.dm_orders doo ON dr.id = doo.restaurant_id
                            JOIN dds.dm_timestamps dt ON doo.timestamp_id = dt.id
                            JOIN dds.fct_product_sales fps ON fps.order_id = doo.id
                            GROUP BY
                                dr.id,
                                dr.restaurant_name,
                                date
                        )
                        SELECT
                            ad.restaurant_id,
                            ad.restaurant_name,
                            ad.settlement_date,
                            ad.orders_count,
                            ad.orders_total_sum,
                            ad.orders_bonus_payment_sum,
                            ad.orders_bonus_granted_sum,
                            ad.order_processing_fee,
                            ad.restaurant_reward_sum
                        FROM
                            aggregated_data ad
                """
            )
            objs = cur.fetchall()
        return objs
    
    def list_courier_ledger_report(self) -> List[CourierLedgerReportObj]:
        with self._db.client().cursor(row_factory=class_row(CourierLedgerReportObj)) as cur:
            cur.execute(
                """
                        SELECT
                            dm_couriers.id AS courier_id,
                            dm_couriers.courier_name AS courier_name,
                            EXTRACT(year FROM dm_timestamps.ts) AS settlement_year,
                            EXTRACT(month FROM dm_timestamps.ts) AS settlement_month,
                            COUNT(DISTINCT dm_orders.id) AS orders_count,
                            SUM(fct_deliveries.sum) AS orders_total_sum,
                            AVG(fct_deliveries.rate) AS rate_avg,
                            SUM(fct_deliveries.sum * 0.25) AS order_processing_fee,
                            SUM(CASE
                                    WHEN fct_deliveries.rate < 4 THEN GREATEST(fct_deliveries.sum * 0.05, 100)
                                    WHEN fct_deliveries.rate >= 4 AND fct_deliveries.rate < 4.5 THEN GREATEST(fct_deliveries.sum * 0.07, 150)
                                    WHEN fct_deliveries.rate >= 4.5 AND fct_deliveries.rate < 4.9 THEN GREATEST(fct_deliveries.sum * 0.08, 175)
                                    WHEN fct_deliveries.rate >= 4.9 THEN GREATEST(fct_deliveries.sum * 0.1, 200)
                                END) AS courier_order_sum,
                            SUM(fct_deliveries.tip_sum) AS courier_tips_sum,
                            SUM((fct_deliveries.tip_sum + CASE
                                    WHEN fct_deliveries.rate < 4 THEN GREATEST(fct_deliveries.sum * 0.05, 100)
                                    WHEN fct_deliveries.rate >= 4 AND fct_deliveries.rate < 4.5 THEN GREATEST(fct_deliveries.sum * 0.07, 150)
                                    WHEN fct_deliveries.rate >= 4.5 AND fct_deliveries.rate < 4.9 THEN GREATEST(fct_deliveries.sum * 0.08, 175)
                                    WHEN fct_deliveries.rate >= 4.9 THEN GREATEST(fct_deliveries.sum * 0.1, 200)
                                END) * 0.95) AS courier_reward_sum
                        FROM
                            dds.dm_couriers
                            JOIN dds.fct_deliveries ON dm_couriers.id = fct_deliveries.courier_id
                            JOIN dds.dm_orders ON fct_deliveries.order_id = dm_orders.id
                            JOIN dds.dm_timestamps ON fct_deliveries.delivery_ts = dm_timestamps.id
                        GROUP BY
                            fct_deliveries.id,
                            dm_couriers.id,
                            dm_couriers.courier_name,
                            settlement_year,
                            settlement_month;
                """
            )
            objs = cur.fetchall()
        return objs