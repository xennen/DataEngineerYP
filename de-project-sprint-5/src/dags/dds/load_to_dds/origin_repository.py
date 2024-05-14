from typing import List
from lib import PgConnect
import json
from psycopg.rows import class_row
from pydantic import BaseModel
from datetime import datetime, time, date


class SalesEventsObj(BaseModel):
    id: int
    event_ts: datetime
    event_type: str
    event_value : str

class PreDeliveriesObj(BaseModel):
    id: int
    object_id: str
    object_value: str
    update_ts: datetime


class DeliveriesObj(BaseModel):
    id: int
    delivery_id: str
    order_ts: int
    delivery_ts: int
    order_id: int
    courier_id: int
    rate: int
    sum: float
    tip_sum: float


class PreCourierObj(BaseModel):
    id: int
    object_id: str
    object_value: str
    update_ts: datetime


class SaleObj(BaseModel):
    id: int
    product_id: str
    order_id: str
    count: int
    price: float
    total_sum: float
    bonus_payment: float
    bonus_grant: float


class CourierObj(BaseModel):
    id: int
    courier_id: str
    courier_name: str
    
    
class PreOrderObj(BaseModel):
    id: int
    object_id: str
    object_value: str
    update_ts : datetime
    
    
class OrderObj(BaseModel):
    id: int
    order_key: str
    order_status: str
    restaurant_id: int
    timestamp_id: int
    user_id: int
    
    
class ProductObj(BaseModel):
    id: int
    product_id: str
    product_name: str
    product_price : float
    active_from: datetime
    active_to: datetime
    restaurant_id: int
    
    
class RestaurantObj(BaseModel):
    id: int
    restaurant_id: str
    restaurant_name : str
    active_from: datetime
    active_to: datetime


class TimestampObj(BaseModel):
    id: int
    ts: datetime
    year: int
    month : int
    day: int
    time: time
    date: date


class UserObj(BaseModel):
    id: int
    user_id : str
    login: str
    name: str


class OriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg
        

    def list_orders(self, threshold: int, limit: int) -> List[OrderObj]:
        with self._db.client().cursor(row_factory=class_row(PreOrderObj)) as cur:
            cur.execute(
                """
                    SELECT id, object_id, object_value, update_ts
                    FROM stg.ordersystem_orders
                    WHERE id > %(threshold)s
                    ORDER BY 1 ASC
                    LIMIT %(limit)s;
                """, {
                    "threshold": threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()

            res = []
            for re_json in objs:
                rest_json = json.loads(re_json.object_value)
                
                with self._db.client().cursor() as cursor:
                    cursor.execute(
                    """
                        SELECT id
                        FROM dds.dm_users
                        WHERE user_id = %(user_id)s
                    """,{
                        "user_id": rest_json['user']['id'],
                        }
                )
                    id_user = cursor.fetchall()
                
                with self._db.client().cursor() as curs:
                    curs.execute(
                    """
                        SELECT id
                        FROM dds.dm_restaurants
                        WHERE restaurant_id = %(restaurant_id)s
                    """,{
                        "restaurant_id": rest_json['restaurant']['id'],
                        }
                )
                    id_restaurant = curs.fetchall()
                
                with self._db.client().cursor() as c:
                    c.execute(
                    """
                        SELECT id
                        FROM dds.dm_timestamps
                        WHERE ts = %(timestamp)s
                    """,{
                        "timestamp": datetime.strptime(rest_json['update_ts'], "%Y-%m-%d %H:%M:%S"),
                        }
                )
                    id_timestamp = c.fetchall()
                if id_user and id_restaurant and id_timestamp:
                    t = OrderObj(id=re_json.id,
                                order_key=re_json.object_id,
                                order_status=rest_json['final_status'],
                                restaurant_id=int(id_restaurant[0][0]),
                                timestamp_id=int(id_timestamp[0][0]),
                                user_id=int(id_user[0][0]),
                        )
                    res.append(t)
        return res
    
    
    def list_sales(self, threshold: int, limit: int) -> List[SaleObj]:
        with self._db.client().cursor(row_factory=class_row(SalesEventsObj)) as cur:
            cur.execute(
                """
                    SELECT id, event_ts, event_type, event_value
                    FROM stg.bonussystem_events
                    WHERE id > %(threshold)s and event_type = %(type)s
                    ORDER BY 1 ASC
                    LIMIT %(limit)s;
                """, {
                    "threshold": threshold,
                    "limit": limit,
                    "type": 'bonus_transaction'
                }
            )
            objs = cur.fetchall()
            
        res = []
        for re_json in objs:
            sales_json = json.loads(re_json.event_value)
            for order_item in sales_json['product_payments']:
                with self._db.client().cursor() as curs:
                    curs.execute(
                    """
                        SELECT id
                        FROM dds.dm_products
                        WHERE product_id = %(product_id)s
                    """,{
                        "product_id": order_item['product_id']
                        }
                )
                    id_product = curs.fetchall()
                
                with self._db.client().cursor() as c:
                    c.execute(
                    """
                        SELECT id
                        FROM dds.dm_orders
                        WHERE order_key = %(order_id)s
                    """,{
                        "order_id": sales_json['order_id']
                        }
                )
                    id_order = c.fetchall()
                if id_order and id_product:         
                    t = SaleObj(id=re_json.id,
                                product_id=id_product[0][0],
                                order_id=id_order[0][0],
                                count=order_item['quantity'],
                                price=order_item['price'],
                                total_sum=order_item['product_cost'],
                                bonus_payment=order_item['bonus_payment'],
                                bonus_grant=order_item['bonus_grant']
                        )
                    res.append(t)
        return res
    
    
    def list_products(self, threshold: int, limit: int) -> List[ProductObj]:
        with self._db.client().cursor() as cur:
            cur.execute(
                """
                    SELECT sor.id, sor.object_value, dr.id as restaurant_id
                    FROM stg.ordersystem_restaurants as sor
                    JOIN dds.dm_restaurants as dr on sor.object_id = dr.restaurant_id
                    WHERE sor.id > %(threshold)s
                    ORDER BY 1 ASC
                    LIMIT %(limit)s;
                """, {
                    "threshold": threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
    
            
            res = []
            for r in objs:
                re_json = json.loads(r[1])
                for products_json in re_json['menu']:
                    t = ProductObj(id=r[0],
                                product_id=products_json['_id'],
                                product_name=products_json['name'],
                                product_price=products_json['price'],
                                active_from=datetime.strptime(re_json['update_ts'], "%Y-%m-%d %H:%M:%S"),
                                active_to=datetime(year=2099, month=12, day=31),
                                restaurant_id=r[2]
                    )
                    res.append(t)
        return res
    
    
    def list_restaurants(self, threshold: int, limit: int) -> List[RestaurantObj]:
        with self._db.client().cursor() as cur:
            cur.execute(
                """
                    SELECT id, object_value
                    FROM stg.ordersystem_restaurants
                    WHERE id > %(threshold)s
                    ORDER BY id ASC
                    LIMIT %(limit)s;
                """, {
                    "threshold": threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
    
            
            res = []
            for r in objs:
                rest_json = json.loads(r[1])
                t = RestaurantObj(id=r[0],
                                 restaurant_id=rest_json['_id'],
                                 restaurant_name=rest_json['name'],
                                 active_from=datetime.strptime(rest_json['update_ts'], "%Y-%m-%d %H:%M:%S"),
                                 active_to=datetime(year=2099, month=12, day=31)
                                 )
                res.append(t)
        return res
    
    
    def list_timestamps(self, threshold: int, limit: int) -> List[TimestampObj]:
        with self._db.client().cursor() as cur:
            cur.execute(
                """
                    SELECT id, object_value
                    FROM stg.ordersystem_orders
                    WHERE id > %(threshold)s
                    ORDER BY id ASC
                    LIMIT %(limit)s;
                """, {
                    "threshold": threshold,
                    "limit": limit
                }
            )
            objs_ord = cur.fetchall()
            
        with self._db.client().cursor() as cursor:
            cursor.execute(
                """
                    SELECT id, object_value
                    FROM stg.deliverysystem_deliveries
                    WHERE id > %(threshold)s
                    ORDER BY id ASC
                    LIMIT %(limit)s;
                """, {
                    "threshold": threshold,
                    "limit": limit
                }
            )
            objs_del = cursor.fetchall()
        res = []
        for d in objs_ord:
            timestamps_json_ord = json.loads(d[1])
            dt = datetime.strptime(timestamps_json_ord['date'], "%Y-%m-%d %H:%M:%S")
            t_ord = TimestampObj(   id=d[0],
                                ts=dt,
                                year=dt.year,
                                month=dt.month,
                                day=dt.day,
                                time=dt.time(),
                                date=dt.date()
                                )
            res.append(t_ord)
        for d_del in objs_del:
            timestamps_json_del = json.loads(d_del[1])
            if len(timestamps_json_del['delivery_ts']) > 19:  
                dt_del = datetime.strptime(timestamps_json_del['delivery_ts'][:-7], "%Y-%m-%d %H:%M:%S")
            else:
                dt_del = datetime.strptime(timestamps_json_del['delivery_ts'], "%Y-%m-%d %H:%M:%S")
            t_del = TimestampObj(   id=d_del[0],
                                ts=dt_del,
                                year=dt_del.year,
                                month=dt_del.month,
                                day=dt_del.day,
                                time=dt_del.time(),
                                date=dt_del.date()
                                )
            res.append(t_del)
        return res
    
    
    def list_users(self, threshold: int, limit: int) -> List[UserObj]:
        with self._db.client().cursor() as cur:
            cur.execute(
                """
                    SELECT id, object_value
                    FROM stg.ordersystem_users
                    WHERE id > %(threshold)s
                    ORDER BY id ASC
                    LIMIT %(limit)s;
                """, {
                    "threshold": threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
            
            res = []
            for i in objs:
                s = '{'
                string = i[1]
                string = string.replace('_id', 'user_id').replace(f'{s}', f'''{s}"id": {i[0]}, ''')
                res.append(UserObj.parse_raw(string))
        return res
    
    
    def list_couriers(self, threshold: int, limit: int) -> List[CourierObj]:
        with self._db.client().cursor(row_factory=class_row(PreCourierObj)) as cur:
            cur.execute(
                """
                    SELECT id, object_id, object_value, update_ts
                    FROM stg.deliverysystem_couriers
                    WHERE id > %(threshold)s
                    ORDER BY id ASC
                    LIMIT %(limit)s;
                """, {
                    "threshold": threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
            
            res = []
        for d in objs:
            courier_json = json.loads(d.object_value)
            t = CourierObj(id = d.id,
                            courier_id=d.object_id,
                            courier_name=courier_json['name']
                                )
            res.append(t)
        return res
    
    
    def list_deliveries(self, threshold: int, limit: int) -> List[DeliveriesObj]:
        with self._db.client().cursor(row_factory=class_row(PreDeliveriesObj)) as cur:
            cur.execute(
                """
                    SELECT id, object_id, object_value, update_ts
                    FROM stg.deliverysystem_deliveries
                    WHERE id > %(threshold)s
                    ORDER BY id ASC
                    LIMIT %(limit)s;
                """, {
                    "threshold": threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
            
            res = []
        for d in objs:
            deliveries_json = json.loads(d.object_value)
            if len(deliveries_json['delivery_ts']) > 19:  
                dt_del = datetime.strptime(deliveries_json['delivery_ts'][:-7], "%Y-%m-%d %H:%M:%S")
            else:
                dt_del = datetime.strptime(deliveries_json['delivery_ts'], "%Y-%m-%d %H:%M:%S")
            if len(deliveries_json['order_ts']) > 19:  
                dt_ord = datetime.strptime(deliveries_json['order_ts'][:-7], "%Y-%m-%d %H:%M:%S")
            else:
                dt_ord = datetime.strptime(deliveries_json['order_ts'], "%Y-%m-%d %H:%M:%S")
            with self._db.client().cursor() as cursor:
                    cursor.execute(
                    """
                        SELECT id
                        FROM dds.dm_orders
                        WHERE order_key = %(order_key)s
                    """,{
                        "order_key": d.object_id,
                        }
                )
                    id_order = cursor.fetchall()
                
            with self._db.client().cursor() as curs:
                    curs.execute(
                    """
                        SELECT id
                        FROM dds.dm_couriers
                        WHERE courier_id = %(courier_id)s
                    """,{
                        "courier_id": deliveries_json['courier_id'],
                        }
                )
                    id_courier = curs.fetchall()
                
            with self._db.client().cursor() as c:
                    c.execute(
                    """
                        SELECT id
                        FROM dds.dm_timestamps
                        WHERE ts = %(timestamp)s
                    """,{
                        "timestamp": dt_ord,
                        }
                )
                    id_ts_order = c.fetchall()
            
            with self._db.client().cursor() as cu:
                
                    cu.execute(
                    """
                        SELECT id
                        FROM dds.dm_timestamps
                        WHERE ts = %(timestamp)s
                    """,{
                        "timestamp": dt_del,
                        }
                )
                    id_ts_delivery = cu.fetchall()
            if id_ts_delivery and id_ts_order and id_courier and id_order:
                t = DeliveriesObj(id = d.id,
                                delivery_id= deliveries_json['delivery_id'],
                                order_ts=id_ts_order[0][0],
                                delivery_ts=id_ts_delivery[0][0],
                                order_id=id_order[0][0],
                                courier_id=id_courier[0][0],
                                rate = deliveries_json['rate'],
                                sum = deliveries_json['sum'],
                                tip_sum = deliveries_json['tip_sum']
                                    )
                res.append(t)
        return res
    
    
    