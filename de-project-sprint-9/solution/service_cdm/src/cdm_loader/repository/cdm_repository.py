import uuid
from logging import Logger
from typing import List

from lib.pg import PgConnect
from cdm_loader.repository.models import *


class CdmRepository:
    def __init__(self, db: PgConnect, logger: Logger) -> None:
        self._db = db
        self._logger = logger
        
    def user_product_counters_insert(self, user_product: List[User_Product_Counters]) -> None:
        for user_product_counter in user_product:
            user_id = user_product_counter.user_id
            product_id = user_product_counter.product_id
            product_name = user_product_counter.product_name
            with self._db.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(f"""
                        SELECT COUNT(*) FROM cdm.user_product_counters 
                        WHERE user_id = '{user_id}' AND product_id = '{product_id}';
                    """)
                    result = cur.fetchone()[0]
                    if result:
                        cur.execute(f"""
                            UPDATE cdm.user_product_counters 
                            SET order_cnt = order_cnt + 1 
                            WHERE user_id = '{user_id}' AND product_id = '{product_id}';
                        """)
                    else:
                        cur.execute("""INSERT INTO cdm.user_product_counters (user_id, product_id, product_name, order_cnt) 
                                    VALUES (%(user_id)s, %(product_id)s, %(product_name)s, %(order_cnt)s)""",
                                    
                                    {
                                        "user_id": user_id,
                                        "product_id": product_id,
                                        "product_name": product_name,
                                        "order_cnt": 1
                                    })

    def user_category_counters_insert(self, user_category: List[User_Category_Counters]) -> None:
        for user_category_counter in user_category:
            user_id = user_category_counter.user_id
            category_id = user_category_counter.category_id
            category_name = user_category_counter.category_name
            with self._db.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(f"""
                        SELECT COUNT(*) FROM cdm.user_category_counters 
                        WHERE user_id = '{user_id}' AND category_id = '{category_id}';
                    """)
                    result = cur.fetchone()[0]
                    if result:
                        cur.execute(f"""
                            UPDATE cdm.user_category_counters 
                            SET order_cnt = order_cnt + 1 
                            WHERE user_id = '{user_id}' AND category_id = '{category_id}';
                        """)
                    else:
                        cur.execute("""INSERT INTO cdm.user_category_counters (user_id, category_id, category_name, order_cnt) 
                                    VALUES (%(user_id)s, %(category_id)s, %(category_name)s, %(order_cnt)s)""",
                                    {
                                        "user_id": user_id,
                                        "category_id": category_id,
                                        "category_name": category_name,
                                        "order_cnt": 1
                                    })