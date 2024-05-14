import uuid
from datetime import datetime
from logging import Logger
from typing import Any, Dict, List

from dds_loader.repository.models import *
from lib.pg import PgConnect


class DdsRepository:
    def __init__(self, db: PgConnect, logger: Logger) -> None:
        self._db = db
        self._logger = logger

    def insert_h_category(self,
                          category_list: List[H_Category]
                          ) -> None:
        for category in category_list:
            with self._db.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO dds.h_category(h_category_pk, category_name, load_dt, load_src)
                        VALUES (%(h_category_pk)s, %(category_name)s, %(load_dt)s, %(load_src)s)
                            ON CONFLICT (h_category_pk) DO UPDATE
                           SET
                               category_name = EXCLUDED.category_name,
                               load_dt = EXCLUDED.load_dt,
                               load_src = EXCLUDED.load_src;
                    """,
                        {
                            'h_category_pk': category.h_category_pk,
                            'category_name': category.category_name,
                            'load_dt': category.load_dt,
                            'load_src': category.load_src
                        }
                    )

    def insert_h_user(self,
                      user: H_User,

                      ) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO dds.h_user(h_user_pk, user_id, load_dt, load_src)
                    VALUES (%(h_user_pk)s, %(user_id)s, %(load_dt)s, %(load_src)s)
                        ON CONFLICT (h_user_pk) DO UPDATE
                       SET
                           user_id = EXCLUDED.user_id,
                           load_dt = EXCLUDED.load_dt,
                           load_src = EXCLUDED.load_src;
                """,
                    {
                        'h_user_pk': user.h_user_pk,
                        'user_id': user.user_id,
                        'load_dt': user.load_dt,
                        'load_src': user.load_src
                    }
                )
    
    def insert_h_restaurant(self,
                            restaurant: H_Restaurant,
                            ) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO dds.h_restaurant(h_restaurant_pk, restaurant_id, load_dt, load_src)
                    VALUES (%(h_restaurant_pk)s, %(restaurant_id)s, %(load_dt)s, %(load_src)s)
                        ON CONFLICT (h_restaurant_pk) DO UPDATE
                       SET
                           restaurant_id = EXCLUDED.restaurant_id,
                           load_dt = EXCLUDED.load_dt,
                           load_src = EXCLUDED.load_src;
                """,
                    {
                        'h_restaurant_pk': restaurant.h_restaurant_pk,
                        'restaurant_id': restaurant.restaurant_id,
                        'load_dt': restaurant.load_dt,
                        'load_src': restaurant.load_src
                    }
                )

    def insert_h_product(self,
                         product_list: List[H_Product]
                         ) -> None:
        for product in product_list:
            with self._db.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO dds.h_product(h_product_pk, product_id, load_dt, load_src)
                        VALUES (%(h_product_pk)s, %(product_id)s, %(load_dt)s, %(load_src)s)
                            ON CONFLICT (h_product_pk) DO UPDATE
                        SET
                            product_id = EXCLUDED.product_id,
                            load_dt = EXCLUDED.load_dt,
                            load_src = EXCLUDED.load_src;
                    """,
                        {
                        'h_product_pk': product.h_product_pk,
                        'product_id': product.product_id,
                        'load_dt': product.load_dt,
                        'load_src': product.load_src
                        }
                    )
        
    def insert_h_order(self,
                       order: H_Order,
                       ) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO dds.h_order(h_order_pk, order_id, order_dt, load_dt, load_src)
                    VALUES (%(h_order_pk)s, %(order_id)s, %(order_dt)s, %(load_dt)s, %(load_src)s)
                        ON CONFLICT (h_order_pk) DO UPDATE
                       SET
                           order_id = EXCLUDED.order_id,
                           order_dt = EXCLUDED.order_dt,
                           load_dt = EXCLUDED.load_dt,
                           load_src = EXCLUDED.load_src;
                """,
                    {
                        'h_order_pk': order.h_order_pk,
                        'order_id': order.order_id,
                        'order_dt': order.order_dt,
                        'load_dt': order.load_dt,
                        'load_src': order.load_src
                    }
                )
        
    def insert_l_order_product(self,
                               order_product_list: List[L_Order_Product]
                               ) -> None:
        for order_product in order_product_list:
            with self._db.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO dds.l_order_product(hk_order_product_pk, h_order_pk, h_product_pk, load_dt, load_src)
                        VALUES (%(hk_order_product_pk)s, %(h_order_pk)s, %(h_product_pk)s, %(load_dt)s, %(load_src)s)
                            ON CONFLICT (hk_order_product_pk) DO UPDATE
                        SET
                            h_order_pk = EXCLUDED.h_order_pk,
                            h_product_pk = EXCLUDED.h_product_pk,
                            load_dt = EXCLUDED.load_dt,
                            load_src = EXCLUDED.load_src;
                    """,
                        {
                            'hk_order_product_pk': order_product.hk_order_product_pk,
                            'h_order_pk': order_product.h_order_pk,
                            'h_product_pk': order_product.h_product_pk,
                            'load_dt': order_product.load_dt,
                            'load_src': order_product.load_src
                        }
                    )

    def insert_l_order_user(self,
                            order_user: L_Order_User
                            ) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO dds.l_order_user(hk_order_user_pk, h_order_pk, h_user_pk, load_dt, load_src)
                    VALUES (%(hk_order_user_pk)s, %(h_order_pk)s, %(h_user_pk)s, %(load_dt)s, %(load_src)s)
                        ON CONFLICT (hk_order_user_pk) DO UPDATE
                       SET
                           h_order_pk = EXCLUDED.h_order_pk,
                           h_user_pk = EXCLUDED.h_user_pk,
                           load_dt = EXCLUDED.load_dt,
                           load_src = EXCLUDED.load_src;
                """,
                    {
                        'hk_order_user_pk': order_user.hk_order_user_pk,
                        'h_order_pk': order_user.h_order_pk,
                        'h_user_pk': order_user.h_user_pk,
                        'load_dt': order_user.load_dt,
                        'load_src': order_user.load_src
                    }
                )

    def insert_l_product_category(self,
                                  product_category_list: List[L_Product_Category]
                                  ) -> None:
        for product_category in product_category_list:
            with self._db.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO dds.l_product_category(hk_product_category_pk, h_product_pk, h_category_pk, load_dt, load_src)
                        VALUES (%(hk_product_category_pk)s, %(h_product_pk)s, %(h_category_pk)s, %(load_dt)s, %(load_src)s)
                            ON CONFLICT (hk_product_category_pk) DO UPDATE
                        SET
                            h_product_pk = EXCLUDED.h_product_pk,
                            h_category_pk = EXCLUDED.h_category_pk,
                            load_dt = EXCLUDED.load_dt,
                            load_src = EXCLUDED.load_src;
                    """,
                        {
                            'hk_product_category_pk': product_category.hk_product_category_pk,
                            'h_product_pk': product_category.h_product_pk,
                            'h_category_pk': product_category.h_category_pk,
                            'load_dt': product_category.load_dt,
                            'load_src': product_category.load_src
                        }
                    )

    def insert_l_product_restaurant(self,
                                    product_restaurant_list: List[L_Product_Restaurant],
                                    ) -> None:
        for product_restaurant in product_restaurant_list:
            with self._db.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO dds.l_product_restaurant(hk_product_restaurant_pk, h_restaurant_pk,  h_product_pk, load_dt, load_src)
                        VALUES (%(hk_product_restaurant_pk)s, %(h_restaurant_pk)s,  %(h_product_pk)s, %(load_dt)s, %(load_src)s)
                            ON CONFLICT (hk_product_restaurant_pk) DO UPDATE
                        SET
                            h_restaurant_pk = EXCLUDED.h_restaurant_pk,
                            h_product_pk = EXCLUDED.h_product_pk,
                            load_dt = EXCLUDED.load_dt,
                            load_src = EXCLUDED.load_src;
                    """,
                        {
                            'hk_product_restaurant_pk': product_restaurant.hk_product_restaurant_pk,
                            'h_product_pk': product_restaurant.h_product_pk,
                            'h_restaurant_pk': product_restaurant.h_restaurant_pk,
                            'load_dt': product_restaurant.load_dt,
                            'load_src': product_restaurant.load_src
                        }
                    )

    def insert_s_order_cost(self,
                            order_cost: S_Order_Cost,
                            ) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO dds.s_order_cost(hk_order_cost_hashdiff, h_order_pk, cost, payment, load_dt, load_src)
                    VALUES (%(hk_order_cost_hashdiff)s, %(h_order_pk)s, %(cost)s,  %(payment)s, %(load_dt)s, %(load_src)s)
                        ON CONFLICT (hk_order_cost_hashdiff) DO UPDATE
                       SET
                           h_order_pk = EXCLUDED.h_order_pk,
                           cost = EXCLUDED.cost,
                           payment = EXCLUDED.payment,
                           load_dt = EXCLUDED.load_dt,
                           load_src = EXCLUDED.load_src;
                """,
                    {
                        'h_order_pk': order_cost.h_order_pk,
                        'cost': order_cost.cost,
                        'payment': order_cost.payment,
                        'load_dt': order_cost.load_dt,
                        'load_src': order_cost.load_src,
                        'hk_order_cost_hashdiff': order_cost.hk_order_cost_hashdiff
                    }
                )

    def insert_s_order_status(self,
                              order_status: S_Order_Status
                              ) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO dds.s_order_status(hk_order_status_hashdiff, h_order_pk, status, load_dt, load_src)
                    VALUES (%(hk_order_status_hashdiff)s, %(h_order_pk)s, %(status)s, %(load_dt)s, %(load_src)s)
                        ON CONFLICT (hk_order_status_hashdiff) DO UPDATE
                       SET
                           h_order_pk = EXCLUDED.h_order_pk,
                           status = EXCLUDED.status,
                           load_dt = EXCLUDED.load_dt,
                           load_src = EXCLUDED.load_src;
                """,
                    {
                        'h_order_pk': order_status.h_order_pk,
                        'status': order_status.status,
                        'load_dt': order_status.load_dt,
                        'load_src': order_status.load_src,
                        'hk_order_status_hashdiff': order_status.hk_order_status_hashdiff
                    }
                )

    def insert_s_product_names(self,
                               product_names_list: List[S_Product_Names]
                               ) -> None:
        for product_names in product_names_list:
            with self._db.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO dds.s_product_names(hk_product_names_hashdiff, h_product_pk, name, load_dt, load_src)
                        VALUES (%(hk_product_names_hashdiff)s, %(h_product_pk)s, %(name)s, %(load_dt)s, %(load_src)s)
                            ON CONFLICT (hk_product_names_hashdiff) DO UPDATE
                        SET
                            h_product_pk = EXCLUDED.h_product_pk,
                            name = EXCLUDED.name,
                            load_dt = EXCLUDED.load_dt,
                            load_src = EXCLUDED.load_src;
                    """,
                        {
                            'h_product_pk': product_names.h_product_pk,
                            'name': product_names.product_name,
                            'load_dt': product_names.load_dt,
                            'load_src': product_names.load_src,
                            'hk_product_names_hashdiff': product_names.hk_product_names_hashdiff
                        }
                    )

    def insert_s_restaurant_names(self,
                                  restaurant_names: S_Restaurant_Names
                                  ) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO dds.s_restaurant_names(hk_restaurant_names_hashdiff, h_restaurant_pk, name, load_dt, load_src)
                    VALUES (%(hk_restaurant_names_hashdiff)s, %(h_restaurant_pk)s, %(name)s, %(load_dt)s, %(load_src)s)
                        ON CONFLICT (hk_restaurant_names_hashdiff) DO UPDATE
                       SET
                           h_restaurant_pk = EXCLUDED.h_restaurant_pk,
                           name = EXCLUDED.name,
                           load_dt = EXCLUDED.load_dt,
                           load_src = EXCLUDED.load_src;
                """,
                    {
                        'h_restaurant_pk': restaurant_names.h_restaurant_pk,
                        'name': restaurant_names.restaurant_name,
                        'load_dt': restaurant_names.load_dt,
                        'load_src': restaurant_names.load_src,
                        'hk_restaurant_names_hashdiff': restaurant_names.hk_restaurant_names_hashdiff
                    }
                )

    def insert_s_user_names(self,
                            user_names: S_User_Names
                            ) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO dds.s_user_names(hk_user_names_hashdiff, h_user_pk, userlogin, username, load_dt, load_src)
                    VALUES (%(hk_user_names_hashdiff)s, %(h_user_pk)s, %(userlogin)s, %(username)s, %(load_dt)s, %(load_src)s)
                        ON CONFLICT (hk_user_names_hashdiff) DO UPDATE
                       SET
                           h_user_pk = EXCLUDED.h_user_pk,
                           userlogin = EXCLUDED.userlogin,
                           username = EXCLUDED.username,
                           load_dt = EXCLUDED.load_dt,
                           load_src = EXCLUDED.load_src;
                """,
                    {
                        'h_user_pk': user_names.h_user_pk,
                        'username': user_names.username,
                        'userlogin': user_names.userlogin,
                        'load_dt': user_names.load_dt,
                        'load_src': user_names.load_src,
                        'hk_user_names_hashdiff': user_names.hk_user_names_hashdiff
                    }
                )