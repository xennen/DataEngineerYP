import uuid
from datetime import datetime
from typing import Any, List

from dds_loader.repository.models import *

class OrderDdsBuilder:
    def __init__(self, order: Order) -> None:
        self._order = order
        self.source_system = "stg_kafka_topic"
        
    def _uuid(self, obj: Any) -> uuid.UUID:
        if isinstance(obj, list):
            res = [str(x) for x in obj]
            return uuid.uuid3(uuid.NAMESPACE_X500, ''.join(res))
        
        return uuid.uuid3(uuid.NAMESPACE_X500, str(obj))
    
    def h_user(self) -> H_User:
        user_id = self._order.user.id
        return H_User(
            h_user_pk=self._uuid(user_id),
            user_id=user_id,
            load_dt=datetime.utcnow(),
            load_src=self.source_system
        )
        
    def h_product(self) -> List[H_Product]:
        products = []
        for prod_dict in self._order.products:
            prod_id = prod_dict.id
            products.append(
                H_Product(
                    h_product_pk=self._uuid(prod_id),
                    product_id=prod_id,
                    load_dt=datetime.utcnow(),
                    load_src=self.source_system
                )
            )
        return products

    def h_category(self) -> List[H_Category]:
        categories = []
        for prod_dict in self._order.products:
            cat_name = prod_dict.category
            categories.append(
                H_Category(
                    h_category_pk=self._uuid(cat_name),
                    category_name=cat_name,
                    load_dt=datetime.utcnow(),
                    load_src=self.source_system
                )
            )
        return categories
    
    def h_restaurant(self) -> H_Restaurant:
        restaurant_id = self._order.restaurant.id
        return H_Restaurant(
            h_restaurant_pk=self._uuid(restaurant_id),
            restaurant_id=restaurant_id,
            load_dt=datetime.utcnow(),
            load_src=self.source_system
        )
    
    def h_order(self) -> H_Order:
        order_id = self._order.id
        return H_Order(
            h_order_pk=self._uuid(order_id),
            order_id=str(order_id),
            order_dt=self._order.date,
            load_dt=datetime.utcnow(),
            load_src=self.source_system
        )
    
    def l_order_product(self) -> List[L_Order_Product]:
        order_id = self._order.id
        order_product = []
        for prod_dict in self._order.products:
            prod_id = prod_dict.id
            order_product.append(
                L_Order_Product(
                    hk_order_product_pk=self._uuid([self._uuid(prod_id), self._uuid(order_id)]),
                    h_order_pk=self._uuid(order_id),
                    h_product_pk=self._uuid(prod_id),
                    load_dt=datetime.utcnow(),
                    load_src=self.source_system
                )
            )
        return order_product
    
    def l_order_user(self) -> L_Order_User:
        order_id = self._order.id
        user_id = self._order.user.id
        return  L_Order_User(
                    hk_order_user_pk=self._uuid([self._uuid(order_id), self._uuid(user_id)]),
                    h_order_pk=self._uuid(order_id),
                    h_user_pk=self._uuid(user_id),
                    load_dt=datetime.utcnow(),
                    load_src=self.source_system
                )
        
    def l_product_category(self) -> List[L_Product_Category]:
        product_category = []
        for prod_dict in self._order.products:
            prod_id = prod_dict.id
            category = prod_dict.category
            product_category.append(
                L_Product_Category(
                    hk_product_category_pk=self._uuid([self._uuid(prod_id), self._uuid(category)]),
                    h_product_pk=self._uuid(prod_id),
                    h_category_pk=self._uuid(category),
                    load_dt=datetime.utcnow(),
                    load_src=self.source_system
                )
            )
        return product_category
    
    def l_product_restaurant(self) -> List[L_Product_Restaurant]:
        restaurant_id = self._order.restaurant.id
        product_category = []
        for prod_dict in self._order.products:
            prod_id = prod_dict.id
            product_category.append(
                L_Product_Restaurant(
                    hk_product_restaurant_pk=self._uuid([self._uuid(prod_id), self._uuid(restaurant_id)]),
                    h_product_pk=self._uuid(prod_id),
                    h_restaurant_pk=self._uuid(restaurant_id),
                    load_dt=datetime.utcnow(),
                    load_src=self.source_system
                )
            )
        return product_category
    
    def s_order_cost(self) -> S_Order_Cost:
        order_id = self._order.id
        cost = self._order.cost
        payment = self._order.payment
        return  S_Order_Cost(
                    hk_order_cost_hashdiff=self._uuid([self._uuid(order_id), self._uuid(cost)]),
                    h_order_pk=self._uuid(order_id),
                    cost=cost,
                    payment=payment,
                    load_dt=datetime.utcnow(),
                    load_src=self.source_system
                )
    
    def s_order_status(self) -> S_Order_Status:
        order_id = self._order.id
        status = self._order.status
        return  S_Order_Status(
                    hk_order_status_hashdiff=self._uuid([self._uuid(order_id), self._uuid(status)]),
                    h_order_pk=self._uuid(order_id),
                    status=status,
                    load_dt=datetime.utcnow(),
                    load_src=self.source_system
                )
    
    def s_product_names(self) -> List[S_Product_Names]:
        product_names = []
        for prod_dict in self._order.products:
            prod_id = prod_dict.id
            prod_name = prod_dict.name
            product_names.append(
                S_Product_Names(
                    hk_product_names_hashdiff=self._uuid([self._uuid(prod_id), self._uuid(prod_name)]),
                    h_product_pk=self._uuid(prod_id),
                    product_name=prod_name,
                    load_dt=datetime.utcnow(),
                    load_src=self.source_system
                )
            )
        return product_names
    
    def s_restaurant_names(self) -> S_Restaurant_Names:
        restaurant_id = self._order.restaurant.id
        restaurant_name = self._order.restaurant.name
        return  S_Restaurant_Names(
                    hk_restaurant_names_hashdiff=self._uuid([self._uuid(restaurant_id), self._uuid(restaurant_name)]),
                    h_restaurant_pk=self._uuid(restaurant_id),
                    restaurant_name=restaurant_name,
                    load_dt=datetime.utcnow(),
                    load_src=self.source_system
                )
        
    def s_user_names(self) -> S_User_Names:
        user_id = self._order.user.id
        user_name = self._order.user.name
        userlogin = self._order.user.login
        return  S_User_Names(
                    hk_user_names_hashdiff=self._uuid([self._uuid(user_id), self._uuid(user_name)]),
                    h_user_pk=self._uuid(user_id),
                    username=user_name,
                    userlogin=userlogin,
                    load_dt=datetime.utcnow(),
                    load_src=self.source_system
                )
        
    def output_message(self) -> OutputMessage:
        om_products = []
        user_id = self._order.user.id
        for product in self._order.products:
            category_name = product.category
            product_id = product.id
            product_name = product.name
            quantity = product.quantity
            om_products.append(
                OMProduct(
                    category_id=self._uuid(category_name),
                    category_name=category_name,
                    product_id=self._uuid(product_id),
                    product_name=product_name,
                    quantity=quantity
                ))
        return OutputMessage(
            user_id=self._uuid(user_id),
            products=om_products
            )