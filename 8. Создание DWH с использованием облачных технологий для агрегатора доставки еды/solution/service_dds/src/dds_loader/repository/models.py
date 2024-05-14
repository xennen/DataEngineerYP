import uuid
from datetime import datetime
from typing import List

from pydantic import BaseModel


class User(BaseModel):
    id: str
    name: str
    login: str

class MenuProduct(BaseModel):
    id: str
    name: str
    category: str
    price: float

class Product(BaseModel):
    id: str
    name: str
    category: str
    price: float
    quantity: int

class Restaurant(BaseModel):
    id: str
    name: str
    menu: List[MenuProduct]

class Order(BaseModel):
    id: int
    date: datetime
    cost: float
    payment: float
    status: str
    restaurant: Restaurant
    user: User
    products: List[Product]

class InputMessage(BaseModel):
    object_id: int
    object_type: str
    payload: Order
    
class H_User(BaseModel):
    h_user_pk: uuid.UUID
    user_id: str
    load_dt: datetime
    load_src: str

class H_Product(BaseModel):
    h_product_pk: uuid.UUID
    product_id: str
    load_dt: datetime
    load_src: str

class H_Category(BaseModel):
    h_category_pk: uuid.UUID
    category_name: str
    load_dt: datetime
    load_src: str

class H_Restaurant(BaseModel):
    h_restaurant_pk: uuid.UUID
    restaurant_id: str
    load_dt: datetime
    load_src: str

class H_Order(BaseModel):
    h_order_pk: uuid.UUID
    order_id: str
    order_dt: datetime
    load_dt: datetime
    load_src: str
    
class L_Order_Product(BaseModel):
    hk_order_product_pk: uuid.UUID
    h_order_pk: uuid.UUID
    h_product_pk: uuid.UUID
    load_dt: datetime
    load_src: str

class L_Order_User(BaseModel):
    hk_order_user_pk: uuid.UUID
    h_order_pk: uuid.UUID
    h_user_pk: uuid.UUID
    load_dt: datetime
    load_src: str
    
class L_Product_Category(BaseModel):
    hk_product_category_pk: uuid.UUID
    h_product_pk: uuid.UUID
    h_category_pk: uuid.UUID
    load_dt: datetime
    load_src: str
    
class L_Product_Restaurant(BaseModel):
    hk_product_restaurant_pk: uuid.UUID
    h_product_pk: uuid.UUID
    h_restaurant_pk: uuid.UUID
    load_dt: datetime
    load_src: str
    
class S_Order_Cost(BaseModel):
    hk_order_cost_hashdiff: uuid.UUID
    h_order_pk: uuid.UUID
    cost: float
    payment: float
    load_dt: datetime
    load_src: str
    
class S_Order_Cost(BaseModel):
    hk_order_cost_hashdiff: uuid.UUID
    h_order_pk: uuid.UUID
    cost: float
    payment: float
    load_dt: datetime
    load_src: str
    
class S_Order_Status(BaseModel):
    hk_order_status_hashdiff: uuid.UUID
    h_order_pk: uuid.UUID
    status: str
    load_dt: datetime
    load_src: str

class S_Product_Names(BaseModel):
    hk_product_names_hashdiff: uuid.UUID
    h_product_pk: uuid.UUID
    product_name: str
    load_dt: datetime
    load_src: str
    
class S_Restaurant_Names(BaseModel):
    hk_restaurant_names_hashdiff: uuid.UUID
    h_restaurant_pk: uuid.UUID
    restaurant_name: str
    load_dt: datetime
    load_src: str
    
class S_User_Names(BaseModel):
    hk_user_names_hashdiff: uuid.UUID
    h_user_pk: uuid.UUID
    username: str
    userlogin: str
    load_dt: datetime
    load_src: str

class OMProduct(BaseModel):
    category_id: uuid.UUID
    category_name: str
    product_id: uuid.UUID
    product_name: str
    quantity:  int

class OutputMessage(BaseModel):
    user_id: uuid.UUID
    products: List[OMProduct]



