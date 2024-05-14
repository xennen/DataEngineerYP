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
    category: str = None
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
    
class OutputMessage(BaseModel):
    object_id: int
    object_type: str
    payload: Order