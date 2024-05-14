import uuid
from typing import List

from pydantic import BaseModel


class Product(BaseModel):
    category_id: uuid.UUID
    category_name: str
    product_id: uuid.UUID
    product_name: str
    quantity: int

class InputMessage(BaseModel):
    user_id: uuid.UUID
    products: List[Product]
    
class User_Category_Counters(BaseModel):
    user_id: uuid.UUID
    category_id: uuid.UUID
    category_name: str

class User_Product_Counters(BaseModel):
    user_id: uuid.UUID
    product_id: uuid.UUID
    product_name: str