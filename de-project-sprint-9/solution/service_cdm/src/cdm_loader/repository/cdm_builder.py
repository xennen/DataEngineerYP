from typing import List

from cdm_loader.repository.models import *


class CdmBuilder:
    def __init__(self, message: InputMessage) -> None:
        self._message = message
    
    def user_product_counters(self) -> List[User_Product_Counters]:
        user_product = []
        user_id = self._message.user_id
        for product in self._message.products:
            product_id = product.product_id
            product_name = product.product_name
            user_product.append(
                User_Product_Counters(
                    user_id=user_id,
                    product_id=product_id,
                    product_name=product_name
            ))
        
        return user_product
    
    def user_category_counters(self) -> List[User_Category_Counters]:
        user_category = []
        user_id = self._message.user_id
        for category in self._message.products:
            category_id = category.category_id
            category_name = category.category_name
            user_category.append(
                User_Category_Counters(
                    user_id=user_id,
                    category_id=category_id,
                    category_name=category_name
            ))
        
        return user_category