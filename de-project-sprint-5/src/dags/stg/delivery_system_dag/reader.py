from datetime import datetime
from typing import Dict, List
import requests
import json

from lib import ApiConnect


class Reader:
    
    def __init__(self, ac: ApiConnect, collection, id_field: str = "_id" ) -> None:
        self.dbs = ac
        self.collection = collection
        self.id_field = id_field
        self.headers = {
        'X-Nickname': self.dbs.nickname,
        'X-Cohort': self.dbs.cohort,
        'X-API-KEY': self.dbs.api_key
                    }

    def get_data(self, threshold: int) -> List[Dict]:
        
        response = requests.get(f'{self.dbs.base_url}/{self.collection}?sort_field={self.dbs.sort_field}&sort_direction={self.dbs.sort_direction}&limit={self.dbs.limit}&offset={threshold}', headers=self.headers)
        json_load = json.loads(response.content)
        result = list(map(lambda obj: {f"{self.id_field}": obj[self.id_field], "object_value": obj, "update_ts": datetime.now(), }, json_load))
        return result
    
    def get_data_delivery(self, load_threshold: datetime) -> List[Dict]:
        
        response = requests.get(f'{self.dbs.base_url}/{self.collection}?sort_field={self.dbs.sort_field}&sort_direction={self.dbs.sort_direction}&limit={self.dbs.limit}&from={load_threshold}', headers=self.headers)
        json_load = json.loads(response.content)
        result = list(map(lambda obj: {f"{self.id_field}": obj[self.id_field], "object_value": obj, "update_ts": obj['order_ts'], }, json_load))
        return result