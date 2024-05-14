from datetime import datetime
from typing import Dict, List

from lib import MongoConnect


class Reader:
    def __init__(self, mc: MongoConnect, collection) -> None:
        self.dbs = mc.client()
        self.collection = collection

    def get_data(self, load_threshold: datetime, limit) -> List[Dict]:
        filter = {'update_ts': {'$gt': load_threshold}}

        sort = [('update_ts', 1)]

        docs = list(self.dbs.get_collection(f'{self.collection}').find(filter=filter, sort=sort, limit=limit))
        return docs