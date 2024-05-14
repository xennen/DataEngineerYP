from airflow.providers.http.hooks.http import HttpHook
from airflow.models import Variable

class ApiConnect:
    http_conn_id = HttpHook.get_connection('DEL_API')
    api_key = http_conn_id.extra_dejson.get('api_key')            
    base_url = http_conn_id.host
    nickname = Variable.get('nickname')
    cohort = Variable.get('cohort')
    method = "GET"
    
    def __init__(self, sort_field: str = "_id", limit: int = 50, sort_direction='asc') -> None:      
        self.sort_field = sort_field
        self.limit = limit
        self.sort_direction = sort_direction