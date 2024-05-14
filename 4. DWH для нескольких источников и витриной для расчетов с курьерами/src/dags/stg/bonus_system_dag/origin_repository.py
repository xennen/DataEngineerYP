from typing import List

from lib import PgConnect
from psycopg.rows import class_row
from pydantic import BaseModel
from datetime import datetime

class EventObj(BaseModel):
    id: int
    event_ts: datetime
    event_type: str
    event_value: str
    
    
class UserObj(BaseModel):
    id: int
    order_user_id: str
    
    
class RankObj(BaseModel):
    id: int
    name: str
    bonus_percent: float
    min_payment_threshold: float


class OriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_events(self, threshold: int, limit: int) -> List[EventObj]:
        with self._db.client().cursor(row_factory=class_row(EventObj)) as cur:
            cur.execute(
                """
                    SELECT id, event_ts, event_type, event_value
                    FROM outbox
                    WHERE id > %(threshold)s
                    ORDER BY id ASC
                    LIMIT %(limit)s;
                """, {
                    "threshold": threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs
    
    
    def list_ranks(self, threshold: int, limit: int) -> List[RankObj]:
        with self._db.client().cursor(row_factory=class_row(RankObj)) as cur:
            cur.execute(
                """
                    SELECT id, name, bonus_percent, min_payment_threshold
                    FROM ranks
                    WHERE id > %(threshold)s
                    ORDER BY id ASC
                    LIMIT %(limit)s;
                """, {
                    "threshold": threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs
    
    
    def list_users(self, threshold: int, limit: int) -> List[UserObj]:
        with self._db.client().cursor(row_factory=class_row(UserObj)) as cur:
            cur.execute(
                """
                    SELECT id, order_user_id
                    FROM users
                    ORDER BY id ASC
                    LIMIT %(limit)s;
                """, {
                    "threshold": threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs