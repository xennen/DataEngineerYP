copy STV2024012248__STAGING.group_log (
        group_id, user_id, user_id_from, event, event_dt)
from local '/data/group_log.csv'
delimiter ','
REJECTED DATA AS TABLE STV2024012248__STAGING.group_log_rej;