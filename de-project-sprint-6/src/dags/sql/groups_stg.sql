copy STV2024012248__STAGING.groups (
        id, admin_id, group_name, registration_dt, is_private)
from local '/data/groups.csv'
delimiter ','
REJECTED DATA AS TABLE STV2024012248__STAGING.groups_rej;