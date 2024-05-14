copy STV2024012248__STAGING.dialogs (
        message_id, message_ts, message_from, message_to, message, message_group)
from local '/data/dialogs.csv'
delimiter ','
REJECTED DATA AS TABLE STV2024012248__STAGING.dialogs_rej;