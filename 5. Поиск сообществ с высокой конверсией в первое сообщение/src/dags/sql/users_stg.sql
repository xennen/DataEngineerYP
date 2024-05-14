copy STV2024012248__STAGING.users (
        id, chat_name, registration_dt, country, age)
from local '/data/users.csv'
delimiter ','
REJECTED DATA AS TABLE STV2024012248__STAGING.users_rej;