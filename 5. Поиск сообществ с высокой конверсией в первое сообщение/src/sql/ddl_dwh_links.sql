DROP TABLE IF EXISTS STV2024012248__DWH.l_user_message cascade;
DROP TABLE IF EXISTS STV2024012248__DWH.l_admins cascade;
DROP TABLE IF EXISTS STV2024012248__DWH.l_groups_dialogs cascade;
DROP TABLE IF EXISTS STV2024012248__DWH.l_user_group_activity cascade;

create table if not exists STV2024012248__DWH.l_user_message
(
    hk_l_user_message bigint primary key,
    hk_user_id bigint not null CONSTRAINT fk_l_user_message_user REFERENCES STV2024012248__DWH.h_users (hk_user_id),
    hk_message_id bigint not null CONSTRAINT fk_l_user_message_message REFERENCES STV2024012248__DWH.h_dialogs (hk_message_id),
    load_dt datetime,
    load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_user_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

create table if not exists STV2024012248__DWH.l_admins
(
    hk_l_admin_id bigint primary key,
    hk_user_id bigint not null CONSTRAINT fk_l_admins_admin_user REFERENCES STV2024012248__DWH.h_users (hk_user_id),
    hk_group_id bigint not null CONSTRAINT fk_l_admins_group_group REFERENCES STV2024012248__DWH.h_groups (hk_group_id),
    load_dt datetime,
    load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_l_admin_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

create table if not exists STV2024012248__DWH.l_groups_dialogs
(
    hk_l_groups_dialogs bigint primary key,
    hk_message_id bigint not null CONSTRAINT fk_l_groups_dialogs_message_message REFERENCES STV2024012248__DWH.h_dialogs (hk_message_id),
    hk_group_id bigint not null CONSTRAINT fk_l_groups_dialogs_group_group REFERENCES STV2024012248__DWH.h_groups (hk_group_id),
    load_dt datetime,
    load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_l_groups_dialogs all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

create table if not exists STV2024012248__DWH.l_user_group_activity
(
    hk_l_user_group_activity int primary key,
    hk_user_id int not null CONSTRAINT fk_l_user_message_user REFERENCES STV2024012248__DWH.h_users (hk_user_id),
    hk_group_id int not null CONSTRAINT fk_l_admins_group_group REFERENCES STV2024012248__DWH.h_groups (hk_group_id),
    load_dt datetime,
    load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_user_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);
