drop table if exists STV2024012248__STAGING.users cascade;
drop table if exists STV2024012248__STAGING.groups cascade;
drop table if exists STV2024012248__STAGING.dialogs cascade;
drop table if exists STV2024012248__STAGING.group_log cascade;

create table if not exists STV2024012248__STAGING.users
(
    id int primary key,
    chat_name varchar(200),
    registration_dt datetime,
    country varchar(200),
    age int
)
order by id
segmented by hash(id) all nodes;


create table if not exists STV2024012248__STAGING.groups
(
    id int primary key,
    admin_id int,
    group_name varchar(100),
    registration_dt datetime,
    is_private boolean
)
order by id, admin_id
segmented by hash(id) all nodes
partition by registration_dt::date
group by calendar_hierarchy_day(registration_dt::date, 3, 2);


create table if not exists STV2024012248__STAGING.dialogs
(
    message_id int primary key,
    message_ts datetime,
    message_from int,
    message_to int,
    message varchar(1000),
    message_group int
)
order by message_id
segmented by hash(message_id) all nodes
partition by message_ts::date
group by calendar_hierarchy_day(message_ts::date, 3, 2);


create table if not exists STV2024012248__STAGING.group_log
(
    id identity primary key,
    group_id int,
    user_id int,
    user_id_from int,
    event varchar(10),
    event_dt datetime
)
order by id, group_id, user_id
segmented by hash(id) all nodes
partition by datetime::date
group by calendar_hierarchy_day(datetime::date, 3, 2);
