with user_group_messages as (
    select 
        lgd.hk_group_id as hk_group_id, 
        count(distinct lum.hk_user_id) as cnt_users_in_group_with_messages
    from 
        STV2024012248__DWH.l_groups_dialogs lgd
    left join 
        STV2024012248__DWH.l_user_message lum using(hk_message_id)
    group by 
        lgd.hk_group_id
), 

user_group_log as (
    select 
        luga.hk_group_id, 
        count(distinct hk_user_id) as cnt_added_users
    from 
        s_auth_history sah
    left join 
        l_user_group_activity luga using(hk_l_user_group_activity)
    left join 
        STV2024012248__DWH.h_groups hg using(hk_group_id)
    where 
        event = 'add' 
        and hg.hk_group_id in (select hk_group_id from STV2024012248__DWH.h_groups order by registration_dt limit 10)
    group by 
        luga.hk_group_id
)

select 
    ugm.hk_group_id, 
    cnt_added_users, 
    cnt_users_in_group_with_messages,
    (ugm.cnt_users_in_group_with_messages / ugl.cnt_added_users * 100)::numeric(5,2) as group_conversion
from 
    user_group_log as ugl
left join 
    user_group_messages as ugm on ugl.hk_group_id = ugm.hk_group_id
order by 
    group_conversion desc;
