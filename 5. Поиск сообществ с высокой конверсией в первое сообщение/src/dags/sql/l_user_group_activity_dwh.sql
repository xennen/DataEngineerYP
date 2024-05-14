INSERT INTO STV2024012248__DWH.l_user_group_activity(hk_l_user_group_activity, hk_user_id,hk_group_id,load_dt,load_src)
select hash(gl.id, hu.hk_user_id, hg.hk_group_id) as hk_l_user_group_activity,
       hu.hk_user_id as hk_user_id,
       hg.hk_group_id as hk_group_id,
       now() as load_dt,
       's3' as load_src
  from STV2024012248__STAGING.group_log as gl
  left join STV2024012248__DWH.h_users hu on hu.user_id = gl.user_id
  left join STV2024012248__DWH.h_groups hg on hg.group_id = gl.group_id;