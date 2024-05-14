insert into STV2024012248__DWH.s_group_private_status(hk_group_id, is_private, load_dt, load_src)
select hg.hk_group_id as hk_group_id,
       g.is_private   as is_private,
       now()          as load_dt,
       's3'           as load_src
  from STV2024012248__DWH.h_groups as hg
  left join STV2024012248__STAGING.groups g on hg.group_id = g.id;