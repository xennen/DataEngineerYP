insert into STV2024012248__DWH.s_group_name(hk_group_id, group_name, load_dt, load_src)
select hg.hk_group_id as hk_group_id,
       g.group_name   as group_name,
       now()          as load_dt,
       's3'           as load_src
  from STV2024012248__DWH.h_groups as hg
  left join STV2024012248__STAGING.groups g on hg.group_id = g.id;