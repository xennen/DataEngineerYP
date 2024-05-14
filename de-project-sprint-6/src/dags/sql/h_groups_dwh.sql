INSERT INTO STV2024012248__DWH.h_groups(hk_group_id, group_id,registration_dt,load_dt,load_src)
select hash(id) as  hk_group_id,
       id as group_id,
       registration_dt,
       now() as load_dt,
       's3' as load_src
       from STV2024012248__STAGING.groups
 where hash(id) not in (select hk_group_id from STV2024012248__DWH.h_groups);