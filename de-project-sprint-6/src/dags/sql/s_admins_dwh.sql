insert into STV2024012248__DWH.s_admins(hk_admin_id, is_admin, admin_from, load_dt, load_src)
select la.hk_l_admin_id   as hk_admin_id,
       True               as is_admin,
       hg.registration_dt as admin_from,
       now()              as load_dt,
       's3'               as load_src
  from STV2024012248__DWH.l_admins as la
  left join STV2024012248__DWH.h_groups as hg on la.hk_group_id = hg.hk_group_id;