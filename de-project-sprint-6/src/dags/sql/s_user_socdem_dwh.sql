insert into STV2024012248__DWH.s_user_socdem(hk_user_id, country, age, load_dt, load_src)
select hu.hk_user_id as hk_user_id,
       u.country     as country,
       u.age         as chat_name,
       now()         as load_dt,
       's3'          as load_src
  from STV2024012248__DWH.h_users as hu
  left join STV2024012248__STAGING.users u on hu.user_id = u.id;