insert into STV2024012248__DWH.s_dialog_info (hk_message_id, message, message_from, message_to, load_dt, load_src)
select hd.hk_message_id as hk_message_id,
       d.message        as message,
       d.message_from   as message_from,
       d.message_to     as message_to,
       now()            as load_dt,
       's3'             as load_src
  from STV2024012248__DWH.h_dialogs hd
  left join dialogs d on hd.message_id = d.message_id;