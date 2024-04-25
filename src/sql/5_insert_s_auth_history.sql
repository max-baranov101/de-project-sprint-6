INSERT INTO
  STV202404101__DWH.s_auth_history(
    hk_l_user_group_activity,
    user_id_from,
    event,
    event_dt,
    load_dt,
    load_src
  )
select
  uga.hk_l_user_group_activity,
  gl.user_id_from,
  gl.event,
  gl.datetime,
  now() as load_dt,
  's3' as load_src
from
  STV202404101__STAGING.group_log as gl
  left join STV202404101__DWH.h_groups as hg on gl.group_id = hg.group_id
  left join STV202404101__DWH.h_users as hu on gl.user_id = hu.user_id
  left join STV202404101__DWH.l_user_group_activity as uga on hg.hk_group_id = uga.hk_group_id
  and hu.hk_user_id = uga.hk_user_id;