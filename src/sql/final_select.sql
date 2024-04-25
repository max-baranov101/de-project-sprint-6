with user_group_log as (
  select
    hg.hk_group_id,
    hg.registration_dt,
    count(DISTINCT luga.hk_user_id) as cnt_added_users
  from
    STV202404101__DWH.h_groups hg
    left join STV202404101__DWH.l_user_group_activity as luga on luga.hk_group_id = hg.hk_group_id
    left join STV202404101__DWH.s_auth_history as sah on sah.hk_l_user_group_activity = luga.hk_l_user_group_activity
  where
    sah.event = 'add'
  group by
    hg.hk_group_id,
    hg.registration_dt
  order by
    hg.registration_dt
  limit
    10
), user_group_messages as (
  select
    lgd.hk_group_id,
    count(distinct lum.hk_user_id) as cnt_users_in_group_with_messages
  from
    STV202404101__DWH.l_groups_dialogs as lgd
    left join STV202404101__DWH.l_user_message as lum on lum.hk_message_id = lgd.hk_message_id
  group by
    lgd.hk_group_id
)
select
  ugl.hk_group_id,
  ugl.cnt_added_users,
  ugm.cnt_users_in_group_with_messages,
  ugm.cnt_users_in_group_with_messages / ugl.cnt_added_users group_conversion
from
  user_group_log as ugl
  left join user_group_messages as ugm on ugl.hk_group_id = ugm.hk_group_id
order by
  ugm.cnt_users_in_group_with_messages / ugl.cnt_added_users desc;