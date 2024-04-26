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
SELECT
  ugl.hk_group_id AS hk_group_id,
  ugl.cnt_added_users AS cnt_added_users,
  ugm.cnt_users_in_group_with_messages AS cnt_users_in_group_with_messages,
  ROUND(ugm.cnt_users_in_group_with_messages / ugl.cnt_added_users, 3) AS group_conversion
FROM
  user_group_log AS ugl
  LEFT JOIN user_group_messages AS ugm ON ugl.hk_group_id = ugm.hk_group_id
ORDER BY
  group_conversion DESC;
