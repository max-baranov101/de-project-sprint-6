-- Удаляем таблицу, если она уже существует, вместе с зависимыми объектами
drop table if exists STV202404101__STAGING.group_log cascade;

-- Создаем таблицу с внешними ключами и остальной структурой
create table STV202404101__STAGING.group_log (
  group_id integer references STV202404101__STAGING.groups (id),
  user_id integer references STV202404101__STAGING.users (id),
  user_id_from integer references STV202404101__STAGING.users (id),
  event varchar(10),
  datetime timestamp
)
order by
  group_id,
  user_id SEGMENTED BY hash(group_id) all nodes PARTITION BY datetime :: date
GROUP BY
  calendar_hierarchy_day(datetime :: date, 3, 2);