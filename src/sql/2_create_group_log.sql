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
  group_id, user_id 
SEGMENTED BY hash(group_id) all nodes 
PARTITION BY datetime :: date
GROUP BY calendar_hierarchy_day(datetime :: date, 3, 2);

/*
drop table if exists STV202404101__STAGING.group_log cascade;

create table STV202404101__STAGING.group_log (
  group_id integer,
  user_id integer,
  user_id_from integer,
  event varchar(10),
  datetime timestamp
)
order by
  group_id,
  user_id SEGMENTED BY hash(group_id) all nodes PARTITION BY datetime :: date
GROUP BY
  calendar_hierarchy_day(datetime :: date, 3, 2);

ALTER TABLE
  STV202404101__STAGING.group_log
ADD
  CONSTRAINT fk_group_log_groups_group_id FOREIGN KEY (group_id) references STV202404101__STAGING.groups (id);

ALTER TABLE
  STV202404101__STAGING.group_log
ADD
  CONSTRAINT fk_group_log_users_user_id_from FOREIGN KEY (user_id) references STV202404101__STAGING.users (id);

ALTER TABLE
  STV202404101__STAGING.group_log
ADD
  CONSTRAINT fk_group_log_users_user_id FOREIGN KEY (user_id_from) references STV202404101__STAGING.users (id);
  *