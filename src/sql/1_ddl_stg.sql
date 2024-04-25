-- create STAGING tables

-- users
create table STV202404101__STAGING.users(
  id int not null,
  chat_name varchar(200),
  registration_dt timestamp,
  country varchar(200),
  age numeric(4, 1),
  CONSTRAINT C_PRIMARY PRIMARY KEY (id) DISABLED
)
ORDER BY
  id SEGMENTED BY HASH(id) ALL NODES;

-- groups
  create table STV202404101__STAGING.groups(
    id int not null,
    admin_id int,
    group_name varchar(100),
    registration_dt timestamp,
    is_private bool,
    CONSTRAINT C_PRIMARY PRIMARY KEY (id) DISABLED
  )
ORDER BY
  id,
  admin_id SEGMENTED BY hash(id) all nodes PARTITION BY registration_dt :: date
GROUP BY
  calendar_hierarchy_day(registration_dt :: date, 3, 2);

-- dialogs
  create table STV202404101__STAGING.dialogs(
    message_id int not null,
    message_ts timestamp,
    message_from int,
    message_to int,
    message varchar(1000),
    message_group int,
    CONSTRAINT C_PRIMARY PRIMARY KEY (message_id) DISABLED
  )
ORDER BY
  message_id SEGMENTED BY hash(message_id) all nodes PARTITION BY message_ts :: date
GROUP BY
  calendar_hierarchy_day(message_ts :: date, 3, 2);