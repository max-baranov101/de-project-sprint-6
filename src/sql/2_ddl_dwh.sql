-- create DWH tables

-- rejected users
  create table STV202404101__DWH.h_users (
    hk_user_id bigint primary key,
    user_id int,
    registration_dt datetime,
    load_dt datetime,
    load_src varchar(20)
  )
order by
  load_dt SEGMENTED BY hk_user_id all nodes PARTITION BY load_dt :: date
GROUP BY
  calendar_hierarchy_day(load_dt :: date, 3, 2);;

-- h_dialogs
  create table STV202404101__DWH.h_dialogs (
    hk_message_id bigint primary key,
    message_id int,
    datetime datetime,
    load_dt datetime,
    load_src varchar(20)
  )
order by
  load_dt SEGMENTED BY hk_message_id all nodes PARTITION BY load_dt :: date
GROUP BY
  calendar_hierarchy_day(load_dt :: date, 3, 2);

-- h_groups
  create table STV202404101__DWH.h_groups (
    hk_group_id bigint primary key,
    group_id int,
    registration_dt datetime,
    load_dt datetime,
    load_src varchar(20)
  )
order by
  load_dt SEGMENTED BY hk_group_id all nodes PARTITION BY load_dt :: date
GROUP BY
  calendar_hierarchy_day(load_dt :: date, 3, 2);

-- l_user_message
  create table STV202404101__DWH.l_user_message (
    hk_l_user_message bigint primary key,
    hk_user_id bigint not null CONSTRAINT fk_l_user_message_user REFERENCES STV202404101__DWH.h_users (hk_user_id),
    hk_message_id bigint not null CONSTRAINT fk_l_user_message_message REFERENCES STV202404101__DWH.h_dialogs (hk_message_id),
    load_dt datetime,
    load_src varchar(20)
  )
order by
  load_dt SEGMENTED BY hk_user_id all nodes PARTITION BY load_dt :: date
GROUP BY
  calendar_hierarchy_day(load_dt :: date, 3, 2);

-- l_admins
  create table STV202404101__DWH.l_admins (
    hk_l_admin_id bigint primary key,
    hk_user_id bigint not null CONSTRAINT fk_l_admins_user REFERENCES STV202404101__DWH.h_users (hk_user_id),
    hk_group_id bigint not null CONSTRAINT fk_l_admins_group REFERENCES STV202404101__DWH.h_groups (hk_group_id),
    load_dt datetime,
    load_src varchar(20)
  )
order by
  load_dt SEGMENTED BY hk_l_admin_id all nodes PARTITION BY load_dt :: date
GROUP BY
  calendar_hierarchy_day(load_dt :: date, 3, 2);

-- l_groups_dialogs
  create table STV202404101__DWH.l_groups_dialogs (
    hk_l_groups_dialogs bigint primary key,
    hk_message_id bigint not null CONSTRAINT fk_l_groups_dialogs_message REFERENCES STV202404101__DWH.h_dialogs (hk_message_id),
    hk_group_id bigint not null CONSTRAINT fk_l_groups_dialogs_group REFERENCES STV202404101__DWH.h_groups (hk_group_id),
    load_dt datetime,
    load_src varchar(20)
  )
order by
  load_dt SEGMENTED BY hk_l_groups_dialogs all nodes PARTITION BY load_dt :: date
GROUP BY
  calendar_hierarchy_day(load_dt :: date, 3, 2);

-- s_admins
  create table STV202404101__DWH.s_admins (
    hk_admin_id bigint not null CONSTRAINT fk_s_admins_l_admins REFERENCES STV202404101__DWH.l_admins (hk_l_admin_id),
    is_admin boolean,
    admin_from datetime,
    load_dt datetime,
    load_src varchar(20)
  )
order by
  load_dt SEGMENTED BY hk_admin_id all nodes PARTITION BY load_dt :: date
GROUP BY
  calendar_hierarchy_day(load_dt :: date, 3, 2);

-- s_user_chatinfo
  create table STV202404101__DWH.s_user_chatinfo (
    hk_user_id bigint not null CONSTRAINT fk_s_user_chatinfo_h_users REFERENCES STV202404101__DWH.h_users (hk_user_id),
    chat_name varchar(200),
    load_dt datetime,
    load_src varchar(20)
  )
order by
  load_dt SEGMENTED BY hk_user_id all nodes PARTITION BY load_dt :: date
GROUP BY
  calendar_hierarchy_day(load_dt :: date, 3, 2);

-- s_group_name
  create table STV202404101__DWH.s_group_name (
    hk_group_id bigint not null CONSTRAINT fk_s_group_name_h_groups REFERENCES STV202404101__DWH.h_groups (hk_group_id),
    group_name varchar(100),
    load_dt datetime,
    load_src varchar(20)
  )
order by
  load_dt SEGMENTED BY hk_group_id all nodes PARTITION BY load_dt :: date
GROUP BY
  calendar_hierarchy_day(load_dt :: date, 3, 2);

-- s_group_private_status
  create table STV202404101__DWH.s_group_private_status (
    hk_group_id bigint not null CONSTRAINT fk_s_group_name_h_groups REFERENCES STV202404101__DWH.h_groups (hk_group_id),
    is_private bool,
    load_dt datetime,
    load_src varchar(20)
  )
order by
  load_dt SEGMENTED BY hk_group_id all nodes PARTITION BY load_dt :: date
GROUP BY
  calendar_hierarchy_day(load_dt :: date, 3, 2);

-- s_dialog_info
  create table STV202404101__DWH.s_dialog_info (
    hk_message_id bigint not null CONSTRAINT fk_s_dialog_info_h_dialogs REFERENCES STV202404101__DWH.h_dialogs (hk_message_id),
    message varchar(1000),
    message_from int,
    message_to int,
    load_dt datetime,
    load_src varchar(20)
  )
order by
  load_dt SEGMENTED BY hk_message_id all nodes PARTITION BY load_dt :: date
GROUP BY
  calendar_hierarchy_day(load_dt :: date, 3, 2);

-- s_user_socdem
  create table STV202404101__DWH.s_user_socdem (
    hk_user_id bigint not null CONSTRAINT fk_s_user_socdem_h_users REFERENCES STV202404101__DWH.h_users (hk_user_id),
    country varchar(200),
    age numeric(4, 1),
    load_dt datetime,
    load_src varchar(20)
  )
order by
  load_dt SEGMENTED BY hk_user_id all nodes PARTITION BY load_dt :: date
GROUP BY
  calendar_hierarchy_day(load_dt :: date, 3, 2);