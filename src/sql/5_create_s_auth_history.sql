-- Удаляем таблицу, если она существует, вместе с зависимыми объектами
drop table if exists STV202404101__DWH.s_auth_history cascade;

-- Создаем таблицу с внешним ключом
create table STV202404101__DWH.s_auth_history (
  hk_l_user_group_activity integer references STV202404101__DWH.l_user_group_activity(hk_l_user_group_activity),
  user_id_from int,
  event varchar(10),
  event_dt timestamp,
  load_dt timestamp not null,
  load_src VARCHAR(20)
);

/*
 drop table if exists STV202404101__DWH.s_auth_history cascade;
 
 create table STV202404101__DWH.s_auth_history (
 hk_l_user_group_activity integer,
 user_id_from int,
 event varchar(10),
 event_dt timestamp,
 load_dt timestamp not null,
 load_src VARCHAR(20)
 );
 
 ALTER TABLE
 STV202404101__DWH.s_auth_history
 ADD
 CONSTRAINT fk_auth_hist FOREIGN KEY (hk_l_user_group_activity) references STV202404101__DWH.l_user_group_activity(hk_l_user_group_activity);
 */