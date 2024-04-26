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
