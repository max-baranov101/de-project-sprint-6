drop table if exists STV202310069__DWH.l_user_group_activity cascade;

create table STV202310069__DWH.l_user_group_activity (
  hk_l_user_group_activity integer PRIMARY KEY, 
  hk_user_id int not null, 
  hk_group_id int not null, 
  load_dt timestamp not null, 
  load_src VARCHAR(20)
);
ALTER TABLE STV202310069__DWH.l_user_group_activity 
ADD CONSTRAINT fk_uga_users_hk_user_id FOREIGN KEY (hk_user_id) references STV202310069__DWH.h_users(hk_user_id);
ALTER TABLE STV202310069__DWH.l_user_group_activity 
ADD CONSTRAINT fk_uga_groups_hk_group_id FOREIGN KEY (hk_group_id) references STV202310069__DWH.h_groups(hk_group_id);