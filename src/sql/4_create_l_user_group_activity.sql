-- Dropping existing table if it exists
DROP TABLE IF EXISTS STV202404101__DWH.l_user_group_activity CASCADE;

-- Creating new table with constraints
CREATE TABLE STV202404101__DWH.l_user_group_activity (
  hk_l_user_group_activity INTEGER PRIMARY KEY, 
  hk_user_id INT NOT NULL, 
  hk_group_id INT NOT NULL, 
  load_dt TIMESTAMP NOT NULL, 
  load_src VARCHAR(20),
  CONSTRAINT fk_uga_users_hk_user_id FOREIGN KEY (hk_user_id) REFERENCES STV202404101__DWH.h_users(hk_user_id),
  CONSTRAINT fk_uga_groups_hk_group_id FOREIGN KEY (hk_group_id) REFERENCES STV202404101__DWH.h_groups(hk_group_id)
);

/*
drop table if exists STV202404101__DWH.l_user_group_activity cascade;

create table STV202404101__DWH.l_user_group_activity (
  hk_l_user_group_activity integer PRIMARY KEY, 
  hk_user_id int not null, 
  hk_group_id int not null, 
  load_dt timestamp not null, 
  load_src VARCHAR(20)
);
ALTER TABLE STV202404101__DWH.l_user_group_activity 
ADD CONSTRAINT fk_uga_users_hk_user_id FOREIGN KEY (hk_user_id) references STV202404101__DWH.h_users(hk_user_id);
ALTER TABLE STV202404101__DWH.l_user_group_activity 
ADD CONSTRAINT fk_uga_groups_hk_group_id FOREIGN KEY (hk_group_id) references STV202404101__DWH.h_groups(hk_group_id);
*/