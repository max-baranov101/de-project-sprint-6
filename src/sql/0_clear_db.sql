-- clean up DWH

-- hubs
drop table if exists 
  STV202404101__DWH.h_users,
  STV202404101__DWH.h_dialogs,
  STV202404101__DWH.h_groups;

-- satellites
drop table if exists 
  STV202404101__DWH.s_admins,
  STV202404101__DWH.s_user_chatinfo,
  STV202404101__DWH.s_group_name,
  STV202404101__DWH.s_group_private_status,
  STV202404101__DWH.s_dialog_info,
  STV202404101__DWH.s_user_socdem;

-- links
drop table if exists 
  STV202404101__DWH.l_groups_dialogs,
  STV202404101__DWH.l_user_message,
  STV202404101__DWH.l_admins;

