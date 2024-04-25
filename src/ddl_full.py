import vertica_python

conn_info = {
    'host': 'vertica.tgcloudenv.ru',
    'port': 5433,
    'user': 'stv202404101',
    'password': 'D1e67eX3ve3Q0PP',
    'database': 'dwh',
    'autocommit': True
}

sql_sripts_folder = 'src/sql/'
sql_sripts = [
    '0_clear_db.sql',
    '1_ddl_stg.sql',
    '2_ddl_dwh.sql',
]

for sql_sript in sql_sripts:
    with open(sql_sripts_folder + sql_sript, 'r') as file:
        print(file.read())
        #with vertica_python.connect(**conn_info) as conn:
            #cur = conn.cursor()
            #cur.execute(file.read())

"""

with vertica_python.connect(**conn_info) as conn:
    cur = conn.cursor()

    cur.execute('''
                

            
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
                

                -- clean up STAGING
                drop table if exists 
                    STV202404101__STAGING.users,
                    STV202404101__STAGING.groups,
                    STV202404101__STAGING.dialogs,
                    STV202404101__STAGING.users_rej,
                    STV202404101__STAGING.groups_rej,
                    STV202404101__STAGING.dialogs_rej;

                
                
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
                ORDER BY id 
                SEGMENTED BY HASH(id) ALL NODES;
                
                -- groups
                create table STV202404101__STAGING.groups(
                    id int not null,
                    admin_id int,
                    group_name varchar(100),
                    registration_dt timestamp,
                    is_private bool,
                    CONSTRAINT C_PRIMARY PRIMARY KEY (id) DISABLED
                )
                ORDER BY id, admin_id 
                SEGMENTED BY hash(id) all nodes 
                PARTITION BY registration_dt :: date
                GROUP BY calendar_hierarchy_day(registration_dt :: date, 3, 2);

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
                ORDER BY message_id 
                SEGMENTED BY hash(message_id) all nodes 
                PARTITION BY message_ts :: date
                GROUP BY calendar_hierarchy_day(message_ts :: date, 3, 2);

                

                -- create DWH tables

                -- rejected users
                create table STV202404101__DWH.h_users (
                    hk_user_id bigint primary key,
                    user_id int,
                    registration_dt datetime,
                    load_dt datetime,
                    load_src varchar(20)
                ) 
                order by load_dt 
                SEGMENTED BY hk_user_id all nodes 
                PARTITION BY load_dt :: date
                GROUP BY calendar_hierarchy_day(load_dt :: date, 3, 2);;

                -- h_dialogs
                create table STV202404101__DWH.h_dialogs (
                    hk_message_id bigint primary key,
                    message_id int,
                    datetime datetime,
                    load_dt datetime,
                    load_src varchar(20)
                )
                order by load_dt 
                SEGMENTED BY hk_message_id all nodes 
                PARTITION BY load_dt :: date
                GROUP BY calendar_hierarchy_day(load_dt :: date, 3, 2);

                -- h_groups
                create table STV202404101__DWH.h_groups (
                    hk_group_id bigint primary key,
                    group_id int,
                    registration_dt datetime,
                    load_dt datetime,
                    load_src varchar(20)
                ) 
                order by load_dt 
                SEGMENTED BY hk_group_id all nodes 
                PARTITION BY load_dt :: date
                GROUP BY calendar_hierarchy_day(load_dt :: date, 3, 2);
                
                -- l_user_message
                create table STV202404101__DWH.l_user_message (
                    hk_l_user_message bigint primary key,
                    hk_user_id bigint not null CONSTRAINT fk_l_user_message_user REFERENCES STV202404101__DWH.h_users (hk_user_id),
                    hk_message_id bigint not null CONSTRAINT fk_l_user_message_message REFERENCES STV202404101__DWH.h_dialogs (hk_message_id),
                    load_dt datetime,
                    load_src varchar(20)
                )
                order by load_dt 
                SEGMENTED BY hk_user_id all nodes 
                PARTITION BY load_dt :: date
                GROUP BY calendar_hierarchy_day(load_dt :: date, 3, 2);
                
                -- l_admins
                create table STV202404101__DWH.l_admins (
                    hk_l_admin_id bigint primary key,
                    hk_user_id bigint not null CONSTRAINT fk_l_admins_user REFERENCES STV202404101__DWH.h_users (hk_user_id),
                    hk_group_id bigint not null CONSTRAINT fk_l_admins_group REFERENCES STV202404101__DWH.h_groups (hk_group_id),
                    load_dt datetime,
                    load_src varchar(20)
                )
                order by load_dt 
                SEGMENTED BY hk_l_admin_id all nodes 
                PARTITION BY load_dt :: date
                GROUP BY calendar_hierarchy_day(load_dt :: date, 3, 2);
                
                -- l_groups_dialogs
                create table STV202404101__DWH.l_groups_dialogs (
                    hk_l_groups_dialogs bigint primary key,
                    hk_message_id bigint not null CONSTRAINT fk_l_groups_dialogs_message REFERENCES STV202404101__DWH.h_dialogs (hk_message_id),
                    hk_group_id bigint not null CONSTRAINT fk_l_groups_dialogs_group REFERENCES STV202404101__DWH.h_groups (hk_group_id),
                    load_dt datetime,
                    load_src varchar(20)
                )
                order by load_dt 
                SEGMENTED BY hk_l_groups_dialogs all nodes 
                PARTITION BY load_dt :: date
                GROUP BY calendar_hierarchy_day(load_dt :: date, 3, 2);
                
                -- s_admins
                create table STV202404101__DWH.s_admins (
                    hk_admin_id bigint not null CONSTRAINT fk_s_admins_l_admins REFERENCES STV202404101__DWH.l_admins (hk_l_admin_id),
                    is_admin boolean,
                    admin_from datetime,
                    load_dt datetime,
                    load_src varchar(20)
                )
                order by load_dt 
                SEGMENTED BY hk_admin_id all nodes 
                PARTITION BY load_dt :: date
                GROUP BY calendar_hierarchy_day(load_dt :: date, 3, 2);
                
                -- s_user_chatinfo
                create table STV202404101__DWH.s_user_chatinfo (
                    hk_user_id bigint not null CONSTRAINT fk_s_user_chatinfo_h_users REFERENCES STV202404101__DWH.h_users (hk_user_id),
                    chat_name varchar(200),
                    load_dt datetime,
                    load_src varchar(20)
                )
                order by load_dt 
                SEGMENTED BY hk_user_id all nodes
                PARTITION BY load_dt :: date
                GROUP BY calendar_hierarchy_day(load_dt :: date, 3, 2);
                
                -- s_group_name
                create table STV202404101__DWH.s_group_name (
                    hk_group_id bigint not null CONSTRAINT fk_s_group_name_h_groups REFERENCES STV202404101__DWH.h_groups (hk_group_id),
                    group_name varchar(100),
                    load_dt datetime,
                    load_src varchar(20)
                )
                order by load_dt 
                SEGMENTED BY hk_group_id all nodes
                PARTITION BY load_dt :: date
                GROUP BY calendar_hierarchy_day(load_dt :: date, 3, 2);

                -- s_group_private_status
                create table STV202404101__DWH.s_group_private_status (
                    hk_group_id bigint not null CONSTRAINT fk_s_group_name_h_groups REFERENCES STV202404101__DWH.h_groups (hk_group_id),
                    is_private bool,
                    load_dt datetime,
                    load_src varchar(20)
                )
                order by load_dt 
                SEGMENTED BY hk_group_id all nodes PARTITION BY load_dt :: date
                GROUP BY calendar_hierarchy_day(load_dt :: date, 3, 2);

                -- s_dialog_info
                create table STV202404101__DWH.s_dialog_info (
                    hk_message_id bigint not null CONSTRAINT fk_s_dialog_info_h_dialogs REFERENCES STV202404101__DWH.h_dialogs (hk_message_id),
                    message varchar(1000),
                    message_from int,
                    message_to int,
                    load_dt datetime,
                    load_src varchar(20)
                )
                order by load_dt 
                SEGMENTED BY hk_message_id all nodes
                PARTITION BY load_dt :: date
                GROUP BY calendar_hierarchy_day(load_dt :: date, 3, 2);

                -- s_user_socdem
                create table STV202404101__DWH.s_user_socdem (
                    hk_user_id bigint not null CONSTRAINT fk_s_user_socdem_h_users REFERENCES STV202404101__DWH.h_users (hk_user_id),
                    country varchar(200),
                    age numeric(4, 1),
                    load_dt datetime,
                    load_src varchar(20)
                )
                order by load_dt 
                SEGMENTED BY hk_user_id all nodes
                PARTITION BY load_dt :: date
                GROUP BY calendar_hierarchy_day(load_dt :: date, 3, 2);
''')
"""