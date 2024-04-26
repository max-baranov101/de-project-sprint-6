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
    '1_ddl_stg.sql',
    '2_create_group_log.sql',
    '3_ddl_dwh.sql',
    '4_create_l_user_group_activity.sql',
    '5_create_s_auth_history.sql',
]

for sql_sript in sql_sripts:
    with open(sql_sripts_folder + sql_sript, 'r') as file:
        try:
            with vertica_python.connect(**conn_info) as conn:
                cur = conn.cursor()
                cur.execute(file.read())
        except Exception as e:
            print(f'Error in {sql_sript}: {e}')
            continue

with vertica_python.connect(**conn_info) as conn:
    cur = conn.cursor()

    # get all schemas
    cur.execute('''
    SELECT table_schema
    FROM v_catalog.tables
    WHERE table_schema NOT IN ('public', 'v_monitor')
    GROUP BY table_schema
    ''')
    schemas = cur.fetchall()

    
    # get all tables in schemas
    for schema in schemas:
        print(schema[0])
        cur.execute(f'''
        SELECT table_name
        FROM v_catalog.tables
        WHERE table_schema = '{schema[0]}'
        ''')
        tables = cur.fetchall()
        for table in tables:
            print(table[0])

        print('\n')
