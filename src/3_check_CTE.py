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
    '8_CTE_user_group_log.sql',
    '9_CTE_user_group_messages.sql'
]

for sql_sript in sql_sripts:
    with open(sql_sripts_folder + sql_sript, 'r') as file:
        try:
            with vertica_python.connect(**conn_info) as conn:
                cur = conn.cursor()
                cur.execute(file.read())
                res = cur.fetchall()
                print(res)
                print()
        except Exception as e:
            print(f'Error in {sql_sript}: {e}')
            continue
