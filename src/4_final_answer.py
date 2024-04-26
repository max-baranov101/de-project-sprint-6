import vertica_python

conn_info = {
    'host': 'vertica.tgcloudenv.ru',
    'port': 5433,
    'user': 'stv202404101',
    'password': 'D1e67eX3ve3Q0PP',
    'database': 'dwh',
    'autocommit': True
}

with open('src/sql/10_final_select.sql', 'r') as file:
    try:
        with vertica_python.connect(**conn_info) as conn:
            cur = conn.cursor()
            cur.execute(file.read())
            res = cur.fetchall()
            print(res)
            # export to csv
            with open('10_final_select.csv', 'w') as f:
                for row in res:
                    f.write(','.join(map(str, row)) + '\n')
    except Exception as e:
        print(f'Error in 10_final_select.sql: {e}')

