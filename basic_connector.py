import snowflake.connector as sf
import pandas as pd
from config import config

conn = sf.connect(user=config.username,password=config.password,account=config.account)

def test_connection(connection,query):
    cursor = connection.cursor()
    cursor.execute(query)
    cursor.close()

try:
    sql='use {}'.format(config.database)
    test_connection(conn,sql)

    sql = 'select * from TRIPS limit 20'
    cursor = conn.cursor()
    cursor.execute(sql)
    df = pd.DataFrame.from_records(iter(cursor),columns=[x[0]for x in cursor.description])
    print(df)

    for c in cursor:
        print(c)

except Exception as e:
    print(e)
