from etlstat.database.oracle import Oracle
import pandas as pd

schema = 'test'
user = 'test'
password = 'test'
host = 'localhost'
port = '1521'
service_name = 'xe'
conn_params = [user, password, host, port, service_name]

data_columns = ['column_int', 'column_string', 'column_float']
data_values1 = [[1, 'stríng1', 456.956], [2, 'string2', 38.905]]
df = pd.DataFrame(data_values1, columns=data_columns)
df.name = 'test_insert'
ora_conn = Oracle(*conn_params)
ora_conn.insert(df)
