import unittest
import pandas as pd
from etlstat.database.mysql import MySQL


class TestMySQL(unittest.TestCase):

    user = 'root'
    password = 'password'
    host = '127.0.0.1'
    port = '3307'
    database = 'mysql_test'
    conn_params = [user, password, host, port, database]

    @classmethod
    def setUpClass(cls):
        user = 'root'
        password = 'password'
        host = '127.0.0.1'
        port = '3307'
        database = ''
        conn_params = [user, password, host, port, database]
        ddl = "CREATE DATABASE IF NOT EXISTS mysql_test"
        my_conn = MySQL(*conn_params)
        status, result = my_conn.execute_sql(ddl)
        print(result)

    def test_create_table(self):
        # Create data frame
        data_columns = ['column_int', 'column_string', 'column_float']
        table = pd.DataFrame(columns=data_columns)

        # Assign columns data types
        table['column_int'] = table['column_int'].astype(int)
        table['column_string'] = table['column_string'].astype(str)
        table['column_float'] = table['column_float'].astype(float)

        # Rename data frame
        table.name = "table_test"
        my_conn = MySQL(*self.conn_params)
        self.assertFalse(my_conn.create(table))

    # TODO: add tests for all class methods

if __name__ == '__main__':
    unittest.main()
