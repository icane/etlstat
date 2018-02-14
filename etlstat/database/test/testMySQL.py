import unittest
import pandas as pd
from etlstat.database.mysql import MySQL


class TestMySQL(unittest.TestCase):

    user = 'test'
    password = 'password'
    host = '127.0.0.1'
    port = '3307'
    database = 'test'
    conn_params = [user, password, host, port, database]

    @classmethod
    def setUpClass(cls):
        user = 'root'
        password = 'password'
        host = '127.0.0.1'
        port = '3307'
        database = ''
        conn_params = [user, password, host, port, database]
        ddl = "DROP DATABASE test"
        my_conn = MySQL(*conn_params)
        status, result = my_conn.execute_sql(ddl)
        print(result)
        ddl = "CREATE DATABASE test"
        status, result = my_conn.execute_sql(ddl)
        print(result)
        sql = "CREATE TABLE test.inf_schema as " \
              "SELECT table_name, avg_row_length, table_collation, create_time " \
              "FROM information_schema.tables"
        status, result = my_conn.execute_sql(sql)
        print(result)
        ddl = "GRANT ALL PRIVILEGES ON test.* to 'test'@'%' IDENTIFIED BY 'password'"
        status, result = my_conn.execute_sql(ddl)
        print(result)

    def testInit(self):
        self.assertEqual(str(MySQL(*self.conn_params).engine),
                         "Engine(mysql+mysqlconnector://test:***@127.0.0.1:3307/test)")

    def testCheckForTable(self):
        my_conn = MySQL(*self.conn_params)
        self.assertTrue(my_conn.check_for_table('inf_schema'))

    def testExecuteSql(self):
        my_conn = MySQL(*self.conn_params)
        sql = "CREATE TABLE table1 (id integer, column1 varchar(100), column2 double)"
        status, result = my_conn.execute_sql(sql)
        self.assertTrue(status)
        sql = "INSERT INTO table1 (id, column1, column2) " \
              "VALUES (1, 'Varchar text (100 char)', 123456789.0123456789)"
        status, result = my_conn.execute_sql(sql)
        self.assertTrue(status)
        sql = 'select * from table1 order by id'
        status, result = my_conn.execute_sql(sql)
        self.assertEqual(result.columns.values[0], 'id')
        self.assertEqual(result.columns.values[1], 'column1')
        self.assertEqual(result.columns.values[2], 'column2')
        self.assertEqual(result.values[0][0], 1)
        self.assertEqual(result.values[0][1], 'Varchar text (100 char)')
        self.assertEqual(result.values[0][2], 123456789.0123456789)
        sql = "UPDATE table1 SET id = 2 WHERE id = 1"
        status, result = my_conn.execute_sql(sql)
        self.assertTrue(status)
        sql = "DELETE FROM table1 WHERE id > 0"
        status, result = my_conn.execute_sql(sql)
        self.assertTrue(status)
        sql = "DROP TABLE table1"
        status, result = my_conn.execute_sql(sql)
        self.assertTrue(status)

    def testCreate(self):
        # Create data frame
        data_columns = ['column_int', 'column_string', 'column_float']
        table = pd.DataFrame(columns=data_columns)

        # Assign columns data types
        table['column_int'] = table['column_int'].astype(int)
        table['column_string'] = table['column_string'].astype(str)
        table['column_float'] = table['column_float'].astype(float)

        # Rename data frame
        table.name = "table2"
        my_conn = MySQL(*self.conn_params)
        self.assertTrue(my_conn.create(table))

    def testSelect(self):
        my_conn = MySQL(*self.conn_params)
        table_name = "inf_schema"
        conditions = "table_name like 'INNO%' AND avg_row_length > 100"
        table_df = my_conn.select(table_name, conditions=conditions)
        self.assertGreaterEqual(len(table_df), 6)

    def testInsert(self):
        data_columns = ['column_int', 'column_string', 'column_float']
        data_values = [[1, 'string1', 456.956], [2, 'string2', 38.905]]
        df = pd.DataFrame(data_values, columns=data_columns)
        df.name = 'test_insert'
        my_conn = MySQL(*self.conn_params)
        self.assertEqual(my_conn.insert(df), 2)

    def testUpdate(self):
        data_columns = ['column_int', 'column_string', 'column_float']
        data_values1 = [[1, 'string1', 456.956], [2, 'string2', 38.905]]
        df1 = pd.DataFrame(data_values1, columns=data_columns)
        df1.name = 'test_update'
        my_conn = MySQL(*self.conn_params)
        my_conn.insert(df1)
        data_values2 = [[1, "updated string1", 456.956], [2, "updated string2", 38.905]]
        df2 = pd.DataFrame(data_values2, columns=data_columns)
        df2.name = 'test_update'
        self.assertEqual(my_conn.update(df2, index=['column_int']), 2)

    def testDelete(self):
        data_columns = ['column_int', 'column_string', 'column_float']
        data_values = [[1, 'string1', 456.956], [2, 'string2', 38.905]]
        df = pd.DataFrame(data_values, columns=data_columns)
        df.name = 'test_delete'
        my_conn = MySQL(*self.conn_params)
        my_conn.insert(df)
        self.assertEqual(my_conn.delete(df.name, conditions="column_int = 2"), 1)

    def testBulkInsert(self):
        data_columns = ['id', 'column_string', 'column_float']
        data_types = {'id': int, 'column_string': object, 'column_float': float}
        data_values = [
            [1, 'Alpha',	5.39282029181928],
            [2,	'Beta',	62.0512266797524],
            [3, 'Gamma', 7.20169799112829],
            [4, 'Delta', 17.2716470303442]
        ]
        df = pd.DataFrame(data=data_values, columns=data_columns)
        for k, v in data_types.items():
            df[k] = df[k].astype(v)

        df.name = 'test_bulk_insert'
        my_conn = MySQL(*self.conn_params)
        self.assertEqual(my_conn.bulk_insert(df,
                                             csv_path='test_bulk_insert.csv', sep=';', header=False, index=False), 4)

if __name__ == '__main__':
    unittest.main()
