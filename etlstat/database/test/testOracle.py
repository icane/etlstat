import unittest
import decimal as dec
from etlstat.database.oracle import Oracle
import os.path
import pandas as pd


class TestOracle(unittest.TestCase):

    user = 'test'
    password = 'test'
    host = 'localhost'
    port = '1521'
    service_name = 'xe'
    conn_params = [user, password, host, port, service_name]

    @classmethod
    def setUpClass(cls):
        user = 'system'
        password = 'oracle'
        host = 'localhost'
        port = '1521'
        service_name = 'xe'
        conn_params = [user, password, host, port, service_name]
        ora_conn = Oracle(*conn_params)
        sql = "DROP USER TEST CASCADE"
        status, result = ora_conn.execute_sql(sql)
        print(result)
        sql = "CREATE USER test IDENTIFIED BY test " \
              "DEFAULT TABLESPACE USERS TEMPORARY TABLESPACE TEMP"
        status, result = ora_conn.execute_sql(sql)
        print(result)
        sql = "ALTER USER test QUOTA UNLIMITED ON USERS"
        status, result = ora_conn.execute_sql(sql)
        print(result)
        sql = "GRANT CONNECT, RESOURCE TO test"
        status, result = ora_conn.execute_sql(sql)
        print(result)

    def testInit(self):
        self.assertEqual(str(Oracle(*self.conn_params).engine),
                         "Engine(oracle+cx_oracle://test:***@localhost:1521/?service_name=xe)")

    def testCheckForTable(self):
        ora_conn = Oracle(*self.conn_params)
        sql = "CREATE TABLE test_check (id integer, column1 varchar2(100), column2 number)"
        ora_conn.execute_sql(sql)
        self.assertTrue(ora_conn.check_for_table('test_check', schema='test'))
        sql = "DROP TABLE test_check"
        ora_conn.execute_sql(sql)

    def testExecuteSql(self):
        ora_conn = Oracle(*self.conn_params)
        sql = "CREATE TABLE test_sql (id integer, column1 varchar2(100), column2 number)"
        status, result = ora_conn.execute_sql(sql)
        self.assertTrue(status)
        sql = "INSERT INTO test_sql (id, column1, column2) " \
              "VALUES (1, 'Varchar text (100 char)', 123456789.012787648484859)"
        status, result = ora_conn.execute_sql(sql)
        self.assertTrue(status)
        sql = 'select * from test_sql order by id'
        status, result = ora_conn.execute_sql(sql)
        self.assertEqual(result.columns.values[0], 'id')
        self.assertEqual(result.columns.values[1], 'column1')
        self.assertEqual(result.columns.values[2], 'column2')
        self.assertEqual(result.values[0][0], 1)
        self.assertEqual(result.values[0][1], 'Varchar text (100 char)')
        self.assertEqual(result.values[0][2], 123456789.012787648484859)
        sql = "UPDATE test_sql SET id = 2 WHERE id = 1"
        status, result = ora_conn.execute_sql(sql)
        self.assertTrue(status)
        sql = "DELETE FROM test_sql WHERE id > 0"
        status, result = ora_conn.execute_sql(sql)
        self.assertTrue(status)
        sql = "DROP TABLE test_sql"
        status, result = ora_conn.execute_sql(sql)
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
        table.name = "test_create"
        ora_conn = Oracle(*self.conn_params)
        self.assertTrue(ora_conn.create(table))
        sql = "DROP TABLE test_create"
        ora_conn.execute_sql(sql)

    def testSelect(self):
        ora_conn = Oracle(*self.conn_params)
        table_name = "all_tables"
        conditions = "owner = 'CTXSYS'"
        df = ora_conn.select(table_name, conditions=conditions)
        self.assertEqual(len(df), 5)

    def testInsert(self):
        data_columns = ['column_int', 'column_string', 'column_float']
        data_values = [[1, 'string1', 456.956], [2, 'string2', 38.905]]
        df = pd.DataFrame(data_values, columns=data_columns)
        df.name = 'test_insert'
        ora_conn = Oracle(*self.conn_params)
        self.assertEqual(ora_conn.insert(df), 2)
        sql = "DROP TABLE test_insert"
        ora_conn.execute_sql(sql)

    def testUpdate(self):
        data_columns = ['column_int', 'column_string', 'column_float']
        data_values1 = [[1, 'string1', 456.956], [2, 'string2', 38.905]]
        df1 = pd.DataFrame(data_values1, columns=data_columns)
        df1.name = 'test_update'
        ora_conn = Oracle(*self.conn_params)
        ora_conn.insert(df1)
        data_values2 = [[1, "updated string1", 456.956], [2, "'updated string2'", 38.905]]
        df2 = pd.DataFrame(data_values2, columns=data_columns)
        df2.name = 'test_update'
        self.assertEqual(ora_conn.update(df2, index=['column_int']), 2)
        sql = "DROP TABLE test_update"
        ora_conn.execute_sql(sql)

    def testDelete(self):
        data_columns = ['column_int', 'column_string', 'column_float']
        data_values = [[1, 'string1', 456.956], [2, 'string2', 38.905]]
        df = pd.DataFrame(data_values, columns=data_columns)
        df.name = 'test_delete'
        ora_conn = Oracle(*self.conn_params)
        ora_conn.insert(df)
        self.assertEqual(ora_conn.delete(df.name, conditions="column_int = 2"), 1)
        sql = "DROP TABLE test_delete"
        ora_conn.execute_sql(sql)
        
    def testBulkInsert(self):
        source_path = '01001.csv'
        table_name = 'px_01001'
        data_file = 'px_01001.csv'
        control_file = 'px_01001.ctl'
        df = pd.read_csv(source_path, header=0, sep=';', encoding='utf8')
        df.name = table_name
        ora_conn = Oracle.__new__(Oracle)
        ora_conn.bulk_insert(df, data_file, control_file, mode="TRUNCATE")
        self.assertTrue(os.path.isfile(data_file))
        self.assertTrue(os.path.isfile(control_file))

if __name__ == '__main__':
    unittest.main()
