import pandas
import unittest
from etlstat.database.oracle import Oracle


class TestOracle(unittest.TestCase):
    user = 'system'
    password = 'oracle'
    host = '127.0.0.1'
    port = '1521'
    service_name = 'xe'
    conn_params = [user, password, host, port, service_name]
    conn_params_test = ['test', 'test', host, port, service_name]

    @classmethod
    def setUpClass(cls):
        user = 'system'
        password = 'oracle'
        host = '127.0.0.1'
        port = '1521'
        service_name = 'xe'
        conn_params = [user, password, host, port, service_name]
        ddl = "CREATE USER test IDENTIFIED BY test " \
              "DEFAULT TABLESPACE USERS TEMPORARY TABLESPACE TEMP"
        status, result = Oracle.execute_ddl(ddl, *conn_params)
        print(result)
        ddl = "ALTER USER test QUOTA UNLIMITED ON USERS"
        status, result = Oracle.execute_ddl(ddl, *conn_params)
        print(result)
        ddl = "GRANT CONNECT, RESOURCE TO test"
        status, result = Oracle.execute_ddl(ddl, *conn_params)
        print(result)
        ddl = "CREATE TABLE test.table1 (id integer, column1 varchar2(100), column2 number)"
        status, result = Oracle.execute_ddl(ddl, *conn_params)
        print(result)

    def testConnect(self):
        ora = Oracle()
        if ora.engine is None:
            ora._connect(*self.conn_params)
        self.assertTrue(ora.engine.has_table('HELP', 'SYSTEM'))
        self.assertTrue(ora.engine.has_table('help', 'system'))

    def testExecuteDdl(self):
        ddl = "CREATE TABLE test.table2 (id integer, column1 varchar2(100), column2 number)"
        status, result = Oracle.execute_ddl(ddl, *self.conn_params_test)
        self.assertTrue(status)

    def testSelect(self):
        sql = 'select * from system.help'
        df = Oracle.select(sql, *self.conn_params)
        self.assertEqual(df.columns.values[0], 'topic')
        self.assertEqual(df.columns.values[1], 'seq')
        self.assertEqual(df.columns.values[2], 'info')
        self.assertEqual(len(df), 919)

    def testInsert(self):
        table_name = 'test.table1'
        field_map = {
            'id': 2,
            'column1': "'Varchar text (100 char)'",
            'column2': 2.02
        }
        self.assertTrue(Oracle.insert(table_name, field_map, *self.conn_params_test))

if __name__ == '__main__':
    unittest.main()
