import unittest
from etlstat.database.oracle import Oracle
import os.path
import pandas


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
        ddl = "DROP USER TEST CASCADE"
        status, result = Oracle.execute_sql(ddl, *conn_params)
        print(result)
        ddl = "CREATE USER test IDENTIFIED BY test " \
              "DEFAULT TABLESPACE USERS TEMPORARY TABLESPACE TEMP"
        status, result = Oracle.execute_sql(ddl, *conn_params)
        print(result)
        ddl = "ALTER USER test QUOTA UNLIMITED ON USERS"
        status, result = Oracle.execute_sql(ddl, *conn_params)
        print(result)
        ddl = "GRANT CONNECT, RESOURCE TO test"
        status, result = Oracle.execute_sql(ddl, *conn_params)
        print(result)
        ddl = "CREATE TABLE test.table1 (id integer, column1 varchar2(100), column2 number)"
        status, result = Oracle.execute_sql(ddl, *conn_params)
        print(result)

    def testConnect(self):
        ora = Oracle()
        if ora.engine is None:
            ora._connect(*self.conn_params)
        self.assertTrue(ora.engine.has_table('HELP', 'SYSTEM'))
        self.assertTrue(ora.engine.has_table('help', 'system'))

    def testExecuteSqlDdl(self):
        ddl = "CREATE TABLE test.table2 (id integer, column1 varchar2(100), column2 number)"
        status, result = Oracle.execute_sql(ddl, *self.conn_params_test)
        self.assertTrue(status)

    def testExecuteSqlSelect(self):
        dml = 'select * from system.help'
        status, result = Oracle.execute_sql(dml, *self.conn_params)
        self.assertEqual(result.columns.values[0], 'topic')
        self.assertEqual(result.columns.values[1], 'seq')
        self.assertEqual(result.columns.values[2], 'info')
        self.assertEqual(len(result), 919)

    def testExecuteSqlDml(self):
        dml = "INSERT INTO test.table1 (id, column1, column2) " \
              "VALUES (1, 'Varchar text (100 char)', 123456789.0123456789)"
        status, result = Oracle.execute_sql(dml, *self.conn_params_test)
        self.assertTrue(status)

    def testBulkInsert(self):
        source_path = '01001.csv'
        table_name = 'px_01001'
        data_file = 'px_01001.csv'
        control_file = 'px_01001.ctl'
        df = pandas.read_csv(source_path, header=0, sep=';', encoding='utf8')
        df.name = table_name
        Oracle.bulk_insert(df, data_file, control_file, mode="TRUNCATE")
        self.assertTrue(os.path.isfile(data_file))
        self.assertTrue(os.path.isfile(control_file))

if __name__ == '__main__':
    unittest.main()
