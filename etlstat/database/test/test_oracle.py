# -*- coding: utf-8 -*-

"""
Unit tests for oracle database module.
"""

import os
import unittest
import pandas as pd
from etlstat.database.oracle import Oracle

os.environ['NLS_LANG'] = '.AL32UTF8'


class TestOracle(unittest.TestCase):
    """Testing methods for Oracle class"""

    def setUp(self):
        """Sets up test variables"""

        self.user = 'system'
        self.password = 'oracle'
        self.host = 'localhost'
        self.port = '1521'
        self.service_name = 'xe'
        self.conn_params = [self.user, self.password, self.host, self.port,
                            self.service_name]
        self.ora_conn = Oracle(*self.conn_params)
        sql = "DROP USER TEST CASCADE"
        status = self.ora_conn.execute_sql(sql)[0]
        self.flag1 = status
        sql = "CREATE USER test IDENTIFIED BY test " \
              "DEFAULT TABLESPACE USERS TEMPORARY TABLESPACE TEMP"
        status = self.ora_conn.execute_sql(sql)[0]
        self.flag2 = status
        sql = "ALTER USER test QUOTA UNLIMITED ON USERS"
        status = self.ora_conn.execute_sql(sql)[0]
        self.flag3 = status
        sql = "GRANT CONNECT, RESOURCE TO test"
        status = self.ora_conn.execute_sql(sql)[0]
        self.flag4 = status
        self.output_path = os.getcwd() + '/etlstat/database/test/'
        self.os_path = '/usr/local/bin:/usr/bin:/bin:' \
                       '/var/opt/oracle/instantclient_18_3'
        self.os_ld_library_path = '/var/opt/oracle/instantclient_18_3'
        self.schema = 'test'
        self.user = 'test'
        self.password = 'test'
        self.conn_params = [self.user, self.password, self.host, self.port,
                            self.service_name]

    def test_connection(self):
        """ Check connection with Oracle database"""

        self.assertTrue(self.flag3)
        self.assertTrue(self.flag4)

    def test_init(self):
        """ Check connection with Oracle database"""
        self.assertEqual(str(Oracle(*self.conn_params).engine),
                         "Engine(oracle+cx_oracle://test:***@localhost:1521"
                         "/?service_name=xe)")

    def test_execute_sql(self):
        """ Check if different queries are correctly executed."""
        ora_conn = Oracle(*self.conn_params)
        sql = "CREATE TABLE test_sql (id integer, column1 varchar2(100), " \
              "column2 number)"
        status, result = ora_conn.execute_sql(sql)
        self.assertTrue(status)
        sql = "INSERT INTO test_sql (id, column1, column2) " \
              "VALUES (1, 'Varchar text (100 char)', " \
              "123456789.012787648484859)"
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

    def test_bulk_insert(self):
        """Check if a bulk insert with sql loader is correctly executed."""
        source_file = self.output_path + '01001.csv'
        table_name = 'px_01001'
        data_file = self.output_path + 'px_01001.dat'
        control_file = self.output_path + 'px_01001.ctl'
        log_file = self.output_path + 'px_01001.log'
        bad_file = self.output_path + 'px_01001.bad'
        data_columns = ['id', 'tipo_indicador', 'nivel_educativo', 'valor']
        table_def = pd.DataFrame(columns=data_columns)
        table_def['id'] = table_def['id'].astype(int)
        table_def['tipo_indicador'] = table_def['tipo_indicador'].astype(str)
        table_def['nivel_educativo'] = table_def['nivel_educativo'].astype(str)
        table_def['valor'] = table_def['valor'].astype(float)
        table_def.name = table_name
        ora_conn = Oracle(*self.conn_params)
        ora_conn.create(table_def, self.schema)

        data = pd.read_csv(source_file, header=0, sep=';', encoding='utf8')
        data.name = table_name
        ora_conn.bulk_insert(self.user,
                             self.password,
                             self.host,
                             self.port,
                             self.service_name,
                             self.schema,
                             data,
                             self.output_path,
                             self.os_path,
                             self.os_ld_library_path,
                             mode="TRUNCATE")
        self.assertTrue(os.path.isfile(data_file))
        self.assertTrue(os.path.isfile(control_file))
        self.assertTrue(os.path.isfile(log_file))
        self.assertFalse(os.path.isfile(bad_file))


if __name__ == '__main__':
    unittest.main()
