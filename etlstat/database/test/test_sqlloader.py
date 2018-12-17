# -*- coding: utf-8 -*-

"""Unit tests for oracle database module."""

import os
import unittest
import pandas
from sqlalchemy.exc import DatabaseError
from etlstat.database.oracle import Oracle

os.environ['NLS_LANG'] = '.AL32UTF8'


class TestSqlLoader(unittest.TestCase):
    """Testing methods for Oracle class."""

    user = 'test'
    password = 'password'
    host = 'localhost'
    port = '1521'
    service_name = 'xe'
    conn_params = [user, password, host, port, service_name]
    output_path = os.getcwd() + '/etlstat/database/test/'
    os_path = '/usr/local/bin:/usr/bin:/bin:' \
        '/opt/oracle/instantclient_18_3'
    os_ld_library_path = '/opt/oracle/instantclient_18_3'

    @classmethod
    def setUpClass(cls):
        """Set up test variables."""
        user = 'system'
        password = 'oracle'
        host = 'localhost'
        port = '1521'
        service_name = 'xe'
        conn_params = [user, password, host, port, service_name]
        ora_conn = Oracle(*conn_params)
        try:
            sql = "CREATE USER test IDENTIFIED BY password " \
                    "DEFAULT TABLESPACE USERS TEMPORARY TABLESPACE TEMP"
            ora_conn.execute(sql)
        except DatabaseError as dbe:
            print(str(dbe))
        sql = "ALTER USER test QUOTA UNLIMITED ON USERS"
        ora_conn.execute(sql)
        sql = "GRANT CONNECT, RESOURCE TO test"
        ora_conn.execute(sql)

    def test_insert(self):
        """Check if a bulk insert with sql loader is correctly executed."""
        table_name = 'px_01001'
        ora_conn = Oracle(*self.conn_params)
        sql = "CREATE TABLE {0} (id INTEGER, tipo_indicador " \
            "VARCHAR(100), nivel_educativo VARCHAR(100), valor NUMBER)". \
            format(table_name)
        try:
            ora_conn.execute(sql)
        except DatabaseError as dbe:
            print(str(dbe))
        px_01001 = ora_conn.get_table(table_name)
        self.assertTrue(px_01001.exists)
        source_file = self.output_path + 'px_01001.csv'
        data_file = self.output_path + 'px_01001.dat'
        control_file = self.output_path + 'px_01001.ctl'
        log_file = self.output_path + 'px_01001.log'
        bad_file = self.output_path + 'px_01001.bad'
        data_columns = ['id', 'tipo_indicador', 'nivel_educativo', 'valor']
        table_def = pandas.DataFrame(columns=data_columns)
        table_def['id'] = table_def['id'].astype(int)
        table_def['tipo_indicador'] = table_def['tipo_indicador'].astype(str)
        table_def['nivel_educativo'] = table_def['nivel_educativo'].astype(str)
        table_def['valor'] = table_def['valor'].astype(float)
        table_def.name = table_name
        data = pandas.read_csv(source_file, header=0, sep=';', encoding='utf8')
        data.name = table_name
        ora_conn.insert(
            *self.conn_params,
            schema=self.conn_params[0],
            table=data,
            output_path=self.output_path,
            os_path=self.os_path,
            os_ld_library_path=self.os_ld_library_path
        )
        self.assertTrue(os.path.isfile(data_file))
        os.remove(data_file)
        self.assertTrue(os.path.isfile(control_file))
        os.remove(control_file)
        self.assertTrue(os.path.isfile(log_file))
        os.remove(log_file)
        self.assertFalse(os.path.isfile(bad_file))


if __name__ == '__main__':
    unittest.main()
