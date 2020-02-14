# -*- coding: utf-8 -*-

"""Integration tests for oracle database module."""

import os
import unittest

from etlstat.database.oracle import Oracle

import pandas

from sqlalchemy.exc import DatabaseError

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
            sql = f"""CREATE USER test IDENTIFIED BY password
                DEFAULT TABLESPACE USERS TEMPORARY TABLESPACE TEMP;"""
            ora_conn.execute(sql)
        except DatabaseError:
            pass
        sql = f"""ALTER USER test QUOTA UNLIMITED ON USERS;
            GRANT CONNECT, RESOURCE TO test;"""
        ora_conn.execute(sql)

    def test_insert(self):
        """Check if a bulk insert with sql loader is correctly executed."""
        ora_conn = Oracle(*self.conn_params)
        sql = """CREATE TABLE px_01001 (id INTEGER, tipo_indicador
            VARCHAR(100), nivel_educativo VARCHAR(100), valor NUMBER)"""
        try:
            ora_conn.execute(sql)
        except DatabaseError:
            pass
        px_01001 = ora_conn.get_table('px_01001')
        self.assertTrue(px_01001.exists)
        source_file = self.output_path + 'px_01001.csv'
        data = pandas.read_csv(
            source_file,
            sep=';',
            encoding='utf8')
        data.name = 'px_01001'
        ora_conn.insert(
            *self.conn_params,
            data_table=data,
            output_path=self.output_path,
            os_path=self.os_path,
            os_ld_library_path=self.os_ld_library_path,
            mode='TRUNCATE',
            columns=['*'],
            schema=self.conn_params[0]
        )

        sql = "SELECT COUNT(id) AS n FROM px_01001"
        try:
            result = ora_conn.execute(sql)
            assert result[0]['n'][0] == 21
        except DatabaseError as dbe:
            print(str(dbe))


if __name__ == '__main__':
    unittest.main()
