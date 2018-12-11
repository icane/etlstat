# -*- coding: utf-8 -*-

"""Unit tests for oracle database module."""

import os
import unittest
import pandas as pd
from sqlalchemy.exc import InvalidRequestError
from etlstat.database.oracle import Oracle

os.environ['NLS_LANG'] = '.AL32UTF8'


class TestOracle(unittest.TestCase):
    """Testing methods for Oracle class."""

    user = 'test'
    password = 'password'
    host = 'localhost'
    port = '1521'
    service_name = 'xe'
    conn_params = [user, password, host, port, service_name]
    output_path = os.getcwd() + '/etlstat/database/test/'
    os_path = '/usr/local/bin:/usr/bin:/bin:' \
        '/var/opt/oracle/instantclient_18_3'
    os_ld_library_path = '/var/opt/oracle/instantclient_18_3'

    @classmethod
    def setUpClass(cls):
        """Set up test variables."""
        user = 'system'
        password = 'oracle'
        host = 'localhost'
        port = '1521'
        service_name = 'xe'
        conn_params = [user, password, host, port, service_name]
        my_conn = Oracle(*conn_params)
        sql = "DROP USER TEST CASCADE"
        my_conn.execute(sql)
        sql = "CREATE USER test IDENTIFIED BY password " \
              "DEFAULT TABLESPACE USERS TEMPORARY TABLESPACE TEMP"
        my_conn.execute(sql)
        sql = "ALTER USER test QUOTA UNLIMITED ON USERS"
        my_conn.execute(sql)
        sql = "GRANT CONNECT, RESOURCE TO test"
        my_conn.execute(sql)

    def test_init(self):
        """Check connection with Oracle database."""
        self.assertEqual(str(Oracle(*self.conn_params).engine),
                         "Engine(oracle+cx_oracle://test:***@localhost:1521"
                         "/?service_name=xe)")

    def test_execute(self):
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

    def test_select(self):
        """ Check select statement using sqlalchemy"""
        my_conn = MySQL(*self.conn_params)
        table_name = "inf_schema"
        inf_schema = my_conn.get_table(table_name)
        # SELECT * FROM inf_schema
        # WHERE table_name like 'INNO%' AND avg_row_length > 100
        results = my_conn.engine.execute(select('*')
                                         .where(inf_schema.c.table_name
                                                .like('INNO%'))
                                         .where(inf_schema.c.avg_row_length >
                                                100)
                                         .select_from(inf_schema)).fetchall()
        table_df = pd.DataFrame(results)
        self.assertGreaterEqual(len(table_df), 6)

    def test_get_table(self):
        """Check get table from the database using SqlAlchemy."""
        my_conn = Oracle(*self.conn_params)
        inf_schema = my_conn.get_table('inf_schema')  # GET TABLE example
        row_count = my_conn.engine.scalar(
            select([func.count('*')]).select_from(inf_schema)
        )
        # The select.columns parameter is not available in the method form of
        # select(), e.g. FromClause.select().
        # See https://docs.sqlalchemy.org/en/latest/core/selectable.html#
        # sqlalchemy.sql.expression.FromClause.select
        my_conn.engine.execute(
            select([inf_schema.c.table_name]).select_from(inf_schema))
        self.assertEqual(row_count, 295)

    def test_create(self):
        """ Check create table using sqlalchemy """
        Base = declarative_base()
        my_conn = MySQL(*self.conn_params)

        # table creation can be done via execute() + raw SQL or using this:
        class Table2(Base):
            """ Auxiliary sqlalchemy table model for the tests"""
            __tablename__ = 'table2'

            column_int = Column(Integer)
            column_string = Column(String(20))
            column_float = Column(Float)
            column_datetime = Column(DateTime)
            column_boolean = Column(Boolean)
            id = Column(Integer, primary_key=True)

        Table2.__table__.create(bind=my_conn.engine)
        table2 = my_conn.get_table('table2')
        self.assertEqual(table2.c.column_datetime.name, 'column_datetime')
        self.assertEqual(len(table2.c), 6)
        my_conn.drop('table2')

    def test_drop(self):
        """ Check drop for an existing table """
        my_conn = Oracle(*self.conn_params)
        sql = "CREATE TABLE table1 (id integer, column1 varchar(100), " \
            "column2 double)"
        my_conn.execute(sql)
        my_conn.get_table('table1')
        my_conn.drop('table1')  # DROP example
        with self.assertRaises(InvalidRequestError):
            my_conn.get_table('table1')

    def test_delete(self):
        """ Check delete rows from table."""
        data_columns = ['column_int', 'column_string', 'column_float']
        data_values = [[1, 'string1', 456.956], [2, 'string2', 38.905]]
        data = pd.DataFrame(data_values, columns=data_columns)
        data.name = 'test_delete'
        my_conn = MySQL(*self.conn_params)
        my_conn.insert(data)
        table = my_conn.get_table(data.name)
        expected = 2
        current = my_conn.engine.scalar(
            select([func.count('*')]).select_from(table)
        )
        self.assertEqual(current, expected)

    def test_load_data(self):
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
