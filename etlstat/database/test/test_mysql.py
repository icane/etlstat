# -*- coding: utf-8 -*-

"""Integration tests for MySQL database module."""

import os
import unittest
import pandas as pd

from etlstat.database.mysql import MySQL
from etlstat.text import utils

from pyaxis import pyaxis

from sqlalchemy import (Boolean, Column, DateTime, Float, Integer, String,
                        func, select)
from sqlalchemy.exc import InvalidRequestError
from sqlalchemy.ext.declarative import declarative_base


class TestMySQL(unittest.TestCase):
    """Testing methods for MySQL class."""

    user = 'test'
    password = 'password'
    host = '127.0.0.1'
    port = '3306'
    database = 'test'
    conn_params = [user, password, host, port, database]

    @classmethod
    def setUpClass(cls):
        """Set up test variables."""
        user = 'root'
        password = 'password'
        host = '127.0.0.1'
        port = '3306'
        database = ''
        conn_params = [user, password, host, port, database]
        ddl = "DROP DATABASE IF EXISTS test"
        my_conn = MySQL(*conn_params)
        my_conn.execute(ddl)
        ddl = "CREATE DATABASE test"
        my_conn.execute(ddl)
        sql = "CREATE TABLE IF NOT EXISTS test.inf_schema as " \
              "SELECT table_name, avg_row_length, " \
              "table_collation, create_time " \
              "FROM information_schema.tables"
        my_conn.execute(sql)
        ddl = "GRANT ALL PRIVILEGES ON test.* to 'test'@'%'" \
              " IDENTIFIED BY 'password'"
        ddl_file = "GRANT FILE ON *.* to 'test'@'%' IDENTIFIED BY 'password'"
        my_conn.execute(ddl)
        my_conn.execute(ddl_file)

    def test_init(self):
        """Check connection with MySQL database."""
        self.assertEqual(str(MySQL(*self.conn_params).engine),
                         "Engine(mysql+mysqlconnector://test:***@127.0.0.1:"
                         "3306/test)")

    def test_get_table(self):
        """Check get table from the database using SqlAlchemy."""
        my_conn = MySQL(*self.conn_params)
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
        self.assertGreaterEqual(row_count, 100)

    def test_execute(self):
        """Check execute method launching arbitrary sql queries."""
        my_conn = MySQL(*self.conn_params)
        sql = f'''CREATE TABLE table1 (id integer, column1 varchar(100),
                  column2 double)'''
        my_conn.execute(sql)
        table1 = my_conn.get_table('table1')
        self.assertEqual(table1.c.column1.name, 'column1')
        sql = "INSERT INTO table1 (id, column1, column2) " \
              "VALUES (1, 'Varchar text (100 char)', 123456789.0123456789)"
        my_conn.execute(sql)  # EXECUTE example
        # The select.columns parameter is not available in the method form of
        # select(), e.g. FromClause.select().
        # See https://docs.sqlalchemy.org/en/latest/core/selectable.html#
        # sqlalchemy.sql.expression.FromClause.select
        results = my_conn.engine.execute(
            select([table1.c.column1]).select_from(table1)).fetchall()
        expected = 'Varchar text (100 char)'
        current = results[0][0]
        # this returns a tuple inside a list and I dont know why
        self.assertEqual(expected, current)
        query = 'select * from table1 order by id'
        result = my_conn.execute(query)
        expected = 1
        current = len(result[0].index)
        self.assertEqual(expected, current)
        my_conn.drop('table1')

    def test_execute_multiple(self):
        """Check if multiple SQL statements are correctly executed."""
        my_conn = MySQL(*self.conn_params)
        sql = f"""CREATE TABLE table1 (id integer, column1 varchar(100),
              column2 double);
              INSERT INTO table1 (id, column1, column2)
              VALUES (1, 'Varchar; text; (100 char)',
              123456789.012787648484859);
              INSERT INTO table1 (id, column1, column2)
              VALUES (2, 'Varchar; text; (100 char)',
              -789.0127876);
              SELECT id, column2 FROM table1;"""
        results = my_conn.execute(sql)
        self.assertEqual(len(results), 4)
        self.assertEqual(len(results[3].index), 2)
        my_conn.drop('table1')

    def test_drop(self):
        """Check drop for an existing table."""
        my_conn = MySQL(*self.conn_params)
        sql = "CREATE TABLE table1 (id integer, column1 varchar(100), " \
            "column2 double)"
        my_conn.execute(sql)
        my_conn.get_table('table1')
        my_conn.drop('table1')  # DROP example
        with self.assertRaises(InvalidRequestError):
            my_conn.get_table('table1')

    def test_create(self):
        """Check create table using sqlalchemy."""
        Base = declarative_base()
        my_conn = MySQL(*self.conn_params)

        # table creation can be done via execute() + raw SQL or using this:
        class Table2(Base):
            """Auxiliary sqlalchemy table model for the tests."""

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

    def test_select(self):
        """Check select statement using sqlalchemy."""
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

    def test_insert(self):
        """Check that insert method inserts rows into a table."""
        my_conn = MySQL(*self.conn_params)
        Base = declarative_base()
        current_dir = os.path.dirname(os.path.abspath(__file__))

        class Pmh(Base):
            """Auxiliary sqlalchemy table model for the tests."""

            __tablename__ = 'pmh'

            id = Column(Integer, primary_key=True)
            anno = Column(Integer)
            cpro = Column(Integer)
            cmun = Column(Integer)
            csexo = Column(Integer)
            sexo = Column(String(20))
            orden_gredad = Column(Integer)
            gredad = Column(String(30))
            personas = Column(Integer)
            codigo_ine = Column(String(50))

        Pmh.__table__.create(bind=my_conn.engine)
        data = pd.read_csv(f'''{current_dir}/pmh.csv''')
        data.name = 'pmh'
        my_conn.insert(data, if_exists='append')
        # my_conn.execute(f'''alter table {data.name} add primary key(id)''')
        actual = my_conn.engine.scalar(
            select([func.count('*')]).select_from(Pmh)
        )
        expected = len(data.index)
        self.assertEqual(actual, expected)
        my_conn.drop('pmh')

    def test_upsert(self):
        """Check that upsert method inserts or updates rows in a table."""
        my_conn = MySQL(*self.conn_params)
        Base = declarative_base()
        current_dir = os.path.dirname(os.path.abspath(__file__))

        # IMPORTANT: table logic reuse pattern with mixins
        class PmhMixin(object):
            """Auxiliary sqlalchemy table model for the tests."""

            __tablename__ = 'pmh'

            id = Column(Integer, primary_key=True)
            anno = Column(Integer)
            cpro = Column(Integer)
            cmun = Column(Integer)
            csexo = Column(Integer)
            sexo = Column(String(20))
            orden_gredad = Column(Integer)
            gredad = Column(String(30))
            personas = Column(Integer)
            codigo_ine = Column(String(50))

        class Pmh(Base, PmhMixin):
            """Auxiliary sqlalchemy table model for the tests."""

            __tablename__ = 'pmh'

        class PmhTmp(Base, PmhMixin):
            """Auxiliary sqlalchemy table model for the tests."""

            __tablename__ = 'tmp_pmh'

        # table to update/insert
        Pmh.__table__.create(bind=my_conn.engine)
        data = pd.read_csv(f'''{current_dir}/pmh.csv''')
        data.name = 'pmh'
        my_conn.insert(data, if_exists='append')
        # https://github.com/PyCQA/pylint/issues/1161
        # there's an issue with pylint and pandas read methods.
        original_table = pd.DataFrame(
            pd.read_sql_table(data.name, my_conn.conn_string))

        # temporary table with data to update/insert
        PmhTmp.__table__.create(bind=my_conn.engine)
        tmp_data = pd.read_csv(f'''{current_dir}/pmh_update.csv''')
        tmp_data.name = 'tmp_pmh'

        sql = f'''INSERT INTO pmh
                   (id, anno, cpro, cmun, csexo, sexo, orden_gredad, gredad,
                  personas, codigo_ine)
                  SELECT *
                  FROM tmp_pmh
                  ON DUPLICATE KEY UPDATE
                  id = tmp_pmh.id,
                  anno = tmp_pmh.anno,
                  cpro = tmp_pmh.cpro,
                  cmun = tmp_pmh.cmun,
                  csexo = tmp_pmh.csexo,
                  orden_gredad = tmp_pmh.orden_gredad,
                  gredad = tmp_pmh.gredad,
                  personas = tmp_pmh.personas,
                  codigo_ine = tmp_pmh.codigo_ine;'''
        expected = 76
        current = original_table.loc[
            original_table['id'] == 5192]['personas'].tolist()[0]
        self.assertEqual(current, expected)
        my_conn.upsert(tmp_data, data.name, sql, if_exists='append')
        updated_table = pd.DataFrame(
            pd.read_sql_table(data.name, my_conn.conn_string))
        expected = 9976
        current = updated_table.loc[
            updated_table['id'] == 5192]['personas'].tolist()[0]
        self.assertEqual(current, expected)
        expected = 46
        current = updated_table.loc[
            updated_table['id'] == 30001]['cmun'].tolist()[0]
        my_conn.drop(data.name)

    def test_delete(self):
        """Check delete rows from table."""
        data_columns = ['id', 'column_string', 'column_float']
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

        # delete from operation
        # the None argument in delete DML is included to avoid pylint E1120
        table.delete(None).where(table.c.id == 2).execute()

        expected = 1
        current = my_conn.engine.scalar(
            select([func.count('*')]).select_from(table)
        )
        self.assertEqual(current, expected)
        my_conn.drop(data.name)

    def test_complete_insert_from_pcaxis(self):
        """Check complete cycle by inserting a pcaxis file into a table."""
        my_conn = MySQL(*self.conn_params)
        Base = declarative_base()
        current_dir = os.path.dirname(os.path.abspath(__file__))
        parsed_pcaxis = pyaxis.parse(current_dir + '/22350.px',
                                     encoding='ISO-8859-2')
        table_data = parsed_pcaxis['DATA']
        table_data = utils.parse_df_columns(table_data)
        table_data.name = 'ipc'

        class Ipc(Base):
            """Auxiliary sqlalchemy table model for the tests."""

            __tablename__ = 'ipc'

            id = Column(Integer, primary_key=True)
            comunidades_y_ciudades_autonomas = Column(String(100))
            grupos_ecoicop = Column(String(50))
            tipo_de_dato = Column(String(50))
            periodo = Column(String(50))
            data = Column(Float)

        Ipc.__table__.create(bind=my_conn.engine)
        my_conn.insert(table_data, if_exists='append')
        actual = my_conn.engine.scalar(
            select([func.count('*')]).select_from(Ipc)
        )
        expected = len(table_data.index)
        self.assertEqual(actual, expected)
        my_conn.drop('ipc')

    def test_insert_selected_columns(self):
        """Check insert method only with selected columns."""
        my_conn = MySQL(*self.conn_params)
        Base = declarative_base()
        current_dir = os.path.dirname(os.path.abspath(__file__))
        parsed_pcaxis = pyaxis.parse(current_dir + '/22350.px',
                                     encoding='ISO-8859-2')
        table_data = parsed_pcaxis['DATA']
        table_data = utils.parse_df_columns(table_data)
        table_data.name = 'ipc'

        class Ipc(Base):
            """Auxiliary sqlalchemy table model for the tests."""

            __tablename__ = 'ipc'

            id = Column(Integer, primary_key=True)
            comunidades_y_ciudades_autonomas = Column(String(100))
            grupos_ecoicop = Column(String(50))
            tipo_de_dato = Column(String(50))
            periodo = Column(String(50))
            data = Column(Float)

        Ipc.__table__.create(bind=my_conn.engine)
        insert_data = table_data[['grupos_ecoicop', 'data']]
        insert_data.name = 'ipc'

        my_conn.insert(insert_data, if_exists='append',
                       columns=['grupos_ecoicop', 'data'])
        result_data = pd.read_sql_query('select * from ipc',
                                        con=my_conn.engine)
        self.assertTrue(
            result_data['comunidades_y_ciudades_autonomas'].isnull().all())
        self.assertTrue(result_data['periodo'].isnull().all())
        self.assertTrue(result_data['tipo_de_dato'].isnull().all())
        self.assertFalse(result_data['grupos_ecoicop'].isnull().all())
        self.assertFalse(result_data['data'].isnull().all())
        my_conn.drop('ipc')


if __name__ == '__main__':
    unittest.main()
