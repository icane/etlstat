# -*- coding: utf-8 -*-

"""Unit tests for MySQL database module."""

import os
import unittest
import pandas as pd
from sqlalchemy.exc import InvalidRequestError
from sqlalchemy import (select, func, Column, Integer, String, Boolean, Float,
                        DateTime)
from sqlalchemy.ext.declarative import declarative_base
from etlstat.database.mysql import MySQL


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
        password = ''
        host = '127.0.0.1'
        port = '3306'
        database = ''
        conn_params = [user, password, host, port, database]
        ddl = "DROP DATABASE IF EXISTS test"
        my_conn = MySQL(*conn_params)
        my_conn.execute(ddl)
        ddl = "CREATE DATABASE test"
        my_conn.execute(ddl)
        sql = "CREATE TABLE test.inf_schema as " \
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
        current = len(result.index)
        self.assertEqual(expected, current)
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
        my_conn.insert(data)
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
        my_conn.insert(data)
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
        print(type(tmp_data))
        my_conn.upsert(tmp_data, data.name, sql)
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

        # delete from operation
        # the None argument in delete DML is included to avoid pylint E1120
        table.delete(None).where(table.c.column_int == 2).execute()

        expected = 1
        current = my_conn.engine.scalar(
            select([func.count('*')]).select_from(table)
        )
        self.assertEqual(current, expected)
        my_conn.drop(data.name)


if __name__ == '__main__':
    unittest.main()
