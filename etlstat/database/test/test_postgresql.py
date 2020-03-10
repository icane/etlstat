"""Integration tests for PostgreSQL database module."""

import unittest

from etlstat.database.postgresql import PostgreSQL


class TestPostgreSQL(unittest.TestCase):
    """Testing methods for Postgresql class."""

    user = 'test'
    password = 'password'
    host = '127.0.0.1'
    port = '5432'
    database = 'postgres'
    conn_params = [user, password, host, port, database]

    @classmethod
    def setUpClass(cls):
        """Set up test variables."""
        user = 'postgres'
        password = 'password'
        host = '127.0.0.1'
        port = '5432'
        database = 'postgres'
        conn_params = [user, password, host, port, database]
        ddl = "DROP SCHEMA IF EXISTS test CASCADE"
        pg_conn = PostgreSQL(*conn_params)
        pg_conn.execute(ddl)
        ddl = "DROP USER IF EXISTS test"
        pg_conn.execute(ddl)
        ddl = "CREATE SCHEMA test"
        pg_conn.execute(ddl)
        sql = "CREATE TABLE IF NOT EXISTS test.inf_schema AS " \
              "SELECT table_catalog, table_schema, " \
              "table_name, table_type " \
              "FROM information_schema.tables"
        pg_conn.execute(sql)
        ddl = "CREATE USER test WITH ENCRYPTED PASSWORD 'password'"
        pg_conn.execute(ddl)
        ddl = "GRANT ALL PRIVILEGES ON SCHEMA test TO test"
        pg_conn.execute(ddl)

    def test_init(self):
        """Check connection with Postgresql database."""
        self.assertEqual(str(PostgreSQL(*self.conn_params).engine),
                         "Engine(postgresql://test:***@127.0.0.1:"
                         "5432/postgres)")

    def test_execute(self):
        """Check execute method launching arbitrary sql queries."""
        pg_conn = PostgreSQL(*self.conn_params)
        sql = f'''CREATE TABLE table1 (id integer, column1 varchar(100),
                  column2 float)'''
        pg_conn.execute(sql)
        sql = "INSERT INTO table1 (id, column1, column2) " \
              "VALUES (1, 'Varchar text (100 char)', 123456789.0123456789)"
        pg_conn.execute(sql)
        result = pg_conn.execute("SELECT * FROM table1")
        self.assertEqual('Varchar text (100 char)', result[0]['column1'][0])
        query = 'SELECT * FROM table1 ORDER BY id'
        result = pg_conn.execute(query)
        expected = 1
        current = len(result[0].index)
        self.assertEqual(expected, current)
        pg_conn.execute('DROP TABLE table1')

    def test_execute_multiple(self):
        """Check if multiple SQL statements are correctly executed."""
        pg_conn = PostgreSQL(*self.conn_params)
        sql = f"""CREATE TABLE table1 (id integer, column1 varchar(100),
              column2 float);
              INSERT INTO table1 (id, column1, column2)
              VALUES (1, 'Varchar; text; (100 char)',
              123456789.012787648484859);
              INSERT INTO table1 (id, column1, column2)
              VALUES (2, 'Varchar; text; (100 char)',
              -789.0127876);
              SELECT id, column2 FROM table1;"""
        result = pg_conn.execute(sql)
        self.assertEqual(len(result), 4)
        self.assertEqual(len(result[3].index), 2)
        pg_conn.execute('DROP TABLE table1')


if __name__ == '__main__':
    unittest.main()
