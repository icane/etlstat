import unittest
from utils.database.dbConnection import DbConnection

my_host = 'sicanedev01.intranet.gobcantabria.es'
my_database = 'test'
my_user = 'test'
my_password = 'test'

ora_host = 'constantino.icane.es:1521'
ora_service = 'CONSTANT.ICANE.ES'
ora_user = 'municipal'
ora_password = 'municipal'
ora_connStr = ora_user + '/' + ora_password + '@' + ora_host + '/' + ora_service


class TestDbConnection(unittest.TestCase):

    def test_mysql_connection(self):
        connection = DbConnection()
        connection.mysql_connection(my_host, my_database, my_user, my_password)
        self.assertEqual(connection.connected)

    def test_ddl_mysql(self):
        str_sql = """
            CREATE TABLE titles (
            emp_no int(11) NOT NULL,
            title varchar(50) NOT NULL,
            from_date date NOT NULL,
            to_date date DEFAULT NULL,
            PRIMARY KEY (emp_no, title, from_date)
            ) ENGINE=InnoDB
        """
        connection = DbConnection()
        connection.mysql_connection(my_host, my_database, my_user, my_password)
        connection.execute(str_sql)

    def test_read_mysql(self):
        str_sql = 'show databases'
        connection = DbConnection()
        connection.mysql_connection(my_host, my_database, my_user, my_password)
        db_cursor = connection.execute(str_sql)
        for (db) in db_cursor:
            print(db)

    def test_oracle_connection(self):
        connection = DbConnection()
        connection.oracle_connection(ora_connStr)
        self.assertEqual(connection.connected)

    def test_read_oracle(self):
        str_sql = 'select codigo_ine, capital from municipal.territorio'
        connection = DbConnection()
        connection.oracle_connection(ora_connStr)
        db_cursor = connection.execute(str_sql)
        for (codigo_ine, capital) in db_cursor:
            print(codigo_ine, capital)

