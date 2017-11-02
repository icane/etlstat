import mysql.connector
import cx_Oracle


class DbConnection(object):
    db_connection = None
    connected = None

    def __init__(self):
        pass

    def mysql_connection(self, host, database, user, password):
        try:
            self.db_connection = mysql.connector.connect(
                host=host,
                database=database,
                user=user,
                password=password
            )
            self.connected = True
        except mysql.connector.Error as e:
            print(e)
            self.connected = False

    def oracle_connection(self, conn_str):
        try:
            self.db_connection = cx_Oracle.connect(conn_str)
            self.connected = True
        except cx_Oracle.Error as e:
            print(e)
            self.connected = False

    def execute(self, str_sql):
        try:
            db_cursor = self.db_connection.cursor()
            db_cursor.execute(str_sql)
            return db_cursor
        except mysql.connector.Error as e:
            print(e)
        except cx_Oracle.Error as e:
            print(e)

    def __del__(self):
        try:
            self.db_connection.close()
        except mysql.connector.Error as e:
            print(e)
        except cx_Oracle.Error as e:
            print(e)
