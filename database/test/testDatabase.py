import unittest
from utils.database.Database import Database


class TestDatabase(unittest.TestCase):

    def testMySqlConnectionOk(self):
        engine_type = 'mysql+mysqlconnector'
        host = 'sicanedev01.intranet.gobcantabria.es'
        port = '3306'
        database = 'test'
        user = 'test'
        password = 'test'

        db = Database(
            engine_type,
            host,
            port,
            database,
            user,
            password
        )

        self.assertTrue(db.state)
        db.close()
        self.assertFalse(db.state)

    def testMySqlConnectionFail(self):
        engine_type = 'mysql+mysqlconnector'
        host = 'sicanedev01.intranet.gobcantabria.es'
        port = '3306'
        database = 'xxxx'
        user = 'test'
        password = 'test'

        db = Database(
            engine_type,
            host,
            port,
            database,
            user,
            password
        )

        self.assertFalse(db.state)

    def testOracleConnectionOk(self):
        engine_type = 'oracle+cx_oracle'
        host = 'constantino.icane.es'
        port = '1521'
        service_name = 'CONSTANT.ICANE.ES'
        user = 'test'
        password = 'test'

        db = Database(
            engine_type,
            host,
            port,
            service_name,
            user,
            password
        )

        self.assertTrue(db.state)
        db.close()
        self.assertFalse(db.state)

    def testOracleConnectionFail(self):
        engine_type = 'oracle+cx_oracle'
        host = 'constantino.icane.es'
        port = '1521'
        service_name = 'CONST'
        user = 'test'
        password = 'test'

        db = Database(
            engine_type,
            host,
            port,
            service_name,
            user,
            password
        )

        self.assertFalse(db.state)
