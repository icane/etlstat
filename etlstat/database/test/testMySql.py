import unittest

import pandas as pd
from config_user import *
from utils.database.MySql import MySql

from etlstat import log

log.basicConfig(level = log.INFO)

log = log.getLogger(__name__)

DATA_PATH = '/var/git/python/icane_etl/etlstat/database/test/'

MYSQL = {
    'SERVER'    : '127.0.0.1',
    'DATABASE'  : 'test',
    'PORT'      : '3306',
    'USER'      : MYSQL_USER,
    'PASSWORD'  : MYSQL_PASSWD
}

INFILE = {
    'PATH'      : DATA_PATH + 'test1.csv'
}

DBTABLE = {
    'TABLE_NAME': 'test'
}


class TestMySql(unittest.TestCase):

    def setUp(self):

        self.mysql = MySql(MYSQL['SERVER'],
                           MYSQL['PORT'],
                           MYSQL['DATABASE'],
                           MYSQL['USER'],
                           MYSQL['PASSWORD'])

        log.info(" Setup : OK\n")

    def test_select_all(self):

        results = [('11', 'AA', '11AA'), ('22', 'BB', '22BB'), ('33', 'CC', '33CC')]

        self.mysql.delete(DBTABLE['TABLE_NAME'])

        self.mysql.insert(DBTABLE['TABLE_NAME'], ["'11'", "'AA'", "'11AA'"])
        self.mysql.insert(DBTABLE['TABLE_NAME'], ["'22'", "'BB'", "'22BB'"])
        self.mysql.insert(DBTABLE['TABLE_NAME'], ["'33'", "'CC'", "'33CC'"])

        db_query = self.mysql.select_all(DBTABLE['TABLE_NAME'])

        i = 0
        assert db_query.rowcount == 3
        for row in db_query:
            assert row == results[i]
            i += 1

        self.mysql.delete(DBTABLE['TABLE_NAME'])

        del self.mysql

        log.info(" test_select_all : OK\n")

    def test_delete(self):

        self.mysql.insert(DBTABLE['TABLE_NAME'], ["'11'", "'AA'", "'11AA'"])

        self.mysql.delete(DBTABLE['TABLE_NAME'])

        db_query = self.mysql.select_all(DBTABLE['TABLE_NAME'])

        assert db_query.rowcount == 0

        del self.mysql

        log.info(" test_delete : OK\n")


    def test_bulk_insert(self):

        dataframe = pd.read_csv(INFILE['PATH'], header = None, sep = ';')

        self.mysql.delete(DBTABLE['TABLE_NAME'])

        self.mysql.bulk_insert(dataframe,
                               DBTABLE['TABLE_NAME'],
                               INFILE['PATH'])
        
        db_query = self.mysql.select_all(DBTABLE['TABLE_NAME'])

        assert db_query.rowcount == 24
            
        del self.mysql

        log.info(" test_bulk_insert : OK\n")

    def test_check_for_table(self):
        #TODO create table t1 automatically
        self.assertTrue(self.mysql.check_for_table('t1'))


if __name__ == '__main__':

    unittest.main()