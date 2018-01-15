import unittest
import webbrowser

from odo import odo, drop
import numpy as np
import pandas as pd
import icaneconfig as ic

from config_test import *
from config_user import *
from etlstat.database.mysql import MySQL

CONFIG_GLOBAL = {
    'metadata': {
        'local': {
            'user': '',
            'password': ''
        }
    },
    'mysql': {
        'connector': 'mysql+mysqlconnector',
        'local': {
            'user': MYSQL_USER_LOCAL,
            'password': MYSQL_PASSWD_LOCAL,
            'host': '127.0.0.1',
            'port': MYSQL_PORT_LOCAL
        }
    },
    'store': '',
    'root_dir': GLOBAL_PATH,
    'test': {
        'database': 'test',
        'table': 'prueba_mysql'
    }
}

config = ic.Configuration(CONFIG_GLOBAL)
config.environment = ic.Env.LOCAL


class TestMySql(unittest.TestCase):

    def test_check(self):
        # (*) Añadir database al conn_string de Configuration??
        conn = '{0}{1}'.format(config.store.conn_string, config.test.database)

        if not MySQL.check_for_table(config.test.table, conn):
            # Create dataframe
            data_columns = ['column_one', 'car_name', 'minutes_spent']
            table = pd.DataFrame(columns=data_columns)
            # Assign columns data types
            table['column_one'] = table['column_one'].astype(int)
            table['car_name'] = table['car_name'].astype(str)
            table['minutes_spent'] = table['minutes_spent'].astype(float)
            # Rename dataframe
            table.name = config.test.table

            # Create database table
            MySQL.create(table)

        self.assertTrue(MySQL.check_for_table(config.test.table))

    def test_select(self):
        conn = '{0}{1}'.format(config.store.conn_string, config.test.database)
        conn_odo = '{0}{1}::{2}'.format(config.store.conn_string,
                                        config.test.database, '01002')

        path = config.root_dir + "/etlstat/database/test/"

        if MySQL.check_for_table('01002', conn):
            MySQL.delete('01002')

        if not MySQL.check_for_table('01002'):
            df = pd.read_csv(path + '01002.csv', sep=';')
            odo(df, conn_odo)

        data = MySQL.select('01002')

        if data is not None:
            odo(data, path + '01002_selected.csv')

    def test_insert(self):
        conn = '{0}{1}'.format(config.store.conn_string, config.test.database)
        conn_odo = '{0}{1}::{2}'.format(config.store.conn_string,
                                        config.test.database, '01002')

        path = config.root_dir + "/etlstat/database/test/"

        if MySQL.check_for_table('01002', conn):
            drop(conn_odo)

        table = pd.read_csv(path + '01002.csv', sep=';')
        table.name = '01002'

        assert(MySQL.insert(table) == 21)

        if MySQL.check_for_table('01002', conn):
            MySQL.delete('01002')

        assert (MySQL.insert(table, rows=[0, 1]) == 2)

        if MySQL.check_for_table('01002', conn):
            MySQL.delete('01002')

        assert(MySQL.bulk_insert(table) == 21)

        table.loc[0, 'valor'] = 4.0

        # print(table)
        assert(MySQL.update(table, index=['id']) == 21)

        # uri_tag = "labour-market-collective-labor-agreements-year-economic-effects"
        # url = "http://icane.es/data/" + uri_tag
        # webbrowser.open(url, new=2)

    def test_update(self):
        conn = '{0}{1}'.format(config.store.conn_string, config.test.database)
        path = config.root_dir + "/etlstat/database/test/"
        print(conn)
        df = pd.read_csv(path + '01001.csv', sep=';')
        df.name='01001'
        MySQL.insert(df, conn)
        conn = '{0}{1}'.format(config.store.conn_string, 'banco')
        MySQL.insert(df, conn)

if __name__ == '__main__':
    unittest.main()