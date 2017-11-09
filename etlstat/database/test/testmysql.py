import unittest

from odo import odo
import numpy as np
import pandas as pd
import icaneconfig as ic

from config_test import *
# from config_user import *
from etlstat.database.mysql import MySQL

MYSQL_USER_LOCAL = 'ign'
MYSQL_PASSWD_LOCAL = 'icane'
MYSQL_PORT_LOCAL = '3333'

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
        conn = '{0}{1}'.format(config.store.conn_string, config.test.database)

        if(not MySQL.check_for_table(config.test.table, conn)):
            data_columns = ['column_one', 'car_name', 'minutes_spent']
            table = pd.DataFrame(columns=data_columns)
            table['column_one'] = table['column_one'].astype(int)
            table['car_name'] = table['car_name'].astype(str)
            table['minutes_spent'] = table['minutes_spent'].astype(float)
            # Probar el método de pandas para asignar el tipo automáticamente
            # table = table.infer_objects()
            table.name = config.test.table

            MySQL.create(table)

        self.assertTrue(MySQL.check_for_table(config.test.table))

    def test_select(self):
        conn = '{0}{1}'.format(config.store.conn_string, config.test.database)
        conn_odo = '{0}{1}::{2}'.format(config.store.conn_string, config.test.database,
                                        '01002')

        path = config.root_dir + "/etlstat/database/test/"

        if(not MySQL.check_for_table('01002', conn)):
            df = pd.read_csv(path + '01002.csv', sep=';')
            odo(df, conn_odo)

        data = MySQL.select('01002')

        odo(data, path + '01002_selected.csv')

if __name__ == '__main__':
    unittest.main()