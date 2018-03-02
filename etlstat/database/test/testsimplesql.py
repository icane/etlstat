import unittest
import pandas as pd

from icaneconfig import Configuration, Env
from etlstat.database.simplesql import SimpleSQL
from etlstat.database.mysql import MySQL


class TestMySQL(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        config_global = {
            'mysql': {
                'connector': 'mysql+mysqlconnector',
                'local': {
                    'user': 'root',
                    'password': 'password',
                    'host': '127.0.0.1',
                    'port': '3307'
                },
                'dev': {
                    'user': 'root',
                    'password': 'password',
                    'host': '127.0.0.1',
                    'port': '3307'
                },
                'pro': {
                    'user': 'root',
                    'password': 'password',
                    'host': '127.0.0.1',
                    'port': '3307'
                }
            },
            'store': ''
        }
        cls.config = Configuration(config_global)

        config_local = {
            'database': 'banco_series',
            'cubes': {
                'ind_gen_destino_econ': {
                    'tables': {
                        'table_agg_84': 'agg_1_px_3284',
                        'table_84': 'px_3284'
                    }
                },
                'ind_sec_div': {
                    'tables': {
                        'table_80': 'agg_1_px_3281',
                        'table_81': 'px_3281'
                    }
                }
            }
        }
        cls.config.add(config_local)
        cls.config.environment = Env.LOCAL

        user = 'root'
        password = 'password'
        host = '127.0.0.1'
        port = '3307'
        database = ''
        conn_params = [user, password, host, port, database]

        ddl = "DROP DATABASE banco_series"
        my_conn = MySQL(*conn_params)
        my_conn.execute_sql(ddl)

        ddl = "CREATE DATABASE banco_series"
        my_conn.execute_sql(ddl)

        sql = "CREATE TABLE test.inf_schema as " \
              "SELECT table_name, avg_row_length, table_collation, create_time " \
              "FROM information_schema.tables"
        my_conn.execute_sql(sql)

        ddl = "GRANT ALL PRIVILEGES ON test.* to 'test'@'%' IDENTIFIED BY 'password'"
        my_conn.execute_sql(ddl)

    def testone(self):
        SimpleSQL.drop(TestMySQL.config.cubes.ind_gen_destino_econ.tables.table_agg_84, TestMySQL.config)

        df = SimpleSQL.pull(TestMySQL.config.cubes.ind_gen_destino_econ.tables.table_agg_84, TestMySQL.config)

        self.assertEqual(df, None)

        data_columns = ['column_int', 'column_string', 'column_float']
        data_values = [[56464, 'string1', 456.956], [23, 'string2', 38.905]]
        df = pd.DataFrame(data_values, columns=data_columns)
        df.name = TestMySQL.config.cubes.ind_gen_destino_econ.tables.table_agg_84
        SimpleSQL.push(df, TestMySQL.config)

        df = pd.DataFrame()
        df.name = TestMySQL.config.cubes.ind_gen_destino_econ.tables.table_agg_84
        rst = SimpleSQL.pull(df, TestMySQL.config)

        self.assertEqual(len(rst), 2)

        SimpleSQL.drop(TestMySQL.config.cubes.ind_gen_destino_econ.tables.table_agg_84, TestMySQL.config)
