import unittest

import numpy as np
import pandas as pd
import sqlalchemy as sq

from etlstat.database.mysql import MySQL


class TestMySql(unittest.TestCase):

    def setUp(self):

        conn_string = "mysql+mysqlconnector://root:password@127.0.0.1:3307/"
        sql = "CREATE DATABASE IF NOT EXISTS mysql_test"

        engine = sq.create_engine(conn_string)
        engine.execute(sql)

    def test_create_table(self):

        conn_string = "mysql+mysqlconnector://root:password@127.0.0.1:3307/mysql_test"

        # Create dataframe
        data_columns = ['column_int', 'column_string', 'column_float']
        table = pd.DataFrame(columns=data_columns)

        # Assign columns data types
        table['column_int'] = table['column_int'].astype(int)
        table['column_string'] = table['column_string'].astype(str)
        table['column_float'] = table['column_float'].astype(float)

        # Rename dataframe
        table.name = "table_test"

        MySQL.create(table, conn_string)

if __name__ == '__main__':
    unittest.main()