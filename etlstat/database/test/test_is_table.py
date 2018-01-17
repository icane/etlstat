import unittest

from icane_etl.config_global import *
from utils.database.is_table import is_table

class TestIsTable(unittest.TestCase):

    def test_is_table(self):
        """
        TODO: Modificar el valor de table por una tabla que exista en tu db local.
        """
        CONFIG_LOCAL = {
            'database': 'banco',
            'test': {
                'table': 'regulacion_empleo'
            }
        }

        config.add(CONFIG_LOCAL)

        config.environment = Env.LOCAL

        assert(is_table(config.store.conn_string + config.database + '::' + config.test.table))

        config.system = Db.ORACLE

        self.assertRaises(NotImplementedError, is_table, config.store.conn_string + config.database + '::' + config.test.table)
