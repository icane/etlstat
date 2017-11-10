# coding: utf-8
"""
    update_date()
    Test.
    
    Date:
        27/10/2017

    Author:
        goi9999

    Version:
        1.0 (pre-launch)

    Notes:


"""
import unittest

from config_user import *
import icaneconfig as ic
from etlstat.metadata.update_date import *

CONFIG_GLOBAL = {
    'metadata': {
        'local': {
            'user': METADATA_USER_LOCAL,
            'password': METADATA_PASSWD_LOCAL
        },
        'dev': {
            'user': METADATA_USER_DEV,
            'password': METADATA_PASSWD_DEV
        },
        'pro': {
            'user': METADATA_USER_PRO,
            'password': METADATA_PASSWD_PRO
        }
    },
    'mysql': {
        'connector': 'mysql+mysqlconnector',
        'local': {
            'user': MYSQL_USER_DEV,
            'password': MYSQL_PASSWD_LOCAL,
            'host': '127.0.0.1',
            'port': '3333'
        },
        'dev': {
            'user': MYSQL_USER_DEV,
            'password': MYSQL_PASSWD_DEV,
            'host': 'sicanedev01.intranet.gobcantabria.es',
            'port': '3306'
        },
        'pro': {
            'user': MYSQL_USER_PRO,
            'password': MYSQL_PASSWD_PRO,
            'host': 'sicanedatamart01.intranet.gobcantabria.es',
            'port': '3306'
        }
    },
    'oracle': {
        'connector': 'oracle+cx_oracle',
        'local': {
            'user': '',
            'password': '',
            'host': '',
            'port': '1521'
        },
        'dev': {
            'password': '',
            'user': '',
            'host': '',
            'port': '1521'
        },
        'pro': {
            'user': ORACLE_USER_PRO,
            'password': ORACLE_PASSWD_PRO,
            'host': '',
            'port': '1521',
            'service_name': ''
        }
    },
    'store': '',
    'root_dir': '/var/git/python/icane_etl/'   # This is your Project Root
}

class TestUpdate_date(unittest.TestCase):

    def setUp(self):
        self.config = ic.Configuration(CONFIG_GLOBAL)
        self.config.environment = ic.Env.LOCAL

    def test_errors(self):
        # Test 1 : Sin configuración
        self.assertFalse(update_date(self.config))

        # Con configuración mínima
        self.config.metadata.mes = 10
        self.config.metadata.anno = 2017
        self.config.metadata.uri_tag = 'cadastre-other-stats-constructions-antiquity-municipality-monthly'
        assert(update_date(self.config))

        # Test 2 : Errores en mes
        self.config.metadata.mes = 133
        self.assertFalse(update_date(self.config))
        self.config.metadata.mes = -24
        self.assertFalse(update_date(self.config))
        self.config.metadata.mes = 0
        self.assertFalse(update_date(self.config))
        self.config.metadata.mes = ''
        self.assertFalse(update_date(self.config))
        self.config.metadata.mes = '?\('
        self.assertFalse(update_date(self.config))
        #del(self.icane_config.metadata.mes)
        #delattr(self.icane_config.metadata, "mes")
        #self.assertFalse(update_date(self.icane_config))

        self.config.metadata.mes = 10

        # Test 3 : Errores en anno
        self.config.metadata.anno = 20456
        self.assertFalse(update_date(self.config))
        self.config.metadata.anno = -203
        self.assertFalse(update_date(self.config))
        self.config.metadata.anno = 0
        self.assertFalse(update_date(self.config))
        self.config.metadata.anno = ''
        self.assertFalse(update_date(self.config))
        self.config.metadata.anno = '$%?'
        self.assertFalse(update_date(self.config))
        # del(self.icane_config.metadata.anno)
        # self.assertFalse(update_date(self.icane_config))

        self.config.metadata.anno = 2017

        # Test 4 : Errores en uri_tag (No genera log.error)
        self.config.metadata.uri_tag = 'cada-other-stats-constructions-antiqui'
        self.assertFalse(update_date(self.config))
        self.config.metadata.uri_tag = ''
        self.assertFalse(update_date(self.config))
        self.config.metadata.uri_tag = '$ª:'
        self.assertFalse(update_date(self.config))
        self.config.metadata.uri_tag = 'cadastre-other-stats-constructions-antiquity-municipality-monthly'
        assert(update_date(self.config))

        # Test 5 : Errores en category_tag (No genera log.error)
        self.config.metadata.category_tag = 'muni'
        self.assertFalse(update_date(self.config))
        self.config.metadata.category_tag = ''
        self.assertFalse(update_date(self.config))
        self.config.metadata.category_tag = '&/·$%'
        self.assertFalse(update_date(self.config))
        self.config.metadata.category_tag = 'municipal-data'
        assert(update_date(self.config))

        # Test 6 : Errores en subsection_tag
        self.config.metadata.subsection_tag = 'muni'
        self.assertFalse(update_date(self.config))
        self.config.metadata.subsection_tag = ''
        self.assertFalse(update_date(self.config))
        self.config.metadata.subsection_tag = '&/·$%'
        self.assertFalse(update_date(self.config))
        self.config.metadata.subsection_tag = ['construction-housing','78']
        self.assertFalse(update_date(self.config))
        self.config.metadata.subsection_tag = ['construction-housing','territory']
        assert(update_date(self.config))

        del self.config

    def test_catas_otr(self):
        dict = {
            'metadata': {
                'anno': 2017,
                'mes': 10,
                'uri_tag': ['cadastre-other-stats-constructions-antiquity-municipality-monthly',
                            'cadastre-other-stats-categories-municipality-period-monthly',
                            'cadastre-other-stats-distribution-uses-municipality-period-monthly',
                            'cadastre-other-stats-vacant-land-municipality-period-monthly'],
                'category_tag': 'municipal-data',
                'subsection_tag': ['construction-housing','territory']
            }
        }

        self.config.add(dict)

        assert(update_date(self.config) == True)

        del self.config

    def test_re(self):
        dict = {
            'metadata': {
                'anno': 2017,
                'mes': 10,
                'uri_tag': ['regulation-employment']
            }
        }

        self.config.add(dict)

        # assert(update_date(self.icane_config) == True)

        del self.config

    def test_ph(self):
        dict = {
            'metadata': {
                'anno': 2017,
                'mes': 10,
                'uri_tag': ['hydrocarbons-prices-series'],
                'category_tag': 'regional-data',
                'subsection_tag': ['industry-energy', 'prices']
            }
        }

        self.config.add(dict)

        assert(update_date(self.config) == True)

        del self.config

    def test_sm(self):
        dict = {
            'metadata': {
                'anno': 2017,
                'mes': 10,
                'uri_tag': ['mercantile-societies-created-increasing-capital-1999',
                            'mercantile-societies-dissolved-cause-1999',
                            'mercantile-societies-disbursing-capital-calls-1999',
                            'mercantile-societies-reducing-capital-1999'],
                'category_tag': 'regional-data',
                'subsection_tag': ['companies-establishments']
            }
        }

        self.config.add(dict)

        assert(update_date(self.config) == True)

        del self.config

if __name__ == '__main__':
    unittest.main()