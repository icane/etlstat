# -*- coding: utf-8 -*-

"""Unit tests for utils module."""
import os
import filecmp
import unittest
import pandas
import etlstat.text.utils as utils


class TestUtils(unittest.TestCase):
    """Testing methods for util functions."""

    def setUp(self):
        """Initialize the test variables and configuration."""
        self.current_dir = os.path.dirname(os.path.abspath(__file__))

    def test_parsed_columns(self):
        """Check column parsing has the desired effect."""
        df = pandas.DataFrame
        df.columns = ['Período',
                      'Clasificación CNAE 2009',
                      'Número de trabajadores',
                      'Índice de variación']
        utils.parse_df_columns(df)
        self.assertEqual(df.columns[0], 'periodo')
        self.assertEqual(df.columns[1], 'clasificacion_cnae_2009')
        self.assertEqual(df.columns[2], 'numero_de_trabajadores')
        self.assertEqual(df.columns[3], 'indice_de_variacion')

    def test_bulk_replace_url_in_xml(self):
        """Check a URL replacement takes place in all files in a directory."""
        utils.bulk_replace_url_in_xml(f'''{self.current_dir}/data/''')
        self.assertTrue(filecmp.cmp(f'''{self.current_dir}'''
                                    f'''/data/jobs/TODAS_Uso de Comercio '''
                                    f'''Electrónico.kjb''',
                                    f'''{self.current_dir}'''
                                    f'''/data/jobs/TODAS_Uso de Comercio '''
                                    f'''Electrónico.kjb'''))


if __name__ == '__main__':
    unittest.main()
