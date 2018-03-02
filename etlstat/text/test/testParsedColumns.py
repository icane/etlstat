import unittest
import pandas
from etlstat.text.parsed_columns import parsed_columns


class TestParsedColumns(unittest.TestCase):

    def testParsedColumns(self):
        df = pandas.DataFrame
        df.columns = ['Período', 'Clasificación CNAE 2009', 'Número de trabajadores', 'Índice de variación']
        parsed_columns(df)
        self.assertEqual(df.columns[0], 'periodo')
        self.assertEqual(df.columns[1], 'clasificacion_cnae_2009')
        self.assertEqual(df.columns[2], 'numero_de_trabajadores')
        self.assertEqual(df.columns[3], 'indice_de_variacion')

if __name__ == '__main__':
    unittest.main()