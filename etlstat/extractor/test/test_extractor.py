# -*- coding: utf-8 -*-

"""Unit tests for etlstat."""

import os
import unittest
import pandas as pd
import etlstat.extractor.extractor as extractor


class TestExtractor(unittest.TestCase):
    """Unit tests for px."""

    def setUp(self):
        """Set initialization variables."""
        self.base_path = os.path.dirname(os.path.abspath(__file__)) + '/data'

    def test_csv(self):
        """Should massively read CSV files.

        Should read every csv file in a directory and generate a
        dict with csv names as keys and dataframes as values.

        """
        dir_path = self.base_path + '/csv/'
        data = extractor.csv(dir_path, sep=";")
        agenv_head = ['NORDEN', 'QI11', 'QI121', 'QI122', 'QI131', 'QI132',
                      'QI1331', 'QI1332', 'QI1333', 'QI1334', 'QI134', 'QI135',
                      'QI135E', 'QI14', 'QI15', 'QI15E', 'QI21', 'QI221',
                      'QI222', 'QI231', 'QI232', 'QI24', 'QI25', 'QJ11',
                      'QJ12', 'QJ131', 'QJ132', 'QJ133', 'QJ134', 'QJ14',
                      'QJ15', 'QJ16', 'QJ17', 'QJ18', 'QJ18E', 'QJ2', 'QJ3',
                      'QJ4', 'QJ5', 'QJ6', 'QJ6E']

        aereo_head = ['NORDEN', 'AI1', 'AI11', 'AI12', 'AI2', 'AI21', 'AI22',
                      'AI3', 'AI4', 'AI5', 'AJ1', 'AJ2', 'AJ3', 'AJ4', 'AJ5',
                      'AJ6', 'AJ7', 'AJ7E', 'AK111', 'AK121', 'AK131',
                      'AK112', 'AK122', 'AK132', 'AK211', 'AK221', 'AK231',
                      'AK212', 'AK222', 'AK232', 'AK213', 'AK223', 'AK233',
                      'AK214', 'AK224', 'AK234', 'AM1', 'AM11', 'AM2']

        aloj_head = ['NORDEN', 'HI11', 'HI12', 'HI13', 'HI14', 'HI15', 'HI16',
                     'HI17', 'HI18', 'HI19', 'HI19E', 'HI21', 'HI221', 'HI222',
                     'HI231', 'HI232', 'HI24', 'HI25', 'HI25E', 'HJ1', 'HJ2',
                     'HJ3', 'HJ4', 'HJ5', 'HJ6', 'HJ7', 'HJ8', 'HJ9', 'HJ10',
                     'HJ10E']

        self.assertEqual(data['AEREO_SER_06_15.csv'].shape[0], 2)
        self.assertEqual(data['AEREO_SER_06_15.csv'].shape[1], 39)
        self.assertEqual(data['AGENV_SER_06_15.csv'].shape[0], 7)
        self.assertEqual(data['AGENV_SER_06_15.csv'].shape[1], 41)
        self.assertEqual(data['ALOJ_SER_06_15.csv'].shape[0], 11)
        self.assertEqual(data['ALOJ_SER_06_15.csv'].shape[1], 30)
        self.assertEqual(list(data['AEREO_SER_06_15.csv'].columns),
                         aereo_head)
        self.assertEqual(list(data['AGENV_SER_06_15.csv'].columns),
                         agenv_head)
        self.assertEqual(list(data['ALOJ_SER_06_15.csv'].columns),
                         aloj_head)

    def test_txt(self):
        """Should massively read positional text files from a directory.

        Should read positional txt files in a directory with their formats
        in csv and return a dict.

        """
        dir_path = self.base_path + '/positional/'
        data = extractor.txt(dir_path, format_path=dir_path + 'format/')

        post_head = ['NORDEN', 'PI1', 'PI2', 'PI2R', 'PI2N', 'PI21', 'PI31',
                     'PI41', 'PI42', 'PJ1', 'PJ11', 'PJ12', 'PJ13', 'PJ2',
                     'PJ21', 'PJ22', 'PJ3', 'PJ4', 'PJ5', 'PK11', 'PK121',
                     'PK122', 'PK13', 'PK14', 'PK15', 'PK16', 'PK17', 'PK17E',
                     'PK21', 'PK22', 'PK23', 'PK31', 'PK32', 'PK33', 'PK34',
                     'PK35', 'PK36', 'PK37', 'PK38', 'PK39', 'PK310', 'PK311',
                     'PK312', 'PK313', 'PK314', 'PK315', 'PK316', 'PK316E',
                     'PL11', 'PL12', 'PL13', 'PL14', 'PL21', 'PL22', 'PL23',
                     'PL24', 'PL31', 'PL32', 'PL33', 'PL34', 'PL41', 'PL42',
                     'PL43', 'PL44', 'PL51', 'PL52', 'PL53', 'PL54', 'PL5E',
                     'PM1', 'PM21', 'PM22', 'PM3', 'PM4']

        post_type = ['O', 'float32', 'float32', 'O', 'O', 'float32', 'float32',
                     'float32', 'float32', 'float32', 'float32', 'float32',
                     'float32', 'float32', 'float32', 'float32', 'float32',
                     'float32', 'float32', 'float32', 'float32', 'float32',
                     'float32', 'float32', 'float32', 'float32', 'float32',
                     'O', 'float32', 'float32', 'float32', 'float32',
                     'float32', 'float32', 'float32', 'float32', 'float32',
                     'float32', 'float32', 'float32', 'float32', 'float32',
                     'float32', 'float32', 'float32', 'float32', 'float32',
                     'O', 'float32', 'float32', 'float32', 'float32',
                     'float32', 'float32', 'float32', 'float32', 'float32',
                     'float32', 'float32', 'float32', 'float32', 'float32',
                     'float32', 'float32', 'float32', 'float32', 'float32',
                     'float32', 'O', 'float32', 'float32', 'float32',
                     'float32', 'float32']

        tec_head = ['NORDEN', 'KI111', 'KI112', 'KI113', 'KI114', 'KI115',
                    'KI116', 'KI121', 'KI122', 'KI123', 'KI124', 'KI125',
                    'KI126', 'KI127', 'KI128', 'KI129', 'I129E', 'KI131',
                    'KI132', 'KI133', 'KI134', 'KI134E', 'KI141', 'KI142',
                    'KI143', 'KI144', 'KI145', 'KI145E', 'KI15', 'KI16',
                    'KI16E', 'KI21', 'KI22', 'KI23', 'KI24', 'KI25', 'KI26',
                    'KI27', 'KI28', 'KI29', 'KI210', 'KI211', 'KI212',
                    'KI212E']

        tec_type = ['O', 'float32', 'float32', 'float32', 'float32', 'float32',
                    'float32', 'float32', 'float32', 'float32', 'float32',
                    'float32', 'float32', 'float32', 'float32', 'float32',
                    'O', 'float32', 'float32', 'float32', 'float32', 'O',
                    'float32', 'float32', 'float32', 'float32', 'float32', 'O',
                    'float32', 'float32', 'O', 'float32', 'float32', 'float32',
                    'float32', 'float32', 'float32', 'float32', 'float32',
                    'float32', 'float32', 'float32', 'float32', 'O']

        self.assertEqual(data['AEREO_SER_06_15.TXT'].shape[0], 2)
        self.assertEqual(data['AEREO_SER_06_15.TXT'].shape[1], 39)
        self.assertEqual(data['AGENV_SER_06_15.TXT'].shape[0], 7)
        self.assertEqual(data['AGENV_SER_06_15.TXT'].shape[1], 41)
        self.assertEqual(data['ALOJ_SER_06_15.TXT'].shape[0], 11)
        self.assertEqual(data['ALOJ_SER_06_15.TXT'].shape[1], 30)
        self.assertEqual(list(data['POST_SER_06_15.TXT'].columns), post_head)
        self.assertEqual(list(data['POST_SER_06_15.TXT'].dtypes), post_type)
        self.assertEqual(list(data['TEC_SER_06_15.TXT'].columns), tec_head)
        self.assertEqual(list(data['TEC_SER_06_15.TXT'].dtypes), tec_type)

    def test_xls(self):
        """Should massively read XLS files from a directory.

        Should read XLS files in a directory  and generate a dict with file and
        sheet names as keys and dataframes with the data as values.

        """
        dir_path = self.base_path + '/excel/'
        data = extractor.xls(dir_path)

        self.assertEqual(len(data['prueba_excel.xls']), 4)
        self.assertEqual(len(data['excel_prueba.xls']), 4)
        self.assertEqual(type(data['prueba_excel.xls']['Hoja1']),
                         pd.core.frame.DataFrame)
        self.assertEqual(type(data['prueba_excel.xls']['Hoja3']),
                         pd.core.frame.DataFrame)
        self.assertEqual(type(data['excel_prueba.xls']['Hoja1']),
                         pd.core.frame.DataFrame)
        self.assertEqual(type(data['excel_prueba.xls']['Hoja2']),
                         pd.core.frame.DataFrame)

    def test_xml(self):
        """Should massively read XML files from a directory.

        Should massively read XML files in a directory and generate a dict with
        the file names as keys and the etree objects as values.

        """
        xml_dict = extractor.xml(self.base_path + '/xml/')
        self.assertEqual(len(xml_dict), 11)
        root = xml_dict['Comex.kjb'].getroot()
        self.assertEqual(root.tag, 'job')
        root = xml_dict['Ec_SE_IEFAZ.ktr'].getroot()
        self.assertEqual(root.tag, 'transformation')

    def test_sql(self):
        """Should massively read SQL files from a directory.

        Should read every SQL file in a given directory and generate a dict
        with the file names as keys and the content of the files as values.

        """
        sql_data = extractor.sql(self.base_path + '/sql/')
        self.assertEqual(len(sql_data['afiliados']), 1582)
        self.assertEqual(len(sql_data['contratos']), 3023)

    def test_px(self):
        """Should massively read PC-Axis files in a directory.

        Should read every px file from the URIs listed in an input CSV file and
        generate a dict with the px name as key and a dataframe with its data
        as value.

        """
        pc_axis_data = extractor.px(self.base_path + '/px/pcaxis_urls.csv')
        self.assertEqual(len(pc_axis_data), 3)
        self.assertEqual(type(pc_axis_data['px_01001']),
                         pd.core.frame.DataFrame)
        self.assertEqual(
            type(pc_axis_data['px_01002']), pd.core.frame.DataFrame)
        self.assertEqual(
            type(pc_axis_data['px_01006']), pd.core.frame.DataFrame)


if __name__ == '__main__':
    unittest.main()
