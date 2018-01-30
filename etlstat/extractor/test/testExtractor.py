import unittest
from etlstat.extractor.extractor import *
import config_test as ct


class TestExtractor(unittest.TestCase):

    base_path = ct.GLOBAL_PATH + '/etlstat/extractor/test/'

    def testSimilar(self):
        string1 = "HOLA"
        string2 = "ADIOS"
        string3 = "HOLA MUNDO"
        self.assertGreater(similar(string1, string3), similar(string1, string2))

        string1 = "IPI_JUNIO_2017.txt"
        string2 = "JUNIO_IPI_2017.csv"
        string3 = "IPI_Junio_2016.csv"
        self.assertGreater(similar(string1, string2), similar(string1, string3))

        string1 = "EEE_2015_TERRI_IND_06.txt"
        string2 = "EEE_TERRI.csv"
        string3 = "EEE_UA.csv"
        self.assertGreater(similar(string1, string2), similar(string1, string3))

        string1 = 'EEE_2015_IDENT_IND_06.TXT'
        string2 = 'EEE_FINAL.csv'
        string3 = 'EEE_IDENT.csv'
        self.assertGreater(similar(string1, string3), similar(string1, string2))

    def testExcelProcessing(self):
        dir_path = self.base_path + 'xls/'
        excel = 'CA_MT_327.xls'
        dict = excel_processing(dir_path, excel)
        assert (dict['Hoja1'] == {'skip_rows': 3, 'footer_rows': 5})
        assert (dict['Hoja2'] == {'skip_rows': 0, 'footer_rows': 0})
        assert (dict['Hoja3'] == {'skip_rows': 0, 'footer_rows': 0})
        assert (len(dict) == 3)

        excel = 'CA_MT_328.xls'
        dict = excel_processing(dir_path, excel)
        assert (dict['Hoja1'] == {'skip_rows': 3, 'footer_rows': 4})
        assert (dict['Hoja2'] == {'skip_rows': 0, 'footer_rows': 0})
        assert (dict['Hoja3'] == {'skip_rows': 0, 'footer_rows': 0})
        assert (len(dict) == 3)

        excel = 'CA_MT_329.xls'
        dict = excel_processing(dir_path, excel)
        assert (dict['Hoja1'] == {'skip_rows': 7, 'footer_rows': 22})
        assert (dict['Hoja2'] == {'skip_rows': 0, 'footer_rows': 0})
        assert (dict['Hoja3'] == {'skip_rows': 0, 'footer_rows': 0})
        assert (len(dict) == 3)

        excel = 'CA_SP_063.xls'
        dict = excel_processing(dir_path, excel)
        assert (dict['Hoja5'] == {'skip_rows': 3, 'footer_rows': 2})
        assert (dict['Comprobación'] == {'skip_rows': 3, 'footer_rows': 22})
        assert (len(dict) == 2)

        excel = 'CA_MT_072.xls'
        dict = excel_processing(dir_path, excel)
        assert (dict['regulacion empleo'] == {'skip_rows': 4, 'footer_rows': 3})
        assert (dict['Comprobación'] == {'skip_rows': 9, 'footer_rows': 437})
        assert (len(dict) == 2)

        excel = 'CA_MT_106.xls'
        dict = excel_processing(dir_path, excel)
        assert (dict['fondo garantia salarial'] == {'skip_rows': 3, 'footer_rows': 3})
        assert (dict['Comprobacion'] != {'skip_rows': 9, 'footer_rows': 175})
        assert (len(dict) == 2)

        excel = 'CA_MT_335.xls'
        dict = excel_processing(dir_path, excel)
        assert (dict['Expedientes'] == {'footer_rows': 3, 'skip_rows': 3})
        assert (dict['comprobacion'] == {'skip_rows': 1, 'footer_rows': 0})
        assert (len(dict) == 2)

        excel = 'CA_EL_009.xls'
        dict = excel_processing(dir_path, excel)
        assert (dict['2015'] == {'footer_rows': 15, 'skip_rows': 3})
        assert (len(dict) == 13)

        excel = 'CA_EL_011.xls'
        dict = excel_processing(dir_path, excel)
        assert (dict['Hoja1'] == {'skip_rows': 3, 'footer_rows': 5})
        assert (len(dict) == 3)

        excel = 'CA_DE_02.xls'
        dict = excel_processing(dir_path, excel)
        assert (dict['Hoja1'] == {'footer_rows': 3, 'skip_rows': 3})
        assert (len(dict) == 3)

        excel = 'CA_CE_008.xls'
        dict = excel_processing(dir_path, excel)
        assert (dict['Hoja1'] == {'skip_rows': 5, 'footer_rows': 11})
        assert (len(dict) == 1)

    # TODO: testExcelProcessing
    """
    def testExcelIn(self):

        dir_path = self.base_path + 'xls/'

        df_dict = excel_in(dir_path, 'Hoja1', pattern="CA_*.xls")
        print(df_dict.keys())
        log.debug(df_dict.keys())
        assert (len(df_dict) == 3)
        assert (isinstance(df_dict['CA_MT_327.xls'], pd.DataFrame))
        assert (isinstance(df_dict['CA_MT_328.xls'], pd.DataFrame))
        assert (isinstance(df_dict['CA_MT_329.xls'], pd.DataFrame))
        assert (df_dict['CA_MT_327.xls'].shape[0] == 108)
        assert (df_dict['CA_MT_327.xls'].shape[1] == 24)
        assert (df_dict['CA_MT_328.xls'].shape[0] == 108)
        assert (df_dict['CA_MT_328.xls'].shape[1] == 24)
        assert (df_dict['CA_MT_329.xls'].shape[0] == 108)
        assert (df_dict['CA_MT_329.xls'].shape[1] == 70)
        df_dict = excel_in(dir_path, 'Hoja5')
        assert ('CA_SP_063.xls' in df_dict)
        assert (isinstance(df_dict['CA_SP_063.xls'], pd.DataFrame))
        assert (len(df_dict['CA_SP_063.xls']) == 19)
        self.assertRaises(KeyError, excel_in, dir_path, 'regulacion empleo')
        df_dict = excel_in(dir_path, 'Hoja1')
        assert ('CA_DE_01.xls' in df_dict)
        assert ('CA_DE_02.xls' in df_dict)
        assert (isinstance(df_dict['CA_DE_01.xls'], pd.DataFrame))
        assert (isinstance(df_dict['CA_DE_02.xls'], pd.DataFrame))
        assert (len(df_dict['CA_DE_01.xls']) == 67)
        assert (len(df_dict['CA_DE_02.xls']) == 67)
    """

    def testCsvIn(self):
        dir_path = self.base_path + 'csv/'
        df = csv_in(dir_path, sep=";")
        log.debug(df.keys())
        assert (len(df) == 2)
        assert (df['AUDIOV_SER_06_15.csv'].shape[0] == 22)
        assert (df['AUDIOV_SER_06_15.csv'].shape[1] == 24)
        assert (df['POST_SER_06_15.csv'].shape[0] == 12)
        assert (df['POST_SER_06_15.csv'].shape[1] == 74)

    def testPositionalIn(self):
        dir_path = self.base_path + 'txt/'
        dataf = positional_in(dir_path)
        assert (len(dataf) == 2)
        assert (dataf['POST_SER_06_15.TXT'].shape[0] == 12)
        assert (dataf['POST_SER_06_15.TXT'].shape[1] == 74)
        assert (dataf['AUDIOV_SER_06_15.TXT'].shape[0] == 22)
        assert (dataf['AUDIOV_SER_06_15.TXT'].shape[1] == 24)

    def testPcAxisIn(self):
        pc_axis_dict = pc_axis_in(self.base_path + 'pcaxis_urls.csv')
        assert (isinstance(pc_axis_dict['px_01001'], pd.DataFrame))
        assert (isinstance(pc_axis_dict['px_01002'], pd.DataFrame))
        assert (isinstance(pc_axis_dict['px_01006'], pd.DataFrame))

    def testXmlIn(self):
        xml_dict = xml_in(self.base_path + 'xml/')
        assert (len(xml_dict) == 2)
        root = xml_dict['Comex.kjb'].getroot()
        assert (root.tag == 'job')
        root = xml_dict['Ec_SE_IEFAZ.ktr'].getroot()
        assert (root.tag == 'transformation')

    def testSqlIn(self):
        sql_data = sql_in(self.base_path + 'sql/')
        self.assertEqual(len(sql_data['afiliados']), 1582)
        self.assertEqual(len(sql_data['contratos']), 3023)
        self.assertEqual(len(sql_data['ratios']), 13616)
