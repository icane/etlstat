import unittest
from etlstat.extractor.extractor import *
import config_test as ct


class TestExtractor(unittest.TestCase):

    base_path = ct.GLOBAL_PATH + '/etlstat/extractor/test/extractor_test_files/'

    def testSimilar(self):
        string1 = "HOLA"
        string2 = "ADIOS"
        string3 = "HOLA MUNDO"
        self.assertGreater(ratio(string1, string3), ratio(string1, string2))

        string1 = "IPI_JUNIO_2017.txt"
        string2 = "JUNIO_IPI_2017.csv"
        string3 = "IPI_Junio_2016.csv"
        self.assertGreater(ratio(string1, string2), ratio(string1, string3))

        string1 = "EEE_2015_TERRI_IND_06.txt"
        string2 = "EEE_TERRI.csv"
        string3 = "EEE_UA.csv"
        self.assertGreater(ratio(string1, string2), ratio(string1, string3))

        string1 = 'EEE_2015_IDENT_IND_06.TXT'
        string2 = 'EEE_FINAL.csv'
        string3 = 'EEE_IDENT.csv'
        self.assertGreater(ratio(string1, string3), ratio(string1, string2))



    def testCsvIn(self):
        dir_path = self.base_path + 'csv/'
        df = csv_in(dir_path, sep=";")
        log.debug(df.keys())
        agenv_head = ['NORDEN', 'QI11', 'QI121', 'QI122', 'QI131', 'QI132', 'QI1331',
       'QI1332', 'QI1333', 'QI1334', 'QI134', 'QI135', 'QI135E', 'QI14',
       'QI15', 'QI15E', 'QI21', 'QI221', 'QI222', 'QI231', 'QI232', 'QI24',
       'QI25', 'QJ11', 'QJ12', 'QJ131', 'QJ132', 'QJ133', 'QJ134', 'QJ14',
       'QJ15', 'QJ16', 'QJ17', 'QJ18', 'QJ18E', 'QJ2', 'QJ3', 'QJ4', 'QJ5',
       'QJ6', 'QJ6E']

        aereo_head = ['NORDEN', 'AI1', 'AI11', 'AI12', 'AI2', 'AI21', 'AI22', 'AI3', 'AI4',
       'AI5', 'AJ1', 'AJ2', 'AJ3', 'AJ4', 'AJ5', 'AJ6', 'AJ7', 'AJ7E', 'AK111',
       'AK121', 'AK131', 'AK112', 'AK122', 'AK132', 'AK211', 'AK221', 'AK231',
       'AK212', 'AK222', 'AK232', 'AK213', 'AK223', 'AK233', 'AK214', 'AK224',
       'AK234', 'AM1', 'AM11', 'AM2']

        aloj_head = ['NORDEN', 'HI11', 'HI12', 'HI13', 'HI14', 'HI15', 'HI16', 'HI17',
       'HI18', 'HI19', 'HI19E', 'HI21', 'HI221', 'HI222', 'HI231', 'HI232',
       'HI24', 'HI25', 'HI25E', 'HJ1', 'HJ2', 'HJ3', 'HJ4', 'HJ5', 'HJ6',
       'HJ7', 'HJ8', 'HJ9', 'HJ10', 'HJ10E']

        aereo_type = ['int32', 'int32', 'float32', 'float32', 'float32', 'float32', 'int32',
        'int32', 'int32', 'float32', 'float32', 'float32', 'float32', 'float32', 'float32',
        'float32', 'float32', 'O', 'float32', 'float32', 'float32', 'float32', 'float32',
        'float32', 'float32', 'float32', 'float32', 'float32', 'float32', 'float32', 'float32',
        'float32', 'float32', 'float32', 'float32', 'float32', 'float32', 'float32', 'float32']

        agenv_type = ['int32', 'float32', 'float32', 'float32', 'float32', 'float32', 'float32',
        'float32', 'float32', 'float32', 'float32', 'float32', 'float32', 'float32', 'float32',
        'O', 'float32', 'float32', 'float32', 'float32', 'float32', 'float32', 'float32', 'float32',
        'float32', 'float32', 'float32', 'float32', 'float32', 'float32', 'float32', 'float32',
        'float32', 'float32', 'float32', 'float32', 'float32', 'float32', 'float32', 'float32', 'O']

        aloj_type = ['int32', 'float32', 'float32', 'float32', 'float32', 'float32', 'float32', 'float32',
        'float32', 'float32', 'O', 'float32', 'float32', 'float32', 'float32', 'float32', 'float32', 'float32',
        'O', 'float32', 'float32', 'float32', 'float32', 'float32', 'float32', 'float32', 'float32', 'float32', 'float32', 'O']



        assert (df['AEREO_SER_06_15.csv'].shape[0] == 2)
        assert (df['AEREO_SER_06_15.csv'].shape[1] == 39)
        assert (df['AGENV_SER_06_15.csv'].shape[0] == 7)
        assert (df['AGENV_SER_06_15.csv'].shape[1] == 41)
        assert (df['ALOJ_SER_06_15.csv'].shape[0] == 11)
        assert (df['ALOJ_SER_06_15.csv'].shape[1] == 30)
        assert (list(df['AEREO_SER_06_15.csv'].columns) == aereo_head)
        assert (list(df['AGENV_SER_06_15.csv'].columns) == agenv_head)
        assert (list(df['ALOJ_SER_06_15.csv'].columns) == aloj_head)
        assert (list(df['AEREO_SER_06_15.csv'].dtypes) == aereo_type)
        assert (list(df['AGENV_SER_06_15.csv'].dtypes) == agenv_type)
        assert (list(df['ALOJ_SER_06_15.csv'].dtypes) == aloj_type)

    def testPositionalIn(self):
        dir_path = self.base_path + 'positional/'
        df = positional_in(dir_path)

        post_head = ['NORDEN', 'PI1', 'PI2', 'PI2R', 'PI2N', 'PI21', 'PI31', 'PI41', 'PI42',
                     'PJ1', 'PJ11', 'PJ12', 'PJ13', 'PJ2', 'PJ21', 'PJ22', 'PJ3', 'PJ4',
                     'PJ5', 'PK11', 'PK121', 'PK122', 'PK13', 'PK14', 'PK15', 'PK16', 'PK17',
                     'PK17E', 'PK21', 'PK22', 'PK23', 'PK31', 'PK32', 'PK33', 'PK34', 'PK35',
                     'PK36', 'PK37', 'PK38', 'PK39', 'PK310', 'PK311', 'PK312', 'PK313',
                     'PK314', 'PK315', 'PK316', 'PK316E', 'PL11', 'PL12', 'PL13', 'PL14',
                     'PL21', 'PL22', 'PL23', 'PL24', 'PL31', 'PL32', 'PL33', 'PL34', 'PL41',
                     'PL42', 'PL43', 'PL44', 'PL51', 'PL52', 'PL53', 'PL54', 'PL5E', 'PM1',
                     'PM21', 'PM22', 'PM3', 'PM4']

        post_type = ['O', 'float32', 'float32', 'O', 'O', 'float32', 'float32', 'float32', 'float32', 'float32',
                     'float32','float32', 'float32', 'float32', 'float32', 'float32', 'float32', 'float32', 'float32',
                     'float32', 'float32', 'float32', 'float32', 'float32', 'float32', 'float32', 'float32', 'O',
                     'float32', 'float32', 'float32', 'float32', 'float32', 'float32', 'float32', 'float32', 'float32',
                     'float32', 'float32', 'float32', 'float32', 'float32', 'float32', 'float32', 'float32', 'float32',
                     'float32', 'O', 'float32', 'float32', 'float32', 'float32', 'float32', 'float32', 'float32',
                     'float32','float32', 'float32', 'float32','float32', 'float32', 'float32', 'float32', 'float32',
                     'float32', 'float32', 'float32', 'float32', 'O', 'float32', 'float32', 'float32', 'float32',
                     'float32']

        tec_head = ['NORDEN', 'KI111', 'KI112', 'KI113', 'KI114', 'KI115', 'KI116', 'KI121',
                    'KI122', 'KI123', 'KI124', 'KI125', 'KI126', 'KI127', 'KI128', 'KI129',
                    'I129E', 'KI131', 'KI132', 'KI133', 'KI134', 'KI134E', 'KI141', 'KI142',
                    'KI143', 'KI144', 'KI145', 'KI145E', 'KI15', 'KI16', 'KI16E', 'KI21',
                    'KI22', 'KI23', 'KI24', 'KI25', 'KI26', 'KI27', 'KI28', 'KI29', 'KI210',
                    'KI211', 'KI212', 'KI212E']

        tec_type = ['O', 'float32', 'float32', 'float32', 'float32', 'float32', 'float32', 'float32', 'float32',
                    'float32', 'float32','float32', 'float32', 'float32', 'float32', 'float32', 'O', 'float32',
                    'float32', 'float32', 'float32', 'O', 'float32', 'float32','float32', 'float32', 'float32', 'O',
                    'float32', 'float32', 'O', 'float32', 'float32', 'float32', 'float32', 'float32', 'float32',
                    'float32', 'float32', 'float32', 'float32', 'float32', 'float32', 'O']

        assert (df['AEREO_SER_06_15.TXT'].shape[0] == 2)
        assert (df['AEREO_SER_06_15.TXT'].shape[1] == 39)
        assert (df['AGENV_SER_06_15.TXT'].shape[0] == 7)
        assert (df['AGENV_SER_06_15.TXT'].shape[1] == 41)
        assert (df['ALOJ_SER_06_15.TXT'].shape[0] == 11)
        assert (df['ALOJ_SER_06_15.TXT'].shape[1] == 30)
        assert (list(df['POST_SER_06_15.TXT'].columns)) == post_head
        assert (list(df['POST_SER_06_15.TXT'].dtypes)) == post_type
        assert (list(df['TEC_SER_06_15.TXT'].columns)) == tec_head
        assert (list(df['TEC_SER_06_15.TXT'].dtypes)) == tec_type

    def testExcelIn(self):
        dir_path = self.base_path + 'excel/'
        df = excel_in(dir_path)

        assert (len(df['Datos_web.xls']) == 6)
        assert (len(df['3 Carga_web.xls']) == 6)
        assert (type(df['3 Carga_web.xls'][2]) == pandas.core.frame.DataFrame)
        assert (type(df['Datos_web.xls'][2]) == pandas.core.frame.DataFrame)
        assert (type(df['Datos_web.xls'][3]) == pandas.core.frame.DataFrame)
        assert (type(df['Datos_web.xls'][4]) == pandas.core.frame.DataFrame)
        assert (type(df['Datos_web.xls'][5]) == pandas.core.frame.DataFrame)
        assert (type(df['Datos_web.xls'][6]) == pandas.core.frame.DataFrame)


    def testXmlIn(self):
        xml_dict = xml_in(self.base_path + 'xml/')
        assert (len(xml_dict) == 11)
        root = xml_dict['Comex.kjb'].getroot()
        assert (root.tag == 'job')
        root = xml_dict['Ec_SE_IEFAZ.ktr'].getroot()
        assert (root.tag == 'transformation')

    def testSqlIn(self):
        sql_data = sql_in(self.base_path + 'sql/')
        self.assertEqual(len(sql_data['afiliados']), 1582)
        self.assertEqual(len(sql_data['contratos']), 3023)











