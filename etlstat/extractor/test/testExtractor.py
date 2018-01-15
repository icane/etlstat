import unittest

from etlstat.extractor.extractor import *


class TestExtractor(unittest.TestCase):

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
        dir_path = '/mnt/gobcan/datos/series actualizadas/CA_MT_mercado_trabajo/Afiliados Seguridad Social/' + \
                   'Nueva tablas/Afiliados medios/'
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

        dir_path = '/mnt/gobcan/datos/series actualizadas/CA_SP_sector primario/' + \
                   'AGRICULTURA/Consejo Regulador Agricultura Ecologica (CRAE)/'

        excel = 'CA_SP_063.xls'
        dict = excel_processing(dir_path, excel)
        assert (dict['Hoja5'] == {'skip_rows': 3, 'footer_rows': 2})
        assert (dict['Comprobación'] == {'skip_rows': 3, 'footer_rows': 22})
        assert (len(dict) == 2)

        dir_path = '/mnt/gobcan/datos/series actualizadas/CA_MT_mercado_trabajo/'

        excel = 'CA_MT_072.xls'
        dict = excel_processing(dir_path, excel)
        print(dict['Comprobación'])
        assert (dict['regulacion empleo'] == {'skip_rows': 4, 'footer_rows': 3})
        # Algoritmo no pensado para hojas de comprobación
        assert (dict['Comprobación'] == {'skip_rows': 9, 'footer_rows': 435})
        assert (len(dict) == 2)

        excel = 'CA_MT_106.xls'
        dict = excel_processing(dir_path, excel)
        assert (dict['fondo garantia salarial'] == {'skip_rows': 3, 'footer_rows': 3})
        # Algoritmo no pensado para hojas de comprobación
        assert (dict['Comprobacion'] != {'skip_rows': 9, 'footer_rows': 175})
        assert (len(dict) == 2)

        excel = 'CA_MT_335.xls'
        dict = excel_processing(dir_path, excel)
        assert (dict['Expedientes'] == {'footer_rows': 3, 'skip_rows': 3})
        assert (dict['comprobacion'] == {'skip_rows': 1, 'footer_rows': 0})
        assert (len(dict) == 2)

        dir_path = '/mnt/gobcan/datos/series actualizadas/CA_EL_Elecciones/'

        excel = 'CA_EL_009.xls'
        dict = excel_processing(dir_path, excel)
        assert (dict['2015'] == {'footer_rows': 15, 'skip_rows': 3})
        assert (len(dict) == 13)

        excel = 'CA_EL_011.xls'
        dict = excel_processing(dir_path, excel)
        assert (dict['Hoja1'] == {'skip_rows': 3, 'footer_rows': 5})
        assert (len(dict) == 3)

        dir_path = '/mnt/gobcan/datos/series actualizadas/CA_DE_Deportes/'

        excel = 'CA_DE_02.xls'
        dict = excel_processing(dir_path, excel)
        assert (dict['Hoja1'] == {'footer_rows': 3, 'skip_rows': 3})
        assert (len(dict) == 3)

        dir_path = '/mnt/gobcan/datos/series actualizadas/CA_CE_comercio_exterior/INVERSIONES_EXTERIORES/'

        excel = 'CA_CE_008.xls'
        dict = excel_processing(dir_path, excel)
        assert (dict['Hoja1'] == {'skip_rows': 5, 'footer_rows': 11})
        assert (len(dict) == 1)

    def testExcelIn(self):
        dir_path = '/mnt/gobcan/datos/series actualizadas/CA_MT_mercado_trabajo/' + \
                   'Afiliados Seguridad Social/Nueva tablas/Afiliados medios/'

        df_dict = excel_in(dir_path, 'Hoja1')
        assert ('CA_MT_327.xls' in df_dict)
        assert ('CA_MT_328.xls' in df_dict)
        assert ('CA_MT_329.xls' in df_dict)
        assert (len(df_dict) == 3)
        assert (isinstance(df_dict['CA_MT_327.xls'], pd.DataFrame))
        assert (isinstance(df_dict['CA_MT_328.xls'], pd.DataFrame))
        assert (isinstance(df_dict['CA_MT_329.xls'], pd.DataFrame))
        assert (len(df_dict['CA_MT_327.xls']) == 104)
        assert (len(df_dict['CA_MT_328.xls']) == 104)
        assert (len(df_dict['CA_MT_329.xls']) == 104)

        dir_path = '/mnt/gobcan/datos/series actualizadas/CA_SP_sector primario/AGRICULTURA/' + \
                   'Consejo Regulador Agricultura Ecologica (CRAE)/'
        df_dict = excel_in(dir_path, 'Hoja5')
        assert ('CA_SP_063.xls' in df_dict)
        assert (isinstance(df_dict['CA_SP_063.xls'], pd.DataFrame))
        assert (len(df_dict['CA_SP_063.xls']) == 19)

        dir_path = '/mnt/gobcan/datos/series actualizadas/CA_MT_mercado_trabajo/'
        self.assertRaises(KeyError, excel_in, dir_path, 'regulacion empleo')

        dir_path = '/mnt/gobcan/datos/series actualizadas/CA_DE_Deportes/'
        df_dict = excel_in(dir_path, 'Hoja1')
        assert ('CA_DE_01.xls' in df_dict)
        assert ('CA_DE_02.xls' in df_dict)
        assert (isinstance(df_dict['CA_DE_01.xls'], pd.DataFrame))
        assert (isinstance(df_dict['CA_DE_02.xls'], pd.DataFrame))
        assert (len(df_dict['CA_DE_01.xls']) == 67)
        assert (len(df_dict['CA_DE_02.xls']) == 67)

    def testPcAxisIn(self):

        pc_axis_dict = pc_axis_in('pcaxis_urls.csv')
        print(pc_axis_dict['px_3280'].info())
        print(pc_axis_dict['px_3281'].info())
        print(pc_axis_dict['px_22350'].info())
        print(pc_axis_dict['px_9681'].info())
        print(pc_axis_dict['px_3284'].info())

