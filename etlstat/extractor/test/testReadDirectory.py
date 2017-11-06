import unittest

from utils.extractor.extractor import *

logging.basicConfig(level=logging.INFO)

log = logging.getLogger(__name__)

MAPPING ={
    'EEE_2015_TERRI_IND_06.TXT': 'EEE_TERRI.csv',
    'EEE_2015_TERRI_SER_06.TXT': 'EEE_TERRI.csv',
    'EEE_2015_FINAL_SER_06.TXT': 'EEE_FINAL.csv',
    'EEE_2015_IDENT_IND_06.TXT': 'EEE_IDENT.csv',
    'EEE_2015_IDENT_SER_06.TXT': 'EEE_IDENT.csv',
    'EEE_2015_IDENT_COM_06.TXT': 'EEE_IDENT.csv',
    'EEE_2015_TERRI_COM_06.TXT': 'EEE_TERRI.csv',
    'EEE_2015_FINAL_COM_06.TXT': 'EEE_FINAL.csv',
    'EEE_2015_UA_IND_06.txt': 'EEE_UA.csv',
    'EEE_2015_FINAL_IND_06.TXT': 'EEE_FINAL.csv'}

MAPPING2 = {
    'AGENV_SER_06_15.TXT': 'AGENV_SER.csv',
    'PUBLI_SER_06_15.TXT': 'PUBLI_SER.csv',
    'TMER_SER_06_15.TXT': 'TMER_SER.csv',
    'LIMPI_SER_06_15.TXT': 'LIMPI_SER.csv',
    'AUDIOV_SER_06_15.TXT': 'AUDIOV_SER.csv',
    'ALOJ_SER_06_15.TXT': 'ALOJ_SER.csv',
    'MENOR_COM_06_15.TXT': 'MENOR_COM.csv',
    'POST_SER_06_15.TXT': 'POST_SER.csv',
    'TFERR_SER_06_15.TXT': 'TFERR_SER.csv',
    'INFOR_SER_06_15.TXT': 'INFOR_SER.csv',
    'OPIN_SER_06_15.TXT': 'OPIN_SER.csv',
    'CONS_SER_06_15.TXT': 'CONS_SER.csv',
    'TUI_SER_06_15.TXT': 'TUI_SER.csv',
    'VEHIC_COM_06_15.TXT': 'VEHIC_COM.csv',
    'TMAR_SER_06_15.TXT': 'TMAR_SER.csv',
    'JURID_SER_06_15.TXT': 'JURID_SER.csv',
    'AEREO_SER_06_15.TXT': 'AEREO_SER.csv',
    'MAYOR_COM_06_15.TXT': 'MAYOR_COM.csv',
    'SSCC_SER_06_15.TXT': 'SSCC_SER.csv',
    'PERSON_SER_06_15.TXT': 'PERSON_SER.csv',
    'TEC_SER_06_15.TXT': 'TEC_SER.csv'}



class TestReadDirectory(unittest.TestCase):

    def test_similar(self):
        string1 = "HOLA"
        string2 = "ADIOS"
        string3 = "HOLA MUNDO"
        assert similar(string1,string2) < similar(string1,string3)

        string1 = "IPI_JUNIO_2017.txt"
        string2 = "JUNIO_IPI_2017.csv"
        string3 = "IPI_Junio_2016.csv"
        assert similar(string1,string2) > similar(string1,string3)

        string1 = "EEE_2015_TERRI_IND_06.txt"
        string2 = "EEE_TERRI.csv"
        string3 = "EEE_UA.csv"
        assert similar(string1,string2) > similar(string1,string3)

        string1 = 'EEE_2015_IDENT_IND_06.TXT'
        string2 = 'EEE_FINAL.csv'
        string3 = 'EEE_IDENT.csv'
        assert similar(string1, string2) < similar(string1, string3)

    def test_excel_processing(self):
        dir_path = '/var/git/python/icane_etl/economia/mercado_trabajo/estadistica_empleo_paro/ass_mm/data/input/'
        excel = 'CA_MT_327.xls'
        dict = excel_processing(dir_path, excel)
        assert(dict['Hoja1'] == {'skip_rows': 3, 'footer_rows': 5})
        assert(dict['Hoja2'] == {'skip_rows': 0, 'footer_rows': 0})
        assert(dict['Hoja3'] == {'skip_rows': 0, 'footer_rows': 0})
        assert(len(dict) == 3)

        excel = 'CA_MT_328.xls'
        dict = excel_processing(dir_path, excel)
        assert(dict['Hoja1'] == {'skip_rows': 3, 'footer_rows': 4})
        assert(dict['Hoja2'] == {'skip_rows': 0, 'footer_rows': 0})
        assert(dict['Hoja3'] == {'skip_rows': 0, 'footer_rows': 0})
        assert(len(dict) == 3)

        excel = 'CA_MT_329.xls'
        dict = excel_processing(dir_path, excel)
        assert(dict['Hoja1'] == {'skip_rows': 7, 'footer_rows': 22})
        assert(dict['Hoja2'] == {'skip_rows': 0, 'footer_rows': 0})
        assert(dict['Hoja3'] == {'skip_rows': 0, 'footer_rows': 0})
        assert(len(dict) == 3)

        dir_path = '/mnt/gobcan/datos/series actualizadas/CA_SP_sector primario/AGRICULTURA/Consejo Regulador Agricultura Ecologica (CRAE)/'

        excel = 'CA_SP_063.xls'
        dict = excel_processing(dir_path, excel)
        assert(dict['Hoja5'] == {'skip_rows': 3, 'footer_rows': 2})
        assert(dict['Comprobación'] == {'skip_rows': 3, 'footer_rows': 22})
        assert(len(dict) == 2)

        dir_path = '/mnt/gobcan/datos/series actualizadas/CA_MT_mercado_trabajo/'

        excel = 'CA_MT_072.xls'
        dict = excel_processing(dir_path, excel)
        assert(dict['regulacion empleo'] == {'skip_rows': 4, 'footer_rows': 3})
        assert(dict['Comprobación'] == {'skip_rows': 9, 'footer_rows': 433})
        assert(len(dict) == 2)

        excel = 'CA_MT_106.xls'
        dict = excel_processing(dir_path, excel)
        assert(dict['fondo garantia salarial'] == {'skip_rows': 3, 'footer_rows': 3})
        assert(dict['Comprobacion'] != {'skip_rows': 9, 'footer_rows': 175}) # Algoritmo no pensado para hojas de comprobación
        assert(len(dict) == 2)

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

    def test_excel_in(self):
        dir_path = '/var/git/python/icane_etl/economia/mercado_trabajo/estadistica_empleo_paro/ass_mm/data/input/'

        df_dict = excel_in(dir_path,'Hoja1')
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

        dir_path = '/mnt/gobcan/datos/series actualizadas/CA_SP_sector primario/AGRICULTURA/Consejo Regulador Agricultura Ecologica (CRAE)/'
        df_dict = excel_in(dir_path,'Hoja5')
        assert ('CA_SP_063.xls' in df_dict)
        assert (isinstance(df_dict['CA_SP_063.xls'], pd.DataFrame))
        assert (len(df_dict['CA_SP_063.xls']) == 19)

        dir_path = '/mnt/gobcan/datos/series actualizadas/CA_MT_mercado_trabajo/'
        self.assertRaises(KeyError,  excel_in,dir_path, 'regulacion empleo')

        dir_path = '/mnt/gobcan/datos/series actualizadas/CA_DE_Deportes/'
        df_dict = excel_in(dir_path, 'Hoja1')
        assert ('CA_DE_01.xls' in df_dict)
        assert ('CA_DE_02.xls' in df_dict)
        assert (isinstance(df_dict['CA_DE_01.xls'], pd.DataFrame))
        assert (isinstance(df_dict['CA_DE_02.xls'], pd.DataFrame))
        assert (len(df_dict['CA_DE_01.xls']) == 67)
        assert (len(df_dict['CA_DE_02.xls']) == 67)

    def test(self):
        pass
        # dir_path = '/var/git/python/icane_etl/economia/industria/eie/data/input'

if __name__ == '__main__':
    unittest.main()