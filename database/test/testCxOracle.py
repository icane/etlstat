import unittest
from utils.database.CxOracle import CxOracle
import pandas


class TestCxOracle(unittest.TestCase):

    user = 'test'
    password = 'test'
    host = 'constantino.icane.es'
    port = '1521'
    service_name = 'CONSTANT.ICANE.ES'

    def testMergeFieldMap(self):
        field_map = {'ANNO': 2015,
                     'NORDEN': 1991163,
                     'AI1': 1132168,
                     'AI11': 99.5,
                     'AI12': 0.5,
                     }
        self.assertTrue(CxOracle.merge_field_map(field_map).find("""NORDEN = 1991163"""))
        self.assertTrue(CxOracle.merge_field_map(field_map).find("""AI12 = 0.5"""))

    def testSplitFieldMap(self):
        field_map = {'ANNO': 2015,
                     'AI12': 0.5,
                     }
        self.assertRegex(CxOracle.split_field_map(field_map), '(\([A-Z0-9\, ]+\) )(VALUES\([0-9\,\. ]+\))')

    def testBulkLoad(self):
        source_path = '01001.csv'
        table_name = 'TEST.PX_01001'
        csv_path = 'PX_01001.csv'
        ctl_path = 'PX_01001.ctl'
        mode = 'TRUNCATE'
        df = pandas.read_csv(source_path, header=0, sep=';', encoding='utf8')
        cxo = CxOracle()
        self.assertTrue(cxo.bulk_insert(df, table_name, csv_path, ctl_path, mode))

    def testConnectOk(self):
        cxo = CxOracle()
        cxo.connect(self.user, self.password, self.host, self.port, self.service_name)
        self.assertTrue(cxo.connection_state)
        cxo.disconnect()
        self.assertFalse(cxo.connection_state)

    def testSetSessionParameter(self):
        cxo = CxOracle()
        cxo.connect(self.user, self.password, self.host, self.port, self.service_name)
        df = cxo.select("SELECT VALUE FROM NLS_SESSION_PARAMETERS WHERE PARAMETER = 'NLS_DATE_FORMAT'")
        date_format_1 = df.values[0][0]
        cxo.set_session_parameter("NLS_DATE_FORMAT", "DD/MM/RR")
        df = cxo.select("SELECT VALUE FROM NLS_SESSION_PARAMETERS WHERE PARAMETER = 'NLS_DATE_FORMAT'")
        date_format_2 = df.values[0][0]
        self.assertNotEqual(date_format_1, date_format_2)
        cxo.disconnect()

    def testConnectFail(self):
        cxo = CxOracle()
        # invalid username/password
        cxo.connect('', self.password, self.host, self.port, self.service_name)
        self.assertFalse(cxo.connection_state)
        # null password given
        cxo.connect(self.user, '', self.host, self.port, self.service_name)
        self.assertFalse(cxo.connection_state)
        # invalid host
        cxo.connect(self.user, self.password, 'constantin', self.port, self.service_name)
        self.assertFalse(cxo.connection_state)
        # wrong port raises a timeout (60s)
        # cxo.connect(self.user, self.password, self.host, '1522', self.service_name)
        # self.assertFalse(cxo.connection_state)
        # service requested not unknown
        cxo.connect(self.user, self.password, self.host, self.port, 'CONSTANT')
        self.assertFalse(cxo.connection_state)

    def testSelectOk(self):
        cxo = CxOracle()
        cxo.connect(self.user, self.password, self.host, self.port, self.service_name)
        sql = "SELECT ID, COLUMN1, COLUMN2 FROM TEST.TABLE1 WHERE COLUMN1 = 'ESPAÑA' OR ID = 1"
        df = cxo.select(sql)
        print(df)
        self.assertEqual(df.size, 6)
        cxo.disconnect()

    def testSelectFail(self):
        cxo = CxOracle()
        cxo.connect(self.user, self.password, self.host, self.port, self.service_name)
        sql = "SELECT XX FROM TEST.TABLE1"
        df = cxo.select(sql)
        self.assertFalse(df)
        cxo.disconnect()

    def testInsertOk(self):
        table_name = 'TEST.TABLE2'
        field_map = {
            'ID': 2,
            'COLUMN1': "'DOS'",
            'COLUMN2': 2.02
        }
        cxo = CxOracle()
        cxo.connect(self.user, self.password, self.host, self.port, self.service_name)
        self.assertTrue(cxo.insert(table_name, field_map))
        cxo.disconnect()

    def testInsertFail(self):
        table_name = 'TEST.TABLE2'
        field_map = {
            'ID': 2,
            'COLUMN1': "DOS",
            'COLUMN2': 2.02
        }
        cxo = CxOracle()
        cxo.connect(self.user, self.password, self.host, self.port, self.service_name)
        self.assertFalse(cxo.insert(table_name, field_map))
        cxo.disconnect()

    def testUpdateIdOk(self):
        table_name = 'TEST.TABLE3'
        rowid = 1
        field_map = {
            'COLUMN1': "'UNO UPDATED'",
            'COLUMN2': 1.0001
        }
        cxo = CxOracle()
        cxo.connect(self.user, self.password, self.host, self.port, self.service_name)
        self.assertTrue(cxo.update(table_name, rowid, field_map))
        cxo.disconnect()

    def testUpdateRowidOk(self):
        table_name = 'TEST.TABLE3'
        rowid = 'AABDoXAAEAAAACjAAB'
        field_map = {
            'COLUMN1': "'DOS UPDATED'",
            'COLUMN2': 2.0002
        }
        cxo = CxOracle()
        cxo.connect(self.user, self.password, self.host, self.port, self.service_name)
        self.assertTrue(cxo.update(table_name, rowid, field_map))
        cxo.disconnect()

    def testUpdateFail(self):
        table_name = 'TEST.TABLE3'
        rowid = 'AABDoXAAEAAAACjAAB'
        field_map = {
            'COLUMN1': 2.0002,
            'COLUMN2': "'DOS UPDATED'"
        }
        cxo = CxOracle()
        cxo.connect(self.user, self.password, self.host, self.port, self.service_name)
        self.assertFalse(cxo.update(table_name, rowid, field_map))
        cxo.disconnect()

    def testDeleteIdOk(self):
        table_name = 'TEST.TABLE4'
        rowid = 1
        cxo = CxOracle()
        cxo.connect(self.user, self.password, self.host, self.port, self.service_name)
        self.assertTrue(cxo.delete(table_name, rowid))
        cxo.disconnect()

    def testDeleteRowidOk(self):
        table_name = 'TEST.TABLE4'
        rowid = 'AABDocAAEAAAAC2AAC'
        cxo = CxOracle()
        cxo.connect(self.user, self.password, self.host, self.port, self.service_name)
        self.assertTrue(cxo.delete(table_name, rowid))
        cxo.disconnect()

    def testDeleteFail(self):
        table_name = 'TEST.TABLE5'
        rowid = 1
        cxo = CxOracle()
        cxo.connect(self.user, self.password, self.host, self.port, self.service_name)
        self.assertFalse(cxo.delete(table_name, rowid))
        cxo.disconnect()
