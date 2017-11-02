import pandas
import unittest
from utils.database.Oracle import Oracle


class TestOracle(unittest.TestCase):

    connector = 'oracle+cx_oracle'
    host = 'constantino.icane.es'
    port = '1521'
    service_name = 'CONSTANT.ICANE.ES'
    user = 'test'
    password = 'test'

    def testMergeFieldMap(self):
        field_map = {'ANNO': 2015,
                     'NORDEN': 1991163,
                     'AI1': 1132168,
                     'AI11': 99.5,
                     'AI12': 0.5,
                     }
        self.assertTrue(Oracle.merge_field_map(field_map).find("""NORDEN = 1991163"""))
        self.assertTrue(Oracle.merge_field_map(field_map).find("""AI12 = 0.5"""))

    def testSplitFieldMap(self):
        field_map = {'ANNO': 2015,
                     'AI12': 0.5,
                     }
        self.assertRegex(Oracle.split_field_map(field_map), '(\([A-Z0-9\, ]+\) )(VALUES\([0-9\,\. ]+\))')

    def testSelectOk(self):
        oracle = Oracle(self.connector, self.host, self.port, self.service_name, self.user, self.password)
        sql = "SELECT ID, COLUMN1, COLUMN2 FROM TEST.TABLE1"
        result = oracle.select(sql)
        i = 0
        for row in result:
            print(row[1])
            i += 1
        self.assertTrue(i > 0)
        oracle.close()

    def testSelectFail(self):
        oracle = Oracle(self.connector, self.host, self.port, self.service_name, self.user, self.password)
        sql = "SELECT XX FROM TEST.TABLE1"
        result = oracle.select(sql)
        self.assertFalse(result)
        oracle.close()

    def testToDataFrameOk(self):
        oracle = Oracle(self.connector, self.host, self.port, self.service_name, self.user, self.password)
        sql = "SELECT * FROM TEST.TABLE1"
        result = oracle.to_data_frame(sql)
        self.assertEqual(result.size, 12)
        oracle.close()

    def testToDataFrameFail(self):
        oracle = Oracle(self.connector, self.host, self.port, self.service_name, self.user, self.password)
        sql = "SELECT XX FROM TEST.TABLE1"
        result = oracle.to_data_frame(sql)
        self.assertFalse(result)
        oracle.close()

    def testInsertOk(self):
        table_name = 'TEST.TABLE2'
        field_map = {
            'ID': 2,
            'COLUMN1': "'DOS'",
            'COLUMN2': 2.02
        }
        oracle = Oracle(self.connector, self.host, self.port, self.service_name, self.user, self.password)
        self.assertTrue(oracle.insert(table_name, field_map))
        oracle.close()

    def testInsertFail(self):
        table_name = 'TEST.TABLE2'
        field_map = {
            'ID': 2,
            'COLUMN1': "DOS",
            'COLUMN2': 2.02
        }
        oracle = Oracle(self.connector, self.host, self.port, self.service_name, self.user, self.password)
        self.assertFalse(oracle.insert(table_name, field_map))
        oracle.close()

    def testUpdateIdOk(self):
        table_name = 'TEST.TABLE3'
        rowid = 1
        field_map = {
            'COLUMN1': "'UNO UPDATED'",
            'COLUMN2': 1.0001
        }
        oracle = Oracle(self.connector, self.host, self.port, self.service_name, self.user, self.password)
        self.assertTrue(oracle.update(table_name, rowid, field_map))
        oracle.close()

    def testUpdateRowidOk(self):
        table_name = 'TEST.TABLE3'
        rowid = 'AABDoXAAEAAAACjAAB'
        field_map = {
            'COLUMN1': "'DOS UPDATED'",
            'COLUMN2': 2.0002
        }
        oracle = Oracle(self.connector, self.host, self.port, self.service_name, self.user, self.password)
        self.assertTrue(oracle.update(table_name, rowid, field_map))
        oracle.close()

    def testUpdateFail(self):
        table_name = 'TEST.TABLE3'
        rowid = 'AABDoXAAEAAAACjAAB'
        field_map = {
            'COLUMN1': 2.0002,
            'COLUMN2': "'DOS UPDATED'"
        }
        oracle = Oracle(self.connector, self.host, self.port, self.service_name, self.user, self.password)
        self.assertFalse(oracle.update(table_name, rowid, field_map))
        oracle.close()

    def testDeleteIdOk(self):
        table_name = 'TEST.TABLE4'
        rowid = 1
        oracle = Oracle(self.connector, self.host, self.port, self.service_name, self.user, self.password)
        self.assertTrue(oracle.delete(table_name, rowid))
        oracle.close()

    def testDeleteRowidOk(self):
        table_name = 'TEST.TABLE4'
        rowid = 'AABDocAAEAAAAC2AAC'
        oracle = Oracle(self.connector, self.host, self.port, self.service_name, self.user, self.password)
        self.assertTrue(oracle.delete(table_name, rowid))
        oracle.close()

    def testDeleteFail(self):
        table_name = 'TEST.TABLE5'
        rowid = 1
        oracle = Oracle(self.connector, self.host, self.port, self.service_name, self.user, self.password)
        self.assertFalse(oracle.delete(table_name, rowid))
        oracle.close()

    def testBulkLoad(self):
        source_path = '01001.csv'
        table_name = 'TEST.PX_01001'
        csv_path = 'PX_01001.csv'
        ctl_path = 'PX_01001.ctl'
        mode = 'TRUNCATE'

        df = pandas.read_csv(source_path, header=0, sep=';', encoding='utf8')
        oracle = Oracle(self.connector, self.host, self.port, self.service_name, self.user, self.password)
        self.assertTrue(oracle.bulk_insert(df, table_name, csv_path, ctl_path, mode))
        oracle.close()
