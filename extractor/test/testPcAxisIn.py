import unittest

from utils.extractor.extractor import pc_axis_in


class TestPcAxisIn(unittest.TestCase):

    def testPcAxisIn(self):

        pcaxis_dict = pc_axis_in('/var/git/python/icane_etl/etl_stat/extractor/test/pcaxis_urls.csv')
        print(pcaxis_dict['px_3280'].info())
        print(pcaxis_dict['px_3281'].info())
        print(pcaxis_dict['px_22350'].info())
        print(pcaxis_dict['px_9681'].info())
        print(pcaxis_dict['px_3284'].info())

