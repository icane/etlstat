import unittest

from etlstat.extractor.extractor import pc_axis_in


class TestExtractor(unittest.TestCase):

    def testPcAxisIn(self):

        pcaxis_dict = pc_axis_in('pcaxis_urls.csv')
        print(pcaxis_dict['px_3280'].info())
        print(pcaxis_dict['px_3281'].info())
        print(pcaxis_dict['px_22350'].info())
        print(pcaxis_dict['px_9681'].info())
        print(pcaxis_dict['px_3284'].info())

