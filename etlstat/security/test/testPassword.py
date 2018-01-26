import unittest

from etlstat.security import password

min_length = 8
max_length = 16


class TestPassword(unittest.TestCase):

    def testDatabasePassword(self):
        pwd = password.database_password(min_length, max_length)
        self.assertTrue(len(pwd) >= min_length)
        self.assertTrue(len(pwd) <= max_length)
        self.assertTrue(pwd[:1].isalpha())

if __name__ == '__main__':
    unittest.main()
