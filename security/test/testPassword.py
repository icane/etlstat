import unittest

from common.security import password

minlength = 8
maxlength = 16

class TestPassword(unittest.TestCase):

    def testDatabasePassword(self):
        pwd = password.database_password(minlength, maxlength)
        self.assertTrue(len(pwd) >= minlength)
        self.assertTrue(len(pwd) <= maxlength)
        self.assertTrue(pwd[:1].isalpha())

if __name__ == '__main__':
    unittest.main()