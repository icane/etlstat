import unittest

from etlstat.security import password

min_length = 8
max_length = 16


class TestPassword(unittest.TestCase):

    def testGeneratePasswordOk(self):
        pwd = password.generate_password(min_length, max_length, first_letter=True)
        self.assertTrue(len(pwd) >= min_length)
        self.assertTrue(len(pwd) <= max_length)
        self.assertTrue(pwd[:1].isalpha())

    def testGeneratePasswordFail(self):
        try:
            password.generate_password(max_length, min_length)
        except TypeError as e:
            self.assertEqual(str(e), 'Minimum password length must be less or equal than maximum.')

if __name__ == '__main__':
    unittest.main()
