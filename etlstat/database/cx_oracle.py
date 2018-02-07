import cx_Oracle
from cx_Oracle import Error

connstr = 'system/oracle@localhost/xe'

def dbcursor(connstr):
    try:
        conn = cx_Oracle.connect(connstr)
        cur = conn.cursor()
        cur.execute('select * from help')
        for result in cur:
            print(result)
        cur.close
        conn.close
        return True
    except Error as e:
        print(e)
        return False

def test_dbcursor():
    assert dbcursor(connstr) == True