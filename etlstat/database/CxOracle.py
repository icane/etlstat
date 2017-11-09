"""
    Class to handle Oracle database connections

    Date:
        17/10/2017

    Authors:
        lla11358

    Version:
        Beta release

    Notes:

"""

import csv
import re

import cx_Oracle
import pandas

from etlstat.log.timing import log

# from config_global import LOG_LEVEL
LOG_LEVEL = 'INFO'
if LOG_LEVEL == 'DEBUG':
    log.basicConfig(level=log.DEBUG)
else:
    log.basicConfig(level=log.INFO)

log = log.getLogger(__name__)


class CxOracle(object):

    _connection = None
    _user = ''
    _password = ''
    _host = ''
    _port = ''
    _service_name = ''
    connection_state = False

    def __init__(self):
        self._connection = cx_Oracle.Connection

    @staticmethod
    def merge_field_map(field_map):
        """
        :param field_map: dictionary of field_name:value
        :return: string of field1 = value1, field2 = value2, ...
        """
        sql = """"""
        for fm in field_map:
            sql += """ """ + fm + """ = """ + str(field_map[fm]) + ""","""
        sql = sql.rstrip(',')
        return sql

    @staticmethod
    def split_field_map(field_map):
        field = []
        value = []
        for fm in field_map:
            field.append(fm)
            value.append(str(field_map[fm]))

        sql = """({0}) VALUES({1})""".format(', '.join(field), ', '.join(value))
        return sql

    @staticmethod
    def bulk_insert(data_frame, table_name, data_file, control_file, mode="APPEND"):
        """
        Generates control and data files for Oracle SQL Loader by extracting field names and data values from a Pandas
        DataFrame.
        Usage of SQL Loader in the database server:

            sqlldr <user>/<password> control=<control_file> [log=<log_file>] [bad=bad_file]

        :param data_frame: pandas DataFrame
        :param table_name: <table> or <schema>.<table>
        :param data_file: path for output data file
        :param control_file: path for output control file
        :param mode: APPEND | REPLACE | TRUNCATE
        :return:
        """
        columns = ",".join(data_frame.columns.values.tolist())

        # control file
        ctl_file = open(control_file, mode='w', encoding='utf8')
        ctl_header = """LOAD DATA\n""" + \
                     """CHARACTERSET UTF8\n""" + \
                     """INFILE '""" + data_file + """'\n""" + \
                     mode + """\n""" + \
                     """INTO TABLE """ + table_name + """\n""" + \
                     """FIELDS TERMINATED BY ';' OPTIONALLY ENCLOSED BY '\"'\n""" + \
                     """TRAILING NULLCOLS\n""" + \
                     """(""" + columns + """)"""
        ctl_file.write(ctl_header)
        ctl_file.close()

        # data file
        data_frame.to_csv(data_file,
                          sep=';',
                          header=False,
                          index=False,
                          doublequote=True,
                          quoting=csv.QUOTE_NONNUMERIC,
                          encoding='utf-8'
                          )

        return True

    def connect(self, user, password, host, port, service_name, encoding="UTF-8", nencoding="UTF-8"):
        self._user = user
        self._password = password
        self._host = host
        self._port = port
        self._service_name = service_name

        url = "{0}/{1}@{2}:{3}/{4}"

        url_str = url.format(
            self._user,
            self._password,
            self._host,
            self._port,
            self._service_name
        )

        try:
            self._connection = cx_Oracle.connect(url_str, encoding=encoding, nencoding=nencoding)
            self.connection_state = True
            log.debug(" Connected to Oracle database version " + self._connection.version)
        except cx_Oracle.DatabaseError as db_error:
            log.debug(db_error)

    def disconnect(self):
        self._connection.close()
        self.connection_state = False

    def set_session_parameter(self, parameter, value):
        """
        Set nls_session_parameters
        :param parameter: parameter name
        :param value: parameter value
        :return: True (success) or False (fail)
        """
        sql = "ALTER SESSION SET " + parameter + " = '" + value + "'"

        try:
            log.debug(" Running SQL: [ %s ]", sql)
            cursor = self._connection.cursor()
            cursor.execute(sql)
            return True
        except cx_Oracle.DatabaseError as db_error:
            log.debug(db_error)
            return False
        finally:
            if cursor is not None:
                cursor.close()

    def select(self, sql):
        """
        :param sql: SQL SELECT query
        :return: pandas data frame (success) or None (fail)
        """
        try:
            log.debug(" Running SQL: [ %s ]", sql)
            cursor = self._connection.cursor()
            cursor.execute(sql)
            names = [x[0] for x in cursor.description]
            rows = cursor.fetchall()
            return pandas.DataFrame(rows, columns=names)
        except cx_Oracle.DatabaseError as db_error:
            log.debug(db_error)
            return None
        finally:
            if cursor is not None:
                cursor.close()

    def insert(self, table_name, field_map):
        """
        :param table_name: <table> or <schema>.<table>
        :param field_map: dictionary of field_name:value
        :return: True (success) or False (fail)
        """
        sql = """INSERT INTO {0} """.format(table_name)
        sql += self.split_field_map(field_map)

        try:
            log.debug(" Running SQL: [ %s ]", sql)
            cursor = self._connection.cursor()
            cursor.execute(sql)
            return True
        except cx_Oracle.DatabaseError as db_error:
            log.debug(db_error)
            return False
        finally:
            if cursor is not None:
                cursor.close()

    def update(self, table_name, rowid, field_map):
        """
        :param table_name: <table> or <schema>.<table>
        :param rowid: ID or ROWID field value
        :param field_map: dictionary of field_name:value
        :return: True (success) or False (fail)
        """
        sql = """UPDATE {0} SET """.format(table_name)
        sql += self.merge_field_map(field_map)
        if re.match('[A-Za-z]+', str(rowid)):
            sql += """ WHERE ROWID = '{0}'""".format(str(rowid))
        else:
            sql += """ WHERE ID = {0}""".format(str(rowid))

        try:
            log.debug(" Running SQL: [ %s ]", sql)
            cursor = self._connection.cursor()
            cursor.execute(sql)
            return True
        except cx_Oracle.DatabaseError as db_error:
            log.debug(db_error)
            return False
        finally:
            if cursor is not None:
                cursor.close()

    def delete(self, table_name, rowid):
        """
        :param table_name: <table> or <schema>.<table>
        :param rowid: ID or ROWID field value
        :return: True (success) or False (fail)
        """
        sql = """DELETE FROM {0} """.format(table_name)
        if re.match('[A-Za-z]+', str(rowid)):
            sql += """ WHERE ROWID = '{0}'""".format(str(rowid))
        else:
            sql += """ WHERE ID = {0}""".format(str(rowid))

        try:
            log.debug(" Running SQL: [ %s ]", sql)
            cursor = self._connection.cursor()
            cursor.execute(sql)
            return True
        except cx_Oracle.DatabaseError as db_error:
            log.debug(db_error)
            return False
        finally:
            if cursor is not None:
                cursor.close()

