"""

"""

import csv
import re

import pandas as pd
import sqlalchemy

from etlstat.database.Database import Database

from etlstat.log.timing import log

LOG_LEVEL = 'INFO'

if LOG_LEVEL == 'DEBUG':
    log.basicConfig(level=log.DEBUG)
else:
    log.basicConfig(level=log.INFO)

log = log.getLogger(__name__)


class Oracle(Database):

    def __init__(self, connector, host, port, service_name, user, password):

        super().__init__(connector, host, port, service_name, user, password)

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

    def select(self, sql):
        """
        :param sql: SQL SELECT query
        :return: DBAPI cursor (success) or None (fail)
        """
        try:
            log.debug(" Running SQL: [ %s ]", sql)
            result = self.query(sql).fetchall()
            log.debug("SELECT executed successfully")
            return result
        except sqlalchemy.exc.SQLAlchemyError as error:
            log.error(" Cannot execute select operation. " + str(error))
            return None
        except sqlalchemy.exc.DatabaseError as error:
            log.error(" Cannot execute select operation. " + str(error))
            return None

    def to_data_frame(self, sql):
        """
        :param sql: SQL SELECT query
        :return: pandas data frame
        """
        try:
            log.debug(" Running SQL: [ %s ]", sql)
            result = pd.read_sql(sql, self._engine)
            log.debug("SELECT executed successfully")
            return result
        except sqlalchemy.exc.SQLAlchemyError as error:
            log.error(" Cannot execute select operation. " + str(error))
            return None
        except sqlalchemy.exc.DatabaseError as error:
            log.error(" Cannot execute select operation. " + str(error))
            return None

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
            self.query(sql)
            log.debug("INSERT executed successfully")
            return True
        except sqlalchemy.exc.SQLAlchemyError as error:
            log.error(" Cannot execute insert operation on " + table_name + " table: " + str(error))
            return False
        except sqlalchemy.exc.DatabaseError as error:
            log.error(" Cannot execute insert operation. " + str(error))
            return False

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
            self.query(sql)
            log.debug("UPDATE executed successfully")
            return True
        except sqlalchemy.exc.SQLAlchemyError as error:
            log.error(" Cannot execute update operation on " + table_name + " table: " + str(error))
            return False
        except sqlalchemy.exc.DatabaseError as error:
            log.error(" Cannot execute update operation. " + str(error))
            return False

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
            self.query(sql)
            log.debug("DELETE executed successfully")
            return True
        except sqlalchemy.exc.SQLAlchemyError as error:
            log.error(" Cannot execute delete operation on " + table_name + " table: " + str(error))
            return False
        except sqlalchemy.exc.DatabaseError as error:
            log.error(" Cannot execute delete operation. " + str(error))
            return False

    @staticmethod
    def bulk_insert(data_frame, table_name, data_file, control_file, mode='APPEND'):
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
