# coding: utf-8
"""
    Oracle

    Date:
        23/01/2018

    Author:
        lla11358

    Version:
        Alpha

    Notes:


"""

import csv
from pandas import DataFrame, notnull
import re
from sqlalchemy import create_engine, inspect, MetaData, Table, Column
from sqlalchemy.exc import DatabaseError


class Oracle:

    engine = None
    # TODO: check this conversion map
    conversion_map = {
        'object': 'VARCHAR2',
        'int': 'INTEGER',
        'float': 'NUMBER'
    }

    @classmethod
    def _connect(cls, user, password, host, port, service_name):
        cls.conn_string = "oracle+cx_oracle://{0}:{1}@{2}:{3}/?service_name={4}".format(
            user,
            password,
            host,
            port,
            service_name)
        cls.engine = create_engine(cls.conn_string)

    @classmethod
    def execute_sql(cls, sql, user, password, host, port, service_name):
        """
        Executes a DDL or DML SQL statement
        :param sql: SQL statement
        :param user: database user
        :param password: database password
        :param host: host name or IP address
        :param port: database connection port
        :param service_name: Oracle instance service name
        :return: status (True | False); result (str or data frame)
        """
        cls._connect(user, password, host, port, service_name)
        connection = cls.engine.connect()
        try:
            result = connection.execute(sql)
            status = True
            if re.match('^[ ]*SELECT .*', sql, re.IGNORECASE):
                rows = result.fetchall()
                result = DataFrame(rows, columns=result.keys())
        except DatabaseError as e:
            result = e
            status = False
        finally:
            connection.close()
        return status, result

    @staticmethod
    def bulk_insert(table, data_file, control_file, mode="APPEND"):
        """
        Generates control and data files for Oracle SQL Loader by extracting field names and data values from a Pandas
        DataFrame.
        Usage of SQL Loader in the database server:

            sqlldr <user>/<password> control=<control_file> [log=<log_file>] [bad=bad_file]

        :param table :obj:DataFrame: DataFrame which name and column's label match with table's name and columns
        in database. It must be filled with data rows.
        :param data_file: path for output data file
        :param control_file: path for output control file
        :param mode: APPEND | REPLACE | TRUNCATE
        :return:
        """
        columns = ",".join(table.columns.values.tolist())

        # control file
        ctl_file = open(control_file, mode='w', encoding='utf8')
        ctl_header = """LOAD DATA\n""" + \
                     """CHARACTERSET UTF8\n""" + \
                     """INFILE '""" + data_file + """'\n""" + \
                     mode + """\n""" + \
                     """INTO TABLE """ + table.name + """\n""" + \
                     """FIELDS TERMINATED BY ';' OPTIONALLY ENCLOSED BY '\"'\n""" + \
                     """TRAILING NULLCOLS\n""" + \
                     """(""" + columns + """)"""
        ctl_file.write(ctl_header)
        ctl_file.close()

        # data file
        table.to_csv(data_file,
                     sep=';',
                     header=False,
                     index=False,
                     doublequote=True,
                     quoting=csv.QUOTE_NONNUMERIC,
                     encoding='utf-8'
                     )
