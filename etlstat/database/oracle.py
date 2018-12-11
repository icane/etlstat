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
import os
import logging
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import DatabaseError

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)


class Oracle:
    """
    Oracle database connection management
    """

    engine = None

    conversion_map = {
        'object': 'VARCHAR2(256)',
        'int32': 'INTEGER',
        'int64': 'INTEGER',
        'float32': 'NUMBER',
        'float64': 'NUMBER'
    }

    def __init__(self, user, password, host, port,
                 service_name, encoding='utf8'):
        conn_string = f'''oracle+cx_oracle://{user}:{password}@{host}:{port}
                       /?service_name={service_name}'''
        self.engine = create_engine(
            conn_string,
            encoding=encoding,
            coerce_to_unicode=True,
            coerce_to_decimal=False)

    def execute_sql(self, sql):
        """
        Executes a DDL or DML SQL statement. Implements a transaction.

        Args:
            sql (str): SQL statement

        Returns:
            result_set (dataframe)
        """
        connection = self.engine.connect()
        trans = connection.begin()
        result_set = pd.DataFrame()
        try:
            result = connection.execute(sql)
            trans.commit()
            if result.returns_rows:
                result_set = pd.DataFrame(result.fetchall())
                result_set.columns = result.keys()
        except DatabaseError as db_error:
            LOGGER.error(db_error)
            raise
        finally:
            connection.close()
        return result_set

    def execute_query(self, sql):
        """
        Executes a DDL or DML SQL statement. Implements a transaction.

        Args:
            sql (str): SQL statement

        Returns:
            result_set (dataframe)
        """
        connection = self.engine.connect()
        result_set = pd.DataFrame()
        try:
            result = connection.execute(sql)
            result_set = pd.DataFrame(result.fetchall())
            result_set.columns = result.keys()
        except DatabaseError as db_error:
            LOGGER.error(db_error)
            raise
        finally:
            connection.close()
        return result_set

    @staticmethod
    def bulk_insert(user,
                    password,
                    host,
                    port,
                    service_name,
                    schema,
                    table,
                    output_path,
                    os_path,
                    os_ld_library_path,
                    mode="APPEND"):
        """
        Generates control and data files for Oracle SQL Loader by extracting
        field names and data values from a Pandas DataFrame. Requires Oracle
        Instant Client and Tools installed in the workstation.
        Destination table must exist in the database.
        Usage of SQL Loader:

            sqlldr <user>/<password> control=<control_file> [log=<log_file>]
            [bad=bad_file]

        Args:
            user (str): database user
            password (str): database password
            host: database server host name or IP address
            port (str): Oracle listener port
            service_name (str): Oracle instance service name
            schema (str): database schema
            table (pandas DataFrame): dataframe with the same name and column
                                      labels as the table in which it's going
                                      to be loaded. It must be filled with
                                      data rows.
            output_path (str): path for output data files
            os_path (str): PATH environment variable
            os_ld_library_path (str): LD_LIBRARY_PATH environment variable
            mode (str): insertion mode: APPEND | REPLACE | TRUNCATE

        Returns:
            True if success or False
        """

        columns = ",".join(table.columns.values.tolist())

        # control file
        ctl_file = open(output_path + table.name + '.ctl', mode='w',
                        encoding='utf8')
        ctl_header = """LOAD DATA\n""" + \
                     """CHARACTERSET UTF8\n""" + \
                     """INFILE '""" + output_path + table.name + '.dat' + \
                     """'\n""" + mode + """\n""" + \
                     """INTO TABLE """ + schema + """.""" + table.name + \
                     """\n""" + \
                     """FIELDS TERMINATED BY ';' """ + \
                     """OPTIONALLY ENCLOSED BY '\"'\n""" + \
                     """TRAILING NULLCOLS\n""" + \
                     """(""" + columns + """)"""

        ctl_file.write(ctl_header)
        ctl_file.close()

        # data file
        table.to_csv(output_path + table.name + '.dat',
                     sep=';',
                     header=False,
                     index=False,
                     doublequote=True,
                     quoting=csv.QUOTE_NONNUMERIC,
                     encoding='utf-8'
                     )

        # execution of Oracle SQL Loader
        os_command = "export PATH=" + os_path + "; export LD_LIBRARY_PATH=" + \
                     os_ld_library_path + "; "
        os_command += "sqlldr " + user + "/" + password + "@" + host + ":" + \
                      port + "/" + service_name + " "
        os_command += "control='" + output_path + table.name + ".ctl' "
        os_command += "log='" + output_path + table.name + ".log' "
        os_command += "bad='" + output_path + table.name + ".bad'"
        print(os_command)

        return os.system(os_command)
