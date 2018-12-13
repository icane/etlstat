# coding: utf-8
"""
This module manages Oracle primitives.

    Date:
        23/01/2018

    Author:
        lla11358

    Version:
        0.1

    Notes:

"""

import csv
import logging
import os
import shlex
import subprocess
import pandas
from sqlalchemy import create_engine, text, MetaData, Table
from sqlalchemy.exc import DatabaseError

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)


class Oracle:
    """
    Manage connections to Oracle databases.

        Oracle class offers some helper methods that encapsulate primitive
        logic to interactuate with the database: insert/upsert, execute,
        drop, etc.
        Some primitives are not included because they can be handled more
        properly with sqlalchemy.

    """

    def __init__(self, *conn_params, encoding='utf8'):
        """
        Initialize the database connection and other relevant data.

            Args:
                *conn_params: list of the following connection parameters:
                    user(string): database user to connect to the schema.
                    password(string): database password of the user.
                    host(string): database management system host.
                    port(string): tcp port where the database is listening.
                    service_name(string): Oracle instance name.
                encoding (string): Charset encoding.
        """
        # connection string in sqlalchemy format
        self.conn_string = f'''oracle+cx_oracle://{conn_params[0]}:''' + \
            f'''{conn_params[1]}@{conn_params[2]}:''' + \
            f'''{conn_params[3]}/{conn_params[4]}'''
        self.engine = create_engine(self.conn_string,
                                    encoding=encoding,
                                    coerce_to_unicode=True,
                                    coerce_to_decimal=False)
        self.schema = conn_params[0]

    def get_table(self, table_name, schema=None):
        """
        Get a database table into a sqlalchemy Table object.

            Args:
                table_name(string): name of the database table to map.
                schema(string): name of the schema to which the table belongs.
                    Defaults to the selected database in the connection.
            Returns:
                table(Table): sqlalchemy Table object referencing the specified
                    database table.

        """
        meta = MetaData(bind=self.engine,
                        schema=schema if schema else self.schema)
        return Table(table_name, meta, autoload=True,
                     autoload_with=self.engine)

    def execute(self, sql, **kwargs):
        """
        Execute a DDL or DML SQL statement.

            Args:
                sql (string): SQL statement
                kwargs (dictionary): optional statement named parameters
            Returns:
                result_set(Dataframe):

        """
        connection = self.engine.connect()
        trans = connection.begin()
        result_set = pandas.DataFrame()
        try:
            result = connection.execute(text(sql), **kwargs)
            trans.commit()
            if result.returns_rows:
                result_set = pandas.DataFrame(result.fetchall())
                result_set.columns = result.keys()
                LOGGER.info('Number of returned rows: %s',
                            str(len(result_set.index)))
        except DatabaseError as db_error:
            LOGGER.error(db_error)
            raise
        finally:
            connection.close()
        return result_set

    def drop(self, table_name, schema=None):
        """
        Drop a table from the database.

        Args:
          table_name(str): name of the table to drop.

        Returns: nothing.

        """
        if not schema:
            schema = self.schema
        db_table = self.get_table(table_name, schema)
        db_table.drop(self.engine, checkfirst=True)
        LOGGER.info('Table %s.%s successfully dropped.', schema, table_name)

        # Placeholders can only represent VALUES. You cannot use them for
        # sql keywords/identifiers.

    @staticmethod
    def insert(
            *conn_params,
            schema,
            table,
            output_path,
            os_path,
            os_ld_library_path,
            mode="APPEND"
    ):
        """
        Load a dataframe into a table via Oracle SQL Loader.

        Extracts field names and data values from a Pandas DataFrame.
        Requires Oracle Instant Client and Tools installed in the workstation.
        Destination table must exist in the database.
        Usage of SQL Loader:

            sqlldr <user>/<password> control=<control_file> [log=<log_file>]
            [bad=bad_file]

        Args:
            *conn_params: list of the following connection parameters:
                    user(string): database user to connect to the schema.
                    password(string): database password of the user.
                    host(string): database management system host.
                    port(string): tcp port where the database is listening.
                    service_name(string): Oracle instance name.
            schema (str): database schema
            table (DataFrame): dataframe with the same name and column
                labels as the table in which it's going to be loaded.
                It must be filled with data rows.
            output_path (str): path for output data files
            os_path (str): PATH environment variable
            os_ld_library_path (str): LD_LIBRARY_PATH environment variable
            mode (str): insertion mode: APPEND | REPLACE | TRUNCATE

        """
        columns = ",".join(table.columns.values.tolist())

        # control file
        ctl_file = open(f'''{output_path}{table.name}.ctl''',
                        mode='w',
                        encoding='utf8')
        ctl_header = f"""LOAD DATA
                     CHARACTERSET UTF8
                     INFILE '{output_path}{table.name}.dat'
                     {mode}
                     INTO TABLE {schema}.{table.name}
                     FIELDS TERMINATED BY ';' OPTIONALLY ENCLOSED BY '\"'
                     TRAILING NULLCOLS
                     ({columns})"""

        ctl_file.write(ctl_header)
        ctl_file.close()

        # data file
        table.to_csv(f'''{output_path}{table.name}.dat''',
                     sep=';',
                     header=False,
                     index=False,
                     doublequote=True,
                     quoting=csv.QUOTE_NONNUMERIC,
                     encoding='utf-8'
                     )

        # set environment variables
        env = os.environ.copy()
        env['PATH'] = os_path
        env['LD_LIBRARY_PATH'] = os_ld_library_path
        # generate SQL Loader arguments
        os_command = f"""sqlldr {conn_params[0]}/{conn_params[1]}""" + \
            f"""@{conn_params[2]}:{conn_params[3]}/{conn_params[4]} """ + \
            f"""control='{output_path}{table.name}.ctl' """ + \
            f"""log='{output_path}{table.name}.log' """ + \
            f"""bad='{output_path}{table.name}.bad'"""
        args = shlex.split(os_command)
        # execution of Oracle SQL Loader
        try:
            subprocess.Popen(args, env=env)
        except subprocess.SubprocessError as sproc_error:
            LOGGER.error(sproc_error)
