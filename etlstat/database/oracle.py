# coding: utf-8
"""
This module manages Oracle primitives.

    Date:
        23/01/2018

    Author:
        lla11358

    Version:
        0.1
"""

import csv
import logging
import os
import shlex
import subprocess

import pandas as pd

from sqlalchemy import MetaData, Table, create_engine, text
from sqlalchemy.exc import DatabaseError

import sqlparse

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
        self.conn_string = f"""oracle+cx_oracle://{conn_params[0]}:""" + \
            f"""{conn_params[1]}@{conn_params[2]}:""" + \
            f"""{conn_params[3]}/?service_name={conn_params[4]}"""
        self.engine = create_engine(self.conn_string,
                                    encoding=encoding,
                                    coerce_to_unicode=True,
                                    coerce_to_decimal=False)
        self.schema = conn_params[0]
        self.encoding = encoding

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
        if not schema:
            schema = self.schema

        meta = MetaData(bind=self.engine, schema=schema)
        return Table(table_name, meta, autoload=True,
                     autoload_with=self.engine)

    def execute(self, sql, **kwargs):
        """
        Execute a DDL or DML SQL statement.

            Args:
                sql (string): SQL statement(s) separated by semicolons (;)
                kwargs (dict): optional statement named parameters
            Returns:
                results (list): list of dataframes. Non-SELECT statements
                    returns empty dataframes.

        """
        results = []
        statements = sqlparse.split(sql)
        connection = self.engine.connect()
        # begin transaction
        trans = connection.begin()
        try:
            for statement in statements:
                result_set = pd.DataFrame()
                result = connection.execute(
                    text(statement.strip(';')), **kwargs)
                if result.returns_rows:
                    result_set = pd.DataFrame(result.fetchall())
                    result_set.columns = result.keys()
                    LOGGER.info('Number of returned rows: %s',
                                str(len(result_set.index)))
                results.append(result_set)
            # end transaction
            trans.commit()
        except DatabaseError as db_error:
            trans.rollback()
            LOGGER.error(db_error)
            raise
        finally:
            connection.close()
        return results

    def drop(self, table_name, schema=None):
        """
        Drop a table from the database.

        Args:
          table_name(string): name of the table to drop.
          schema (string): optional schema name.

        Returns: nothing.

        """
        if not schema:
            schema = self.schema

        db_table = self.get_table(table_name, schema=schema)
        db_table.drop(self.engine, checkfirst=True)
        LOGGER.info('Table %s.%s successfully dropped.', schema, table_name)

        # Placeholders can only represent VALUES. You cannot use them for
        # sql keywords/identifiers.

    @staticmethod
    def insert(
            *conn_params,
            data_table,
            output_path,
            os_path,
            os_ld_library_path,
            mode='INSERT',
            columns=['*'],
            schema=None,
            errors='0',
            remove_data=True):
        """
        Insert a dataframe into a table via Oracle SQL Loader.

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
            data_table(Dataframe): dataframe with the data to load. Must
                contain the target table name in its 'name' attribute.
            output_path (str): path for output data files
            os_path (str): PATH environment variable
            os_ld_library_path (str): LD_LIBRARY_PATH environment variable
            mode (str): insertion mode:
                INSERT: Specifies that you are loading into an empty table.
                    SQL*Loader will abort the load if the table contains data
                    to start with. This is the default.
                APPEND: If we want to load the data into a table which is
                    already containing some rows.
                REPLACE: Specifies that, we want to replace the data in the
                    table before loading. Will 'DELETE' all the existing
                    records and replace them with new.
                TRUNCATE: This is same as 'REPLACE', but SQL*Loader will
                    use the 'TRUNCATE' command instead of 'DELETE' command.
            columns (list): list of str containing the column names to load to
                a table. Defaults to ['*'] (all columns).
            schema (str): database schema. Defaults to connection 'user'.
            errors (str): number of errors to allow. Defaults to 0 (no errors
                allowed).
            remove_data (bool): to remove or not the log and data files
                generated. Defaults to True.

        """
        if columns == ['*']:
            columns = ', '.join(data_table.columns)
        else:
            columns = ', '.join(columns)

        if not schema:
            schema = conn_params[0]

        # control file
        ctl_file = open(f"""{output_path}{data_table.name}.ctl""",
                        mode='w',
                        encoding='utf8')
        ctl_header = f"""LOAD DATA
                     CHARACTERSET UTF8
                     INFILE '{output_path}{data_table.name}.dat'
                     {mode}
                     INTO TABLE {schema}.{data_table.name}
                     FIELDS TERMINATED BY ';' OPTIONALLY ENCLOSED BY '\"'
                     TRAILING NULLCOLS
                     ({columns})"""

        ctl_file.write(ctl_header)
        ctl_file.close()

        # data file
        data_table.to_csv(
            f"""{output_path}{data_table.name}.dat""",
            columns=data_table.columns,
            sep=';',
            header=False,
            index=False,
            doublequote=True,
            quoting=csv.QUOTE_NONNUMERIC,
            encoding='utf8')

        # set environment variables
        env = os.environ.copy()
        env['PATH'] = os_path
        env['LD_LIBRARY_PATH'] = os_ld_library_path

        # generate SQL Loader arguments
        os_command = f"""sqlldr {conn_params[0]}/{conn_params[1]}""" + \
            f"""@{conn_params[2]}:{conn_params[3]}/{conn_params[4]} """ + \
            f"""control='{output_path}{data_table.name}.ctl' """ + \
            f"""log='{output_path}{data_table.name}.log' """ + \
            f"""bad='{output_path}{data_table.name}.bad' """ + \
            f"""errors={errors}"""
        args = shlex.split(os_command)

        # execution of Oracle SQL Loader
        try:
            subprocess.call(args, env=env)
        except subprocess.SubprocessError as sproc_error:
            LOGGER.error(sproc_error)

        # deleting output files
        if remove_data:
            try:
                os.remove(f"""{output_path}{data_table.name}.dat""")
                os.remove(f"""{output_path}{data_table.name}.log""")
                os.remove(f"""{output_path}{data_table.name}.bad""")
            except FileNotFoundError:
                pass
