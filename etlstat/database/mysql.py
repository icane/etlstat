# coding: utf-8
"""
This module manages MySQL primitives.

    Date:
        27/11/2018

    Author:
        emm13775

    Version:
        0.1

    Notes:

"""

import os
import logging
import sqlparse
from sqlalchemy import create_engine, text, select, func, MetaData, Table
from sqlalchemy.exc import DatabaseError
import pandas as pd

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)


class MySQL:
    """
    Manage connections to MySQL databases.

    MySQL class offers some helper methods that encapsulate primitive logic
    to interactuate with the database: insert/upsert, execute, drop, etc.
    Some primitives are not included because they can be handled more
    properly with sqlalchemy.

    """

    def __init__(self, *conn_params):
        """
        Initialize the database connection and other relevant data.

        Args:
            *conn_params: list with the following connection parameters:
                user(string): database user to connect to the schema.
                password(string): database password of the user.
                host(string): database management system host.
                port(string): tcp port where the database is listening.
                database(string): schema or database name.

        """
        # connection string in sqlalchemy format
        self.conn_string = f'''mysql+mysqlconnector://{conn_params[0]}:''' + \
            f'''{conn_params[1]}@{conn_params[2]}:''' + \
            f'''{str(conn_params[3])}/{conn_params[4]}'''
        self.engine = create_engine(self.conn_string)
        self.database = conn_params[4]

    def get_table(self, table_name, schema=None):
        """
        Get a database table into a sqlalchemy Table object.

        Args:
            table_name(string): name of the database table to map.
            schema(string): name of the schema to which the table belongs.
                        Defaults to the selected database in the
                        connection.
        Returns:
            table(Table): sqlalchemy Table object referencing the specified
                            database table.

        """
        if not schema:
            schema = self.database
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
          table_name(str): name of the table to drop.
          schema (string): optional database name.

        Returns: nothing.

        """
        if not schema:
            schema = self.database
        db_table = self.get_table(table_name, schema)
        db_table.drop(self.engine, checkfirst=True)
        LOGGER.info('Table %s.%s successfully dropped.', schema, table_name)

        # Placeholders can only represent VALUES. You cannot use them for
        # sql keywords/identifiers.

    def insert(self, data_table, if_exists='fail',
               columns=['*'], schema=None, rm_tmp=True):
        r"""
        Insert a dataframe into a table.

        Converts the dataframe to CSV format and bulk loads it.

        Args:
          data_table(Dataframe): dataframe with the data to load.
                                 Must contain the target table name in
                                 its name attribute.
          if_exists(string): {‘fail’, ‘replace’, ‘append’}, default ‘fail’.
                             See `pandas.to_sql()` for details. Warning: if
                             replace is chosen, PKs will be deleted and will
                             have to be recreated.
          columns(list): list of str containing the column names to load to a
                         table. Defaults to ['*'] (all columns).
          schema (string): name of the database that contains the
                           destination table.
          rm_tmp (bool): remove or not the temporary csv file. Defaults to
                         True.
        Returns:
          db_table(Table): sqlalchemy table mapping the table with the inserted
                           records.

        """
        tmpfile = 'tmp.csv'  # filename for temporary file to load from
        sep = ';'  # separator for temp file
        quotechar = '"'  # Character used to quote fields
        line_terminated_by = '\n'  # termination character for file lines

        if columns == ['*']:
            columns = ', '.join(data_table.columns)
        else:
            columns = ', '.join(columns)

        if not schema:
            schema = self.database

        connection = self.engine.connect()
        db_table = Table()
        if isinstance(data_table, pd.DataFrame):
            data_table[:0].to_sql(data_table.name, self.engine,
                                  if_exists=if_exists, index=False)
        else:
            raise TypeError("data_table must be a DataFrame.")
        try:
            LOGGER.info('creating %s ok', tmpfile)
            data_table.to_csv(tmpfile, na_rep='\\N', header=False,
                              index=False, sep=sep, quotechar=quotechar)
            LOGGER.info('loading %s ok', tmpfile)
            sql_load = f'''LOAD DATA LOCAL INFILE '{tmpfile}' INTO TABLE
                           {schema}.{data_table.name}
                           FIELDS TERMINATED BY '{sep}'
                           OPTIONALLY ENCLOSED BY '{quotechar}'
                           LINES TERMINATED BY '{line_terminated_by}'
                           ({columns});'''
            connection.execute(sql_load)
            if rm_tmp:
                os.remove(tmpfile)
            db_table = self.get_table(data_table.name)
            row_count = connection.engine.scalar(
                select([func.count('*')]).select_from(db_table))
            LOGGER.info('Number of inserted rows: %s', str(row_count))
        except Exception as exception:
            LOGGER.error(exception)
            raise
        finally:
            connection.close()
        return db_table

    def upsert(self, tmp_data, table_name, sql, if_exists='fail',
               columns=['*'], rm_tmp=True, schema=None):
        r"""
        Update/insert a dataframe into a table.

        Converts the dataframe to CSV format, and bulk loads it to a temporary
        table and then executing a raw update or update/insert query from a
        text file which extracts records from the temporary table and loads
        them into the definitive one.

        Args:
            tmp_data(Dataframe): dataframe with the data to load in a
                                    temporary table.
            table_name(String): name of the table to be
                                updated/inserted to.
            sql(string): string with the SQL update/insert query.
            if_exists(string): {‘fail’, ‘replace’, ‘append’}, default ‘fail’.
                               See `pandas.to_sql()` for details. Warning: if
                               replace is chosen, PKs will be deleted and will
                               have to be recreated.
            columns(list): list of str containing the column names to load to a
                         table. Defaults to ['*'] (all columns).
            rm_tmp(Boolean): Defauls to True. Determines if the temporary
                             table should be dropped (expected behaviour)
                             or not (for debugging purposes). It affects to
                             csv temporary file created by 'insert' method.
            schema (string): name of the database that contains the
                             destination table.
        Returns:
            db_table(Table): sqlalchemy table mapping the table with the
                                inserted/updated records.

        """
        if not schema:
            schema = self.database

        connection = self.engine.connect()
        try:
            self.insert(tmp_data, if_exists=if_exists, columns=columns,
                        rm_tmp=rm_tmp, schema=schema)
            connection.execute(sql)  # update/insert query
            if rm_tmp:
                self.drop(tmp_data.name)  # remove temporary table
            db_table = self.get_table(table_name)
            row_count = connection.engine.scalar(
                select([func.count('*')]).select_from(db_table)
            )
            LOGGER.info('Number of rows in the updated table: %s',
                        str(row_count))
        except Exception as exception:
            LOGGER.exception(exception)
            raise
        finally:
            connection.close()
        return db_table
