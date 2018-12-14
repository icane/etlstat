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
from sqlalchemy import create_engine, text, select, func, MetaData, Table
from sqlalchemy.exc import DatabaseError
import pandas as pd
import numpy as np
from odo import odo


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
        meta = MetaData(bind=self.engine,
                        schema=schema if schema else self.database)
        return Table(table_name, meta, autoload=True,
                     autoload_with=self.engine)

    def execute(self, sql, **kwargs):
        """
        Execute a DDL or DML SQL statement.

        Args:
            sql: SQL statement
        Returns:
            result_set(Dataframe):

        """
        connection = self.engine.connect()
        trans = connection.begin()
        result_set = pd.DataFrame()
        try:
            result = connection.execute(text(sql), **kwargs)
            trans.commit()
            if result.returns_rows:
                result_set = pd.DataFrame(result.fetchall())
                result_set.columns = result.keys()
                LOGGER.info('Number of returned rows: %s',
                            str(len(result_set.index)))
        except DatabaseError as db_error:
            LOGGER.error(db_error)
            raise
        finally:
            connection.close()
        return result_set

    def drop(self, table_name):
        """
        Drop a table from the database.

        Args:
          table_name(str): name of the table to drop.

        Returns: nothing.

        """
        db_table = self.get_table(table_name)
        db_table.drop(self.engine, checkfirst=True)
        LOGGER.info('Table %s successfully dropped.', table_name)

        # Placeholders can only represent VALUES. You cannot use them for
        # sql keywords/identifiers.

    def insert(self, data_table, csv_path='temp.csv'):
        """
        Insert a dataframe into a table.

        Converts the dataframe to CSV format and uses odo
        bulk load functionality.

        Args:
          data_table(Dataframe): dataframe with the data to load.
          csv_path(string): path to the CSV temporal file which will be loaded
                            into the table.

        Returns:
          db_table(Table): sqlalchemy table mapping the table with the inserted
                           records.

        """
        connection = self.engine.connect()
        db_table = Table()

        if isinstance(data_table, pd.DataFrame):
            aux = data_table.replace(np.NaN, "\\N")
            aux.to_csv(csv_path, index=False)
        else:
            raise TypeError("data_table must be a DataFrame.")
        try:
            db_table = odo(csv_path,
                           f'''{self.conn_string}::{data_table.name}''',
                           local='LOCAL', has_header=True)
            row_count = connection.engine.scalar(
                select([func.count('*')]).select_from(db_table)
            )
            LOGGER.info('Number of inserted rows: %s', str(row_count))
        except Exception as exception:
            LOGGER.error(exception)
            raise
        finally:
            connection.close()
        os.remove(csv_path)
        return db_table

    def upsert(self, tmp_data, table_name, sql, csv_path='temp.csv',
               rm_tmp=True):
        """
        Update/insert a dataframe into a table.

        Converts the dataframe to CSV format, and uses odo bulk load
        functionality to load it to a temporary table and then executing
        a raw update or update/insert query from a text file which
        extracts records from the temporary table and loads them into the
        definitive one.

        Args:
            tmp_data(Dataframe): dataframe with the data to load in a
                                    temporary table.
            table_name(String): name of the table to be
                                updated/inserted to.
            sql(string): string with the SQL update/insert query.
            csv_path(string): path to the CSV temporal file which will be
                                loaded into the table.
                                Defaults to 'temp.csv'.
            rm_tmp(Boolean): Defauls to True. Determines if the temporary
                                table should be dropped (expected behaviour)
                                or not (for debugging purposes).

        Returns:
            db_table(Table): sqlalchemy table mapping the table with the
                                inserted/updated records.

        """
        connection = self.engine.connect()
        db_table = Table()
        if isinstance(tmp_data, pd.DataFrame):
            aux = tmp_data.replace(np.NaN, "\\N")
            aux.to_csv(csv_path, index=False)
        else:
            raise TypeError("tmp_table must be a DataFrame.")
        try:
            tmp_table = odo(csv_path,
                            f'''{self.conn_string}::{tmp_data.name}''',
                            local='LOCAL', has_header=True)
            tmp_row_count = connection.engine.scalar(
                select([func.count('*')]).select_from(tmp_table)
            )
            LOGGER.info('Number of temp table rows: %s', str(tmp_row_count))
            os.remove(csv_path)  # remove temporary file
            connection.execute(sql)  # update/insert query
            if rm_tmp:
                self.drop(tmp_table.name)  # remove temporary table
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
