"""
This module manages PostgreSQL primitives.

    Date:
        25/09/2019

    Author:
        lla11358

    Version:
        0.1

    Notes:

"""

import logging
import sqlparse
from sqlalchemy import create_engine, text
from sqlalchemy.exc import DatabaseError
import pandas as pd

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)


class PostgreSQL:
    """
    Manage connections to Postgresql databases.

    Postgresql class offers some helper methods that encapsulate
    primitive logic to interactuate with the database: insert/upsert,
    execute, drop, etc.
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
        self.conn_string = f'''postgresql://{conn_params[0]}:''' + \
            f'''{conn_params[1]}@{conn_params[2]}:''' + \
            f'''{str(conn_params[3])}/{conn_params[4]}'''
        self.engine = create_engine(self.conn_string)
        self.database = conn_params[4]

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
