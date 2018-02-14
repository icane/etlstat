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
from pandas import DataFrame, isnull
import re
from sqlalchemy import create_engine
from sqlalchemy.exc import DatabaseError


class Oracle:

    engine = None

    # TODO: check this conversion map
    conversion_map = {
        'object': 'VARCHAR2(256)',
        'int64': 'INTEGER',
        'float64': 'NUMBER'
    }

    def __init__(self, user, password, host, port, service_name):
        conn_string = "oracle+cx_oracle://{0}:{1}@{2}:{3}/?service_name={4}".format(
            user,
            password,
            host,
            port,
            service_name)
        self.engine = create_engine(conn_string, coerce_to_unicode=True, coerce_to_decimal=False)

    def check_for_table(self, table, schema=None):
        """
        Check if table exists in database.

        :param table: (:obj:`str`): Database table's name.
        :param schema: (:obj:`str`): Database schema's name
        Returns:
            bool: True if table exists, False in otherwise.
        """
        return self.engine.dialect.has_table(self.engine, table, schema=schema)

    def execute_sql(self, sql):
        """
        Executes a DDL or DML SQL statement
        :param sql: SQL statement
        :return: status (True | False); result (data frame or error)
        """
        connection = self.engine.connect()
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

    def create(self, table):
        """
        Create a new table in database from DataFrame format.

        :param table: DataFrame which name and column's label match with database
        table's name and columns that you wish to create.
        """
        connection = self.engine.connect()

        if not self.check_for_table(table.name):
            sql = "CREATE TABLE"

            if isinstance(table, DataFrame):
                sql += " {0} (".format(table.name)

                for label in table:
                    sql += "{0} {1}, ".format(label, self.conversion_map[str(table[label].dtype)])
                sql = sql[:-2] + ')'
                try:
                    rts = connection.execute(sql)
                    rts.close()
                    status = True
                except DatabaseError as e:
                    print(e)
                    status = False
                finally:
                    connection.close()

                return status

        return False

    def select(self, table, conditions=''):
        """
        Select data from table.

        :param table: (:obj:`str` or :obj:`DataFrame`): Table's name in database if
                    you want read all fields in database table or a DataFrame
                    which name and column's label match with table's name and
                    columns table that you want read from database.
        :param conditions: (:obj:`str` or :obj:`list` of :obj:`str`, optional):
                    A select condition or list of select conditions with sql
                    syntax.
        Returns:
            :obj:`DataFrame`: A DataFrame with data from database.

        """
        connection = self.engine.connect()

        df = None
        sql = "SELECT"
        if isinstance(table, str):
            sql += " {0} FROM {1}".format('*', table)
        elif isinstance(table, DataFrame):
            for label in list(table):
                sql += " {0},".format(label)

            sql = sql[:-1]
            sql += " FROM {0}".format(table.name)
        else:
            raise TypeError("table must be a string or DataFrame.")

        if isinstance(conditions, str) and conditions is not '':
            sql += " WHERE {0}".format(conditions)
        elif isinstance(conditions, list) and len(conditions) > 0:
            sql += " WHERE"
            for condition in conditions:
                sql += " {0},".format(condition)
            sql = sql[:-1]

        try:
            rts = connection.execute(sql)
            df = DataFrame(rts.fetchall(), columns=rts.keys())
            rts.close()
        except DatabaseError as e:
            print(e)
        finally:
            connection.close()

        return df

    def insert(self, table, rows=None):
        """
        Insert DataFrame's rows in a database table.

        :param table: (:obj:`DataFrame`): DataFrame which name and column's label
                    match with table's name and column's name in database. It
                    must filled with data rows.
        :param rows: (:obj:`list` of int, optional): A list of row's indexes that you
                    want insert to database.
        Returns:
            int: number of rows matched.
        """
        rows_matched = 0

        connection = self.engine.connect()

        if isinstance(table, DataFrame):
            if not self.check_for_table(table.name):
                self.create(table)

            sql = "INSERT INTO"
            sql += " {0}".format(table.name)
            sql += ' ('
            for label in list(table):
                sql += "{0}, ".format(label)
            sql = sql[:-2]
            sql += ')'

            for index, row in table.iterrows():

                if rows is None or index in rows:
                    sql_insert = sql
                    sql_insert += " VALUES ("
                    for value in row:
                        if isinstance(value, str):
                            sql_insert += "'{0}', ".format(value)
                        else:
                            if isnull(value):
                                sql_insert += "{0}, ".format('NULL')
                            else:
                                sql_insert += "{0}, ".format(value)
                    sql_insert = sql_insert[:-2] + ')'

                    rts = connection.execute(sql_insert)
                    rows_matched += rts.rowcount

            rts.close()
            connection.close()

        else:
            raise TypeError("table must be a DataFrame.")

        return rows_matched

    def update(self, table, index=None):
        """
        Update rows in a database table.

        :param table: (:obj:`DataFrame`): DataFrame which name and column's label
                    match with table's name and columns name in database. It must
                    be filled with data rows.
        :param index: (:obj:`list` of name columns): list of DataFrame's columns names
                    use as index in the update search. Other columns will be
                    updated in database.
        Returns:
            int: number of rows matched.
        """
        rows_matched = 0

        connection = self.engine.connect()

        if isinstance(table, DataFrame):
            if isinstance(index, list):
                for row in table.values:
                    sql = "UPDATE {0} SET".format(table.name)
                    sql_conditions = ''
                    sql_updates = ''
                    for id, label in enumerate(table):
                        if label not in index:
                            if isinstance(row[id], str):
                                sql_updates += " {0}={1},".format(label, row[id])
                            else:
                                sql_updates += " {0}={1},".format(label, row[id])
                        else:
                            if isinstance(row[id], str):
                                sql_conditions += " {0}={1} and".format(label, row[id])
                            else:
                                sql_conditions += " {0}={1} and".format(label, row[id])
                    sql += sql_updates[:-1]

                    if len(sql_conditions) > 1:
                        sql += ' WHERE' + sql_conditions[:-4]

                    rts = connection.execute(sql)
                    rows_matched += rts.rowcount
                    rts.close()

                connection.close()
        else:
            raise TypeError("table must be a DataFrame.")

        return rows_matched

    def delete(self, table, conditions=''):
        """
        Delete data from table.

        Args:
        :param table: (:obj:`str`): Database table name that you wish delete rows.
        :param conditions: (:obj:`str`, optional): A string of select conditions
            with sql syntax.
        Returns:
            int: number of rows matched.
        """
        connection = self.engine.connect()

        sql = "DELETE FROM {0}".format(table)

        if isinstance(conditions, str) and conditions is not '':
            sql += ' WHERE ' + conditions

        try:
            rts = connection.execute(sql)
            rows_matched = rts.rowcount
            rts.close()
        except DatabaseError as e:
            rows_matched = None
            print(e)
        finally:
            connection.close()

        return rows_matched

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
