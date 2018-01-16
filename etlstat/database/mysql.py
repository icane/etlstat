# coding: utf-8
"""
    MySQL

    Date:
        31/10/2017

    Author:
        goi9999

    Version:
        Alpha

    Notes:


"""
import os
import MySQLdb

import numpy as np
from pandas import DataFrame, notnull
from sqlalchemy import create_engine, inspect, MetaData, Table


class MySQL:
    engine = None
    conn_string = ''
    conversion_map = {
        'object': 'VARCHAR(255)',
        'int64': 'INT',
        'float32': 'DECIMAL(20,6)',
        'float64': 'DECIMAL(20,6)'
    }

    @classmethod
    def _connect(cls, conn_string):
        if isinstance(conn_string, str):
            if (cls.conn_string is not conn_string and conn_string is not ''):
                connector, conn_data = conn_string.split('://')
                username, link, port_db = conn_data.split(':')
                port, db = port_db.split('/')
                password, ip = link.split('@')

                if connector != 'mysql+mysqlconnector':
                    raise NotImplementedError("Engine type not supported.")

                url = "{0}://{1}:{2}@{3}:{5}/{4}"

                url_str = url.format(connector, username, password, ip, db, port)

                cls.engine = create_engine(conn_string)

                cls.conn_string = conn_string
        else:
            raise TypeError("conn_string must be a string connector.")

    @classmethod
    def create(cls, table, conn_string=''):
        """
        Create a new table in database from DataFrame format.

        Args:
            table (:obj:`DataFrame`): DataFrame which name and column's label
                    match with database table's name and columns that you wish
                    create.
            conn_string (:obj:`str`, optional): String with sqlalchemy connection
                    format. Opcional if you have used before an operation with
                    the same engine.
        """
        cls._connect(conn_string)

        if(not cls.check_for_table(table.name)):
            sql = "CREATE TABLE"
            print(cls.engine)
            if isinstance(table, DataFrame):
                sql += " `{0}` (".format(table.name)

                for label in table:
                    sql += "`{0}` {1}, ".format(label, cls.conversion_map[str(table[label].dtype)])
                sql = sql[:-2] + ')'

                rts = cls.engine.execute(sql)
                rts.close()

                meta = MetaData()
                messages = Table(table.name, meta, autoload=True, autoload_with=cls.engine)
                rts_columns = [c.name for c in messages.columns]

                if len(set(list(table.columns.values)) & set(rts_columns)) == len(table.columns):
                    return True

                #print(rts.keys())
                if rts.returns_rows:
                    print(rts.rowcount)

        return False

    @classmethod
    def select(cls, table, conn_string='', conditions=''):
        """
        Select data from table.

        Args:
            table (:obj:`str` or :obj:`DataFrame`): Table's name in database if
                    you want read all fields in database table or a DataFrame
                    which name and column's label match with table's name and
                    columns table that you want read from database.
            conn_string (:obj:`str`, optional): String with sqlalchemy connection
                    format. Opcional if you have used before an operation with
                    the same engine.
            conditions (:obj:`str` or :obj:`list` of :obj:`str`, optional):
                    A select condition or list of select conditions with sql
                    syntax.
        Returns:
            :obj:`DataFrame`: A DataFrame with data from database.

        """
        cls._connect(conn_string)

        df = None
        sql = "SELECT"
        if isinstance(table, str):
            sql += " {0} FROM `{1}`".format('*', table)
        elif isinstance(table, DataFrame):
            for label in list(table):
                sql += " {0},".format(label)

            sql = sql[:-1]
            sql += " FROM `{0}`".format(table.name)
        else:
            raise TypeError("table must be a string or DataFrame.")

        if isinstance(conditions, str) and conditions is not '':
            sql += " WHERE {0}".format(conditions)
        elif isinstance(conditions, list) and len(conditions) > 0:
            sql += " WHERE"
            for condition in conditions:
                sql += " {0},".format(condition)
            sql = sql[:-1]

        rts = cls.engine.execute(MySQLdb.escape_string(sql))   # ResultProxy

        if rts.rowcount > 0:
            df = DataFrame(rts.fetchall())
            df.columns = rts.keys()

        rts.close()

        return df

    @classmethod
    def insert(cls, table, conn_string='', rows=None):
        """
        Insert DataFrame's rows in a database table.

        Args:
            table (:obj:`DataFrame`): DataFrame which name and column's label
                    match with table's name and column's name in database. It
                    must filled with data rows.
            conn_string (:obj:`str`, optional): String with sqlalchemy connection
                    format. Opcional if you have used before an operation with
                    the same engine.
            rows (:obj:`list` of int, optional): A list of row's indexes that you
                    want insert to database.
        Returns:
            int: number of rows matched.
        """
        rows_matched = 0

        cls._connect(conn_string)

        if isinstance(table, DataFrame):
            if not cls.check_for_table(table.name):
                cls.create(table)

            sql = "INSERT INTO"
            sql += " `{0}`".format(table.name)
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
                            if np.isnan(value):
                                sql_insert += "{0}, ".format('NULL')
                            else:
                                sql_insert += "{0}, ".format(value)
                    sql_insert = sql_insert[:-2] + ')'

                    rts = cls.engine.execute(sql_insert)  # ResultProxy

                    rows_matched += rts.rowcount

                    rts.close()
        else:
            raise TypeError("table must be a DataFrame.")

        return rows_matched

    @classmethod
    def update(cls, table, conn_string='', index=None):
        """
        Update rows in a database table.

        Args:
            table (:obj:`DataFrame`): DataFrame which name and column's label
                    match with table's name and columns name in database. It must
                    filled with data rows.
            conn_string (:obj:`str`, optional): String with sqlalchemy connection
                    format. Opcional if you have used before an operation with
                    the same engine.
            index (:obj:`list` of name columns): list of DataFrame's columns names
                    use as index in the update search. Other columns will be
                    updated in database.
        Returns:
            int: number of rows matched.
        """
        rows_matched = 0

        cls._connect(conn_string)

        if isinstance(table, DataFrame):
            if isinstance(index, list):
                for row in table.values:
                    sql = "UPDATE `{0}` SET".format(table.name)
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
                    rts = cls.engine.execute(MySQLdb.escape_string(sql))  # ResultProxy

                    rows_matched += rts.rowcount

                    rts.close()
        else:
            raise TypeError("table must be a DataFrame.")

        return rows_matched

    @classmethod
    def bulk_insert(cls, table, conn_string=''):
        """
        Make a bulk insert in database.

        Args:
            table (:obj:`DataFrame`): DataFrame which name and column's label
                    match with table's name and columns in database. It must
                    filled with data rows.
            conn_string (:obj:`str`, optional): String with sqlalchemy connection
                    format. Opcional if you have used before an operation with
                    the same engine.
        Returns:
            int: number of rows matched.
        """
        rows_matched = 0
        csv_path = 'temp.csv'

        cls._connect(conn_string)

        if not cls.check_for_table(table.name):
            cls.create(table)

        if isinstance(table, DataFrame):
            aux = table.replace(np.NaN,"\\N")
            aux.to_csv(csv_path, sep=';', header=False, index=False)
        else:
            raise TypeError("table must be a DataFrame.")

        sql = """LOAD DATA LOCAL INFILE '{0}' INTO TABLE `{1}` FIELDS TERMINATED BY ';' ENCLOSED BY '"' """\
              .format(csv_path, table.name)

        rts = cls.engine.execute(sql)

        rows_matched = rts.rowcount
        rts.close()

        os.remove(csv_path)

        return rows_matched

    @classmethod
    def delete(cls, table, conn_string='', conditions=''):
        """
        Delete data from table.

        Args:
            table (:obj:`str`): Database table name that you wish delete rows.
            conn_string (:obj:`str`, optional): String with sqlalchemy connection
                    format. Opcional if you have used before an operation with
                    the same engine.
            conditions (:obj:`str`, optional): A string of select conditions
            with sql syntax.
        Returns:
            int: number of rows matched.
        """
        cls._connect(conn_string)

        sql = "DELETE FROM `{0}`".format(table)

        if isinstance(conditions, str) and conditions is not '':
            sql += ' WHERE ' + conditions

        rts = cls.engine.execute(MySQLdb.escape_string(sql))

        rows_matched = rts.rowcount

        rts.close()

        return rows_matched

    @classmethod
    def check_for_table(cls, table, conn_string=''):
        """
        Check if table exists in database.

        Args:
            table (:obj:`str`): Database table's name.
            conn_string (:obj:`str`, optional): String with sqlalchemy connection
                        format. Opcional if you have used before an operation with
                        the same engine.
        Returns:
            bool: True if table exists, False in otherwise.
        """
        cls._connect(conn_string)

        return cls.engine.dialect.has_table(cls.engine, table)


