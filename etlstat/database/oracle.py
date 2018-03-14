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
import os
import re
from sqlalchemy import create_engine
from sqlalchemy.exc import DatabaseError


class Oracle:

    engine = None

    # TODO: check this conversion map
    conversion_map = {
        'object': 'VARCHAR2(256)',
        'int32': 'INTEGER',
        'int64': 'INTEGER',
        'float32': 'NUMBER',
        'float64': 'NUMBER'
    }

    def __init__(self, user, password, host, port, service_name):
        conn_string = "oracle+cx_oracle://{0}:{1}@{2}:{3}/?service_name={4}".format(
            user,
            password,
            host,
            port,
            service_name)
        self.engine = create_engine(
            conn_string,
            encoding='utf8',
            coerce_to_unicode=True,
            coerce_to_decimal=False)

    def check_for_table(self, table, schema=None):
        """
        Check if table exists in database.

        Args:
            table (str): Database table's name.
            schema (str): Database schema's name.

        Returns:
            bool: True if table exists, False in otherwise.
        """
        return self.engine.dialect.has_table(self.engine, table, schema=schema)

    def execute_sql(self, sql):
        """
        Executes a DDL or DML SQL statement.

        Args:
            sql (str): SQL statement

        Returns:
            status (bool)
            result (data frame or error)
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

    def create(self, table, schema=None):
        """
        Create a new table in database from DataFrame format.

        Args:
            table (DataFrame): its name and column's label match with database table.
            schema (str): database schema. If None, then the table is created in the
            user's default schema.
        """
        connection = self.engine.connect()

        if not self.check_for_table(table.name, schema=schema):
            sql = "CREATE TABLE "
            if schema:
                sql += "{0}.".format(schema)

            if isinstance(table, DataFrame):
                sql += "{0} (".format(table.name)

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

    def select(self, table, schema=None, conditions=''):
        """
        Select data from table.

        Args:
            table: (obj str or obj DataFrame): Table's name in database if
            you want read all fields in database table, or a DataFrame whose
            name and column's label match with table's name and columns table
            that you want to read from database.
            schema (str): database schema
            conditions (str or list): optional. A select condition or list of
            select conditions with sql syntax.

        Returns:
            df (obj DataFrame): a DataFrame with data from database.

        """
        connection = self.engine.connect()

        df = None
        sql = "SELECT "

        if schema:
            sql += "{0}.".format(schema)

        if isinstance(table, str):
            sql += "{0} FROM {1}".format('*', table)
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

    def insert(self, table, schema=None, rows=None):
        """
        Insert DataFrame's rows in a database table.

        Args:
            table (obj DataFrame): DataFrame whose name and column's label
            match with table's name and column's name in database. It must be
            filled with data rows.
            schema (str): database schema
            rows (obj list): A list of row's indexes that you want to insert into database.

        Returns:
            rows_matched (int): number of rows matched.
        """
        rows_matched = 0

        connection = self.engine.connect()

        if isinstance(table, DataFrame):
            if not self.check_for_table(table.name, schema=schema):
                self.create(table, schema=schema)

            sql = "INSERT INTO "
            if schema:
                sql += "{0}.".format(schema)
            sql += "{0}".format(table.name)
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

    def insert_many(self, table, schema=None):
        """
        Insert all DataFrame's rows in a database table.

        Args:
            table (obj DataFrame): DataFrame whose name and column's label
            match with table's name and column's name in database. It must be
            filled with data rows.
            schema (str): database schema

        Returns:
            rows_inserted (int): number of inserted rows.
        """
        if isinstance(table, DataFrame):
            if not self.check_for_table(table.name, schema=schema):
                self.create(table, schema=schema)

            sql_fields = "INSERT INTO "
            sql_values = " VALUES ("

            if schema:
                sql_fields += "{0}.".format(schema)
            sql_fields += "{0}".format(table.name)
            sql_fields += ' ('
            i = 0
            for label in list(table):
                i += 1
                sql_fields += "{0}, ".format(label)
                sql_values += " :" + str(i) + ","
            sql_fields = sql_fields[:-2]
            sql_values = sql_values[:-1]
            sql_fields += ")"
            sql_values += ")"

            rows = []
            for row in table.iterrows():
                index, data = row
                rows.append(data.tolist())

            try:
                cursor = self.engine.raw_connection().cursor()
                cursor.prepare(sql_fields + sql_values)
                cursor.executemany(None, rows)
                self.engine.raw_connection().commit()
                cursor.close()
                rows_inserted = rows.__len__()
            except DatabaseError as e:
                rows_inserted = 0
                self.engine.raw_connection().rollback()
                print(e)
            finally:
                self.engine.raw_connection().close()
        else:
            raise TypeError("table must be a DataFrame.")

        return rows_inserted

    def update(self, table, schema=None, index=None):
        """
        Update rows in a database table.

        Args:
            table: (obj DataFrame): DataFrame whose name and column's label
            match with table's name and columns name in database. It must
            be filled with data rows.
            schema (str): database schema.
            index: (obj list): list of DataFrame's columns names used as index
            in the update search. Other columns will be updated in database.

        Returns:
            rows_matched (int): number of rows matched.
        """
        rows_matched = 0

        connection = self.engine.connect()

        if isinstance(table, DataFrame):
            if isinstance(index, list):
                for row in table.values:
                    sql = "UPDATE "
                    if schema:
                        sql += "{0}.".format(schema)
                    sql += "{0} SET".format(table.name)
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

    def delete(self, table, schema=None, conditions=''):
        """
        Delete data from table.

        Args:
            table: (str): Database table name that you wish delete rows.
            schema (str): database schema
            conditions (str): optional. A string of select conditions
            to be added to WHERE clause.

        Returns:
            rows_matched (int): number of rows matched.
        """
        connection = self.engine.connect()

        sql = "DELETE FROM "
        if schema:
            sql += "{0}.".format(schema)
        sql += "{0}".format(table)

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
        Generates control and data files for Oracle SQL Loader by extracting field names and data values from a Pandas
        DataFrame. Requires Oracle Instant Client and Tools installed in the workstation.
        Destination table must exist in the database.
        Usage of SQL Loader:

            sqlldr <user>/<password> control=<control_file> [log=<log_file>] [bad=bad_file]

        Args:
            user (str): database user
            password (str): database password
            host: database server host name or IP address
            port (str): Oracle listener port
            service_name (str): Oracle instance service name
            schema (str): database schema
            table (pandas DataFrame): data frame whose name and column's label match with
            destination table's name and columns in database. It must be filled with data rows.
            output_path (str): path for output data files
            os_path (str): PATH environment variable
            os_ld_library_path (str): LD_LIBRARY_PATH environment variable
            mode (str): insertion mode: APPEND | REPLACE | TRUNCATE

        Returns:
            True if success or False
        """

        columns = ",".join(table.columns.values.tolist())

        # control file
        ctl_file = open(output_path + table.name + '.ctl', mode='w', encoding='utf8')
        ctl_header = """LOAD DATA\n""" + \
                     """CHARACTERSET UTF8\n""" + \
                     """INFILE '""" + output_path + table.name + '.dat' + """'\n""" + \
                     mode + """\n""" + \
                     """INTO TABLE """ + schema + """.""" + table.name + """\n""" + \
                     """FIELDS TERMINATED BY ';' OPTIONALLY ENCLOSED BY '\"'\n""" + \
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
        os_command = "export PATH=" + os_path + "; export LD_LIBRARY_PATH=" + os_ld_library_path + "; "
        os_command += "sqlldr " + user + "/" + password + "@" + host + ":" + port + "/" + service_name + " "
        os_command += "control='" + output_path + table.name + ".ctl' "
        os_command += "log='" + output_path + table.name + ".log' "
        os_command += "bad='" + output_path + table.name + ".bad'"

        return os.system(os_command)
