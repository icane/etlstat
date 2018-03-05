# coding: utf-8
"""
    SimpleSQL

    Date:
        02/03/2018

    Author:
        slavebuffer

    Version:
        Alpha

    Notes:


"""

from etlstat.database.mysql import MySQL


class SimpleSQL:

    @classmethod
    def parse_conn_string(cls, config):
        """
        Split conn_string, acquired from Configuration object into db_params and
        database_name.

        Args:
            config (Configuration): object Configuration filled with all params
                                    needed.
        Returns:
            db_params (str[]): user, password, host and port terms into list.
            database_name (str): name of database schema.
        """
        conn_string = config.store.conn_string
        connector, conn_data = conn_string.split('//')
        user, link, port_db = conn_data.split(':')
        port, database_name = port_db.split('/')
        password, host = link.split('@')

        db_params = None
        if connector == 'mysql+mysqlconnector:':
            db_params = [user, password, host, port]
        else:
            raise NotImplementedError("Not implemented driver for this connection!")

        return db_params, database_name

    @classmethod
    def push(cls, table, config):
        """
        Insert dataframe's data into database table. If table doesn't exist, push
        will create it.

        Args:
            table (DataFrame): a pandas DataFrame object filled with data that you
                               want insert into database.
            config (Configuration): object Configuration filled with all params
                                    needed.
        """
        db_params, database_name = cls.parse_conn_string(config)

        conn = MySQL(*db_params, database_name)

        if not conn.check_for_table(table.name):
            conn.create(table)
            conn.execute_sql("ALTER TABLE %s ADD COLUMN `id` INT PRIMARY KEY AUTO_INCREMENT" % table.name)
            if len(table) > 0 and len(table) < 10:
                conn.insert(table)
            elif len(table) > 9:
                conn.bulk_insert(table)
        else:
            conn.insert(table)

    @classmethod
    def pull(cls, table, config):
        """
        Request data columns dataframe from database.

        Args:
            table (DataFrame): a empty pandas DataFrame object structured
                               with data columns.
            config (Configuration): object Configuration filled with all params
                                    needed.
        Returns:
            result (DataFrame): a pandas DataFrame filled with data from database.
        """
        db_params, database_name = cls.parse_conn_string(config)

        conn = MySQL(*db_params, database_name)

        result = None
        if isinstance(table, str):
            if conn.check_for_table(table):
                result = conn.select(table)
        else:
            if conn.check_for_table(table.name):
                result = conn.select(table)

        return result

    @classmethod
    def drop(cls, table_name, config):
        """
        Drop database table called table_name.

        Args:
            table_name (str): database table name.
            config (Configuration): object Configuration filled with all params
                                    needed.
        """
        db_params, database_name = cls.parse_conn_string(config)

        conn = MySQL(*db_params, database_name)

        if conn.check_for_table(table_name):
            conn.execute_sql("DROP TABLE %s" % table_name)