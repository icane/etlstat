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
import os

import numpy as np
from pandas import DataFrame, notnull
from sqlalchemy import create_engine, inspect, MetaData, Table, Column
from sqlalchemy.exc import DatabaseError


class Oracle:
    engine = None
    conn_string = ''
    conversion_map = {
        'object': 'VARCHAR2',
        'int': 'INTEGER',
        'float': 'NUMBER'
    }

    @staticmethod
    def split_field_map(field_map):
        field = []
        value = []
        for fm in field_map:
            field.append(fm)
            value.append(str(field_map[fm]))

        fm = """({0}) VALUES({1})""".format(', '.join(field), ', '.join(value))
        return fm

    @classmethod
    def _connect(cls, user, password, host, port, service_name):
        cls.conn_string = "oracle+cx_oracle://{0}:{1}@{2}:{3}/?service_name={4}".format(
            user,
            password,
            host,
            port,
            service_name)
        cls.engine = create_engine(cls.conn_string)

    @classmethod
    def execute_ddl(cls, ddl, user, password, host, port, service_name):
        """
        Executes a DDL query
        :param ddl: DDL SQL statement
        :param user: database user
        :param password: database password
        :param host: host name or IP address
        :param port: database connection port
        :param service_name: Oracle instance service name
        :return: status (True | False),
        """
        cls._connect(user, password, host, port, service_name)
        connection = cls.engine.connect()
        try:
            result = connection.execute(ddl)
            status = True
        except DatabaseError as e:
            result = e
            status = False
        finally:
            connection.close()
        return status, result

    @classmethod
    def select(cls, sql, user, password, host, port, service_name):
        """
        Executes a SELECT query and returns a data frame
        :param sql: SELECT query
        :param user: database user
        :param password: database password
        :param host: host name or IP address
        :param port: database connection port
        :param service_name: Oracle instance service name
        :return: pandas data frame
        """
        cls._connect(user, password, host, port, service_name)
        connection = cls.engine.connect()
        result = connection.execute(sql)
        rows = result.fetchall()
        df = DataFrame(rows, columns=result.keys())
        connection.close()
        return df

    @classmethod
    def insert(cls, table_name, field_map, user, password, host, port, service_name):
        """
        :param table_name: <table> or <schema>.<table>
        :param field_map: dictionary of field_name:value
        :param user: database user
        :param password: database password
        :param host: host name or IP address
        :param port: database connection port
        :param service_name: Oracle instance service name
        :return: True (success) or False (fail)
        """
        sql = """INSERT INTO {0} """.format(table_name)
        sql += cls.split_field_map(field_map)
        cls._connect(user, password, host, port, service_name)
        connection = cls.engine.connect()
        try:
            result = connection.execute(sql)
            status = True
        except DatabaseError as e:
            result = e
            status = False
        finally:
            connection.close()
        return status, result
