# coding: utf-8
from etlstat.database.MySql import MySql

def is_table(conn_string):
    """
    Comprueba si la tabla existe en la base de datos.
    Args:
        conn_string:    Cadena de conexión con el formato Configuration.store.conn_string.
    :return:
        True si la tabla existe en la base de datos
        False en caso contrario
    """
    first, table = conn_string.split('::')
    connector, conn_data = first.split('//')
    username, link, port_db = conn_data.split(':')
    port, database = port_db.split('/')
    password, ip = link.split('@')

    answer = False

    if connector == 'mysql+mysqlconnector:':
        mysql = MySql(ip, port, database, username, password)
        answer = mysql.check_for_table(table)
    else:
        raise NotImplementedError("Not yet!")

    return answer
