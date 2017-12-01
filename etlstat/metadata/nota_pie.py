# coding: utf-8
"""
    MySQL

    Date:
        15/11/2017

    Author:
        goi9999

    Version:
        Alpha

    Notes:


"""
import pandas as pd

import icaneconfig as ic
from etlstat.database.mysql import MySQL

def nota_pie(conn_string, uri_tag, description, tabname='time_series'):
    """

    """
    answer = False

    columns = ['description', 'uri_tag']
    table = pd.DataFrame(columns=columns)
    table.loc[-1] = [description, uri_tag]
    table.index = table.index + 1
    table.name = tabname

    if MySQL.check_for_table(tabname, conn_string):
        num_rows = MySQL.update(table, index=['uri_tag'])
        if num_rows >= 1:
            answer = True

    return answer

if __name__ == '__main__':
    print(nota_pie('mysql+mysqlconnector://ign:icane@127.0.0.1:3333/metadata',
             'immigrants-persons-living-household',
             'Personas que viven en la vivienda con el inmi'))