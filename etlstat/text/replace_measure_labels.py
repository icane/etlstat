"""
    Actualiza tabla metadata.measure
    Reemplaza las etiquetas en el fichero de esquema

    Date:
        16/04/2018

    Author:
        lla11358, rsj9999

    Notes:

"""


import csv
from etlstat.database.mysql import *

user = 'metadata'
password = 'u8ytrFkV'
host = 'sicanedev01.intranet.gobcantabria.es'
port = '3306'
database = 'metadata'
conn_params = [user, password, host, port, database]
mysql = MySQL(*conn_params)

def replace_measure_labels(input_file, output_file):
    """Replace measure labels in xml files and measure table in database. Produces an xml file written into
       the output file path parameter.

          Args:
              input_file (str): input file path
              output_file: output file path
    """

    # Leer fichero csv
    with open(input_file) as csvfile:
        # input_file example: '/var/git/python/icanetl/economia/id_innovacion_tic/tic/data/input/etiquetas_medidas_TIC.csv'
        reader = csv.DictReader(csvfile, delimiter=';')
        for row in reader:

            str_sql = "UPDATE measure SET title = '{0}' WHERE node_id = (SELECT id FROM time_series " \
                      "WHERE uri_tag = '{1}') AND title = '{2}'".format(row['ETIQUETA_NUEVA'], row['URI_TAG'], row['ETIQUETA_VIEJA'])

            mysql.execute_sql(str_sql)

            sed = "sed -i 's${0}${1}$g'" + output_file + "." + format(row['ETIQUETA_VIEJA'], row['ETIQUETA_NUEVA'])
            # output_file example: /var/git/python/icanetl/text/economy.xml
            print(row['URI_TAG'])
            print(row['ETIQUETA_VIEJA'])
            os.system(sed)



