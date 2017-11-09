"""
    Script para reemplazar masivamente rutas de pc-axis en TICH.
    La estructura de los archivos y directorios de salida (output) que se genera es la misma
    que la de entrada (input). Dicha estructura se almacena en un directorio con el nombre de
    la encuesta TICH, tal y como se indica en el archivo de configuracion.

    Date:
        October 2017

    Author:
        rsj9999, emm13775

    Version:


    Notes:

"""
import logging as log

from etlstat.extractor.extractor import xml_in, csv_in
from etlstat.text.replace_in_xml import replace_in_xml
import os

# log configuration
log.basicConfig(level=log.INFO)
LOGGER = log.getLogger(__name__)


def main():
    #TODO: Sacar como parámetro INPUT_DATA_PATH
    job_dirs = [directory[0] + '/' for directory in os.walk(INPUT_DATA_PATH)][1:]
    for job_dir in job_dirs:
        output_dir = job_dir.replace('input', 'output')
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        jobs = xml_in(job_dir)
        mappings = csv_in(job_dir, sep=',')
        LOGGER.info("Using mapping file: " + next(iter(mappings)))
        for key, value in jobs.items():
            replace_in_xml(value, mappings[next(iter(mappings))], output_dir + key)

if __name__ == "__main__":
    main()
