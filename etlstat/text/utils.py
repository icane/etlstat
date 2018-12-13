# coding: utf-8
"""Text parsing related utils.

This module contains several text related utils which have been considered
to be reusable in different ETL projects.

Date: 11/12/2018
Author: emm13775
Version: 0.1
Notes:

"""

import os
import re
import logging
from unidecode import unidecode
from etlstat.extractor.extractor import xml, csv

# log configuration
logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)


def parse_df_columns(data):
    """Parse columns in a dataframe.

    Converts capital letters into lowercase letters, white spaces into low
    bar and removes accent marks.

    Args:
        data(Dataframe): pandas dataframe with the columns to parse.
    Returns:
        data(Dataframe): pandas dataframe with the parsed columns.

    """
    parsed_columns = []
    for column in data.columns:
        to_replace = re.sub(r'[ ]{3,}', ' ',
                            unidecode(column.lower().replace(',', '')))
        parsed_columns.append(re.sub(r'[ ]{2,}', '',
                                     to_replace).replace(' ', '_'))
    data.columns = parsed_columns
    return data


def replace_urls_in_xml(tree, urls_df, output_file):
    """Replace URLs in Pentaho Data Integration (Kettlt) xml files.

    First thought for PC-Axis URLs, but it can be used with any generic URL.
    Produces a xml file which is written to the output file path parameter.


    Args:
        tree (ElementTree): ElementTree object with the parsed xml file.
        urls_df (DataFrame): pandas DataFrame with a mapping between the
        old and the new urls. This dataframe MUST HAVE two columns:
        old_urls and new_urls, containing the URLs to replace and the new
        ones respectively.
        output_file (str): output file path including file name.

    """
    urls_dict = dict(zip(urls_df.old_urls, urls_df.new_urls))
    root = tree.getroot()
    entries = root.find('entries')

    urls = []
    found = {}

    for entry in entries:
        url = entry.find('url')
        if url is not None:
            splitted = re.split('&url=', url.text)
            url_found = urls_dict.get(splitted[1], None)
            if url_found:
                LOGGER.info("URL found: %s", str(url_found))
                found[splitted[1]] = found.get(splitted[1], 0) + 1
                url.text = splitted[0] + '&url=' + urls_dict[splitted[1]]
                urls.append(url.text)
            else:
                LOGGER.info("URL NOT found: %s", splitted[1])
    LOGGER.info("Found total unique URLS:  %s", str(len(found.keys())))
    tree.write(output_file)


def bulk_replace_url_in_xml(input_data_path, output_data_path=None):
    """Massively replaces URLs in a bunch of PDI (Kettle) XML files.

    Args:
        input_data_path: path to the directory which contains the URL mappings
        in a two column CSV file and the PDI (Kettle) XML Files.
        output_data_path: path to the directory which contains the parsed XML
        files. If not specified, defaults to None and then an 'output'
        directory is created inside of 'input_data_path' directory, so that
        the results can be written there.
    """
    job_dirs = [directory[0] +
                '/' for directory in os.walk(input_data_path)][1:]
    for job_dir in job_dirs:
        if os.listdir(job_dir):  # check if directory is empty
            output_dir = output_data_path if output_data_path \
                        else input_data_path + 'output/'
            if not os.path.exists(output_dir):
                os.makedirs(output_dir)
            jobs = xml(job_dir)
            mappings = csv(job_dir, sep=',')
            if mappings:
                LOGGER.info("Using mapping file: %s",
                            str(next(iter(mappings))))
                for key, value in jobs.items():
                    replace_urls_in_xml(value,
                                        mappings[next(iter(mappings))],
                                        output_dir + key)
