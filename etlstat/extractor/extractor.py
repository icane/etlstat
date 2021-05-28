# -*- coding: utf-8 -*-

"""Extractor module: collection of functions for massively data extraction.

This module offers several helping methods and functions to ease massively
data extraction from different file formats. Support for XLS, CSV, PX, XML, SQL
and TXT positional files is included. Extractor allows to read every file in
a directory and creates a dictionary keeping their file names as keys and
their data as values. If possible, these data are parsed into a pandas
dataframe.

"""

import fnmatch
import os
from contextlib import ExitStack

import Levenshtein

import defusedxml.ElementTree as ET

import numpy as np

import pandas as pd

from pyaxis import pyaxis


def match_data_format(data_path, data_extension,
                      format_path, format_extension):
    """Match format files(csv) with data files (txt) for positional files.

    Args:
        data_path (str): directory containing data files.
        data_extension (str): standard for data filenames extensions.
        format_path (str): directory containing format files.
        format_extension (str): standard for format filenames extensions.

    Returns:
        dict: Data filenames (TXT) as KEYS and format filenames (CSV) as
              VALUES.

    """
    assignation_map = {}
    # Contains data filenames
    data_list = []
    os.chdir(data_path)
    for file in os.listdir('.'):
        # Finds every data file in a directory
        if fnmatch.fnmatch(file, data_extension):
            data_list.append(file)
    if not data_list:
        raise FileNotFoundError("No data files found in data directory")
    data_list = set(data_list)
    data_list = list(data_list)

    # Contains format filenames
    format_list = []
    os.chdir(format_path)
    for file in os.listdir('.'):
        # Finds every format file in a directory
        if fnmatch.fnmatch(file, format_extension):
            format_list.append(file)
    if not format_list:
        raise FileNotFoundError("No format files found in format directory")
    format_list = set(format_list)
    format_list = list(format_list)

    for item in data_list:
        max_similarity = 0
        max_element = None
        for element in format_list:
            similarity = Levenshtein.ratio(item, element)
            if similarity > max_similarity:
                max_similarity = similarity
                max_element = element
        assignation_map[item] = max_element
        # logger.info("Matched data file: " + item + " with: " +
        #             assignation_map[item])
    return assignation_map


def xls(dir_path, sep=';', encoding='utf-8',
        data_extension='*.[xX][lL][sS]', na_values=None):
    """Massively read XLS files from a directory.

    Read excel files in a directory and generate a dict with xls names and
    sheetnames as keys and dataframes with the data as values.

    Args:
        dir_path (str): directory containing Excel files.
        sep (str): field separator. Unused. Keep for backwards compatibility.
        encoding (str): file encoding. Unused. Keep for backwards compatibility.
        data_extension (str): standard for data filenames extensions.
        na_values (scalar, str, list-like, or dict) : Additional strings to
                                                      recognize as NA/NaN.

    Returns:
        dict: Excel name and sheet_names as KEYS and dataframe as VALUE.

    """
    os.chdir(dir_path)
    data = {}
    for excel in os.listdir('.'):
        if fnmatch.fnmatch(excel, data_extension):
            data[excel] = dict()
            data[excel] = pd.read_excel(open(dir_path + excel, 'rb'),
                                        sheet_name=None,
                                        na_values=na_values
                                        )
            for sheet in data[excel]:
                data[excel][sheet].name = sheet
    return data


def xlsx(dir_path, sep=';', encoding='utf-8',
         data_extension='*.[xX][lL][sS][xX]',
         na_values=None):
    """Massively read XLSX files from a directory.

    Read excel files in a directory and generate a dict with xls names and
    sheetnames as keys and dataframes with the data as values.

    Args:
        dir_path (str): directory containing Excel files.
        sep (str): field separator. Unused. Keep for backwards compatibility.
        encoding (str): file encoding. Unused. Keep for backwards compatibility.
        data_extension (str): standard for data filenames extensions.
        na_values (scalar, str, list-like, or dict) : Additional strings to
                                                      recognize as NA/NaN.

    Returns:
        dict: Excel name and sheet_names as KEYS and dataframe as VALUE.

    """
    return xls(dir_path,
               data_extension=data_extension, na_values=na_values)


def csv(
        dir_path,
        data_extension='*.[cC][sS][vV]',
        dtype=None, encoding='utf-8',
        na_values=None,
        sep=';',
        skipinitialspace=False
):
    """Massively read CSV files from a directory.

    Read csv files in a directory and generate a dict with csv names as keys
    and dataframes as values.

    Args:
        dir_path (str): directory containing Csv files.
        data_extension (str): standard for data filenames extensions.
        dtype (str, dict): data type for data or columns.
        encoding (str): file encoding.
        na_values (scalar, str, list-like, or dict) : Additional strings to
                                                      recognize as NA/NaN.
        sep (str): field separator.
        skipinitialspace (bool): skip spaces after delimiter.

    Returns:
        dict: Csv name as KEY and dataframe as VALUE

    """
    os.chdir(dir_path)
    data = {}
    for file in os.listdir('.'):
        if fnmatch.fnmatch(file, data_extension):
            data[file] = pd.read_csv(dir_path + file,
                                     dtype=dtype,
                                     encoding=encoding,
                                     na_values=na_values,
                                     sep=sep,
                                     skipinitialspace=skipinitialspace)
            data[file].name = file
    return data


def px(filename, sep=",", csv_encoding='windows-1252',
       px_encoding='ISO-8859-2', timeout=10, null_values=r'^"\."$',
       sd_values=r'"\.\."'):
    """Massively read PC-Axis files from a list of URLs in a CSV file.

    Read and convert PC-Axis files to dataframes from URIs listed in a CSV
    file.

    Args:
        filename (str): CSV FILE with uris file path (including file name) or
                        directory containing data files.
        sep (str): field separator for the CSV files with the URLs.
        encoding (str): file encoding for both the CSV and px file.
        timeout (int): request timeout in seconds; optional
        null_values(str): regex with the pattern for the null values in the px
                          file. Defaults to '.'.
        sd_values(str): regex with the pattern for the statistical disclosured
                        values in the px file. Defaults to '..'.
    Returns:
        dict: file names as keys and dataframes as values.

    """
    data = {}
    if fnmatch.fnmatch(filename, '*.csv'):
        data = _px_from_urls_in_csv(filename, sep=sep,
                                    csv_encoding=csv_encoding,
                                    px_encoding=px_encoding, timeout=timeout,
                                    null_values=null_values,
                                    sd_values=sd_values)
    elif os.path.isdir(filename):
        data = _px_from_path(filename, encoding=px_encoding, timeout=timeout,
                             null_values=null_values, sd_values=sd_values)
    else:
        raise TypeError
    return data


def _px_from_urls_in_csv(filename, sep=",", csv_encoding='windows-1252',
                         px_encoding='ISO-8859-2', timeout=10,
                         null_values=r'^"\."$', sd_values=r'"\.\."'):
    """Massively read PC-Axis files from a list of URLs in a CSV file.

    Read and convert PC-Axis files to dataframes from URIs listed in a CSV
    file.

    Args:
        filename (str): CSV FILE with uris file path (including file name).
        sep (str): field separator for the CSV files with the URLs.
        csv_encoding (str): file encoding for the CSV file.
        px_encoding (str): file encoding for the px file.
        timeout (int): request timeout in seconds; optional
        null_values(str): regex with the pattern for the null values in the px
                          file. Defaults to '.'.
        sd_values(str): regex with the pattern for the statistical disclosured
                        values in the px file. Defaults to '..'.
    Returns:
        dict: file names as keys and dataframes as values.

    """
    uris = pd.read_csv(filename,
                       sep=sep,
                       encoding=csv_encoding)
    data = {}
    uris['data'] = uris.apply(lambda row: pyaxis.parse(
        row['url'], px_encoding, timeout=timeout, null_values=null_values,
        sd_values=sd_values)['DATA'], axis=1)
    data = pd.Series(uris['data'].values, index=uris['id']).to_dict()
    return data


def _px_from_path(dir_path, encoding='ISO-8859-2', timeout=10,
                  null_values=r'^"\."$', sd_values=r'"\.\."'):
    """Massively read PC-Axis files from a directory.

    Read files in a directory, convert to dataframe and store in a dict.

    Args:
        dir_path (str): directory containing data files.
        encoding (str): file encoding for both the px file.
        timeout (int): request timeout in seconds; optional
        null_values(str): regex with the pattern for the null values in the px
                          file. Defaults to '.'.
        sd_values(str): regex with the pattern for the statistical disclosured
                        values in the px file. Defaults to '..'.

    Returns:
        dict: Name of px file as KEY and dataframe as VALUE.

    """
    files = {}
    os.chdir(dir_path)
    with ExitStack() as context_manager:
        for filename in os.listdir('.'):
            if fnmatch.fnmatch(filename, '*.px'):
                px_df = pyaxis.parse(filename, encoding,
                                     timeout=timeout, null_values=null_values,
                                     sd_values=sd_values)['DATA']
                files[filename.split(dir_path, 1)[-1][:-3]] = px_df
        context_manager.pop_all().close()
    return files


def txt(dir_path, sep=';', encoding='windows-1252',
        format_extension='*.[cC][sS][vV]', data_extension='*.[tT][xX][tT]',
        na_values=None, format_path=None):
    """Massively read positional text files from a directory.

    Read files in a directory, generate a correspondence between data and
    format files and return a dict (MICRODATA). It uses match_data_format() in
    order to generate the dataframe.

    Args:
        dir_path (str): directory containing data files.
        sep (str): field separator.
        encoding (str): file encoding.
        format_extension (str): standard for format name.
        data_extension (str): standard for data name.
        na_values (scalar, str, list-like, or dict) : Additional strings to
                                                      recognize as NA/NaN.
        format_path (str): directory containing format files.
                           Defaults to dir_path.


    Returns:
        dict: Name of data file as KEY and dataframe as VALUE.

    """
    conversion_map = {
        'STRING': str,
        'NUMBER': np.float32,
        'DECIMAL': np.float32,
        'INTEGER': np.float32
    }

    if not format_path:
        format_path = dir_path

    assignation_map = match_data_format(dir_path, data_extension, format_path,
                                        format_extension)
    for txt_file in assignation_map:
        conversion = dict()
        assignation_map[txt_file] = pd.read_csv(format_path +
                                                assignation_map[txt_file],
                                                sep=sep,
                                                encoding=encoding)
        for line in range(len(assignation_map[txt_file])):
            conversion[assignation_map[txt_file]['FIELD_NAME'][line]
                       ] = conversion_map[assignation_map[txt_file]
                                          ['DATA_TYPE'][line]]
        assignation_map[txt_file] = pd.read_fwf(dir_path + txt_file,
                                                widths=assignation_map
                                                [txt_file]['LENGTH'].tolist(),
                                                names=assignation_map[txt_file]
                                                ['FIELD_NAME'].tolist(),
                                                dtype=conversion,
                                                nwords=0,
                                                encoding=encoding,
                                                na_values=na_values)
        assignation_map[txt_file].name = txt_file
    return assignation_map


def xml(dir_path, pattern='*.[xXkK][mMtTjJ][lLrRbB]'):
    """Massively read XML files from a directory.

    Read files in a directory filtered by regEx and generate a dict with xml
    file names as keys and etree objects as values.

    Args:
        dir_path (str): directory containing XML files.
        pattern (str): regEx to filter data names.

    Returns:
        dict: XML name as KEY and etree object as VALUE.

    """
    xml_list = []
    os.chdir(dir_path)
    for file in os.listdir('.'):
        if fnmatch.fnmatch(file, pattern):
            xml_list.append(file)
    xml_list = set(xml_list)
    xml_list = list(xml_list)
    df_dict = dict.fromkeys(xml_list, '')

    for i in enumerate(xml_list):  # warning: enumerate returns tuples
        df_dict[xml_list[i[0]]] = ET.parse(dir_path + xml_list[i[0]])
    return df_dict


def sql(dir_path):
    """Massively read SQL files from a directory.

    Read every SQL file in a directory and generates a dict with the file
    names as keys and the content of the files as values.

    Args:
        dir_path (str): Path of DIR read.

    Returns:
        dict: query name  as KEY and query as VALUE.

    """
    files = {}
    os.chdir(dir_path)
    with ExitStack() as context_manager:
        for filename in os.listdir('.'):
            if fnmatch.fnmatch(filename, '*.sql'):
                sql_file = context_manager.enter_context(open(filename, 'r'))
                files[filename.split(dir_path, 1)[-1][:-4]] = sql_file.read()
        context_manager.pop_all().close()
    return files
