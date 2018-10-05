import csv
import fnmatch
import os
import xml.etree.ElementTree as ET
from contextlib import ExitStack
import xlrd

import numpy as np
import pandas as pd
from Levenshtein import ratio
from etlstat.extractor.pcaxis import *


def data_format(data_path, data_extension, format_path, format_extension):
    """
    Function that matches format files(csv) with data files (txt) in a directory
    for positional files. (Internal Method)

    Args:
        data_path (str): directory containing data files.
        data_extension (str): standard for data filenames extensions.
        format_path (str): directory containing format files.
        format_extension (str): standard for format filenames extensions.

    Returns:
        dict: Data filenames (TXT) as KEYS and format filenames (CSV) as VALUES.
    """
    assignation_map = {}
    # Contains data filenames
    data_list = []
    os.chdir(data_path)
    for file in os.listdir('.'):
        # Finds all data files in a directory
        if fnmatch.fnmatch(file, data_extension):
            data_list.append(file)
    if len(data_list) == 0:
        raise FileNotFoundError("No data files found in data directory")
    data_list = set(data_list)
    data_list = list(data_list)

    # Contains format filenames
    format_list = []
    os.chdir(format_path)
    for file in os.listdir('.'):
        # Finds all format files in a directory
        if fnmatch.fnmatch(file, format_extension):
            format_list.append(file)
    if len(format_list) == 0:
        raise FileNotFoundError("No format files found in format directory")
    format_list = set(format_list)
    format_list = list(format_list)

    for item in data_list:
        max_similarity = 0
        max_element = None
        for element in format_list:
            similarity = ratio(item, element)
            if similarity > max_similarity:
                max_similarity = similarity
                max_element = element
        assignation_map[item] = max_element
        logger.info("Matched data file: " + item + " with: " + assignation_map[item])
    return assignation_map


def excel_in(dir_path, sep=';', encoding='utf-8', data_extension='*.[xX][lL][sS]', na_values=None):
    """
    Function that reads excel files in a directory and generates a
    dict with xls names and sheetnames as keys of the dataframe.


    Args:
        dir_path (str): directory containing Excel files.
        sep (str): field separator.
        encoding (str): file encoding.
        data_extension (str): standard for data filenames extensions.
        na_values (scalar, str, list-like, or dict) : Additional strings to recognize as NA/NaN.

    Returns:
        dict: Excel name and sheet_names as KEYS and dataframe as VALUE.
    """
    os.chdir(dir_path)
    data = {}
    for excel in os.listdir('.'):
        if fnmatch.fnmatch(excel, data_extension):
            data[excel] = dict()
            book = xlrd.open_workbook(dir_path + excel)
            for sheet in book.sheet_names():
                data[excel][sheet] = pd.read_excel(open(dir_path + excel, 'rb'),
                                                   encoding=encoding,
                                                   sep=sep,
                                                   sheet_name=sheet,
                                                   na_values=na_values
                                                   )
            data[excel][sheet].name = sheet
    return data


def csv_in(
        dir_path,
        data_extension='*.[cC][sS][vV]',
        dtype=None, encoding='utf-8',
        na_values=None,
        sep=';',
        skipinitialspace=False
):
    """
    Function that reads csv files in a directory and generates a
    dict with csv names as keys of the dataframe.

    Args:
        dir_path (str): directory containing Csv files.
        data_extension (str): standard for data filenames extensions.
        dtype (str, dict): data type for data or columns.
        encoding (str): file encoding.
        na_values (scalar, str, list-like, or dict) : Additional strings to recognize as NA/NaN.
        sep (str): field separator.
        skipinitialspace (bool): skip spaces after delimiter.

    Returns:
        dict: Csv name as KEY and dataframe as VALUE
    """
    os.chdir(dir_path)
    data = {}
    for csv in os.listdir('.'):
        if fnmatch.fnmatch(csv, data_extension):
            data[csv] = pd.read_csv(dir_path + csv,
                                    dtype=dtype,
                                    encoding=encoding,
                                    na_values=na_values,
                                    sep=sep,
                                    skipinitialspace=skipinitialspace)
        data[csv].name = csv
    return data


def pc_axis_in(filename, sep=",", encoding='windows-1252'):
    """
    Reads and converts Pc-Axis files to dataframe from URIs listed
    in a CSV file.

    Args:
        filename (str): CSV FILE with uris file path (including file name).
        sep (str): field separator.
        encoding (str): file encoding.
    Returns:
        dict: px name as KEY and dataframe as VALUE
    """
    csv = pd.read_csv(filename,
                      sep=sep,
                      encoding=encoding,
                      header=None)
    data = {}
    for row in range(len(csv)):
        data[csv[0][row]] = from_pc_axis(csv[1][row], encoding)['DATA']
    return data


def positional_in(dir_path, sep=';', encoding='windows-1252', format_extension='*.[cC][sS][vV]',
                  data_extension='*.[tT][xX][tT]', na_values=None, format_path=None):
    """
    Function that reads files in a directory, generates a correspondence
    between data files and format files and returning a dict. (MICRODATA).
    This method also calls data_format() in order to generate
    the dataframe.

    Args:
        dir_path (str): directory containing data files.
        sep (str): field separator.
        encoding (str): file encoding.
        format_extension (str): standard for format name.
        data_extension (str): standard for data name.
        na_values (scalar, str, list-like, or dict) : Additional strings to recognize as NA/NaN.
        format_path (str): directory containing format files. Defaults to dir_path


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

    assignation_map = data_format(dir_path, data_extension, format_path, format_extension)
    for txt in assignation_map:
        conversion = dict()
        assignation_map[txt] = pd.read_csv(format_path + assignation_map[txt], sep=sep,
                                           encoding=encoding)
        for line in range(len(assignation_map[txt])):
            conversion[assignation_map[txt]['FIELD_NAME'][line]] = conversion_map[assignation_map[txt]['DATA_TYPE'][line]]
        assignation_map[txt] = pd.read_fwf(dir_path + txt,
                                           widths=assignation_map[txt]['LENGTH'].tolist(),
                                           names=assignation_map[txt]['FIELD_NAME'].tolist(),
                                           dtype=conversion,
                                           nwords=0,
                                           encoding=encoding,
                                           na_values=na_values)
        assignation_map[txt].name = txt
    return assignation_map


def xml_in(dir_path, pattern='*.[xXkK][mMtTjJ][lLrRbB]'):
    """
    Function that reads files in a directory filtered by regEx and generates a
    dict with xml names.

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
    for i in range(len(xml_list)):
        df_dict[xml_list[i]] = ET.parse(dir_path + xml_list[i])
    return df_dict


def sql_in(dir_path):
    """
    Function that open sql files in the sql directory using a context manager.
    Args:
        dir_path (str): Path of DIR readed.

    Returns:
        dict: query name  as KEY and query as VALUE.
    """
    files = {}
    os.chdir(dir_path)
    with ExitStack() as cm:
        for filename in os.listdir('.'):
            if fnmatch.fnmatch(filename, '*.sql'):
                f = cm.enter_context(open(filename, 'r'))
                files[filename.split(dir_path, 1)[-1][:-4]] = f.read()
        cm.pop_all().close()
    return files
