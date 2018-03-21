import csv
import fnmatch
import os
import xml.etree.ElementTree as ET
from contextlib import ExitStack

import numpy as np
import pandas as pd
from Levenshtein import ratio
from etlstat.extractor.pcaxis import *

conversion_map = {
    'STRING': str,
    'NUMBER': np.float32,
    'DECIMAL': np.float32,
    'INTEGER': np.int32
}


def data_format(dir_path, data, suffix='formato'):
    """
    Function that matches format files(csv's) with data files in a directory.

    Args:
        dir_path (str): directory containing files.
        data (str): type of data format(excel,csv,txt)
        suffix (str): standard for format name

    Returns:
        dict:   Excel name and sheet_names as KEYS and data frame as VALUE if we read excels.
                Data files names as KEYS and data frame as VALUE in other cases.
    """
    format_map = {
        'excel': '*.[xX][lL][sS]',
        'txt': '*.[tT][xX][tT]',
        'csv': '*[!' + suffix + '].[cC][sS][vV]'
    }
    # regEx for the name of format files
    format = '*' + suffix + '*.[cC][sS][vV]'
    assignation_map = {}
    # Contains data file names
    data_list = []
    # Contains format file names
    format_list = []
    os.chdir(dir_path)
    for file in os.listdir('.'):
        # Finds all format files in a directory
        if fnmatch.fnmatch(file, format):
            format_list.append(file)
        # Finds all data files in a directory
        if fnmatch.fnmatch(file, format_map[data]):
            data_list.append(file)
    format_list = set(format_list)
    format_list = list(format_list)
    if len(format_list) == 0:
        raise FileNotFoundError("Not format files found in the directory")
    data_list = set(data_list)
    data_list = list(data_list)
    for item in data_list:
        if (data == 'excel'):
            max_format = None
            max_data = None
            max_format_similarity = 0
            max_data_similarity = 0
            # I create a key for each excel
            assignation_map[item] = dict()
            assignation_map[item]['data'] = None
            assignation_map[item]['format'] = None
            # I add '_datos' and '_hoja' to data string in order to ensure a correct assignment
            data_str = item[:-4] + '_datos' + item[-4:]
            sheet_str = item[:-4] + '_hoja' + item[-4:]
            for element in format_list:
                # I check the similarity between format files and data format files
                similarity_data = ratio(data_str, element)
                # I check the similarity between format files and sheet format files
                similarity_sheet = ratio(sheet_str, element)
                if similarity_data > max_data_similarity:
                    max_data_similarity = similarity_data
                    # I take the maximum format data element
                    max_data = element
                if similarity_sheet > max_format_similarity:
                    max_format_similarity = similarity_sheet
                    # I take the maximum format sheet element
                    max_format = element
            assignation_map[item]['data'] = max_data
            assignation_map[item]['format'] = max_format
            logger.info("Matched data file: " + item + " with: " + assignation_map[item]['data'] + ' and ' +
                        assignation_map[item]['format'])

        else:
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


def excel_in(dir_path, sep=';', encoding='utf-8', suffix='formato', na_values=None):
    """
    Function that reads excel files in a directory and generates a
    dict with xls names and sheetnames as keys of the dataframe.
    This method also calls data_format() in order to generate
    the data frame.

    Args:
        dir_path (str): directory containing Excel files.
        sep (str): field separator.
        encoding (str): file encoding.
        suffix (str): standard for format name.
        na_values (scalar, str, list-like, or dict) : Additional strings to recognize as NA/NaN.

    Returns:
        dict: Excel name and sheet_names as KEYS and data frame as VALUE
    """
    data = dict()
    assignation_map = data_format(dir_path, 'excel', suffix)
    for excel in assignation_map:
        data[excel] = dict()
        # I read sheet format files (csv)
        file = pd.read_csv(dir_path + assignation_map[excel]['format'], sep=sep,
                           encoding=encoding)
        for line in range(len(file)):
            # I create a key for each sheet_name (data[excel][sheet_name])
            sheetname = file['SHEET_NAME'][line]
            data[excel][sheetname] = dict()
            data[excel][sheetname]['START_ROW'] = file['START_ROW'][line]
            data[excel][sheetname]['START_COLUMN'] = file['START_COLUMN'][line]
            data[excel][sheetname]['SKIP_FOOTER'] = file['SKIP_FOOTER'][line]
            data[excel][sheetname]['FIELD_NAME'] = []
            data[excel][sheetname]['DATA_TYPE'] = dict()
        # I read data format files (csv)
        file = pd.read_csv(dir_path + assignation_map[excel]['data'], sep=sep,
                           encoding=encoding)
        # I declare FIELD NAMES and DATA_TYPE(must be a DICT)
        for line in range(len(file)):
            sheetname = file['SHEET_NAME'][line]
            data[excel][sheetname]['FIELD_NAME'].append(file['FIELD_NAME'][line])
            data[excel][sheetname]['DATA_TYPE'][str(file['FIELD_NAME'][line])] = conversion_map[
                file['DATA_TYPE'][line]]

        for sheet in data[excel]:
            data[excel][sheet] = pd.read_excel(open(dir_path + excel, 'rb'),
                                               sheet_name=str(sheet),
                                               skiprows=data[excel][sheet]['START_ROW'] - 1,
                                               skip_footer=data[excel][sheet]['SKIP_FOOTER'],
                                               names=data[excel][sheet]['FIELD_NAME'],
                                               dtype=data[excel][sheet]['DATA_TYPE'],
                                               encoding=encoding,
                                               na_values=na_values)
            data[excel][sheet].name = sheet

    return data


def csv_in(dir_path, sep=';', encoding='utf-8', suffix='formato', na_values=None):
    """
    Function that reads csv files in a directory and generates a
    dict with csv names as keys of the dataframe.
    This method also calls data_format() in order to generate
    the data frame.

    Args:
        dir_path (str): directory containing Csv files.
        sep (str): field separator
        encoding (str): file encoding
        suffix (str): standard for format name
        na_values (scalar, str, list-like, or dict) : Additional strings to recognize as NA/NaN.

    Returns:
        dict: Csv name as KEY and data frame as VALUE
    """
    assignation_map = data_format(dir_path, 'csv', suffix)
    for csv in assignation_map:
        conversion = dict()
        assignation_map[csv] = pd.read_csv(dir_path + assignation_map[csv], sep=sep,
                                encoding=encoding)
        for line in range(len(assignation_map[csv])):
            conversion[assignation_map[csv]['FIELD_NAME'][line]] = conversion_map[assignation_map[csv]['DATA_TYPE'][line]]
        assignation_map[csv] = pd.read_csv(dir_path + csv,
                                dtype=conversion,
                                encoding=encoding,
                                na_values=na_values,
                                sep=sep)
        assignation_map[csv].name = csv
    return assignation_map


def pc_axis_in(dir_path, sep=",", encoding='windows-1252'):
    """
    Reads and converts Pc-Axis files to dataframe from URIs listed
    in a CSV file.

    Args:
        dir_path (str): extractor with uris file path (including file name).
        sep (str): field separator
        encoding (str): file encoding
    Returns:
        dict: URL as KEY and dataframe as VALUE
    """
    pc_axis_dict = {}
    with open(dir_path, "rt") as f:
        reader = csv.reader(f, delimiter=sep)
        for row in reader:
            return_dict = from_pc_axis(row[1], encoding)
            pc_axis_dict[row[0]] = return_dict['DATA']

    return pc_axis_dict


def positional_in(dir_path, sep=';', encoding='windows-1252', suffix='', na_values=None):
    """
    Function that reads files in a directory, generates a correspondence
    between data files and format files and returning a dict. (MICRODATA).
    This method also calls data_format() in order to generate
    the data frame.

    Args:
        dir_path (str): directory containing Positional files.
        sep (str): field separator.
        encoding (str): file encoding.
        suffix (str): standard for format name
        na_values (scalar, str, list-like, or dict) : Additional strings to recognize as NA/NaN.


    Returns:
        dict: Name of data file as KEY and dataframe as VALUE
    """
    assignation_map = data_format(dir_path, 'txt', suffix)
    for txt in assignation_map:
        conversion = dict()
        assignation_map[txt] = pd.read_csv(dir_path + assignation_map[txt], sep=sep,
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
        pattern (str): regEx to filter data names

    Returns:
        dict: XML name as KEY and etree object as VALUE
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
    Function that open sql files in the sql directory using a context manager
    Args:
        dir_path (str): Path of DIR readed.

    Returns:
        dict: query name  as KEY and query as VALUE
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
