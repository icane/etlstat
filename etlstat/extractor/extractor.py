import csv
import fnmatch
import os
import xml.etree.ElementTree as ET
from contextlib import ExitStack

import numpy as np
import pandas as pd
import xlrd
from Levenshtein import ratio

from etlstat.extractor.pcaxis import *


def similar(a, b):
    """
    Args:
        param a (str): First String to compare.
        param b (str): Second String to compare.

    Returns:
        int: Value of similarity between two strings.
    """
    return ratio(a, b)


def excel_processing(dir_path, excel):
    """
    Function  (internal method) that reads and process an excel returning in a dict the skip_rows and skip_footer values
     for every sheet of this excel,
     Note: If a data column has a txt note in the same row this method will fail.

    Args:
        dir_path (str): directory containing Excel file.
        excel (str): Excel file name.

    Returns:
        dict: information (ini and end rows) about sheets of an excel.
    """
    xls_map = {}
    workbook = xlrd.open_workbook(dir_path + excel)
    sheet_names = workbook.sheet_names()
    dict_exists = 0
    sheet_pointer = 0
    for sheet in workbook.sheets():
        list_a = [0, 0]
        sum_a = 0
        flag = 0
        footer = 0
        for row in range(sheet.nrows):
            if list_a[1] < sum_a:
                list_a[0] = row - 1
                list_a[1] = sum_a
                flag = 1
            elif sum_a == 0 and flag == 1:
                footer = sheet.nrows - row + 1
                flag = 0
            sum_a = 0
            for column in range(sheet.ncols):
                if sheet.cell(row, column).value != '':
                    sum_a += 1
        if dict_exists == 0:
            for name in sheet_names:
                xls_map[name] = dict()
                xls_map[name]['skip_rows'] = 0
                xls_map[name]['footer_rows'] = 0
            dict_exists = 1
        xls_map[sheet_names[sheet_pointer]]['skip_rows'] = list_a[0]
        xls_map[sheet_names[sheet_pointer]]['footer_rows'] = footer
        sheet_pointer += 1
    return xls_map


def excel_in(dir_path, sheet_name, pattern='*.xls', encoding='utf-8'):
    # TODO: debug get 0 rows when sheet has no footer
    """
    Function that reads files in a directory filtered by regEx and generates a
    map with xls names. This method also calls excel_processing in order to generate
    the data frame in a proper way.

    Args:
        dir_path (str): directory containing Excel files.
        sheet_name (str):
        pattern (str): regEx to filter data names (Avoid adding format extension to regEx)
        encoding (str): file encoding

    Returns:
        dict: Excel name as KEY and data frame as VALUE
    """
    excel_files = []
    os.chdir(dir_path)
    for file in os.listdir('.'):
        if fnmatch.fnmatch(file, pattern):
            excel_files.append(file)
    # In order to have unique keys
    keys = set(excel_files)
    keys = list(keys)
    df_dict = dict.fromkeys(keys, '')
    for j in range(len(keys)):
        aux = excel_processing(dir_path, keys[j])
        print(aux)
        ini = aux[sheet_name]['skip_rows']
        fin = aux[sheet_name]['footer_rows']
        logger.info('Reading file: ' + keys[j] + ' with skip_rows = ' + str(ini))
        df_dict[keys[j]] = pd.read_excel(open(dir_path + keys[j], 'rb'),
                                         sheetname=sheet_name,
                                         skiprows=ini,
                                         encoding=encoding,
                                         skip_footer=fin)
    return df_dict


def csv_in(dir_path, pattern='*.csv', sep=',', encoding='utf-8'):
    """
    Function that reads files in a directory filtered by regEx and generates a
    dict with csv names.

    Args:
        dir_path (str): directory containing Excel files.
        pattern (str): regEx to filter data names (Avoid adding format extension to regEx)
        sep (str): field separator character
        encoding (str): file encoding

    Returns:
        dict: Csv name as KEY and data frame as VALUE
    """
    csv_files = []
    os.chdir(dir_path)
    for file in os.listdir('.'):
        if fnmatch.fnmatch(file, pattern):
            csv_files.append(file)
    keys = set(csv_files)
    keys = list(keys)
    df_dict = dict.fromkeys(keys,'')
    for i in range(len(keys)):
        df_dict[keys[i]] = pd.read_csv(dir_path + keys[i], sep=sep, encoding=encoding)
    return df_dict


def pc_axis_in(file_path, sep=",", encoding='windows-1252'):
    """
    Reads and converts Pc-Axis files to dataframe from URIs listed
    in a CSV file.

    Args:
        file_path (str): extractor with uris file path (including file name).
        sep (str): field separator
        encoding (str): file encoding
    Returns:
        dict: URL as KEY and dataframe as VALUE
    """
    pc_axis_dict = {}
    with open(file_path, "rt") as f:
        reader = csv.reader(f, delimiter=sep)

        for row in reader:
            md, df = from_pc_axis(row[1], encoding)
            pc_axis_dict[row[0]] = df

    return pc_axis_dict


def positional_in(dir_path, pattern_csv='*.[cC][sS][vV]', pattern_txt="*.[tT][xX][tT]", sep=';', encoding='utf-8'):
    """
    Function that reads files in a directory filtered by regEx, generates a correspondence
    between data files and format files and returning a dict. (MICRODATA)

    Args:
        dir_path (str): Path of DIR readed.
        reg_ex (str): regEx to filter data|format names (Avoid adding format extension to regEx)

    Returns:
        dict: Name of data file as KEY and dataframe as VALUE
    """
    conversion_map = {
        'STRING': str,
        'NUMBER': np.float32,
        'DOUBLE': np.float32,
        'INTEGER': np.int32
    }
    field_name = 'field_name'
    data_type = 'data_type'
    field_length = 'field_length'
    assignation_map = {}
    aux = None
    csv_s = []
    txt_s = []
    os.chdir(dir_path)
    for file in os.listdir('.'):
        if fnmatch.fnmatch(file, pattern_csv):
            csv_s.append(file)
        if fnmatch.fnmatch(file, pattern_txt):
            txt_s.append(file)
    csv_list = set(csv_s)
    csv_list = list(csv_list)
    keys = set(txt_s)
    keys = list(keys)
    correspondence_map = dict.fromkeys(keys, dict())
    max_similarity = 0
    for item in keys:
        for element in csv_list:
            similarity = similar(item, element)
            if similarity > max_similarity:
                max_similarity = similarity
                aux = element
        max_similarity = 0
        assignation_map[item] = aux  # CREATE MAP BETWEEN FILE NAME AND FORMAT NAME
        logger.info("Matched data file: " + item + " with: " + assignation_map[item])
    for i in range(len(assignation_map)):
        assignation_map[keys[i]] = pd.read_csv(dir_path + assignation_map[keys[i]], sep=sep,
                                              encoding=encoding)
    for l in range(len(assignation_map)):
        for m in range(len(assignation_map[keys[l]])):
            correspondence_map[keys[l]][assignation_map[keys[l]][field_name][m]] = \
                conversion_map[assignation_map[keys[l]][data_type][m]]

    for j in range(len(assignation_map)):
        aux = pd.read_fwf(dir_path + keys[j],
                          widths=assignation_map[keys[j]][field_length].tolist(),
                          names=assignation_map[keys[j]][field_name].tolist(),
                          dtype=correspondence_map[keys[j]],
                          nwords=0)

        aux.name = keys[j]
        assignation_map[keys[j]] = aux

    return assignation_map


def xml_in(dir_path, pattern=('.xml','.ktr','.kjb','.XML','.KTR','.KJB')):

    # TODO: refactorizar con pattern y fnmatch
    """
    Function that reads files in a directory filtered by regEx and generates a
    dict with xml names.

    Args:
        dir_path (str): Path of DIR readed.
        reg_ex (str): regEx to filter data names (Avoid adding format extension to regEx)

    Returns:
        dict: XML name as KEY and etree object as VALUE
    """
    xmls = []
    os.chdir(dir_path)
    for file in os.listdir('.'):
        if file.endswith(pattern):
            xmls.append(file)
    keys = set(xmls)
    keys = list(keys)
    df_dict = dict.fromkeys(keys,'')
    for i in range(len(keys)):
        df_dict[keys[i]] = ET.parse(dir_path + keys[i])

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