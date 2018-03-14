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
    'ENTERO': np.int32
}

def similar(a, b):
    """
    Args:
        param a (str): First String to compare.
        param b (str): Second String to compare.

    Returns:
        int: Value of similarity between two strings.
    """
    return ratio(a, b)

def data_format(dir_path,data,suffix='formato'):
    """
    Function that matchs formats files(csv's) with data files in a directory.

    Args:
        dir_path (str): directory containing files.
        data (str): type of data format(excel,csv,txt)

    Returns:
        dict: Excel name and sheetnames as KEYS and data frame as VALUE
    """
    excel_flag = 0
    format = '*.[cC][sS][vV]'
    if(data == 'excel'):
        max_format = 0
        excel_flag = 1
        data = "*.[xX][lL][sS]"
    if(data == 'txt'):
        data = "*.[tT][xX][tT]"
    if(data == 'csv'):
        data = '*[!'+suffix+'].[cC][sS][vV]'
        format = '*'+suffix+'.[cC][sS][vV]'
    assignation_map = {}
    data_s = []
    format_s = []
    os.chdir(dir_path)
    for file in os.listdir('.'):
        if fnmatch.fnmatch(file, format):
            format_s.append(file)
        if fnmatch.fnmatch(file, data):
            data_s.append(file)
    csv_list = set(format_s)
    csv_list = list(csv_list)
    if len(csv_list) == 0:
        raise FileNotFoundError ("Not format files found in the directory")
    keys = set(data_s)
    keys = list(keys)
    max_similarity = 0
    for item in keys:
        if(excel_flag):
            assignation_map[item] = dict()
            assignation_map[item]['data'] = None
            assignation_map[item]['format'] = None
            data_str = item[:-4] + '_datos' + item[-4:]
            format_str = item[:-4] + '_hoja' + item[-4:]
            for element in csv_list:
                similarity_data = similar(data_str, element)
                similarity_format = similar(format_str, element)
                if similarity_data > max_similarity:
                    max_similarity = similarity_data
                    aux_data = element
                if similarity_format > max_format:
                    max_format = similarity_format
                    aux_format = element
            max_similarity = 0
            max_format = 0
            assignation_map[item]['data'] = aux_data
            assignation_map[item]['format'] = aux_format
            logger.info("Matched data file: " + item + " with: " + assignation_map[item]['data'] + ' and ' + assignation_map[item]['format'])

        else:
            for element in csv_list:
                similarity = similar(item, element)
                if similarity > max_similarity:
                    max_similarity = similarity
                    aux = element
            max_similarity = 0
            assignation_map[item] = aux  # CREATE MAP BETWEEN FILE NAME AND FORMAT NAME
            logger.info("Matched data file: " + item + " with: " + assignation_map[item])
    return assignation_map


def excel_in(dir_path, sep=';', encoding='utf-8'):
    """
    Function that reads excel files in a directory and generates a
    dict with xls names and sheetnames as keys of the dataframe.
    This method also calls data_format() in order to generate
    the data frame.

    Args:
        dir_path (str): directory containing Excel files.
        encoding (str): file encoding

    Returns:
        dict: Excel name and sheetnames as KEYS and data frame as VALUE
    """
    data = dict()
    assignation_map = data_format(dir_path,'excel')
    for excel in assignation_map:
        data[excel] = dict()
        aux = pd.read_csv(dir_path + assignation_map[excel]['format'], sep=sep,
                encoding=encoding)
        for line in range(len(aux)):
            data[excel][aux['SHEET_NAME'][line]] = dict()
            data[excel][aux['SHEET_NAME'][line]]['START_ROW'] = [aux['START_ROW'][line]]
            data[excel][aux['SHEET_NAME'][line]]['START_COLUMN'] = [aux['START_COLUMN'][line]]
            data[excel][aux['SHEET_NAME'][line]]['FIELD_NAME'] = []
            data[excel][aux['SHEET_NAME'][line]]['DATA_TYPE'] = dict()
        aux = pd.read_csv(dir_path + assignation_map[excel]['data'], sep=sep,
                encoding=encoding)
        for line in range(len(aux)):
            #print(data[excel][aux['SHEET_NAME'][line]]['DATA_TYPE'])
            data[excel][aux['SHEET_NAME'][line]]['FIELD_NAME'].append(aux['FIELD_NAME'][line])
            data[excel][aux['SHEET_NAME'][line]]['DATA_TYPE'][str(aux['FIELD_NAME'][line])] = conversion_map[aux['DATA_TYPE'][line]]

        for sheet in data[excel]:
            #print(data[excel][sheet]['DATA_TYPE'])
            data[excel][sheet] = pd.read_excel(open(dir_path + excel, 'rb'),
                                         sheetname=str(sheet),
                                         skiprows=data[excel][sheet]['START_ROW'][0]-1,
                                         names=data[excel][sheet]['FIELD_NAME'],
                                         dtype=data[excel][sheet]['DATA_TYPE'],
                                         encoding=encoding)
            data[excel][sheet].name = sheet


    return data


def csv_in(dir_path, sep=';', encoding='utf-8', suffix='formato',na_values=None):
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
    data = data_format(dir_path, 'csv', suffix)
    for csv in data:
        conversion = dict()
        data[csv] = pd.read_csv(dir_path + data[csv], sep=sep,
                                encoding=encoding)
        for line in range(len(data[csv])):
            conversion[data[csv]['FIELD_NAME'][line]] = conversion_map[data[csv]['DATA_TYPE'][line]]
        data[csv] = pd.read_csv(dir_path + csv,
                                dtype=conversion,
                                na_values=na_values,
                                sep=sep)
        data[csv].name = csv
    return data

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

def positional_in(dir_path, sep=';', encoding='utf-8'):
    """
    Function that reads files in a directory, generates a correspondence
    between data files and format files and returning a dict. (MICRODATA)

    Args:
        dir_path (str): Path of DIR readed.

    Returns:
        dict: Name of data file as KEY and dataframe as VALUE
    """
    data = data_format(dir_path,'txt')
    for txt in data:
        conversion = dict()
        data[txt] = pd.read_csv(dir_path + data[txt], sep=sep,
                                encoding=encoding)
        for line in range(len(data[txt])):
            conversion[data[txt]['FIELD_NAME'][line]] = conversion_map[data[txt]['DATA_TYPE'][line]]
        data[txt] = pd.read_fwf(dir_path + txt,
                                widths=data[txt]['LENGTH'].tolist(),
                                names=data[txt]['FIELD_NAME'].tolist(),
                                dtype=conversion,
                                nwords=0)
        data[txt].name = txt
    return data


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