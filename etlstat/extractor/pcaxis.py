"""
    Obtains a pandas DataFrame of tabular data from a PC-Axis file or URL.
    Read all metadata from PC-Axis and returns a dictionary containing it.
    Reference: https://www.scb.se/Upload/PC-Axis/Support/Documents/PX-file_format_specification_2013.pdf
    Regex debugging: https://pythex.org/

    Date:
        25/09/2017

    Authors:
        slave110
        lla11358
"""

import itertools
import numpy
import pandas
import re
import requests
from etlstat.log.timing import timeit, log

log.basicConfig(level=log.ERROR)
logger = log.getLogger(__name__)


def uri_type(uri):
    """
    Determines the type of URI
    :param uri: pc-axis file name or URL
    :return: uri_type (string)
    """
    if re.match('http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', uri):
        return 'URL'
    else:
        return 'FILE'


@timeit
def read(uri, encoding, timeout=10):
    """
    Read a text file from file system or URL
    :param uri: file name or URL
    :param encoding: charset encoding
    :param timeout: request timeout; optional
    :return: file contents as string
    """

    pc_axis = """
        STUB="";
        HEADING="";
        VALUES("")=""
        DATA=".."
        """

    if uri_type(uri) == 'URL':
        try:
            response = requests.get(uri, stream=True, timeout=timeout)
        except requests.exceptions.ConnectionError as connection_error:
            logger.error('ConnectionError = ' + str(connection_error))
        except requests.exceptions.HTTPError as http_error:
            logger.error('HTTPError = ' + http_error.response.status_code + ' ' + http_error.response.reason)
        except requests.exceptions.InvalidURL as url_error:
            logger.error('URLError = ' + url_error.response.status_code + ' ' + url_error.response.reason)
        except Exception:
            import traceback
            logger.error('Generic exception: ' + traceback.format_exc())
            raise
        finally:
            if 'response' in locals():
                if response.status_code == 200:
                    response.encoding = encoding
                    pc_axis = response.text
                response.close()
    else:
        file_object = open(uri, encoding=encoding)
        pc_axis = file_object.read()
        file_object.close()

    return pc_axis


def meta_data_split(pc_axis):
    """
    Extract metadata and data. Remove new line and semicolons
    :param pc_axis: string containing the pc_axis file
    :return:
    """
    # replace new line characters with blank
    pc_axis = pc_axis.replace('\n', ' ').replace('\r', ' ')

    # split file into metadata and data sections
    meta_data, data = pc_axis.split('DATA=')
    # meta: list of strings that conforms to pattern ATTRIBUTE=VALUES
    meta = re.findall('([^=]+=[^=]+)(?:;|$)', meta_data)
    # remove trailing blanks and final semicolon
    data = data.strip().rstrip(';')
    for i, item in enumerate(meta):
        meta[i] = item.strip().rstrip(';')

    return meta, data


def meta_split(meta_list):
    """
    Splits the list of metadata elements into a dictionary of multi-valued keys
    Sets self._meta_dict -> dictionary
    """
    # TODO: "NOTE" attribute can be multiple, but only the last one is added to the dictionary
    meta_dict = {}

    for m in meta_list:
        name, values = m.split('=')
        # remove double quotes from key
        name = name.replace('"', '')
        # split values delimited by double quotes into list
        # additionally strip leading and trailing blanks
        v_list = re.findall('"[ ]*(.+?)[ ]*"+?', values)
        meta_dict[name] = v_list

    return meta_dict


def dimensions(meta_dict):
    """
    Reads STUB and HEADING values
    :param meta_dict: dictionary of metadata
    :return: dimension_names, dimension_members
    """

    dimension_names = []
    dimension_members = []

    # add STUB and HEADING elements to a list of dimension names
    st = meta_dict['STUB']
    for s in st:
        dimension_names.append(s)
    hd = meta_dict['HEADING']
    for h in hd:
        dimension_names.append(h)

    # add VALUES of STUB and HEADING to a list of dimension members
    st = meta_dict['STUB']
    for s in st:
        s_val = []
        values = meta_dict['VALUES(' + s + ')']
        for v in values:
            s_val.append(v)
        dimension_members.append(s_val)

    # add HEADING values to the list of dimension members
    hd = meta_dict['HEADING']
    for h in hd:
        h_val = []
        values = meta_dict['VALUES(' + h + ')']
        for v in values:
            h_val.append(v)
        dimension_members.append(h_val)

    return dimension_names, dimension_members


def to_data_frame(dimension_names, dimension_members, data_list):
    """
    Builds a data frame by adding the cartesian product of dimension members,
    plus series of data
    :return: pandas data frame object
    """

    # cartesian product of dimension members
    dim_exploded = list(itertools.product(*dimension_members))

    data_frame = pandas.DataFrame(data=dim_exploded, columns=dimension_names)

    # convert data values from string to float
    for index, value in enumerate(data_list):
        try:
            data_list[index] = float(value)
        except ValueError:
            data_list[index] = numpy.nan

    # column of data values
    data_frame['DATA'] = pandas.Series(data_list)

    return data_frame


@timeit
def from_pc_axis(uri, encoding, timeout=10):
    """
    Extracts metadata and data sections from pc-axis.
    :param uri: file name or URL
    :param encoding: charset encoding
    :param timeout: request timeout; optional
    :return metadata dictionary, data frame
    """

    # get file content or URL stream
    pc_axis = read(uri, encoding, timeout)

    # metadata and data extraction and cleaning
    meta, data = meta_data_split(pc_axis)

    # stores raw metadata into a dictionary
    meta_dict = meta_split(meta)

    # explode raw data into a list of float values
    data_list = data.split()

    # extract dimension names and members from 'meta_dict' STUB and HEADING keys
    dimension_names, dimension_members = dimensions(meta_dict)

    # build a data frame
    data_frame = to_data_frame(dimension_names, dimension_members, data_list)

    return meta_dict, data_frame
