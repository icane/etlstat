"""
    Function to massively replace pc-axis paths in kjb file.

    Date:
        October 2017

    Author:
        rsj9999, emm13775

    Version:


    Notes:

"""

import re
import logging as log

# log configuration
log.basicConfig(level=log.INFO)
LOGGER = log.getLogger(__name__)


def replace_in_xml(tree, urls_df, output_file):
    """Function to massively replace pc-axis urls in xml files. Produces an xml file written into
    the output file path parameter.

       Args:
           tree (ElementTree): ElementTree object with the parsed xml file.
           urls_df (DataFrame): pandas DataFrame with a mapping between the old and
           the new pc-axis urls.
           output_file (str): output file path

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
                LOGGER.info("URL found: " + str(url_found))
                found[splitted[1]] = found.get(splitted[1], 0) + 1
                url.text = splitted[0] + '&url=' + urls_dict[splitted[1]]
                urls.append(url.text)
            else:
                LOGGER.info("URL NOT found: " + splitted[1])
    LOGGER.info("Found "+ str(len(found.keys())) + " unique URLs")
    tree.write(output_file)
