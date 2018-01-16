import re
from unidecode import unidecode

def parsed_columns(df):
    """Function to parse columns in a dataframe: conversion of capital letters in lowercase letters, white spaces in low bar and remove accent mark.
    """
    pars_columns = []
    for column in df.columns:
        to_replace = re.sub(r'[ ]{3,}', ' ',
                            unidecode(column.lower().replace(',', '')))
        pars_columns.append(re.sub(r'[ ]{2,}',
                                     '', to_replace).replace(' ', '_'))
    df.columns = pars_columns