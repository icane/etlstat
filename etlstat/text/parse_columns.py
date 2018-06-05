import re
from unidecode import unidecode


def parse_columns(df):
    """Parses columns in a data frame: converts capital letters into lowercase letters,
    white spaces into low bar and remove accent mark.
    """
    pars_columns = []
    for column in df.columns:
        to_replace = re.sub(r'[ ]{3,}', ' ',
                            unidecode(column.lower().replace(',', '')))
        pars_columns.append(re.sub(r'[ ]{2,}', '', to_replace).replace(' ', '_'))
    df.columns = pars_columns
