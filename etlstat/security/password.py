import string
from random import *


def generate_password(
        min_length,
        max_length,
        first_letter=False,
        upper_case=False,
        lower_case=False,
        punctuation=True,
        digits=True
):
    """
    Generates a random password.
    :param min_length: (:int:): password min. length
    :param max_length: (:int:): password max. length
    :param first_letter: (:bool:): True if first character must be a letter
    :param upper_case: (:bool:): True if only uppercase letters
    :param lower_case: (:bool:): True if only lowercase letters
    :param punctuation: (:bool:): by default, use punctuation signs
    :param digits: (:bool:): by default, use digits
    :return: (:obj:str:): generated password
    """

    if min_length > max_length:
        raise TypeError("Minimum password length must be less or equal than maximum.")

    if upper_case and not lower_case:
        letters = string.ascii_uppercase
    elif lower_case and not upper_case:
        letters = string.ascii_lowercase
    else:
        letters = string.ascii_letters

    if first_letter:
        first = choice(letters)
    else:
        first = ''

    valid_characters = letters
    if punctuation:
        valid_characters += string.punctuation

    if digits:
        valid_characters += string.digits

    password = first + ''.join(choice(valid_characters) for x in range(randint(min_length - 1, max_length - 1)))
    return password
