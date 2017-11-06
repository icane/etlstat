import string
from random import *


def database_password(minlength, maxlength):
    """
    Generación de una contraseña aleatoria, formada por una letra inicial más una combinación de letras,
    dígitos y signos de puntuación
    :param minlength: longitud mínima de la contraseña
    :param maxlength: longitud máxima de la contraseña
    :return: contraseña generada
    """
    if (minlength > maxlength):
        min = maxlength
        max = minlength
    else:
        min = minlength
        max = maxlength

    # in Oracle, first character must be a letter
    first = choice(string.ascii_letters)
    # valid characters: letters (lower and upper case), punctuation signs and digits
    characters = string.ascii_letters + string.punctuation + string.digits
    password = first + ''.join(choice(characters) for x in range(randint(min - 1, max - 1)))
    return password