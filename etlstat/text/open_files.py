"""
    Function to open and read multiple files

        Args:
            file_path: relative files path: './metadata/sql/*.sql'
            path: relative path without files
        Return:
            variable: content file
"""
from contextlib import ExitStack
import glob

# absolute path
dir = '/var/git/python/icanetl/'

dictionary = {}
def open_files(file_path, path):
# Open sql files in the sql directory using a context manager
    with ExitStack() as cm:
        for filename in glob.glob(dir + file_path):
            f = cm.enter_context(open(filename, 'r'))
            dictionary[filename.split(dir + path, 1)[-1][:-4]] = f.read().replace('\n', ' ')
        cm.pop_all().close()
    return dictionary