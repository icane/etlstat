"""
    Function to open and read files

        Args:
            file_path: relative file path
        Return:
            variable: content file
"""

# absolute path
dir = '/var/git/python/icanetl/'

def open_file(file_path):
    with open(dir + file_path, 'r') as file:
        variable = file.read().replace('\n', ' ')
    file.close()
    return variable