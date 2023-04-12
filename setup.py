# -*- coding: utf-8 -*-
import toml

from setuptools import find_packages, setup


def get_install_requirements():
    try:
        # read my pipfile
        with open('Pipfile', 'r') as fh:
            pipfile = fh.read()
        # parse the toml
        pipfile_toml = toml.loads(pipfile)
    except FileNotFoundError:
        return []  # if the package's key isn't there then just return an empty list
    try:
        required_packages = pipfile_toml['packages'].items()
    except KeyError:
        return []
     # If a version/range is specified in the Pipfile honor it
     # otherwise just list the package
    return ["{0}{1}".format(pkg, ver) if ver != "*"
            else pkg for pkg, ver in required_packages]


setup(
    name='etlstat',
    version='0.9.2',
    author='Instituto Cántabro de Estadística',
    author_email='icane@cantabria.es',
    packages=find_packages(),
    url='https://github.com/icane/etlstat.git',
    license='Apache License 2.0',
    description='ETL utils for statistical offices data processing',
    long_description=open('README.rst').read(),
    install_requires=get_install_requirements(),
    test_suite='extractor.test, database.test, text.test',
    keywords=['etl', 'icane', 'statistics', 'utils'],
    classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Console',
        'Intended Audience :: Developers',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: MacOS :: MacOS X',
        'Operating System :: Microsoft :: Windows',
        'Operating System :: POSIX',
        'Programming Language :: Python',
        'Topic :: Scientific/Engineering :: Information Analysis',
        'Topic :: Software Development :: Libraries'
    ],
)
