# -*- coding: utf-8 -*-
from setuptools import find_packages, setup

setup(
    name='etlstat',
    version='0.7.4',
    author='Instituto Cántabro de Estadística',
    author_email='icane@cantabria.es',
    packages=find_packages(),
    url='https://github.com/icane/etlstat.git',
    license='Apache License 2.0',
    description='ETL utils for statistical offices data processing',
    long_description=open('README.rst').read(),
    install_requires=[
        'cx_Oracle',
        'sqlparse',
        'mysql-connector>=2.1.4',
        'SQLAlchemy>=1.3',
        'Unidecode>=1.0.22',
        'xlrd>=2.0.1',
        'defusedxml>=0.5.0',
        'pyaxis>=0.3.4',
        'numpy>=1.15.4',
        'pandas>=1.2.4',
        'python_Levenshtein>=0.12.0',
        'psycopg2',
        'openpyxl>=3.0.7'
    ],
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
