# -*- coding: utf-8 -*-
from setuptools import find_packages, setup

setup(
    name='etlstat',
    version='0.8.2',
    author='Instituto Cántabro de Estadística',
    author_email='icane@cantabria.es',
    packages=find_packages(),
    url='https://github.com/icane/etlstat.git',
    license='Apache License 2.0',
    description='ETL utils for statistical offices data processing',
    long_description=open('README.rst').read(),
    install_requires=[
        'cx_Oracle==7.3.0',
        'sqlparse==0.3.1',
        'mysql-connector==2.2.9',
        'SQLAlchemy==1.3.15',
        'Unidecode==1.1.1',
        'xlrd==2.0.1',
        'defusedxml==0.6.0',
        'pyaxis==0.3.4',
        'numpy==1.23.0',
        'pandas==1.4.3',
        'python_Levenshtein==0.12.0',
        'psycopg2==2.8.4',
        'openpyxl==3.0.9',
        'beautifulsoup4==4.10.0'
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
