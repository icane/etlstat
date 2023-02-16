# -*- coding: utf-8 -*-
from setuptools import find_packages, setup

setup(
    name='etlstat',
    version='0.9.0',
    author='Instituto Cántabro de Estadística',
    author_email='icane@cantabria.es',
    packages=find_packages(),
    url='https://github.com/icane/etlstat.git',
    license='Apache License 2.0',
    description='ETL utils for statistical offices data processing',
    long_description=open('README.rst').read(),
    install_requires=[
        'beautifulsoup4==4.11.2'
        'certifi==2022.12.7'
        'charset-normalizer==3.0.1'
        'cx-oracle==7.3.0'
        'defusedxml==0.6.0'
        'et-xmlfile==1.1.0'
        'greenlet==2.0.2'
        'idna==3.4'
        'mysql-connector==2.2.9'
        'numpy==1.23.5'
        'openpyxl==3.0.10'
        'pandas==1.4.4'
        'psycopg2==2.8.6'
        'pyaxis==0.3.4'
        'pyjstat==2.3.0'
        'python-dateutil==2.8.2'
        'python-levenshtein==0.12.2'
        'pytz==2022.7.1'
        'requests==2.28.2'
        'six==1.16.0'
        'soupsieve==2.4'
        'sqlalchemy==1.4.46'
        'sqlparse==0.3.1'
        'unidecode==1.1.2'
        'urllib3==1.26.14'
        'xlrd==2.0.1'
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
