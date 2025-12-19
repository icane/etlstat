# -*- coding: utf-8 -*-
from setuptools import find_packages, setup


setup(
    name='etlstat',
    version='0.9.3',
    author='Instituto Cántabro de Estadística',
    author_email='icane@cantabria.es',
    packages=find_packages(),
    url='https://github.com/icane/etlstat.git',
    license='Apache License 2.0',
    description='ETL utils for statistical offices data processing',
    long_description=open('README.rst').read(),
    install_requires=[
        'beautifulsoup4==4.*',
        'defusedxml==0.6.*',
        'pandas==1.4.*',
        'pyaxis==0.3.*',
        'python_Levenshtein==0.20.*',
        'SQLAlchemy==1.4.*',
        'sqlparse==0.3.*',
        'Unidecode==1.1.*',
        'cx_Oracle==7.3.*',
        'mysql-connector==2.2.*',
        'xlrd==2.0.*',
        'openpyxl==3.0.*'
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
