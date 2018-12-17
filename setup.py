# -*- coding: utf-8 -*-
from setuptools import setup, find_packages

setup(
    name='etlstat',
    version='0.2.0',
    author='Instituto Cántabro de Estadística',
    author_email='icane@cantabria.es',
    packages=find_packages(),
    url='https://github.com/icane/etlstat.git',
    license='Apache License 2.0',
    description='ETL utils for statistical offices data processing',
    long_description=open('README.rst').read(),
    install_requires=[
        'cx_Oracle',
        'mysql-connector>=2.1.4',
        'SQLAlchemy>=1.2.14',
        'odo>=0.5.0',
        'Unidecode>=1.0.22',
        'xlrd>=1.1.0',
        'defusedxml>=0.5.0',
        'pyaxis>=0.1.0',
        'numpy>=1.15.4',
        'pandas>=0.23.4',
        'python_Levenshtein>=0.12.0'
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
