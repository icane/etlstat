# -*- coding: utf-8 -*-
from setuptools import setup, find_packages

setup(
    name='etlstat',
    version='0.1',
    author='Instituto Cántabro de Estadística',
    author_email='icane.datos@cantabria.es',
    packages=find_packages(),
    # packages=['etlstat', 'etlstat.database', 'etlstat.extractor', 'etlstat.log', 'etlstat.metadata',
    #           'etlstat.security', 'etlstat.text', 'etlstat.database.test', 'etlstat.extractor.test',
    #           'etlstat.metadata.test', 'etlstat.security.test'],
    url='https://gitlab.com/icane-tic/etlstat.git',
    license='Apache License 2.0',
    description='Python package that contains extract-transform-load utils for statistical offices data processing',
    long_description=open('README.md').read(),
    install_requires=[ 'requests' ],
    # test_suite='etlstat.database.test',
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