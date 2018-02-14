# -*- coding: utf-8 -*-
from setuptools import setup, find_packages

setup(
    name='etlstat',
    version='0.1.1',
    author='Instituto Cántabro de Estadística',
    author_email='icane.datos@cantabria.es',
    packages=find_packages(),
    url='https://github.com/icane/etlstat.git',
    download_url='https://github.com/icane/etlstat/archive/0.1.1.tar.gz',
    license='Apache License 2.0',
    description='Python package that contains extract-transform-load utils for statistical offices data processing',
    long_description=open('README.md').read(),
    install_requires=['requests'],
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