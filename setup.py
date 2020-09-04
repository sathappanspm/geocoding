#!/usr/bin/env python
#-*- coding:utf-8 -*-

"""
    *.py: Description of what * does.
    Last Modified:
"""

__author__ = "Sathappan Muthiah"
__email__ = "sathap1@vt.edu"
__version__ = "0.0.1"

from distutils.core import setup
from glob import glob
import os

datafiles = list(glob('./TextGeo/**/*.txt', recursive=True)) + list(glob('./TextGeo/**/*.json', recursive=True))
datafiles = [os.path.relpath(path, 'TextGeo') for path in datafiles]
#print(datafiles)

setup(
    name="TextGeo", # Replace with your own username
    version="0.0.1",
    url="https://github.com/sathappanspm/geocoding",
    author="Sathappan Muthiah",
    author_email="sathap1@vt.edu",
    description="Geocode Text Articles",
    packages=['TextGeo', 'TextGeo.src', 'TextGeo.src.geoutils'],
    package_dir={'': './'},
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    package_data={'TextGeo': datafiles},
    python_requires='>=3.6',
)
