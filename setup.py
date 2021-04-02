#!/usr/bin/env python

from setuptools import setup

setup(
    name = 'dspace-reports',
    version='1.1.0-SNAPSHOT',
    url = 'https://github.com/TexasDigitalLibrary/dspace-reports',
    author = 'Nicholas Woodward',
    author_email = 'njw@austin.utexas.edu',
    license = 'MIT',
    packages = ['dspace-reports'],
    install_requires = [''],
    description = 'Generate and email statistical reports for content stored in a DSpace repository - https://github.com/DSpace/DSpace',
    classifiers = [
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Environment :: Console",
        "Programming Language :: Python :: 3",
    ],
    test_suite = 'test',
)
