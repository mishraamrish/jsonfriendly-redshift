#!/usr/bin/env python

# distutils/setuptools install script for jsonfriendly_redshift
import os
from setuptools import setup, find_packages

# Package info
NAME = 'jsonfriendly_redshift'
ROOT = os.path.dirname(__file__)
VERSION = __import__(NAME).__version__

# Requirements
requirements = []

with open(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'requirements.txt')) as f:
    for r in f.readlines():
        requirements.append(r.strip())
with open('README.md') as f:
    log_description = f.read()

# Setup
setup(
    name=NAME,
    version=VERSION,
    description='A high level Python wrapper using pandas. It is meant to provide a point-in-time json data handling for redshift load Job.',
    long_description_content_type='text/markdown',
    long_description=log_description,
    author='Amrish Mishra',
    url='https://github.com/mishraamrish/jsonfriendly_redshift',
    install_requires=requirements,
    license='MIT License',
    packages=find_packages(),
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'Intended Audience :: Information Technology',
        'Intended Audience :: System Administrators',
        'Natural Language :: English',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8'
    ],
)
