#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys

py_version = sys.version_info[:2]

if py_version < (2, 6):
    raise RuntimeError(
        'On Python 2, supervisor-serialrestart requires Python 2.6 or later')
elif (3, 0) < py_version < (3, 2):
    raise RuntimeError(
        'On Python 3, supervisor-serialrestart requires Python 3.2 or later')

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

if sys.argv[-1] == 'publish':
    os.system('python setup.py sdist upload')
    sys.exit()

setup(
    name='supervisor-mightyreload',
    version='1.5',
    description='Adds mightyerload command to Supervisor.',
    author='Vitaliy Okulov',
    author_email='vitaliy.okulov@gmail.com',
    url='https://github.com/mightydok/supervisor-mightyreload',
    include_package_data=True,
    install_requires=[
    ],
    packages=[
        'supervisor_mightyreload',
    ],
    package_dir={'supervisor_mightyreload': 'supervisor_mightyreload'},
    license="BSD",
    zip_safe=False,
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Natural Language :: English',
        "Programming Language :: Python :: 2",
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.2',
        'Programming Language :: Python :: 3.3',
    ],
    test_suite='tests.run_tests.run_all',
)
