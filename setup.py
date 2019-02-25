__version__ = '1.0'

import os
import sys

py_version = sys.version_info[:2]

if py_version < (2, 6):
    raise RuntimeError(
        'On Python 2, supervisor_mightyreload requires Python 2.6 or later')
elif (3, 0) < py_version < (3, 2):
    raise RuntimeError(
        'On Python 3, supervisor_mightyreload requires Python 3.2 or later')

tests_require = []
if py_version < (3, 3):
    tests_require.append('mock')

from setuptools import setup, find_packages
here = os.path.abspath(os.path.dirname(__file__))

DESC = """\
supervisor_mightyreload is an RPC extension for Supervisor that allows
Supervisor's configuration and state to be manipulated in ways that are not
normally possible at runtime."""

CLASSIFIERS = [
    'Development Status :: 5 - Production/Stable',
    'Environment :: No Input/Output (Daemon)',
    'Intended Audience :: System Administrators',
    'License :: OSI Approved :: BSD License',
    'Natural Language :: English',
    'Operating System :: POSIX',
    'Programming Language :: Python :: 2',
    'Programming Language :: Python :: 2.6',
    'Programming Language :: Python :: 2.7',
    'Programming Language :: Python :: 3',
    'Programming Language :: Python :: 3.2',
    'Programming Language :: Python :: 3.3',
    'Programming Language :: Python :: 3.4',
    'Topic :: System :: Boot',
    'Topic :: System :: Systems Administration',
    ]

setup(
    name = 'supervisor_mightyreload',
    version = __version__,
    license = 'License :: OSI Approved :: BSD License',
    url = 'https://github.com/mightydok/supervisor_mightyreload',
    description = "supervisor_mightyreload RPC extension for Supervisor",
    long_description= DESC,
    classifiers = CLASSIFIERS,
    author = "Vitaliy Okulov",
    author_email = "vitaliy.okulov@gmail.com",
    maintainer = "Vitaliy Okulov",
    maintainer_email = "vitaliy.okulov@gmail.com",
    packages = find_packages(),
    install_requires = ['supervisor >= 3.3.5'],
    tests_require = tests_require,
    include_package_data = True,
    zip_safe = False,
    namespace_packages = ['supervisor_mightyreload'],
    test_suite = 'supervisor_mightyreload.tests'
)
