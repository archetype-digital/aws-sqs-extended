#! env python

"""
The MIT License (MIT)

Copyright (c) 2021 Archetype Digital Inc.

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""

import os
import re
import sys

from setuptools import find_packages, setup

sys.path.append('./')

PACKAGE_NAME = 'aws_sqs_ext_client'
requires = ['boto3~=1.21']
extras_requires = {
    'dev': ['flake8', 'autopep8'],
    'test': ['pytest', 'pytest-cov', 'moto[all]'],
}

with open(os.path.join(
        os.path.dirname(__file__), PACKAGE_NAME, '__init__.py'), 'r') as f:
    m = re.search(r'''__version__ = ['"]([0-9.]+)['"]''', f.read())
    version = m.group(1) if m is not None else ''

with open('./README.md', 'r') as f:
    long_description = f.read()

setup(
    name='aws-sqs-ext-client',
    version=version,
    description=(
        'The Amazon SQS Extended Client Library for Python '
        'to send, receive, and delete large messages via S3'
    ),
    long_description=long_description,
    long_description_content_type='text/markdown',
    author='Hiroshi Nakagoe',
    author_email='hiroshi.nakakgoe@archetypedigital.us',
    url='https://github.com/archetype-digital/aws-sqs-extended',
    # `pip install aws_sqs_ext_client` or `pip install -e .`
    install_requires=requires,
    # `pip install aws_sqs_ext_client[dev|test]` or `pip install -e ".[dev]"`
    extras_require=extras_requires,
    # `python setup.py test` requires this option
    setup_requires=['pytest-runner'],
    tests_require=extras_requires['test'],
    packages=find_packages(
        include=[PACKAGE_NAME, f'{PACKAGE_NAME}.*'],
        exclude=[
            'tests', 'tests.*', '*.tests.*', '*.test.*',
            'docs', 'docs.*', '*.docs.*',
        ]
    ),
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3 :: Only',
    ],
    python_requires='>=3.7',
)
