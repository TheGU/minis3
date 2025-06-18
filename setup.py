# -*- coding: utf-8 -*-
from __future__ import absolute_import, division, print_function, unicode_literals

import sys

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

install_requires = ["requests >= 1.2.0"]
if sys.version_info < (3, 2):
    install_requires.insert(0, "futures >= 2.1.3")

setup(
    name="minis3",
    version="1.0.0",
    description=(
        "A enhanced fork of tinys3 - A small library for uploading files to S3-compatible storage,"
        "With support of async uploads, worker pools, cache headers etc"
    ),
    author="Pattapong Jantarach",
    author_email="me@pattapongj.com",
    url="https://github.com/TheGU/minis3/",
    packages=["minis3"],
    license="MIT",
    classifiers=[
        # make sure to use :: Python *and* :: Python :: 3 so
        # that pypi can list the package on the python 3 page
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
    ],
    platforms="Any",
    keywords=("amazon", "aws", "s3", "s3-client", "upload", "minio", "s3-compatible"),
    package_dir={"": "."},
    install_requires=install_requires,
    tests_require=["nose", "flexmock"],
    test_suite="minis3.tests",
)
