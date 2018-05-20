#! /usr/bin/env python
# coding: utf-8

#  __author__ = 'meisanggou'

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

import sys

if sys.version_info <= (2, 7):
    sys.stderr.write("ERROR: mysqldb-rich requires Python Version 2.7 or above.\n")
    sys.stderr.write("Your Python Version is %s.%s.%s.\n" % sys.version_info[:3])
    sys.exit(1)

name = "mysqldb-rich"
version = "0.10"
url = "https://github.com/meisanggou/mysqldb-rich"
license = "MIT"
author = "meisanggou"
short_description = "rich mysqldb"
long_description = """rich operation for mysql"""
keywords = "flask_helper"
install_requires = [""]

setup(name=name,
      version=version,
      author=author,
      author_email="zhouheng@gene.ac",
      url=url,
      packages=["mysqldb_rich"],
      license=license,
      description=short_description,
      long_description=long_description,
      keywords=keywords,
      install_requires=install_requires
      )
