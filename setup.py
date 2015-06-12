#!/usr/bin/env python

from os.path import exists
from setuptools import setup

setup(name='partd',
      version='0.3.0',
      description='Appendable key-value storage',
      url='http://github.com/mrocklin/partd/',
      maintainer='Matthew Rocklin',
      maintainer_email='mrocklin@gmail.com',
      license='BSD',
      keywords='',
      packages=['partd'],
      install_requires=list(open('requirements.txt').read().strip().split('\n')),
      long_description=(open('README.rst').read() if exists('README.rst')
                        else ''),
      zip_safe=False)
