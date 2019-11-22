#!/usr/bin/env python

from os.path import exists
from setuptools import setup
import versioneer

setup(name='partd',
      version=versioneer.get_version(),
      cmdclass=versioneer.get_cmdclass(),
      description='Appendable key-value storage',
      url='http://github.com/dask/partd/',
      maintainer='Matthew Rocklin',
      maintainer_email='mrocklin@gmail.com',
      license='BSD',
      keywords='',
      packages=['partd'],
      install_requires=list(open('requirements.txt').read().strip().split('\n')),
      python_requires=">=3.5",
      classifiers=[
          "Programming Language :: Python :: 3",
          "Programming Language :: Python :: 3.5",
          "Programming Language :: Python :: 3.6",
          "Programming Language :: Python :: 3.7",
          "Programming Language :: Python :: 3.8",
      ],
      long_description=(open('README.rst').read() if exists('README.rst')
                        else ''),
      extras_require={'complete': [
          'numpy >= 1.9.0',
          'pandas >=0.19.0',
          'pyzmq',
          'blosc',
      ]},
      zip_safe=False)
