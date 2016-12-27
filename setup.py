#!/usr/bin/env python
# coding: utf-8

from os.path import exists

from setuptools import setup

setup(name='malefico',
      version='0.2.2',
      description='Extensible Python Framework for Apache Mesos',
      url='http://github.com/lensacom/malefico',
      maintainer='Krisztián Szűcs',
      maintainer_email='krisztian.szucs@lensa.com',
      license='Apache License, Version 2.0',
      keywords='mesos framework multiprocessing',
      packages=['malefico', 'malefico.core', 'malefico.apis'],
      long_description="",
      install_requires=['cloudpickle', 'zoonado', 'futures', 'tornado','six'],
      setup_requires=['pytest-runner'],
      tests_require=['pytest-mock', 'pytest'],
      zip_safe=False)