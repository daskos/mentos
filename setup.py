#!/usr/bin/env python
# coding: utf-8

from os.path import exists

from setuptools import setup

setup(name='malefico',
      version='0.1.0',
      description='Python Mesos scheduler and executor',
      url='http://github.com/arttii/malefico',
      maintainer='Artyom Topchyan',
      maintainer_email='a.topchyan@reply.de',
      author="Artyom Topchyan",
      author_email="artyom.topchyan@live.com",
      license='Apache License, Version 2.0',
      keywords='mesos scheduler executor http',
      packages=['malefico'],
      long_description=(open('README.md').read() if exists('README.md')
                        else ''),
      install_requires=['zoonado', 'tornado', 'six'],
      setup_requires=['pytest-runner'],
      tests_require=['pytest-mock', 'pytest'],
      zip_safe=True)
