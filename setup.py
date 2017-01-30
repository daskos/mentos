#!/usr/bin/env python
# coding: utf-8

from os.path import exists

from setuptools import setup

setup(name='mentos',
      version='0.1.4',
      description='Fresh Python Mesos HTTP Scheduler and Executor',
      url='http://github.com/arttii/mentos',
      maintainer='Artyom Topchyan',
      maintainer_email='a.topchyan@reply.de',
      author="Artyom Topchyan",
      author_email="artyom.topchyan@live.com",
      license='Apache License, Version 2.0',
      keywords='mesos scheduler executor http',
      packages=['mentos'],
      long_description=(open('README.md').read() if exists('README.md')
                        else ''),
      install_requires=['zoonado', 'tornado', 'six', 'toolz'],
      setup_requires=['pytest-runner'],
      tests_require=['pytest-mock', 'pytest', 'mock', 'pytest-tornado','pytest-cov'],
      zip_safe=True)
