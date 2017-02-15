#!/usr/bin/env python
# coding: utf-8

from os.path import exists

from setuptools import setup

try:
    from pypandoc import convert
    read_md = lambda f: convert(f, 'rst')
except ImportError:
    print("warning: pypandoc module not found, could not convert Markdown to RST")
    read_md = lambda f: open(f, 'r').read()
setup(name='mentos',
      version='0.1.8',
      description='Fresh Python Mesos HTTP Scheduler and Executor',
      url='http://github.com/arttii/mentos',
      maintainer='Artyom Topchyan',
      maintainer_email='a.topchyan@reply.de',
      author="Artyom Topchyan",
      author_email="artyom.topchyan@live.com",
      license='Apache License, Version 2.0',
      keywords='mesos scheduler executor http distributed',
      packages=['mentos'],
      long_description=read_md('README.md'),
      classifiers=[
            "Intended Audience :: Developers",
            'License :: OSI Approved :: Apache Software License',
            "Programming Language :: Python :: 2.7",
            "Programming Language :: Python :: 3",
      ],
      install_requires=['zoonado', 'tornado', 'six', 'toolz'],
      setup_requires=['pytest-runner','pypandoc'],
      tests_require=['pytest-mock', 'pytest', 'mock', 'pytest-tornado','pytest-cov'],
      zip_safe=True)
