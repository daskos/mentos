#!/usr/bin/env python

from setuptools import setup

try:
    from pypandoc import convert
    description = convert('README.md', 'rst')
except ImportError:
    print('Warning: pypandoc module not found, could not convert Markdown to RST')
    with open('README.md', 'r') as fp:
        description = fp.read()


setup(name='mentos',
      version='0.1.8',
      description='Fresh Python Mesos HTTP Scheduler and Executor',
      url='http://github.com/daskos/mentos',
      maintainer='Artyom Topchyan',
      maintainer_email='a.topchyan@reply.de',
      author='Artyom Topchyan',
      author_email='artyom.topchyan@live.com',
      license='Apache License, Version 2.0',
      keywords='mesos scheduler executor http distributed',
      packages=['mentos'],
      long_description=description,
      classifiers=[
          'Intended Audience :: Developers',
          'License :: OSI Approved :: Apache Software License',
          'Programming Language :: Python :: 2.7',
          'Programming Language :: Python :: 3',
      ],
      install_requires=['zoonado', 'tornado', 'six', 'toolz'],
      setup_requires=['pytest-runner', 'pypandoc'],
      tests_require=['pytest-mock', 'pytest',
                     'mock', 'pytest-tornado', 'pytest-cov'],
      zip_safe=True)
