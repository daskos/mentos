
# mentos - A Python implementation  of a Mesos Scheduler/Executor driver
###### Log Lasting Mesos freshness for Python

[![Build Status](https://travis-ci.org/Arttii/mentos.svg?branch=master)](https://travis-ci.org/Arttii/mentos)

The main goal is to provide a low-complexity and feature rich support for pure python Mesos Frameworks, but also to learn things.

## There be dragons
mentos is quite experimental right now. It currently uses Tornado as the networking engine, but this might change in later release.  So the API might break.

This is mostly a learning project right now, but seems to work ok.

## Notable Features

- Pure python so no C++ meddling
- Full featured Zookeeper and Redirect based Master detection
- Dict based for simplicity
- Task scheduling should be quite fast due to the asynchronous nature of the networking engine
- Fairly simple codebase

## Install

Not on pypi right now. Install from this repository.

Tested Python Versions:
- 2.7
- 3.5
- 3.6

Requirements:
- Mesos 0.28.0 and up
- Zookeeper

## Examples
An example Mesos Scheduler and Executor can be found in the examples folder. It runs one task and then starts declining offers. The Task basically transmits and prints a message. Excuse the magic.

## Tests
Also not there yet.


## Documentation
Not there yet

## Points to consider

 1. Unit Tests missing

## Outlook
The long term goal is for this to serve as a base for Satyr and other more high level Python based frameworks.
This might go fully asynchronous based event handler. I am currently looking at a better Tornado, Twisted, Asyncio or Curio implementation.

## Acknowledgements
This has been heavily based on [zoonado](https://github.com/wglass/zoonado) and influenced [Satyr](https://github.com/lensacom/satyr) and [PyMesos](https://github.com/douban/pymesos) and shares some utility code with both.
The RecordIO format parsing was lifted from mrocklins [gist](https://gist.github.com/mrocklin/72cfd17a9f097e7880730d66cbde16a0)
