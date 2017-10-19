
# mentos - A Pure Python implementation of a Mesos Scheduler/Executor driver
###### Long Lasting Mesos freshness for Python

[![Build Status](https://travis-ci.org/daskos/mentos.svg?branch=master)](https://travis-ci.org/daskos/mentos)
[![Coverage Status](https://coveralls.io/repos/github/daskos/mentos/badge.svg)](https://coveralls.io/github/daskos/mentos)
[![Coding Hours](https://api.gitential.com/accounts/6/projects/119/badges/coding-hours.svg)](https://gitential.com/accounts/6/projects/119/share?uuid=7e476249-f7e2-4838-831f-cc340e3b3e96&utm_source=shield&utm_medium=shield&utm_campaign=119)

The main goal is to provide a low-complexity and feature rich support for pure python Mesos Frameworks, but also to learn things.

## There be dragons
mentos is still quite experimental right now. No production frameworks have been built using it.

## Notable Features

- Pure python so no C++ meddling
- Full featured Zookeeper and Redirect based Master detection
- Dict based for simplicity
- Task scheduling should be quite fast due to the asynchronous nature of the networking engine
- Nice policy based reconnect and retry system
- Fancy Testing and Development enviroment based on docker-compose

## Install

Not on pypi right now. Install from this repository.

Tested Python Versions:
- 2.7
- 3.5
- 3.6

Requirements:
- Mesos > 0.28
- Zookeeper


## Development
Run `docker-compose up` to get a working instalation of Mesos going.

Requirements:
- docker
- docker-compose > 1.6.0


## Examples
An example Mesos Scheduler and Executor can be found in the examples folder. It runs one task and then starts declining offers. The Task basically transmits and prints a message. Excuse the magic.

## Tests
- [x] utils
- [x] states
- [x] interface
- [x] retry
- [x] exceptions
- [x] connection
- [x] subscription
- [x] executor
- [x] scheduler

## Documentation
Not there yet

## Outlook
The long term goal is for this to serve as a base for [Satyr](https://github.com/lensacom/satyr) and other more high level Python based frameworks.

## Acknowledgements
This has been heavily based on [zoonado](https://github.com/wglass/zoonado) and was influenced by [Satyr](https://github.com/lensacom/satyr) and [PyMesos](https://github.com/douban/pymesos) and shares some utility code with both.
The RecordIO format parsing was lifted from mrocklins [gist](https://gist.github.com/mrocklin/72cfd17a9f097e7880730d66cbde16a0).
